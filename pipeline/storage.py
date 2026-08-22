"""Lake storage abstraction: identical API over local disk and Azure Blob.

Everything in the pipeline addresses data by a *key* — a POSIX-style relative
path inside the lake, e.g. ``raw/dt=2026-08-22/batch_0600_a1b2c3d4.parquet``.

Locally a key maps to ``<LAKE_LOCAL_ROOT>/<key>``.
On Azure it maps to blob ``<key>`` inside container ``AZURE_BLOB_CONTAINER``.

Because the key never changes, the bronze/silver/gold layout on disk is
byte-for-byte the same layout that lands in Blob Storage. Developing locally is
therefore a genuine rehearsal of the cloud deployment, not an approximation.
"""

from __future__ import annotations

import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)

# zstd gives ~2x better compression than snappy on this telemetry at comparable
# read speed, and both DuckDB and pyarrow read it natively.
PARQUET_COMPRESSION = "zstd"


class LakeStorage(ABC):
    """Read/write Parquet and raw bytes at a key inside the lake."""

    @abstractmethod
    def write_parquet(
        self, df: pd.DataFrame, key: str, schema: pa.Schema | None = None
    ) -> str:
        """Write ``df`` as a single Parquet object at ``key``. Returns the URI."""

    @abstractmethod
    def read_parquet(self, key: str) -> pd.DataFrame:
        """Read a single Parquet object at ``key``."""

    @abstractmethod
    def list_keys(self, prefix: str, suffix: str = ".parquet") -> list[str]:
        """List keys under ``prefix``, sorted, filtered by ``suffix``."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> str: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under ``prefix``. Returns the count removed."""

    @abstractmethod
    def uri(self, key: str) -> str:
        """Fully-qualified location of ``key``, for logs and DuckDB scans."""

    def read_parquet_many(self, keys: list[str]) -> pd.DataFrame:
        """Concatenate several Parquet objects into one DataFrame."""
        frames = [self.read_parquet(k) for k in keys]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


def _to_table(df: pd.DataFrame, schema: pa.Schema | None) -> pa.Table:
    if schema is not None:
        # preserve_index=False keeps the pandas RangeIndex out of the file, so
        # the Parquet schema is exactly the declared contract.
        return pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    return pa.Table.from_pandas(df, preserve_index=False)


class LocalLakeStorage(LakeStorage):
    """Filesystem-backed lake. Used for local dev and the whole test suite."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_parquet(
        self, df: pd.DataFrame, key: str, schema: pa.Schema | None = None
    ) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(_to_table(df, schema), path, compression=PARQUET_COMPRESSION)
        return str(path)

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pq.read_table(self._path(key)).to_pandas()

    def list_keys(self, prefix: str, suffix: str = ".parquet") -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in base.rglob("*")
            if p.is_file() and p.name.endswith(suffix)
        )

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def write_bytes(self, key: str, data: bytes) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def delete_prefix(self, prefix: str) -> int:
        base = self._path(prefix)
        if not base.exists():
            return 0
        removed = 0
        for p in sorted(base.rglob("*"), reverse=True):
            if p.is_file():
                p.unlink()
                removed += 1
            elif p.is_dir():
                p.rmdir()
        base.rmdir()
        return removed

    def uri(self, key: str) -> str:
        return str(self._path(key))

    def __repr__(self) -> str:
        return f"LocalLakeStorage(root={self.root})"


class AzureBlobLakeStorage(LakeStorage):
    """Azure Blob Storage-backed lake (the deployed target).

    The azure SDK is imported lazily so that a purely local run — or a test
    environment without the SDK installed — never pays for the import.
    """

    def __init__(self, connection_string: str, container: str):
        from azure.storage.blob import BlobServiceClient

        self.container_name = container
        self._service = BlobServiceClient.from_connection_string(connection_string)
        self._container = self._service.get_container_client(container)
        try:
            self._container.create_container()
            log.info("Created blob container %s", container)
        except Exception:  # already exists — the common path
            pass

    def _blob(self, key: str):
        return self._container.get_blob_client(key)

    def write_parquet(
        self, df: pd.DataFrame, key: str, schema: pa.Schema | None = None
    ) -> str:
        buf = io.BytesIO()
        pq.write_table(_to_table(df, schema), buf, compression=PARQUET_COMPRESSION)
        buf.seek(0)
        self._blob(key).upload_blob(buf, overwrite=True)
        return self.uri(key)

    def read_parquet(self, key: str) -> pd.DataFrame:
        raw = self._blob(key).download_blob().readall()
        return pq.read_table(io.BytesIO(raw)).to_pandas()

    def list_keys(self, prefix: str, suffix: str = ".parquet") -> list[str]:
        return sorted(
            b.name
            for b in self._container.list_blobs(name_starts_with=prefix)
            if b.name.endswith(suffix)
        )

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()

    def write_bytes(self, key: str, data: bytes) -> str:
        self._blob(key).upload_blob(data, overwrite=True)
        return self.uri(key)

    def read_bytes(self, key: str) -> bytes:
        return self._blob(key).download_blob().readall()

    def delete_prefix(self, prefix: str) -> int:
        names = [b.name for b in self._container.list_blobs(name_starts_with=prefix)]
        for name in names:
            self._container.delete_blob(name)
        return len(names)

    def uri(self, key: str) -> str:
        return f"az://{self.container_name}/{key}"

    def __repr__(self) -> str:
        return f"AzureBlobLakeStorage(container={self.container_name})"


def get_storage(settings: Settings | None = None) -> LakeStorage:
    """Factory: return the backend named by LAKE_BACKEND."""
    settings = settings or get_settings()
    if settings.lake_backend == "azure":
        return AzureBlobLakeStorage(
            settings.azure_connection_string, settings.azure_container
        )
    return LocalLakeStorage(settings.lake_local_root)
