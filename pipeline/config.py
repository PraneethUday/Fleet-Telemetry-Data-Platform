"""Central configuration, resolved from environment variables only.

Why env vars and not a config file: the identical container image has to run on
a laptop (writing to ./data) and as an Azure Container Apps Job (writing to Blob
Storage). Any hardcoded path would break that. The only thing that changes
between the two is LAKE_BACKEND and the Azure credentials, which Container Apps
injects as *secrets*.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Repo root = parent of the `pipeline` package. Used to resolve relative paths
# from .env so `LAKE_LOCAL_ROOT=./data` means the same thing regardless of the
# working directory the process was launched from.
REPO_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(REPO_ROOT / ".env")


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = _env(key)
    return int(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of the process configuration."""

    lake_backend: str
    lake_local_root: Path
    azure_connection_string: str
    azure_container: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    fleet_size: int
    generator_seed: int

    def validate(self) -> None:
        if self.lake_backend not in ("local", "azure"):
            raise ValueError(
                f"LAKE_BACKEND must be 'local' or 'azure', got {self.lake_backend!r}"
            )
        if self.lake_backend == "azure":
            if not self.azure_connection_string:
                raise ValueError(
                    "LAKE_BACKEND=azure requires AZURE_STORAGE_CONNECTION_STRING"
                )
            if not self.azure_container:
                raise ValueError("LAKE_BACKEND=azure requires AZURE_BLOB_CONTAINER")


def _resolve(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def get_settings() -> Settings:
    """Build a Settings from the current environment.

    Deliberately not cached: tests and the Prefect flows override env vars at
    runtime, and a module-level singleton would freeze the first value read.
    """
    settings = Settings(
        lake_backend=_env("LAKE_BACKEND", "local").lower(),
        lake_local_root=_resolve(_env("LAKE_LOCAL_ROOT", "./data")),
        azure_connection_string=_env("AZURE_STORAGE_CONNECTION_STRING"),
        azure_container=_env("AZURE_BLOB_CONTAINER", "fleet-lake"),
        bronze_prefix=_env("BRONZE_PREFIX", "raw"),
        silver_prefix=_env("SILVER_PREFIX", "silver"),
        gold_prefix=_env("GOLD_PREFIX", "gold"),
        fleet_size=_env_int("FLEET_SIZE", 500),
        generator_seed=_env_int("GENERATOR_SEED", 42),
    )
    settings.validate()
    return settings
