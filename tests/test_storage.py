"""The storage abstraction is what makes 'runs locally, deploys to Azure' true.

These tests pin the local implementation's contract. The Azure implementation
satisfies the same abstract base class, so a contract change here surfaces as a
type error there rather than as a runtime surprise in a deployed job.
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from pipeline.storage import LakeStorage, LocalLakeStorage, get_storage


@pytest.fixture
def frame():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def test_factory_returns_local_backend(lake_env):
    assert isinstance(get_storage(lake_env), LocalLakeStorage)


def test_parquet_roundtrip(storage, frame):
    storage.write_parquet(frame, "raw/dt=2026-08-20/part-0000.parquet")
    back = storage.read_parquet("raw/dt=2026-08-20/part-0000.parquet")
    pd.testing.assert_frame_equal(frame, back)


def test_declared_schema_is_honoured(storage):
    """Pinning the schema is what stops per-batch type drift across a prefix."""
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    df = pd.DataFrame({"a": [1], "b": [None]})  # would infer b as all-null
    storage.write_parquet(df, "raw/x.parquet", schema=schema)
    assert storage.read_parquet("raw/x.parquet")["b"].dtype == object


def test_write_creates_intermediate_directories(storage, frame):
    storage.write_parquet(frame, "a/b/c/d/part.parquet")
    assert storage.exists("a/b/c/d/part.parquet")


def test_list_keys_is_recursive_sorted_and_filtered(storage, frame):
    for key in ["raw/dt=2026-08-21/p.parquet", "raw/dt=2026-08-20/p.parquet"]:
        storage.write_parquet(frame, key)
    storage.write_bytes("raw/_SUCCESS", b"")
    keys = storage.list_keys("raw")
    assert keys == ["raw/dt=2026-08-20/p.parquet", "raw/dt=2026-08-21/p.parquet"]


def test_list_keys_on_missing_prefix_is_empty_not_an_error(storage):
    """An empty layer is a normal first-run state, not a failure."""
    assert storage.list_keys("gold") == []


def test_read_parquet_many_concatenates(storage, frame):
    storage.write_parquet(frame, "raw/a.parquet")
    storage.write_parquet(frame, "raw/b.parquet")
    assert len(storage.read_parquet_many(storage.list_keys("raw"))) == 6


def test_read_parquet_many_on_empty_list(storage):
    assert storage.read_parquet_many([]).empty


def test_delete_prefix_removes_everything_under_it(storage, frame):
    storage.write_parquet(frame, "raw/dt=2026-08-20/p.parquet")
    storage.write_parquet(frame, "raw/dt=2026-08-21/p.parquet")
    assert storage.delete_prefix("raw") == 2
    assert storage.list_keys("raw") == []


def test_bytes_roundtrip(storage):
    storage.write_bytes("_state/gen.json", b'{"v":1}')
    assert storage.read_bytes("_state/gen.json") == b'{"v":1}'


def test_both_backends_implement_the_same_contract():
    """Guards against the Azure class drifting away from the local one."""
    from pipeline.storage import AzureBlobLakeStorage

    abstract = {n for n, v in vars(LakeStorage).items() if getattr(v, "__isabstractmethod__", False)}
    assert abstract  # sanity: the ABC actually declares abstract methods
    for impl in (LocalLakeStorage, AzureBlobLakeStorage):
        assert not (abstract - set(dir(impl)))
        assert not getattr(impl, "__abstractmethods__", frozenset())
