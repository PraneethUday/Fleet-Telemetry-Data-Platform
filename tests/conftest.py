"""Shared fixtures.

Every test runs against a throwaway local lake in tmp_path. Nothing in the suite
touches Azure or the developer's real ./data directory, which is what lets the
whole pipeline be verified before a single cloud resource exists.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.config import get_settings
from pipeline.generator.simulator import FleetSimulator, GeneratorConfig
from pipeline.storage import LocalLakeStorage


@pytest.fixture(scope="session", autouse=True)
def prefect_harness():
    """Give the whole session one throwaway Prefect database.

    Without this, every flow call spins up its own ephemeral Prefect server and
    tears it down again — seconds of startup per test, and a wall of shutdown
    noise on stderr. The harness is also what keeps the suite from writing to
    the developer's real ~/.prefect state.
    """
    from prefect.testing.utilities import prefect_test_harness

    with prefect_test_harness():
        yield


@pytest.fixture
def lake_env(tmp_path, monkeypatch):
    """Point the whole pipeline at an isolated local lake."""
    monkeypatch.setenv("LAKE_BACKEND", "local")
    monkeypatch.setenv("LAKE_LOCAL_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("BRONZE_PREFIX", "raw")
    monkeypatch.setenv("SILVER_PREFIX", "silver")
    monkeypatch.setenv("GOLD_PREFIX", "gold")
    return get_settings()


@pytest.fixture
def storage(lake_env):
    return LocalLakeStorage(lake_env.lake_local_root)


@pytest.fixture
def window():
    """A fixed 6-hour UTC window, so tests never depend on wall-clock time."""
    start = datetime(2026, 8, 20, 0, 0, tzinfo=timezone.utc)
    return start, start + timedelta(hours=6)


@pytest.fixture
def small_config():
    """A 40-machine fleet — big enough for every type to appear, fast to run."""
    return GeneratorConfig(fleet_size=40, seed=7, interval_seconds=300, batch_minutes=60)


@pytest.fixture
def batches(small_config, window):
    start, end = window
    return list(FleetSimulator(small_config).simulate(start, end))
