"""Configuration is the seam between local dev and the cloud, so it gets tested."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.config import REPO_ROOT, get_settings


def test_defaults_to_local_backend(monkeypatch):
    monkeypatch.delenv("LAKE_BACKEND", raising=False)
    assert get_settings().lake_backend == "local"


def test_relative_lake_root_resolves_from_repo_root(monkeypatch):
    """`./data` must mean the same directory no matter where the process started.

    A container's working directory is not the developer's, so a relative path
    that resolved against cwd would silently write to the wrong place.
    """
    monkeypatch.setenv("LAKE_LOCAL_ROOT", "./data")
    assert get_settings().lake_local_root == (REPO_ROOT / "data").resolve()


def test_absolute_lake_root_is_left_alone(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKE_LOCAL_ROOT", str(tmp_path))
    assert get_settings().lake_local_root == Path(tmp_path)


def test_settings_are_reread_not_cached(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKE_LOCAL_ROOT", str(tmp_path / "a"))
    first = get_settings().lake_local_root
    monkeypatch.setenv("LAKE_LOCAL_ROOT", str(tmp_path / "b"))
    assert get_settings().lake_local_root != first


def test_azure_backend_requires_credentials(monkeypatch):
    """Fail loudly at startup rather than halfway through a batch write."""
    monkeypatch.setenv("LAKE_BACKEND", "azure")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")
    with pytest.raises(ValueError, match="AZURE_STORAGE_CONNECTION_STRING"):
        get_settings()


def test_unknown_backend_rejected(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "s3")
    with pytest.raises(ValueError, match="LAKE_BACKEND"):
        get_settings()
