"""Tests for safe (atomic) saving.

A save must leave either the complete old file or the complete new one --
never a half-written mess -- and must not leave scrap files behind.
"""

import os
from pathlib import Path

import pytest

from pydb.atomic import atomic_write_text

OLD_TEXT = '{"version": "old"}'
NEW_TEXT = '{"version": "new"}'
OWNER_ONLY = 0o600


class _SimulatedCrashError(OSError):
    """Stands in for the power going out part-way through a save."""


def test_writes_a_new_file(tmp_path: Path) -> None:
    """Saving to a path that doesn't exist yet creates the file."""
    path = tmp_path / "data.json"
    atomic_write_text(path, NEW_TEXT)
    assert path.read_text(encoding="utf-8") == NEW_TEXT


def test_replaces_an_existing_file(tmp_path: Path) -> None:
    """Saving again swaps in the new contents completely."""
    path = tmp_path / "data.json"
    path.write_text(OLD_TEXT, encoding="utf-8")
    atomic_write_text(path, NEW_TEXT)
    assert path.read_text(encoding="utf-8") == NEW_TEXT


def test_crash_before_the_swap_keeps_the_old_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If we crash before swapping, the old file is untouched and no scrap is left."""
    path = tmp_path / "data.json"
    path.write_text(OLD_TEXT, encoding="utf-8")

    def crash(*_args: object) -> None:
        raise _SimulatedCrashError

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(_SimulatedCrashError):
        atomic_write_text(path, NEW_TEXT)
    assert path.read_text(encoding="utf-8") == OLD_TEXT
    assert [p.name for p in tmp_path.iterdir()] == ["data.json"]


def test_new_contents_are_forced_to_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The new contents are fsynced before they replace the old file."""
    synced: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        synced.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    atomic_write_text(tmp_path / "data.json", NEW_TEXT)
    assert synced, "expected at least one fsync"


def test_keeps_the_existing_file_permissions(tmp_path: Path) -> None:
    """Replacing a file keeps who-can-read-it the same as before."""
    path = tmp_path / "data.json"
    path.write_text(OLD_TEXT, encoding="utf-8")
    path.chmod(OWNER_ONLY)
    atomic_write_text(path, NEW_TEXT)
    assert path.stat().st_mode & 0o777 == OWNER_ONLY
