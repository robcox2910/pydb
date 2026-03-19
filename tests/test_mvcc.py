"""Tests for Multi-Version Concurrency Control (MVCC).

MVCC keeps multiple versions of rows so readers and writers don't
interfere. These tests verify snapshot isolation and version visibility.
"""

from pydb.mvcc import MVCCStore, VersionedRow

ONE_ROW = 1
TWO_ROWS = 2
ZERO_ROWS = 0


class TestVersionedRow:
    """Verify row visibility logic."""

    def test_visible_at_creation_version(self) -> None:
        """A row should be visible at the version it was created."""
        row = VersionedRow(row_id=1, data={"name": "Pikachu"}, created_at=1)
        assert row.visible_at(1)

    def test_visible_at_later_version(self) -> None:
        """A row should be visible at versions after creation."""
        row = VersionedRow(row_id=1, data={"name": "Pikachu"}, created_at=1)
        assert row.visible_at(5)

    def test_not_visible_before_creation(self) -> None:
        """A row should not be visible before it was created."""
        row = VersionedRow(row_id=1, data={"name": "Pikachu"}, created_at=3)
        assert not row.visible_at(2)

    def test_not_visible_after_deletion(self) -> None:
        """A row should not be visible at or after its deletion version."""
        row = VersionedRow(row_id=1, data={"name": "Pikachu"}, created_at=1, deleted_at=3)
        assert not row.visible_at(3)

    def test_visible_before_deletion(self) -> None:
        """A row should be visible between creation and deletion."""
        row = VersionedRow(row_id=1, data={"name": "Pikachu"}, created_at=1, deleted_at=3)
        assert row.visible_at(2)


class TestMVCCStore:
    """Verify the MVCC store operations."""

    def test_insert_and_read(self) -> None:
        """An inserted row should be visible at its version."""
        store = MVCCStore()
        v1 = store.begin_version()
        store.insert({"name": "Pikachu"}, v1)
        rows = store.read(v1)
        assert len(rows) == ONE_ROW
        assert rows[0].data["name"] == "Pikachu"

    def test_insert_not_visible_at_earlier_version(self) -> None:
        """A row inserted at v2 should not be visible at v1."""
        store = MVCCStore()
        v1 = store.begin_version()
        v2 = store.begin_version()
        store.insert({"name": "Pikachu"}, v2)
        assert store.read(v1) == []

    def test_delete_hides_row(self) -> None:
        """A deleted row should not be visible at the deletion version."""
        store = MVCCStore()
        v1 = store.begin_version()
        row_id = store.insert({"name": "Pikachu"}, v1)
        v2 = store.begin_version()
        store.delete(row_id, v2)
        assert len(store.read(v1)) == ONE_ROW
        assert len(store.read(v2)) == ZERO_ROWS

    def test_snapshot_isolation(self) -> None:
        """Two versions should see different states of the data."""
        store = MVCCStore()
        v1 = store.begin_version()
        store.insert({"name": "Pikachu"}, v1)

        v2 = store.begin_version()
        store.insert({"name": "Charmander"}, v2)

        # v1 reader sees only Pikachu.
        assert len(store.read(v1)) == ONE_ROW
        # v2 reader sees both.
        assert len(store.read(v2)) == TWO_ROWS

    def test_delete_nonexistent_returns_false(self) -> None:
        """Deleting a non-existent row should return False."""
        store = MVCCStore()
        v1 = store.begin_version()
        assert not store.delete(999, v1)

    def test_version_increments(self) -> None:
        """Each begin_version should increment the counter."""
        store = MVCCStore()
        v1 = store.begin_version()
        v2 = store.begin_version()
        assert v2 == v1 + 1


class TestMVCCVacuum:
    """Verify garbage collection of old row versions."""

    def test_vacuum_removes_old_deleted_rows(self) -> None:
        """Vacuum should remove rows deleted before the oldest active version."""
        store = MVCCStore()
        v1 = store.begin_version()
        row_id = store.insert({"name": "Pikachu"}, v1)
        v2 = store.begin_version()
        store.delete(row_id, v2)
        v3 = store.begin_version()

        # Vacuum with oldest active = v3; row deleted at v2 is safe to remove.
        cleaned = store.vacuum(v3)
        assert cleaned == ONE_ROW

    def test_vacuum_keeps_active_rows(self) -> None:
        """Vacuum should not remove rows that are still visible."""
        store = MVCCStore()
        v1 = store.begin_version()
        store.insert({"name": "Pikachu"}, v1)
        cleaned = store.vacuum(v1)
        assert cleaned == ZERO_ROWS
        assert len(store.read(v1)) == ONE_ROW

    def test_vacuum_keeps_recently_deleted(self) -> None:
        """Vacuum should keep rows deleted at or after the oldest active version."""
        store = MVCCStore()
        v1 = store.begin_version()
        row_id = store.insert({"name": "Pikachu"}, v1)
        v2 = store.begin_version()
        store.delete(row_id, v2)

        # Oldest active is v1, so row deleted at v2 is still needed.
        cleaned = store.vacuum(v1)
        assert cleaned == ZERO_ROWS
