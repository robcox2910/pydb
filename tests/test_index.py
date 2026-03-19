"""Tests for the Index class.

An index is the back-of-the-textbook lookup for a single column. These
tests verify that the index wrapper around the B-Tree works correctly.
"""

from pydb.index import Index

# Named constants.
RECORD_1 = 1
RECORD_2 = 2
RECORD_3 = 3
RECORD_4 = 4
RECORD_5 = 5


class TestIndexCreation:
    """Verify initial state of a new index."""

    def test_index_has_name(self) -> None:
        """An index should know its name."""
        idx = Index(name="idx_name", column="name")
        assert idx.name == "idx_name"

    def test_index_has_column(self) -> None:
        """An index should know which column it covers."""
        idx = Index(name="idx_name", column="name")
        assert idx.column == "name"

    def test_repr(self) -> None:
        """The repr should show name and column."""
        idx = Index(name="idx_name", column="name")
        result = repr(idx)
        assert "idx_name" in result
        assert "name" in result


class TestIndexInsertAndFind:
    """Verify insert and find operations."""

    def test_insert_and_find(self) -> None:
        """Inserting a value should make it findable."""
        idx = Index(name="idx_name", column="name")
        idx.insert("Pikachu", RECORD_1)
        assert idx.find("Pikachu") == [RECORD_1]

    def test_find_missing_returns_empty(self) -> None:
        """Finding a value that doesn't exist should return empty."""
        idx = Index(name="idx_name", column="name")
        assert idx.find("missing") == []

    def test_multiple_records_same_value(self) -> None:
        """Multiple records with the same value should all be found."""
        idx = Index(name="idx_type", column="type")
        idx.insert("Electric", RECORD_1)
        idx.insert("Electric", RECORD_4)
        assert idx.find("Electric") == [RECORD_1, RECORD_4]

    def test_different_values(self) -> None:
        """Different values should be found independently."""
        idx = Index(name="idx_name", column="name")
        idx.insert("Pikachu", RECORD_1)
        idx.insert("Charmander", RECORD_2)
        assert idx.find("Pikachu") == [RECORD_1]
        assert idx.find("Charmander") == [RECORD_2]

    def test_integer_keys(self) -> None:
        """Indexes should work with integer values."""
        idx = Index(name="idx_power", column="power")
        idx.insert(55, RECORD_1)
        idx.insert(52, RECORD_2)
        assert idx.find(55) == [RECORD_1]


class TestIndexDelete:
    """Verify delete operations."""

    def test_delete_removes_mapping(self) -> None:
        """Deleting should remove the key-record mapping."""
        idx = Index(name="idx_name", column="name")
        idx.insert("Pikachu", RECORD_1)
        assert idx.delete("Pikachu", RECORD_1)
        assert idx.find("Pikachu") == []

    def test_delete_returns_false_for_missing(self) -> None:
        """Deleting a non-existent mapping should return False."""
        idx = Index(name="idx_name", column="name")
        assert not idx.delete("missing", RECORD_1)

    def test_delete_one_of_many(self) -> None:
        """Deleting one record should leave others."""
        idx = Index(name="idx_type", column="type")
        idx.insert("Electric", RECORD_1)
        idx.insert("Electric", RECORD_4)
        idx.delete("Electric", RECORD_1)
        assert idx.find("Electric") == [RECORD_4]


class TestIndexRangeSearch:
    """Verify range queries through the index."""

    def test_range_find(self) -> None:
        """Range search should return records in the range."""
        idx = Index(name="idx_power", column="power")
        idx.insert(10, RECORD_1)
        idx.insert(20, RECORD_2)
        idx.insert(30, RECORD_3)
        idx.insert(40, RECORD_4)
        idx.insert(50, RECORD_5)
        result = idx.find_range(20, 40)
        assert sorted(result) == [RECORD_2, RECORD_3, RECORD_4]

    def test_range_no_results(self) -> None:
        """Range search with no matches should return empty."""
        idx = Index(name="idx_power", column="power")
        idx.insert(10, RECORD_1)
        idx.insert(50, RECORD_2)
        assert idx.find_range(20, 40) == []
