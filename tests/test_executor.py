"""Tests for the query executor.

The executor is the librarian who answers your questions. These tests
verify that SELECT, WHERE, ORDER BY, LIMIT, and projections all work
correctly against real tables.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import And, Condition, Operator, Or, OrderBy, Query, SortDirection
from pydb.schema import Column, Schema
from pydb.types import DataType

# Named constants.
POWER_55 = 55
POWER_52 = 52
POWER_48 = 48
POWER_30 = 30
POWER_THRESHOLD = 50
LIMIT_2 = 2
EXPECTED_3 = 3


def _make_db(tmp_path: Path) -> Database:
    """Create a test database with a populated cards table."""
    db = Database(path=tmp_path)
    schema = Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="type", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )
    table = db.create_table("cards", schema)
    table.insert({"name": "Pikachu", "type": "Electric", "power": POWER_55})
    table.insert({"name": "Charmander", "type": "Fire", "power": POWER_52})
    table.insert({"name": "Squirtle", "type": "Water", "power": POWER_48})
    return db


class TestSelectAll:
    """Verify queries with no filtering."""

    def test_select_all_rows(self, tmp_path: Path) -> None:
        """A query with no WHERE should return all rows."""
        db = _make_db(tmp_path)
        results = execute(Query(table="cards"), db)
        assert len(results) == EXPECTED_3

    def test_select_all_includes_all_columns(self, tmp_path: Path) -> None:
        """With no column list, all columns should be included."""
        db = _make_db(tmp_path)
        results = execute(Query(table="cards"), db)
        assert set(results[0].keys()) == {"name", "type", "power"}


class TestWhereCondition:
    """Verify WHERE filtering with various operators."""

    def test_eq_filter(self, tmp_path: Path) -> None:
        """EQ should return only matching rows."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("name", Operator.EQ, "Pikachu"))
        results = execute(q, db)
        assert len(results) == 1
        assert results[0]["name"] == "Pikachu"

    def test_gt_filter(self, tmp_path: Path) -> None:
        """GT should return rows where column > value."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("power", Operator.GT, POWER_THRESHOLD))
        results = execute(q, db)
        assert len(results) == LIMIT_2
        names = {r["name"] for r in results}
        assert names == {"Pikachu", "Charmander"}

    def test_le_filter(self, tmp_path: Path) -> None:
        """LE should return rows where column <= value."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("power", Operator.LE, POWER_48))
        results = execute(q, db)
        assert len(results) == 1
        assert results[0]["name"] == "Squirtle"

    def test_ne_filter(self, tmp_path: Path) -> None:
        """NE should return rows where column != value."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("type", Operator.NE, "Fire"))
        results = execute(q, db)
        assert len(results) == LIMIT_2

    def test_no_matches(self, tmp_path: Path) -> None:
        """A WHERE that matches nothing should return empty."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("name", Operator.EQ, "MissingNo"))
        results = execute(q, db)
        assert results == []


class TestAndOrFilters:
    """Verify AND and OR in WHERE clauses."""

    def test_and_filter(self, tmp_path: Path) -> None:
        """AND should require both conditions to be true."""
        db = _make_db(tmp_path)
        q = Query(
            table="cards",
            where=And(
                Condition("type", Operator.EQ, "Electric"),
                Condition("power", Operator.GT, POWER_THRESHOLD),
            ),
        )
        results = execute(q, db)
        assert len(results) == 1
        assert results[0]["name"] == "Pikachu"

    def test_or_filter(self, tmp_path: Path) -> None:
        """OR should require at least one condition to be true."""
        db = _make_db(tmp_path)
        q = Query(
            table="cards",
            where=Or(
                Condition("type", Operator.EQ, "Electric"),
                Condition("type", Operator.EQ, "Water"),
            ),
        )
        results = execute(q, db)
        assert len(results) == LIMIT_2
        names = {r["name"] for r in results}
        assert names == {"Pikachu", "Squirtle"}


class TestProjection:
    """Verify column projection (SELECT specific columns)."""

    def test_select_specific_columns(self, tmp_path: Path) -> None:
        """Only requested columns should appear in results."""
        db = _make_db(tmp_path)
        q = Query(table="cards", columns=["name", "power"])
        results = execute(q, db)
        assert set(results[0].keys()) == {"name", "power"}
        assert "type" not in results[0]

    def test_select_single_column(self, tmp_path: Path) -> None:
        """A single column projection should work."""
        db = _make_db(tmp_path)
        q = Query(table="cards", columns=["name"])
        results = execute(q, db)
        assert list(results[0].keys()) == ["name"]

    def test_unknown_column_raises(self, tmp_path: Path) -> None:
        """Selecting a non-existent column should raise QueryError."""
        db = _make_db(tmp_path)
        q = Query(table="cards", columns=["name", "missing"])
        with pytest.raises(QueryError, match="Unknown column"):
            execute(q, db)


class TestOrderBy:
    """Verify result ordering."""

    def test_order_by_asc(self, tmp_path: Path) -> None:
        """ORDER BY ASC should sort in ascending order."""
        db = _make_db(tmp_path)
        q = Query(table="cards", columns=["name"], order_by=OrderBy("name"))
        results = execute(q, db)
        names = [r["name"] for r in results]
        assert names == ["Charmander", "Pikachu", "Squirtle"]

    def test_order_by_desc(self, tmp_path: Path) -> None:
        """ORDER BY DESC should sort in descending order."""
        db = _make_db(tmp_path)
        q = Query(
            table="cards",
            columns=["name"],
            order_by=OrderBy("name", direction=SortDirection.DESC),
        )
        results = execute(q, db)
        names = [r["name"] for r in results]
        assert names == ["Squirtle", "Pikachu", "Charmander"]

    def test_order_by_numeric(self, tmp_path: Path) -> None:
        """ORDER BY should work with numeric columns."""
        db = _make_db(tmp_path)
        q = Query(table="cards", columns=["name", "power"], order_by=OrderBy("power"))
        results = execute(q, db)
        powers = [r["power"] for r in results]
        assert powers == [POWER_48, POWER_52, POWER_55]

    def test_order_by_unknown_column_raises(self, tmp_path: Path) -> None:
        """ORDER BY on a non-existent column should raise QueryError."""
        db = _make_db(tmp_path)
        q = Query(table="cards", order_by=OrderBy("missing"))
        with pytest.raises(QueryError, match="unknown column"):
            execute(q, db)


class TestLimit:
    """Verify result limiting."""

    def test_limit_caps_results(self, tmp_path: Path) -> None:
        """LIMIT should cap the number of returned rows."""
        db = _make_db(tmp_path)
        q = Query(table="cards", limit=LIMIT_2)
        results = execute(q, db)
        assert len(results) == LIMIT_2

    def test_limit_with_order(self, tmp_path: Path) -> None:
        """LIMIT after ORDER BY should return the first N sorted rows."""
        db = _make_db(tmp_path)
        q = Query(
            table="cards",
            columns=["name"],
            order_by=OrderBy("name"),
            limit=LIMIT_2,
        )
        results = execute(q, db)
        names = [r["name"] for r in results]
        assert names == ["Charmander", "Pikachu"]

    def test_limit_larger_than_rows(self, tmp_path: Path) -> None:
        """LIMIT larger than row count should return all rows."""
        db = _make_db(tmp_path)
        q = Query(table="cards", limit=100)
        results = execute(q, db)
        assert len(results) == EXPECTED_3


class TestErrorHandling:
    """Verify error cases."""

    def test_nonexistent_table_raises(self, tmp_path: Path) -> None:
        """Querying a table that doesn't exist should raise QueryError."""
        db = Database(path=tmp_path)
        q = Query(table="missing")
        with pytest.raises(QueryError, match="Query failed"):
            execute(q, db)

    def test_unknown_where_column_raises(self, tmp_path: Path) -> None:
        """WHERE on a non-existent column should raise QueryError."""
        db = _make_db(tmp_path)
        q = Query(table="cards", where=Condition("missing", Operator.EQ, "x"))
        with pytest.raises(QueryError, match="Unknown column"):
            execute(q, db)


class TestCombined:
    """Verify combinations of WHERE, ORDER BY, LIMIT, and projection."""

    def test_where_order_limit_project(self, tmp_path: Path) -> None:
        """All clauses should work together."""
        db = _make_db(tmp_path)
        q = Query(
            table="cards",
            columns=["name"],
            where=Condition("power", Operator.GE, POWER_48),
            order_by=OrderBy("power", direction=SortDirection.DESC),
            limit=LIMIT_2,
        )
        results = execute(q, db)
        assert len(results) == LIMIT_2
        assert results[0] == {"name": "Pikachu"}
        assert results[1] == {"name": "Charmander"}
