"""Tests for the query planner, EXPLAIN, CREATE INDEX, and DROP INDEX.

The planner is the smart librarian who decides whether to walk every
shelf or use the card catalog. These tests verify the planner's
decisions and the SQL commands that manage indexes.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.index import Index
from pydb.planner import QueryPlan, plan_query
from pydb.query import Condition, Operator, Query
from pydb.record import Record, Value
from pydb.sql_parser import parse_sql
from pydb.statements import CreateIndexStatement, DropIndexStatement, ExplainStatement
from pydb.table import Table

# Named constants.
ONE_ROW = 1
TWO_ROWS = 2
THREE_ROWS = 3
POWER_55 = 55


def _make_db(tmp_path: Path) -> Database:
    """Create a database with a cards table."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 48)"), db)
    return db


class TestPlannerDecision:
    """Verify the planner chooses the right strategy."""

    def test_full_scan_without_index(self, tmp_path: Path) -> None:
        """Without an index, the planner should choose a full table scan."""
        db = _make_db(tmp_path)
        table = db.get_table("cards")
        query = Query(table="cards", where=Condition("name", Operator.EQ, "Pikachu"))
        plan = plan_query(query, table)
        assert not plan.use_index
        assert "Full table scan" in plan.strategy

    def test_index_lookup_with_index(self, tmp_path: Path) -> None:
        """With an index on the WHERE column, the planner should use it."""
        db = _make_db(tmp_path)
        table = db.get_table("cards")
        table.create_index("idx_name", "name")
        query = Query(table="cards", where=Condition("name", Operator.EQ, "Pikachu"))
        plan = plan_query(query, table)
        assert plan.use_index
        assert "Index lookup" in plan.strategy
        assert "idx_name" in plan.strategy

    def test_full_scan_for_non_eq_operator(self, tmp_path: Path) -> None:
        """Non-equality operators should use full scan even with an index."""
        db = _make_db(tmp_path)
        table = db.get_table("cards")
        table.create_index("idx_power", "power")
        query = Query(table="cards", where=Condition("power", Operator.GT, 50))
        plan = plan_query(query, table)
        assert not plan.use_index

    def test_full_scan_without_where(self, tmp_path: Path) -> None:
        """A query with no WHERE should always use full scan."""
        db = _make_db(tmp_path)
        table = db.get_table("cards")
        table.create_index("idx_name", "name")
        query = Query(table="cards")
        plan = plan_query(query, table)
        assert not plan.use_index


class TestParseIndexStatements:
    """Verify parsing of CREATE INDEX and DROP INDEX."""

    def test_parse_create_index(self) -> None:
        """CREATE INDEX should parse correctly."""
        stmt = parse_sql("CREATE INDEX idx_name ON cards (name)")
        assert isinstance(stmt, CreateIndexStatement)
        assert stmt.index_name == "idx_name"
        assert stmt.table == "cards"
        assert stmt.column == "name"

    def test_parse_drop_index(self) -> None:
        """DROP INDEX should parse correctly."""
        stmt = parse_sql("DROP INDEX idx_name ON cards")
        assert isinstance(stmt, DropIndexStatement)
        assert stmt.index_name == "idx_name"
        assert stmt.table == "cards"

    def test_parse_explain(self) -> None:
        """EXPLAIN SELECT should parse correctly."""
        stmt = parse_sql("EXPLAIN SELECT * FROM cards WHERE name = 'Pikachu'")
        assert isinstance(stmt, ExplainStatement)
        assert stmt.query.table == "cards"


class TestExecuteCreateIndex:
    """Verify CREATE INDEX execution."""

    def test_create_index(self, tmp_path: Path) -> None:
        """CREATE INDEX should add an index to the table."""
        db = _make_db(tmp_path)
        result = execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        assert "created" in str(result[0]["result"])
        table = db.get_table("cards")
        assert "idx_name" in table.indexes

    def test_create_duplicate_raises(self, tmp_path: Path) -> None:
        """Creating a duplicate index should raise."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        with pytest.raises(QueryError, match="CREATE INDEX failed"):
            execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)


class TestExecuteDropIndex:
    """Verify DROP INDEX execution."""

    def test_drop_index(self, tmp_path: Path) -> None:
        """DROP INDEX should remove the index."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        result = execute(parse_sql("DROP INDEX idx_name ON cards"), db)
        assert "dropped" in str(result[0]["result"])
        table = db.get_table("cards")
        assert "idx_name" not in table.indexes

    def test_drop_nonexistent_raises(self, tmp_path: Path) -> None:
        """Dropping a non-existent index should raise."""
        db = _make_db(tmp_path)
        with pytest.raises(QueryError, match="DROP INDEX failed"):
            execute(parse_sql("DROP INDEX missing ON cards"), db)


class TestExecuteExplain:
    """Verify EXPLAIN execution."""

    def test_explain_full_scan(self, tmp_path: Path) -> None:
        """EXPLAIN without an index should show full table scan."""
        db = _make_db(tmp_path)
        result = execute(parse_sql("EXPLAIN SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert "Full table scan" in str(result[0]["plan"])

    def test_explain_index_lookup(self, tmp_path: Path) -> None:
        """EXPLAIN with an index should show index lookup."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        result = execute(parse_sql("EXPLAIN SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert "Index lookup" in str(result[0]["plan"])
        assert "idx_name" in str(result[0]["plan"])


class TestIndexMaintenance:
    """Verify that indexes are auto-maintained on INSERT, UPDATE, DELETE."""

    def test_index_updated_on_insert(self, tmp_path: Path) -> None:
        """An insert should update the index."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Raichu', 60)"), db)
        table = db.get_table("cards")
        idx = table.indexes["idx_name"]
        assert idx.find("Raichu") != []

    def test_index_updated_on_delete(self, tmp_path: Path) -> None:
        """A delete should update the index."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        execute(parse_sql("DELETE FROM cards WHERE name = 'Pikachu'"), db)
        table = db.get_table("cards")
        idx = table.indexes["idx_name"]
        assert idx.find("Pikachu") == []

    def test_index_updated_on_update(self, tmp_path: Path) -> None:
        """An update should update the index."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        execute(parse_sql("UPDATE cards SET name = 'Raichu' WHERE name = 'Pikachu'"), db)
        table = db.get_table("cards")
        idx = table.indexes["idx_name"]
        assert idx.find("Pikachu") == []
        assert idx.find("Raichu") != []

    def test_query_same_results_with_and_without_index(self, tmp_path: Path) -> None:
        """A query should return the same results whether or not an index exists."""
        db = _make_db(tmp_path)
        without_idx = execute(parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)
        with_idx = execute(parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert without_idx == with_idx


class TestIndexUsedInExecution:
    """Verify equality queries actually use the index when executed."""

    def test_equality_query_uses_index(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`col = value` on an indexed column goes through select_by_index."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_name ON cards (name)"), db)

        calls: list[Value] = []
        original = Table.select_by_index

        def spy(self: Table, index: Index, key: Value) -> list[Record]:
            calls.append(key)
            return original(self, index, key)

        monkeypatch.setattr(Table, "select_by_index", spy)

        rows = execute(parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert calls == ["Pikachu"]
        assert len(rows) == ONE_ROW
        assert rows[0]["name"] == "Pikachu"

    def test_query_without_index_does_full_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without an index, execution never calls select_by_index."""
        db = _make_db(tmp_path)

        calls: list[Value] = []
        original = Table.select_by_index

        def spy(self: Table, index: Index, key: Value) -> list[Record]:
            calls.append(key)
            return original(self, index, key)

        monkeypatch.setattr(Table, "select_by_index", spy)

        rows = execute(parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert calls == []
        assert len(rows) == ONE_ROW

    def test_non_equality_query_does_not_use_index(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `>` query does not use the index even when one exists."""
        db = _make_db(tmp_path)
        execute(parse_sql("CREATE INDEX idx_power ON cards (power)"), db)

        calls: list[Value] = []
        original = Table.select_by_index

        def spy(self: Table, index: Index, key: Value) -> list[Record]:
            calls.append(key)
            return original(self, index, key)

        monkeypatch.setattr(Table, "select_by_index", spy)

        rows = execute(parse_sql("SELECT * FROM cards WHERE power > 50"), db)
        assert calls == []
        assert len(rows) == TWO_ROWS


class TestQueryPlanModel:
    """Verify the QueryPlan dataclass."""

    def test_default_fields(self) -> None:
        """A plan should have sensible defaults."""
        plan = QueryPlan(strategy="Full table scan on cards")
        assert not plan.use_index
        assert plan.index_name == ""
