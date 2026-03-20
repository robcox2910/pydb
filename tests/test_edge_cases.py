"""Edge case tests identified during code review.

These tests cover boundary conditions, error paths, and corner cases
that weren't covered by the feature-specific test files.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import Condition, Operator
from pydb.record import Record
from pydb.sql_parser import ParseError, parse_sql

ZERO_ROWS = 0
ONE_ROW = 1
FOUR_ROWS = 4
TWO_ROWS = 2
EXPECTED_SUM = 4.0


class TestAggregateEdgeCases:
    """Verify aggregates on empty tables and zero-match WHERE."""

    def test_count_empty_table(self, tmp_path: Path) -> None:
        """COUNT(*) on an empty table should return 0."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        rows = execute(parse_sql("SELECT COUNT(*) FROM t"), db)
        assert rows[0]["COUNT(*)"] == ZERO_ROWS

    def test_sum_empty_table(self, tmp_path: Path) -> None:
        """SUM on an empty table should return 0."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        rows = execute(parse_sql("SELECT SUM(val) FROM t"), db)
        assert rows[0]["SUM(val)"] == ZERO_ROWS

    def test_avg_empty_table(self, tmp_path: Path) -> None:
        """AVG on an empty table should return 0.0 (not divide by zero)."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        rows = execute(parse_sql("SELECT AVG(val) FROM t"), db)
        assert rows[0]["AVG(val)"] == 0.0

    def test_count_after_where_filters_all(self, tmp_path: Path) -> None:
        """COUNT after WHERE that matches nothing should return 0."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1)"), db)
        rows = execute(parse_sql("SELECT COUNT(*) FROM t WHERE val > 100"), db)
        assert rows[0]["COUNT(*)"] == ZERO_ROWS

    def test_sum_float_values(self, tmp_path: Path) -> None:
        """SUM on float values should not truncate."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val FLOAT)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1.5)"), db)
        execute(parse_sql("INSERT INTO t VALUES (2.5)"), db)
        rows = execute(parse_sql("SELECT SUM(val) FROM t"), db)
        assert rows[0]["SUM(val)"] == EXPECTED_SUM


class TestSubqueryEdgeCases:
    """Verify subquery error handling."""

    def test_scalar_subquery_multiple_rows_raises(self, tmp_path: Path) -> None:
        """A scalar subquery returning multiple rows should raise."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1)"), db)
        execute(parse_sql("INSERT INTO t VALUES (2)"), db)
        with pytest.raises(QueryError, match="Scalar subquery"):
            execute(parse_sql("SELECT * FROM t WHERE val = (SELECT val FROM t)"), db)


class TestLimitEdgeCases:
    """Verify LIMIT boundary cases."""

    def test_limit_zero(self, tmp_path: Path) -> None:
        """LIMIT 0 should return zero rows."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (val INTEGER)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1)"), db)
        rows = execute(parse_sql("SELECT * FROM t LIMIT 0"), db)
        assert rows == []


class TestParserEdgeCases:
    """Verify parser error handling."""

    def test_empty_sql_raises(self) -> None:
        """Empty SQL string should raise ParseError."""
        with pytest.raises(ParseError):
            parse_sql("")


class TestViewWithOuterClauses:
    """Verify that views respect outer query clauses."""

    def test_view_with_where(self, tmp_path: Path) -> None:
        """WHERE on a view should filter the view's results."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 52)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 48)"), db)
        execute(parse_sql("CREATE VIEW all_cards AS SELECT name, power FROM cards"), db)
        rows = execute(parse_sql("SELECT * FROM all_cards WHERE power > 50"), db)
        assert len(rows) == TWO_ROWS
        names = {str(r["name"]) for r in rows}
        assert "Squirtle" not in names

    def test_view_with_limit(self, tmp_path: Path) -> None:
        """LIMIT on a view should cap the results."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 52)"), db)
        execute(parse_sql("CREATE VIEW all_cards AS SELECT name, power FROM cards"), db)
        rows = execute(parse_sql("SELECT * FROM all_cards LIMIT 1"), db)
        assert len(rows) == ONE_ROW


class TestConditionIN:
    """Verify the IN operator at the query model level."""

    def test_in_operator_matches(self) -> None:
        """IN operator should match values in the list."""
        record = Record(record_id=1, data={"type": "Fire"})
        cond = Condition("type", Operator.IN, ["Fire", "Water"])
        assert cond.matches(record)

    def test_in_operator_rejects(self) -> None:
        """IN operator should reject values not in the list."""
        record = Record(record_id=1, data={"type": "Electric"})
        cond = Condition("type", Operator.IN, ["Fire", "Water"])
        assert not cond.matches(record)
