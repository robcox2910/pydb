"""Tests for aggregate functions and GROUP BY.

Aggregations are like sorting cards into piles and then counting,
summing, or averaging each pile. These tests verify that all five
aggregate functions work with and without GROUP BY.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import AggFunc, AggregateColumn, Query
from pydb.sql_parser import parse_sql

# Named constants.
ONE_ROW = 1
TWO_ROWS = 2
THREE_ROWS = 3
FOUR_ROWS = 4
TOTAL_SUM = 215
AVERAGE_POWER = 53.75
MIN_POWER = 48
MAX_POWER = 60
ELECTRIC_SUM = 115
FIRE_SUM = 52


def _make_db(tmp_path: Path) -> Database:
    """Create a database with a cards table for aggregation tests."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, type TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 'Electric', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Raichu', 'Electric', 60)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 'Fire', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 'Water', 48)"), db)
    return db


class TestParseAggregates:
    """Verify parsing of aggregate function calls."""

    def test_count_star(self) -> None:
        """COUNT(*) should parse correctly."""
        q = parse_sql("SELECT COUNT(*) FROM cards")
        assert isinstance(q, Query)
        assert len(q.aggregates) == ONE_ROW
        assert q.aggregates[0].function == AggFunc.COUNT
        assert q.aggregates[0].column == "*"
        assert q.aggregates[0].alias == "COUNT(*)"

    def test_sum(self) -> None:
        """SUM(power) should parse correctly."""
        q = parse_sql("SELECT SUM(power) FROM cards")
        assert isinstance(q, Query)
        assert q.aggregates[0].function == AggFunc.SUM
        assert q.aggregates[0].column == "power"

    def test_avg(self) -> None:
        """AVG(power) should parse correctly."""
        q = parse_sql("SELECT AVG(power) FROM cards")
        assert isinstance(q, Query)
        assert q.aggregates[0].function == AggFunc.AVG

    def test_min(self) -> None:
        """MIN(power) should parse correctly."""
        q = parse_sql("SELECT MIN(power) FROM cards")
        assert isinstance(q, Query)
        assert q.aggregates[0].function == AggFunc.MIN

    def test_max(self) -> None:
        """MAX(power) should parse correctly."""
        q = parse_sql("SELECT MAX(power) FROM cards")
        assert isinstance(q, Query)
        assert q.aggregates[0].function == AggFunc.MAX

    def test_multiple_aggregates(self) -> None:
        """Multiple aggregate functions should parse together."""
        q = parse_sql("SELECT COUNT(*), SUM(power), AVG(power) FROM cards")
        assert isinstance(q, Query)
        assert len(q.aggregates) == THREE_ROWS

    def test_aggregate_with_group_by(self) -> None:
        """Aggregate with GROUP BY should parse both."""
        q = parse_sql("SELECT type, COUNT(*) FROM cards GROUP BY type")
        assert isinstance(q, Query)
        assert q.columns == ["type"]
        assert len(q.aggregates) == ONE_ROW
        assert q.group_by == ["type"]

    def test_having_clause(self) -> None:
        """HAVING should parse a condition."""
        q = parse_sql("SELECT type, COUNT(*) FROM cards GROUP BY type HAVING COUNT(*) > 1")
        assert isinstance(q, Query)
        assert q.having is not None


class TestExecuteAggregatesNoGroupBy:
    """Verify aggregate functions on the whole table (no GROUP BY)."""

    def test_count_star(self, tmp_path: Path) -> None:
        """COUNT(*) should count all rows."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT COUNT(*) FROM cards"), db)
        assert len(rows) == ONE_ROW
        assert rows[0]["COUNT(*)"] == FOUR_ROWS

    def test_sum(self, tmp_path: Path) -> None:
        """SUM(power) should add up all power values."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT SUM(power) FROM cards"), db)
        assert rows[0]["SUM(power)"] == TOTAL_SUM

    def test_avg(self, tmp_path: Path) -> None:
        """AVG(power) should calculate the average."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT AVG(power) FROM cards"), db)
        assert rows[0]["AVG(power)"] == AVERAGE_POWER

    def test_min(self, tmp_path: Path) -> None:
        """MIN(power) should find the smallest value."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT MIN(power) FROM cards"), db)
        assert rows[0]["MIN(power)"] == MIN_POWER

    def test_max(self, tmp_path: Path) -> None:
        """MAX(power) should find the largest value."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT MAX(power) FROM cards"), db)
        assert rows[0]["MAX(power)"] == MAX_POWER

    def test_multiple_aggregates(self, tmp_path: Path) -> None:
        """Multiple aggregates in one query should all compute."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT COUNT(*), MIN(power), MAX(power) FROM cards"), db)
        assert len(rows) == ONE_ROW
        assert rows[0]["COUNT(*)"] == FOUR_ROWS
        assert rows[0]["MIN(power)"] == MIN_POWER
        assert rows[0]["MAX(power)"] == MAX_POWER


class TestExecuteGroupBy:
    """Verify GROUP BY creates correct groups."""

    def test_group_by_with_count(self, tmp_path: Path) -> None:
        """GROUP BY type with COUNT should show count per type."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT type, COUNT(*) FROM cards GROUP BY type"), db)
        type_counts = {str(r["type"]): r["COUNT(*)"] for r in rows}
        assert type_counts["Electric"] == TWO_ROWS
        assert type_counts["Fire"] == ONE_ROW
        assert type_counts["Water"] == ONE_ROW

    def test_group_by_with_sum(self, tmp_path: Path) -> None:
        """GROUP BY type with SUM should show total power per type."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT type, SUM(power) FROM cards GROUP BY type"), db)
        type_sums = {str(r["type"]): r["SUM(power)"] for r in rows}
        assert type_sums["Electric"] == ELECTRIC_SUM
        assert type_sums["Fire"] == FIRE_SUM

    def test_group_by_with_max(self, tmp_path: Path) -> None:
        """GROUP BY type with MAX should show strongest per type."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT type, MAX(power) FROM cards GROUP BY type"), db)
        type_max = {str(r["type"]): r["MAX(power)"] for r in rows}
        assert type_max["Electric"] == MAX_POWER
        assert type_max["Water"] == MIN_POWER


class TestExecuteHaving:
    """Verify HAVING filters groups."""

    def test_having_filters_groups(self, tmp_path: Path) -> None:
        """HAVING COUNT(*) > 1 should only show types with multiple cards."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT type, COUNT(*) FROM cards GROUP BY type HAVING COUNT(*) > 1"),
            db,
        )
        assert len(rows) == ONE_ROW
        assert rows[0]["type"] == "Electric"

    def test_having_no_matches(self, tmp_path: Path) -> None:
        """HAVING that matches no groups should return empty."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT type, COUNT(*) FROM cards GROUP BY type HAVING COUNT(*) > 10"),
            db,
        )
        assert rows == []


class TestExecuteAggregateWithWhere:
    """Verify WHERE filters before aggregation."""

    def test_where_then_count(self, tmp_path: Path) -> None:
        """WHERE should filter rows before COUNT."""
        db = _make_db(tmp_path)
        rows = execute(parse_sql("SELECT COUNT(*) FROM cards WHERE power > 50"), db)
        assert rows[0]["COUNT(*)"] == THREE_ROWS


class TestAggregateModel:
    """Verify AggregateColumn dataclass."""

    def test_aggregate_column_fields(self) -> None:
        """AggregateColumn should store function, column, and alias."""
        agg = AggregateColumn(function=AggFunc.COUNT, column="*", alias="COUNT(*)")
        assert agg.function == AggFunc.COUNT
        assert agg.column == "*"
        assert agg.alias == "COUNT(*)"


class TestAggregateErrors:
    """Verify error handling for invalid aggregate queries."""

    def test_non_grouped_column_raises(self, tmp_path: Path) -> None:
        """A column not in GROUP BY should raise QueryError."""
        db = _make_db(tmp_path)
        with pytest.raises(QueryError, match="must appear in GROUP BY"):
            execute(
                parse_sql("SELECT name, COUNT(*) FROM cards GROUP BY type"),
                db,
            )
