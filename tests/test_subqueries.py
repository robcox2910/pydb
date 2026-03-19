"""Tests for subquery support.

Subqueries are questions inside questions. These tests verify that
scalar subqueries and IN subqueries work correctly.
"""

from pathlib import Path

from pydb.database import Database
from pydb.executor import execute
from pydb.query import Condition, Operator, Query, Subquery
from pydb.sql_parser import parse_sql

# Named constants.
ONE_ROW = 1
TWO_ROWS = 2
THREE_ROWS = 3


def _make_db(tmp_path: Path) -> Database:
    """Create a database with cards and trainers tables."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, type TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 'Electric', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Raichu', 'Electric', 60)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 'Fire', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 'Water', 48)"), db)

    execute(parse_sql("CREATE TABLE trainers (name TEXT, pokemon TEXT)"), db)
    execute(parse_sql("INSERT INTO trainers VALUES ('Ash', 'Pikachu')"), db)
    execute(parse_sql("INSERT INTO trainers VALUES ('Misty', 'Squirtle')"), db)
    return db


class TestParseSubqueries:
    """Verify subquery parsing."""

    def test_scalar_subquery(self) -> None:
        """A scalar subquery should parse as Subquery value."""
        result = parse_sql("SELECT * FROM cards WHERE power > (SELECT AVG(power) FROM cards)")
        assert isinstance(result, Query)
        assert isinstance(result.where, Condition)
        assert isinstance(result.where.value, Subquery)
        assert result.where.operator == Operator.GT

    def test_in_subquery(self) -> None:
        """An IN subquery should parse correctly."""
        result = parse_sql("SELECT * FROM trainers WHERE name IN (SELECT pokemon FROM cards)")
        assert isinstance(result, Query)
        assert isinstance(result.where, Condition)
        assert result.where.operator == Operator.IN
        assert isinstance(result.where.value, Subquery)


class TestExecuteScalarSubquery:
    """Verify scalar subquery execution."""

    def test_greater_than_avg(self, tmp_path: Path) -> None:
        """Cards with power > average should be returned."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT name FROM cards WHERE power > (SELECT AVG(power) FROM cards)"),
            db,
        )
        names = {str(r["name"]) for r in rows}
        assert names == {"Pikachu", "Raichu"}

    def test_equal_to_max(self, tmp_path: Path) -> None:
        """Card with max power should be returned."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT name FROM cards WHERE power = (SELECT MAX(power) FROM cards)"),
            db,
        )
        assert len(rows) == ONE_ROW
        assert rows[0]["name"] == "Raichu"

    def test_equal_to_min(self, tmp_path: Path) -> None:
        """Card with min power should be returned."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT name FROM cards WHERE power = (SELECT MIN(power) FROM cards)"),
            db,
        )
        assert len(rows) == ONE_ROW
        assert rows[0]["name"] == "Squirtle"


class TestExecuteInSubquery:
    """Verify IN subquery execution."""

    def test_in_subquery(self, tmp_path: Path) -> None:
        """IN subquery should match rows with values in the subquery result."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT * FROM cards WHERE name IN (SELECT pokemon FROM trainers)"),
            db,
        )
        names = {str(r["name"]) for r in rows}
        assert names == {"Pikachu", "Squirtle"}

    def test_in_subquery_no_matches(self, tmp_path: Path) -> None:
        """IN subquery with no matches should return empty."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT * FROM cards WHERE name IN "
                "(SELECT pokemon FROM trainers WHERE name = 'Nobody')"
            ),
            db,
        )
        assert rows == []

    def test_in_subquery_with_where(self, tmp_path: Path) -> None:
        """IN subquery with inner WHERE should filter correctly."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT * FROM cards WHERE name IN "
                "(SELECT pokemon FROM trainers WHERE name = 'Ash')"
            ),
            db,
        )
        assert len(rows) == ONE_ROW
        assert rows[0]["name"] == "Pikachu"


class TestSubqueryModel:
    """Verify the Subquery dataclass."""

    def test_subquery_wraps_query(self) -> None:
        """Subquery should wrap a Query."""
        inner = Query(table="cards")
        sq = Subquery(query=inner)
        assert sq.query.table == "cards"
