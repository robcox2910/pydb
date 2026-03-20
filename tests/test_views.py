"""Tests for views (saved queries as virtual tables).

A view is like a label on the binder -- it doesn't store data, it
re-runs a saved query every time you access it.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import Query
from pydb.sql_parser import parse_sql
from pydb.statements import CreateViewStatement, DropViewStatement

ONE_ROW = 1
TWO_ROWS = 2
THREE_ROWS = 3


def _make_db(tmp_path: Path) -> Database:
    """Create a database with a cards table."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, type TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 'Electric', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Raichu', 'Electric', 60)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 'Fire', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 'Water', 48)"), db)
    return db


class TestParseViews:
    """Verify CREATE VIEW and DROP VIEW parsing."""

    def test_create_view(self) -> None:
        """CREATE VIEW should parse correctly."""
        stmt = parse_sql("CREATE VIEW strong AS SELECT name, power FROM cards WHERE power > 50")
        assert isinstance(stmt, CreateViewStatement)
        assert stmt.name == "strong"
        assert isinstance(stmt.query, Query)
        assert stmt.query.table == "cards"

    def test_drop_view(self) -> None:
        """DROP VIEW should parse correctly."""
        stmt = parse_sql("DROP VIEW strong")
        assert isinstance(stmt, DropViewStatement)
        assert stmt.name == "strong"


class TestExecuteViews:
    """Verify view creation, querying, and dropping."""

    def test_create_and_query_view(self, tmp_path: Path) -> None:
        """A view should return the results of its stored query."""
        db = _make_db(tmp_path)
        execute(
            parse_sql("CREATE VIEW strong AS SELECT name, power FROM cards WHERE power > 50"),
            db,
        )
        rows = execute(parse_sql("SELECT * FROM strong"), db)
        assert len(rows) == THREE_ROWS
        names = {str(r["name"]) for r in rows}
        assert "Pikachu" in names
        assert "Squirtle" not in names

    def test_view_reflects_data_changes(self, tmp_path: Path) -> None:
        """A view should reflect data changes (it re-runs the query)."""
        db = _make_db(tmp_path)
        execute(
            parse_sql("CREATE VIEW strong AS SELECT name FROM cards WHERE power > 50"),
            db,
        )
        execute(parse_sql("INSERT INTO cards VALUES ('Jolteon', 'Electric', 65)"), db)
        rows = execute(parse_sql("SELECT * FROM strong"), db)
        names = {str(r["name"]) for r in rows}
        assert "Jolteon" in names

    def test_drop_view(self, tmp_path: Path) -> None:
        """Dropping a view should make it no longer queryable."""
        db = _make_db(tmp_path)
        execute(
            parse_sql("CREATE VIEW strong AS SELECT name FROM cards WHERE power > 50"),
            db,
        )
        execute(parse_sql("DROP VIEW strong"), db)
        with pytest.raises(QueryError):
            execute(parse_sql("SELECT * FROM strong"), db)

    def test_duplicate_view_raises(self, tmp_path: Path) -> None:
        """Creating a view with an existing name should raise."""
        db = _make_db(tmp_path)
        execute(
            parse_sql("CREATE VIEW strong AS SELECT name FROM cards WHERE power > 50"),
            db,
        )
        with pytest.raises(QueryError, match="CREATE VIEW failed"):
            execute(
                parse_sql("CREATE VIEW strong AS SELECT name FROM cards"),
                db,
            )

    def test_drop_nonexistent_view_raises(self, tmp_path: Path) -> None:
        """Dropping a non-existent view should raise."""
        db = _make_db(tmp_path)
        with pytest.raises(QueryError, match="DROP VIEW failed"):
            execute(parse_sql("DROP VIEW missing"), db)

    def test_view_names_listed(self, tmp_path: Path) -> None:
        """View names should be accessible from the database."""
        db = _make_db(tmp_path)
        execute(
            parse_sql("CREATE VIEW strong AS SELECT name FROM cards WHERE power > 50"),
            db,
        )
        assert "strong" in db.view_names()
