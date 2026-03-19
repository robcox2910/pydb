"""Tests for column constraints (PRIMARY KEY, NOT NULL, UNIQUE).

Constraints are the rules of the collection -- they keep data honest
by rejecting values that break the rules.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.schema import Column
from pydb.sql_parser import parse_sql
from pydb.statements import CreateTableStatement
from pydb.types import DataType

ONE_ROW = 1


class TestParseConstraints:
    """Verify constraint parsing in CREATE TABLE."""

    def test_primary_key(self) -> None:
        """PRIMARY KEY should be parsed."""
        stmt = parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].primary_key
        assert stmt.columns[0].not_null

    def test_not_null(self) -> None:
        """NOT NULL should be parsed."""
        stmt = parse_sql("CREATE TABLE t (name TEXT NOT NULL)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].not_null
        assert not stmt.columns[0].primary_key

    def test_unique(self) -> None:
        """UNIQUE should be parsed."""
        stmt = parse_sql("CREATE TABLE t (email TEXT UNIQUE)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].unique

    def test_combined_constraints(self) -> None:
        """Multiple constraints on one column should all be parsed."""
        stmt = parse_sql("CREATE TABLE t (email TEXT NOT NULL UNIQUE)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].not_null
        assert stmt.columns[0].unique

    def test_mixed_columns(self) -> None:
        """Some columns with constraints, some without."""
        stmt = parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, bio TEXT)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].primary_key
        assert stmt.columns[1].not_null
        assert not stmt.columns[2].not_null
        assert not stmt.columns[2].unique


class TestPrimaryKey:
    """Verify PRIMARY KEY enforcement."""

    def test_rejects_duplicate(self, tmp_path: Path) -> None:
        """Inserting a duplicate primary key should fail."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1, 'Alice')"), db)
        with pytest.raises(QueryError, match="PRIMARY KEY"):
            execute(parse_sql("INSERT INTO t VALUES (1, 'Bob')"), db)

    def test_allows_different_values(self, tmp_path: Path) -> None:
        """Different primary key values should succeed."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1, 'Alice')"), db)
        execute(parse_sql("INSERT INTO t VALUES (2, 'Bob')"), db)
        rows = execute(parse_sql("SELECT * FROM t"), db)
        assert len(rows) == 2  # noqa: PLR2004

    def test_update_preserves_own_pk(self, tmp_path: Path) -> None:
        """Updating a row without changing the PK should succeed."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES (1, 'Alice')"), db)
        execute(parse_sql("UPDATE t SET name = 'Alicia' WHERE id = 1"), db)
        rows = execute(parse_sql("SELECT * FROM t WHERE id = 1"), db)
        assert rows[0]["name"] == "Alicia"


class TestNotNull:
    """Verify NOT NULL enforcement."""

    def test_column_model(self) -> None:
        """Column with not_null=True should store the flag."""
        col = Column(name="name", data_type=DataType.TEXT, not_null=True)
        assert col.not_null


class TestUnique:
    """Verify UNIQUE enforcement."""

    def test_rejects_duplicate(self, tmp_path: Path) -> None:
        """Inserting a duplicate unique value should fail."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (email TEXT UNIQUE, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES ('a@b.com', 'Alice')"), db)
        with pytest.raises(QueryError, match="UNIQUE"):
            execute(parse_sql("INSERT INTO t VALUES ('a@b.com', 'Bob')"), db)

    def test_allows_different_values(self, tmp_path: Path) -> None:
        """Different unique values should succeed."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (email TEXT UNIQUE, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES ('a@b.com', 'Alice')"), db)
        execute(parse_sql("INSERT INTO t VALUES ('b@b.com', 'Bob')"), db)
        rows = execute(parse_sql("SELECT * FROM t"), db)
        assert len(rows) == 2  # noqa: PLR2004

    def test_update_rejects_duplicate(self, tmp_path: Path) -> None:
        """Updating to a duplicate unique value should fail."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (email TEXT UNIQUE, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES ('a@b.com', 'Alice')"), db)
        execute(parse_sql("INSERT INTO t VALUES ('b@b.com', 'Bob')"), db)
        with pytest.raises(QueryError, match="UNIQUE"):
            execute(parse_sql("UPDATE t SET email = 'a@b.com' WHERE name = 'Bob'"), db)

    def test_update_preserves_own_value(self, tmp_path: Path) -> None:
        """Updating a row without changing the unique column should succeed."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE t (email TEXT UNIQUE, name TEXT)"), db)
        execute(parse_sql("INSERT INTO t VALUES ('a@b.com', 'Alice')"), db)
        execute(parse_sql("UPDATE t SET name = 'Alicia' WHERE email = 'a@b.com'"), db)
        rows = execute(parse_sql("SELECT * FROM t"), db)
        assert rows[0]["name"] == "Alicia"


class TestEndToEndConstraints:
    """Verify constraints work in a full SQL workflow."""

    def test_full_workflow(self, tmp_path: Path) -> None:
        """Create a table with constraints and verify enforcement."""
        db = Database(path=tmp_path)
        execute(
            parse_sql(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT NOT NULL)"
            ),
            db,
        )
        execute(parse_sql("INSERT INTO users VALUES (1, 'alice@test.com', 'Alice')"), db)
        execute(parse_sql("INSERT INTO users VALUES (2, 'bob@test.com', 'Bob')"), db)

        # Verify data.
        rows = execute(parse_sql("SELECT * FROM users ORDER BY id"), db)
        assert len(rows) == 2  # noqa: PLR2004
        assert rows[0]["name"] == "Alice"

        # Verify PK uniqueness.
        with pytest.raises(QueryError, match="PRIMARY KEY"):
            execute(parse_sql("INSERT INTO users VALUES (1, 'new@test.com', 'New')"), db)

        # Verify email uniqueness.
        with pytest.raises(QueryError, match="UNIQUE"):
            execute(parse_sql("INSERT INTO users VALUES (3, 'alice@test.com', 'Clone')"), db)
