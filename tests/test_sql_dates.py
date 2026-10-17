"""Tests for DATE and TIMESTAMP in SQL.

A DATE is a calendar page and a TIMESTAMP is a stopwatch reading on a shared
clock. These tests check that SQL can store, filter, sort and summarise both.
"""

from datetime import UTC, date, datetime
from enum import IntEnum, StrEnum
from pathlib import Path

import pytest

from pydb.database import Database
from pydb.errors import SchemaError
from pydb.executor import QueryError, execute
from pydb.query import Query
from pydb.schema import Column, Schema
from pydb.sql_parser import ParseError, parse_sql
from pydb.statements import CreateTableStatement
from pydb.types import DataType, check_value

TWO = 2
THREE = 3
OCT_17 = date(2026, 10, 17)
OCT_18 = date(2026, 10, 18)
OCT_19 = date(2026, 10, 19)
NINE_THIRTY = datetime(2026, 10, 17, 9, 30, tzinfo=UTC)
TEN = datetime(2026, 10, 17, 10, 0, tzinfo=UTC)
ELEVEN = datetime(2026, 10, 17, 11, 0, tzinfo=UTC)
SEVEN = 7


def _db(tmp_path: Path) -> Database:
    """Build a database holding three job runs."""
    db = Database(tmp_path / "db")
    execute(
        parse_sql("CREATE TABLE runs (job TEXT, run_date DATE, started TIMESTAMP)"),
        db,
    )
    rows = [
        ("a", "2026-10-17", "2026-10-17T09:30:00Z"),
        ("b", "2026-10-18", "2026-10-17T11:00:00+00:00"),
        ("c", "2026-10-19", "2026-10-17T12:00:00+02:00"),
    ]
    for job, day, moment in rows:
        sql = f"INSERT INTO runs VALUES ('{job}', DATE '{day}', TIMESTAMP '{moment}')"
        execute(parse_sql(sql), db)
    return db


def _jobs(db: Database, sql: str) -> list[str]:
    """Run a SELECT and return the job names in order."""
    return [str(row["job"]) for row in execute(parse_sql(sql), db)]


class TestParsing:
    """Verify type names and typed literals parse."""

    def test_create_table_types(self) -> None:
        """DATE and TIMESTAMP are accepted column types."""
        stmt = parse_sql("CREATE TABLE t (d DATE, s timestamp)")
        assert isinstance(stmt, CreateTableStatement)
        assert [c.data_type for c in stmt.columns] == [DataType.DATE, DataType.TIMESTAMP]

    def test_bad_date_literal(self) -> None:
        """A malformed DATE literal is a ParseError with a clear message."""
        with pytest.raises(ParseError, match="DATE"):
            parse_sql("INSERT INTO t VALUES (DATE '2026-13-45')")

    def test_naive_timestamp_literal(self) -> None:
        """A TIMESTAMP literal without a timezone is a ParseError."""
        with pytest.raises(ParseError, match="TIMESTAMP"):
            parse_sql("INSERT INTO t VALUES (TIMESTAMP '2026-10-17T09:30:00')")

    def test_column_named_date_still_works(self) -> None:
        """An identifier called date is not hijacked by the literal syntax."""
        query = parse_sql("SELECT date FROM t WHERE date = 5")
        assert isinstance(query, Query)
        assert query.columns == ["date"]


class TestExecution:
    """Verify dates flow through INSERT and SELECT."""

    def test_insert_and_select(self, tmp_path: Path) -> None:
        """Inserted literals come back as date and datetime values."""
        db = _db(tmp_path)
        row = execute(parse_sql("SELECT * FROM runs WHERE job = 'a'"), db)[0]
        assert row["run_date"] == OCT_17
        assert row["started"] == NINE_THIRTY

    def test_offset_normalised_to_utc(self, tmp_path: Path) -> None:
        """A +02:00 literal is stored in UTC."""
        db = _db(tmp_path)
        row = execute(parse_sql("SELECT * FROM runs WHERE job = 'c'"), db)[0]
        assert row["started"] == TEN

    def test_equality(self, tmp_path: Path) -> None:
        """WHERE date = DATE literal matches one row."""
        db = _db(tmp_path)
        assert _jobs(db, "SELECT * FROM runs WHERE run_date = DATE '2026-10-18'") == ["b"]

    def test_less_than_and_ge(self, tmp_path: Path) -> None:
        """Range operators work on dates."""
        db = _db(tmp_path)
        assert _jobs(db, "SELECT * FROM runs WHERE run_date < DATE '2026-10-18'") == ["a"]
        assert _jobs(db, "SELECT * FROM runs WHERE run_date >= DATE '2026-10-18'") == ["b", "c"]

    def test_timestamp_compare(self, tmp_path: Path) -> None:
        """Timestamps compare on the shared UTC clock."""
        db = _db(tmp_path)
        sql = "SELECT * FROM runs WHERE started > TIMESTAMP '2026-10-17T09:45:00Z'"
        assert _jobs(db, sql) == ["b", "c"]

    def test_between(self, tmp_path: Path) -> None:
        """BETWEEN is inclusive at both ends."""
        db = _db(tmp_path)
        sql = "SELECT * FROM runs WHERE run_date BETWEEN DATE '2026-10-17' AND DATE '2026-10-18'"
        assert _jobs(db, sql) == ["a", "b"]

    def test_between_then_and(self, tmp_path: Path) -> None:
        """AND after a BETWEEN still combines conditions."""
        db = _db(tmp_path)
        sql = (
            "SELECT * FROM runs WHERE run_date BETWEEN DATE '2026-10-17' "
            "AND DATE '2026-10-19' AND job != 'b'"
        )
        assert _jobs(db, sql) == ["a", "c"]

    def test_order_by_desc(self, tmp_path: Path) -> None:
        """ORDER BY a TIMESTAMP column descending sorts newest first."""
        db = _db(tmp_path)
        assert _jobs(db, "SELECT * FROM runs ORDER BY started DESC") == ["b", "c", "a"]

    def test_min_max(self, tmp_path: Path) -> None:
        """MIN and MAX work on dates and timestamps."""
        db = _db(tmp_path)
        row = execute(parse_sql("SELECT MIN(run_date), MAX(started) FROM runs"), db)[0]
        assert row["MIN(run_date)"] == OCT_17
        assert row["MAX(started)"] == ELEVEN

    @pytest.mark.parametrize("func", ["AVG", "SUM"])
    def test_avg_sum_rejected(self, tmp_path: Path, func: str) -> None:
        """AVG and SUM over dates raise QueryError, not TypeError."""
        db = _db(tmp_path)
        with pytest.raises(QueryError, match=func):
            execute(parse_sql(f"SELECT {func}(run_date) FROM runs"), db)

    def test_update_and_delete_with_dates(self, tmp_path: Path) -> None:
        """UPDATE and DELETE can filter and set dates."""
        db = _db(tmp_path)
        execute(parse_sql("UPDATE runs SET run_date = DATE '2026-11-01' WHERE job = 'a'"), db)
        execute(parse_sql("DELETE FROM runs WHERE run_date < DATE '2026-10-19'"), db)
        assert _jobs(db, "SELECT * FROM runs") == ["a", "c"]


class TestNoImplicitConversion:
    """Verify strings never silently stand in for dates."""

    @pytest.mark.parametrize("op", ["=", "<", "!="])
    def test_date_vs_string(self, tmp_path: Path, op: str) -> None:
        """Comparing a DATE column to a bare string is a QueryError."""
        db = _db(tmp_path)
        with pytest.raises(QueryError, match="run_date"):
            execute(parse_sql(f"SELECT * FROM runs WHERE run_date {op} '2026-10-17'"), db)

    def test_timestamp_vs_date(self, tmp_path: Path) -> None:
        """A TIMESTAMP column compared to a DATE literal is a QueryError."""
        db = _db(tmp_path)
        with pytest.raises(QueryError, match="started"):
            execute(parse_sql("SELECT * FROM runs WHERE started > DATE '2026-10-17'"), db)

    def test_date_vs_timestamp(self, tmp_path: Path) -> None:
        """A DATE column compared to a TIMESTAMP literal is a QueryError."""
        db = _db(tmp_path)
        sql = "DELETE FROM runs WHERE run_date = TIMESTAMP '2026-10-17T00:00:00Z'"
        with pytest.raises(QueryError, match="run_date"):
            execute(parse_sql(sql), db)

    def test_insert_string_into_date(self, tmp_path: Path) -> None:
        """Inserting a bare string into a DATE column is rejected."""
        db = _db(tmp_path)
        with pytest.raises(QueryError):
            execute(
                parse_sql(
                    "INSERT INTO runs VALUES ('x', '2026-10-17', TIMESTAMP '2026-10-17T09:00:00Z')"
                ),
                db,
            )


class _Name(StrEnum):
    """A string enum used to test subclass acceptance."""

    A = "a"


class _Size(IntEnum):
    """An int enum used to test subclass acceptance."""

    BIG = 7


class TestCheckValueSubclasses:
    """Verify check_value keeps accepting subclasses as it always did."""

    def test_strenum_into_text(self) -> None:
        """A StrEnum member fits a TEXT column."""
        assert check_value(_Name.A, DataType.TEXT) == "a"

    def test_intenum_into_integer(self) -> None:
        """An IntEnum member fits an INTEGER column."""
        assert check_value(_Size.BIG, DataType.INTEGER) == SEVEN

    def test_schema_accepts_enums(self) -> None:
        """Schema.validate accepts enum members as before."""
        schema = Schema([Column("n", DataType.TEXT), Column("s", DataType.INTEGER)])
        schema.validate({"n": _Name.A, "s": _Size.BIG})

    def test_bool_still_not_integer(self) -> None:
        """A bool is still not an INTEGER."""
        with pytest.raises(SchemaError):
            check_value(True, DataType.INTEGER)

    def test_datetime_still_not_date(self) -> None:
        """A datetime is still not a DATE."""
        with pytest.raises(SchemaError):
            check_value(NINE_THIRTY, DataType.DATE)

    def test_int_not_float_in_validate(self) -> None:
        """An int is still rejected by check_value for FLOAT (schema widens first)."""
        with pytest.raises(SchemaError):
            check_value(1, DataType.FLOAT)
