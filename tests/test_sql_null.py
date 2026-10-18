"""Tests for NULL in SQL: literals, IS NULL, three-valued WHERE, ORDER BY, aggregates.

``NULL = NULL`` is not TRUE -- it is UNKNOWN, because two unknown things
might or might not be the same. A WHERE clause keeps a row only when its
condition is TRUE, so UNKNOWN rows are left out.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import ExecuteResult, QueryError, execute
from pydb.planner import plan_query
from pydb.query import Condition, Not, Operator, Query
from pydb.record import Value
from pydb.sql_parser import ParseError, parse_sql

ZERO = 0
ONE_ROW = 1
TWO_ROWS = 2
THREE = 3
FOUR = 4
SEVEN = 7
TEN = 10
SUM_OF_CODES = 10
AVG_OF_CODES = 5.0


def _run(db: Database, sql: str) -> ExecuteResult:
    """Parse and execute *sql*."""
    return execute(parse_sql(sql), db)


def _db(tmp_path: Path) -> Database:
    """Return a database with a jobs table: two codes known, two unknown."""
    db = Database(tmp_path)
    _run(db, "CREATE TABLE jobs (name TEXT NOT NULL, code INTEGER, team TEXT)")
    _run(db, "INSERT INTO jobs VALUES ('a', 3, 'red')")
    _run(db, "INSERT INTO jobs VALUES ('b', NULL, 'red')")
    _run(db, "INSERT INTO jobs (name, team) VALUES ('c', NULL)")
    _run(db, "INSERT INTO jobs (name, code) VALUES ('d', 7)")
    return db


def _names(rows: ExecuteResult) -> list[Value]:
    """Return the name column of each row."""
    return [row["name"] for row in rows]


class TestParsing:
    """NULL, IS NULL, IS NOT NULL and NOT are parsed."""

    def test_null_literal_in_values(self) -> None:
        """NULL in VALUES becomes None."""
        stmt = parse_sql("INSERT INTO t VALUES (NULL, 1)")
        assert stmt.values == [None, 1]  # type: ignore[union-attr]

    def test_is_null(self) -> None:
        """``col IS NULL`` becomes an IS_NULL condition."""
        query = parse_sql("SELECT * FROM t WHERE code IS NULL")
        assert isinstance(query, Query)
        assert query.where == Condition("code", Operator.IS_NULL, None)

    def test_is_not_null(self) -> None:
        """``col IS NOT NULL`` becomes an IS_NOT_NULL condition."""
        query = parse_sql("SELECT * FROM t WHERE code IS NOT NULL")
        assert isinstance(query, Query)
        assert query.where == Condition("code", Operator.IS_NOT_NULL, None)

    def test_is_needs_null(self) -> None:
        """``IS 5`` is not valid SQL here."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * FROM t WHERE code IS 5")

    def test_not(self) -> None:
        """``NOT cond`` wraps the condition."""
        query = parse_sql("SELECT * FROM t WHERE NOT code = 3")
        assert isinstance(query, Query)
        assert query.where == Not(Condition("code", Operator.EQ, THREE))

    def test_infix_not_in_and_not_between(self) -> None:
        """``x NOT IN`` and ``x NOT BETWEEN`` mean ``NOT x IN`` / ``NOT x BETWEEN``."""
        for infix, prefix in [
            ("code NOT BETWEEN 1 AND 5", "NOT code BETWEEN 1 AND 5"),
            ("code NOT IN (SELECT code FROM t)", "NOT code IN (SELECT code FROM t)"),
        ]:
            assert parse_sql(f"SELECT * FROM t WHERE {infix}") == parse_sql(
                f"SELECT * FROM t WHERE {prefix}"
            )

    def test_not_must_be_followed_by_in_or_between(self) -> None:
        """``x NOT = 3`` is not valid."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * FROM t WHERE code NOT = 3")

    def test_parenthesised_condition(self) -> None:
        """Parentheses group conditions."""
        query = parse_sql("SELECT * FROM t WHERE NOT (code = 3 OR code = 7)")
        assert isinstance(query, Query)
        assert isinstance(query.where, Not)


class TestInsertAndUpdate:
    """NULL can be written with INSERT and UPDATE."""

    def test_insert_null_and_missing(self, tmp_path: Path) -> None:
        """Both an explicit NULL and a left-out column store None."""
        rows = _run(_db(tmp_path), "SELECT * FROM jobs")
        assert rows[1]["code"] is None
        assert rows[2]["code"] is None
        assert rows[3]["team"] is None

    def test_insert_null_into_not_null_fails(self, tmp_path: Path) -> None:
        """A NOT NULL column refuses NULL."""
        with pytest.raises(QueryError, match="cannot be NULL"):
            _run(_db(tmp_path), "INSERT INTO jobs VALUES (NULL, 1, 'x')")

    def test_update_set_null(self, tmp_path: Path) -> None:
        """UPDATE ... SET col = NULL empties the column."""
        db = _db(tmp_path)
        _run(db, "UPDATE jobs SET code = NULL WHERE name = 'a'")
        assert _run(db, "SELECT code FROM jobs WHERE name = 'a'") == [{"code": None}]


class TestWhere:
    """WHERE keeps a row only when the condition is TRUE."""

    @pytest.mark.parametrize(
        ("where", "expected"),
        [
            ("code IS NULL", ["b", "c"]),
            ("code IS NOT NULL", ["a", "d"]),
            ("code = NULL", []),
            ("code != NULL", []),
            ("code > 1", ["a", "d"]),
            ("code != 3", ["d"]),
            ("NOT code = 3", ["d"]),
            ("NOT code IS NULL", ["a", "d"]),
            ("code = 3 OR team = 'red'", ["a", "b"]),
            ("code > 1 AND team IS NULL", ["d"]),
            ("NOT (code = 3 AND team = 'red')", ["d"]),
            ("NOT (code = 3 OR team IS NULL)", []),
            ("NOT (code = 7 OR team IS NULL)", ["a"]),
            ("code BETWEEN 1 AND 5", ["a"]),
            ("NOT code BETWEEN 1 AND 5", ["d"]),
            ("code BETWEEN NULL AND 5", []),
            ("NOT code BETWEEN 5 AND NULL", ["a"]),
            ("code NOT BETWEEN 1 AND 5", ["d"]),
        ],
    )
    def test_three_valued_filters(self, tmp_path: Path, where: str, expected: list[str]) -> None:
        """Each filter keeps exactly the rows whose answer is TRUE."""
        rows = _run(_db(tmp_path), f"SELECT name FROM jobs WHERE {where}")
        assert _names(rows) == expected

    def test_in_subquery_with_null(self, tmp_path: Path) -> None:
        """``x IN`` a list with NULL is TRUE on a match and UNKNOWN otherwise."""
        db = _db(tmp_path)
        assert _names(_run(db, "SELECT name FROM jobs WHERE code IN (SELECT code FROM jobs)")) == [
            "a",
            "d",
        ]
        # NOT IN a list containing NULL is never TRUE.
        sql = "SELECT name FROM jobs WHERE NOT code IN (SELECT code FROM jobs WHERE name != 'a')"
        assert _run(db, sql) == []
        sql = "SELECT name FROM jobs WHERE code NOT IN (SELECT code FROM jobs WHERE name = 'a')"
        assert _names(_run(db, sql)) == ["d"]

    def test_scalar_subquery_returning_null(self, tmp_path: Path) -> None:
        """Comparing with a subquery whose answer is NULL keeps nothing."""
        db = _db(tmp_path)
        sql = "SELECT name FROM jobs WHERE code > (SELECT MAX(code) FROM jobs WHERE name = 'b')"
        assert _run(db, sql) == []

    def test_update_and_delete_follow_three_valued_logic(self, tmp_path: Path) -> None:
        """UPDATE and DELETE touch only rows whose condition is TRUE."""
        db = _db(tmp_path)
        assert _run(db, "UPDATE jobs SET team = 'blue' WHERE NOT code = 3") == [
            {"result": "1 row updated"}
        ]
        assert _run(db, "DELETE FROM jobs WHERE code IS NULL") == [{"result": "2 rows deleted"}]
        assert _names(_run(db, "SELECT name FROM jobs")) == ["a", "d"]

    def test_null_date_column_compares_with_null(self, tmp_path: Path) -> None:
        """A DATE column may be compared with NULL without a type error."""
        db = Database(tmp_path)
        _run(db, "CREATE TABLE e (name TEXT, day DATE)")
        _run(db, "INSERT INTO e VALUES ('x', NULL)")
        _run(db, "INSERT INTO e VALUES ('y', DATE '2026-10-18')")
        assert _names(_run(db, "SELECT name FROM e WHERE day = NULL")) == []
        assert _names(_run(db, "SELECT name FROM e WHERE day IS NULL")) == ["x"]
        rows = _run(db, "SELECT name FROM e WHERE day >= DATE '2026-01-01'")
        assert _names(rows) == ["y"]
        assert _names(_run(db, "SELECT name FROM e WHERE NOT day IS NULL")) == ["y"]

    def test_unknown_column_inside_not(self, tmp_path: Path) -> None:
        """Column checks reach inside NOT."""
        with pytest.raises(QueryError, match="Unknown column"):
            _run(_db(tmp_path), "SELECT * FROM jobs WHERE NOT nope = 1")


class TestIndexes:
    """An index is never used for ``col = NULL``."""

    def test_eq_null_with_index_finds_nothing(self, tmp_path: Path) -> None:
        """``code = NULL`` is UNKNOWN for every row, index or not."""
        db = _db(tmp_path)
        _run(db, "CREATE INDEX by_code ON jobs (code)")
        assert _run(db, "SELECT name FROM jobs WHERE code = NULL") == []
        assert _names(_run(db, "SELECT name FROM jobs WHERE code IS NULL")) == ["b", "c"]
        assert _names(_run(db, "SELECT name FROM jobs WHERE code = 7")) == ["d"]

    def test_planner_does_not_use_index_for_null(self, tmp_path: Path) -> None:
        """EXPLAIN shows a full scan for ``= NULL`` and for IS NULL."""
        db = _db(tmp_path)
        _run(db, "CREATE INDEX by_code ON jobs (code)")
        table = db.get_table("jobs")
        for sql in (
            "SELECT * FROM jobs WHERE code = NULL",
            "SELECT * FROM jobs WHERE code IS NULL",
        ):
            query = parse_sql(sql)
            assert isinstance(query, Query)
            assert not plan_query(query, table).use_index
        query = parse_sql("SELECT * FROM jobs WHERE code = 7")
        assert isinstance(query, Query)
        assert plan_query(query, table).use_index


class TestOrderBy:
    """NULLs come first ascending and last descending."""

    def test_ascending(self, tmp_path: Path) -> None:
        """ASC puts NULLs first."""
        rows = _run(_db(tmp_path), "SELECT name FROM jobs ORDER BY code")
        assert _names(rows) == ["b", "c", "a", "d"]

    def test_descending(self, tmp_path: Path) -> None:
        """DESC puts NULLs last."""
        rows = _run(_db(tmp_path), "SELECT name FROM jobs ORDER BY code DESC")
        assert _names(rows) == ["d", "a", "b", "c"]

    def test_order_by_on_aggregate_rows(self, tmp_path: Path) -> None:
        """Group rows with a NULL key sort without a TypeError."""
        rows = _run(_db(tmp_path), "SELECT team, COUNT(*) FROM jobs GROUP BY team ORDER BY team")
        assert [row["team"] for row in rows] == [None, "red"]


class TestAggregates:
    """COUNT(*) counts rows; everything else skips NULLs."""

    def test_skip_nulls(self, tmp_path: Path) -> None:
        """COUNT(col), SUM, AVG, MIN, MAX ignore NULL codes."""
        sql = "SELECT COUNT(*), COUNT(code), SUM(code), AVG(code), MIN(code), MAX(code) FROM jobs"
        row = _run(_db(tmp_path), sql)[0]
        assert row == {
            "COUNT(*)": FOUR,
            "COUNT(code)": TWO_ROWS,
            "SUM(code)": SUM_OF_CODES,
            "AVG(code)": AVG_OF_CODES,
            "MIN(code)": THREE,
            "MAX(code)": SEVEN,
        }

    def test_all_null_column(self, tmp_path: Path) -> None:
        """Over no non-NULL values the answer is NULL (COUNT is 0)."""
        sql = (
            "SELECT COUNT(*), COUNT(code), SUM(code), AVG(code), MIN(code), MAX(code) "
            "FROM jobs WHERE code IS NULL"
        )
        row = _run(_db(tmp_path), sql)[0]
        assert row == {
            "COUNT(*)": TWO_ROWS,
            "COUNT(code)": ZERO,
            "SUM(code)": None,
            "AVG(code)": None,
            "MIN(code)": None,
            "MAX(code)": None,
        }

    def test_group_by_null_is_one_group(self, tmp_path: Path) -> None:
        """All NULL teams land in one group."""
        rows = _run(_db(tmp_path), "SELECT team, COUNT(*) FROM jobs GROUP BY team")
        assert {row["team"]: row["COUNT(*)"] for row in rows} == {"red": TWO_ROWS, None: TWO_ROWS}

    def test_having_with_null_aggregate(self, tmp_path: Path) -> None:
        """HAVING follows three-valued logic too."""
        db = _db(tmp_path)
        sql = "SELECT team, SUM(code) FROM jobs GROUP BY team HAVING SUM(code) > 1"
        assert _run(db, sql) == [
            {"team": "red", "SUM(code)": THREE},
            {"team": None, "SUM(code)": SEVEN},
        ]
        _run(db, "UPDATE jobs SET code = NULL WHERE name = 'd'")
        sql = "SELECT team, SUM(code) FROM jobs GROUP BY team HAVING SUM(code) IS NULL"
        assert _run(db, sql) == [{"team": None, "SUM(code)": None}]

    def test_count_of_unknown_column(self, tmp_path: Path) -> None:
        """COUNT(nope) is an error, not a silent row count."""
        with pytest.raises(QueryError, match="Unknown column"):
            _run(_db(tmp_path), "SELECT COUNT(nope) FROM jobs")

    @pytest.mark.parametrize("func", ["COUNT", "SUM", "AVG", "MIN", "MAX"])
    def test_unknown_column_with_no_rows(self, tmp_path: Path, func: str) -> None:
        """An unknown column is an error even when no row is left to read."""
        with pytest.raises(QueryError, match="Unknown column"):
            _run(_db(tmp_path), f"SELECT {func}(nope) FROM jobs WHERE code > 100")

    def test_unknown_column_with_no_rows_after_join(self, tmp_path: Path) -> None:
        """The check also covers joined rows."""
        db = _db(tmp_path)
        _run(db, "CREATE TABLE teams (team TEXT, size INTEGER)")
        sql = "SELECT SUM(teams.nope) FROM jobs JOIN teams ON jobs.team = teams.team"
        with pytest.raises(QueryError, match="Unknown column"):
            _run(db, sql)

    def test_unknown_column_with_no_rows_in_a_view(self, tmp_path: Path) -> None:
        """...and rows from a view."""
        db = _db(tmp_path)
        _run(db, "CREATE VIEW v AS SELECT name, code FROM jobs WHERE code > 100")
        with pytest.raises(QueryError, match="Unknown column"):
            _run(db, "SELECT MAX(team) FROM v")
        assert _run(db, "SELECT MAX(code) FROM v") == [{"MAX(code)": None}]

    def test_min_max_on_empty_table(self, tmp_path: Path) -> None:
        """MIN and MAX of an empty table are NULL."""
        db = Database(tmp_path)
        _run(db, "CREATE TABLE t (val INTEGER)")
        assert _run(db, "SELECT MIN(val), MAX(val) FROM t") == [
            {"MIN(val)": None, "MAX(val)": None}
        ]


class TestJoin:
    """A JOIN condition comparing NULLs never matches."""

    def test_null_keys_do_not_join(self, tmp_path: Path) -> None:
        """Rows whose join keys are NULL are left out, even NULL with NULL."""
        db = _db(tmp_path)
        _run(db, "CREATE TABLE teams (team TEXT, size INTEGER)")
        _run(db, "INSERT INTO teams VALUES ('red', 2)")
        _run(db, "INSERT INTO teams VALUES (NULL, 9)")
        rows = _run(db, "SELECT jobs.name FROM jobs JOIN teams ON jobs.team = teams.team")
        assert [row["jobs.name"] for row in rows] == ["a", "b"]

    def test_where_on_joined_rows(self, tmp_path: Path) -> None:
        """WHERE over joined rows uses three-valued logic."""
        db = _db(tmp_path)
        _run(db, "CREATE TABLE teams (team TEXT, size INTEGER)")
        _run(db, "INSERT INTO teams VALUES ('red', 2)")
        sql = "SELECT jobs.name FROM jobs JOIN teams ON jobs.team = teams.team WHERE NOT jobs.code = 3"
        assert _run(db, sql) == []


class TestViews:
    """Views run the same three-valued filters."""

    def test_view_where_is_null(self, tmp_path: Path) -> None:
        """WHERE on a view keeps only TRUE rows."""
        db = _db(tmp_path)
        _run(db, "CREATE VIEW v AS SELECT name, code FROM jobs")
        assert _names(_run(db, "SELECT name FROM v WHERE code IS NULL")) == ["b", "c"]
        assert _names(_run(db, "SELECT name FROM v WHERE code > 1 ORDER BY code DESC")) == [
            "d",
            "a",
        ]
        assert _names(_run(db, "SELECT name FROM v WHERE NOT code = 7")) == ["a"]


class TestPythonApi:
    """Condition objects used directly also follow three-valued logic."""

    def test_condition_matches(self, tmp_path: Path) -> None:
        """``Condition.matches`` is TRUE-only; ``Not`` of UNKNOWN stays out."""
        table = _db(tmp_path).get_table("jobs")
        assert len(table.select(Condition("code", Operator.EQ, None).matches)) == ZERO
        assert len(table.select(Not(Condition("code", Operator.GT, TEN)).matches)) == TWO_ROWS
