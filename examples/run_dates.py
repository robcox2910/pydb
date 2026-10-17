"""Ask a job-run table questions about dates and times with SQL.

A job scheduler keeps a diary of every run: which job, which calendar day
it belonged to (a ``DATE``), and the exact moment it started (a
``TIMESTAMP``). Timestamps are kept on one shared clock -- UTC -- so a run
started at 12:15 in Paris (two hours ahead) is stored as 10:15 UTC.

This example fills the table with SQL, then asks it the questions a
scheduler cares about: what ran on a day, what ran across a few days,
and what started inside a window of time.

Run it with ``uv run python examples/run_dates.py``.
"""

import tempfile
from datetime import date
from pathlib import Path

from pydb.database import Database
from pydb.executor import ExecuteResult, execute
from pydb.formatter import format_results
from pydb.record import Value
from pydb.sql_parser import parse_sql
from pydb.types import to_iso

RUNS = (
    ("nightly-backup", "2026-10-15", "2026-10-15T02:00:00Z"),
    ("tidy-logs", "2026-10-15", "2026-10-15T03:15:00Z"),
    ("nightly-backup", "2026-10-16", "2026-10-16T02:00:00Z"),
    ("send-report", "2026-10-16", "2026-10-16T07:30:00Z"),
    ("nightly-backup", "2026-10-17", "2026-10-17T02:00:00Z"),
    ("send-report", "2026-10-17", "2026-10-17T07:30:00Z"),
    ("paris-sync", "2026-10-17", "2026-10-17T12:15:00+02:00"),
    ("late-cleanup", "2026-10-17", "2026-10-17T22:45:00Z"),
)


def sql(db: Database, text: str) -> ExecuteResult:
    """Parse and run one SQL statement."""
    return execute(parse_sql(text), db)


def show(value: Value) -> str:
    """Return *value* as text, with dates and timestamps in their ISO form."""
    return to_iso(value) if isinstance(value, date) else str(value)


def main() -> None:
    """Fill a job-run table and ask it date and time questions."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Database(Path(tmp) / "runs-db")
        sql(db, "CREATE TABLE runs (job TEXT, run_date DATE, started TIMESTAMP)")
        for job, day, moment in RUNS:
            sql(db, f"INSERT INTO runs VALUES ('{job}', DATE '{day}', TIMESTAMP '{moment}')")

        one_day = sql(db, "SELECT * FROM runs WHERE run_date = DATE '2026-10-16'")
        print(f"Runs on 2026-10-16: {len(one_day)}")

        span = sql(
            db,
            "SELECT job, run_date FROM runs "
            "WHERE run_date BETWEEN DATE '2026-10-15' AND DATE '2026-10-16'",
        )
        print(f"Runs from 2026-10-15 to 2026-10-16: {len(span)}")

        window = sql(
            db,
            "SELECT job, started FROM runs "
            "WHERE started >= TIMESTAMP '2026-10-17T06:00:00Z' "
            "AND started < TIMESTAMP '2026-10-17T11:00:00Z' ORDER BY started",
        )
        print(f"Runs that started between 06:00 and 11:00 UTC on 2026-10-17: {len(window)}")
        for row in window:
            print(f"  {row['job']} at {show(row['started'])}")

        paris = sql(db, "SELECT started FROM runs WHERE job = 'paris-sync'")[0]
        print(
            "The 12:15 run in Paris (+02:00) is stored in UTC as " + show(paris["started"]),
        )

        latest = sql(db, "SELECT MIN(run_date), MAX(started) FROM runs")[0]
        print(
            f"First run date: {show(latest['MIN(run_date)'])}, "
            f"latest start: {show(latest['MAX(started)'])}"
        )

        print()
        print("Every run on 2026-10-17, newest first:")
        newest = sql(
            db,
            "SELECT job, started FROM runs WHERE run_date = DATE '2026-10-17' "
            "ORDER BY started DESC",
        )
        print(format_results(list(newest)))


if __name__ == "__main__":
    main()
