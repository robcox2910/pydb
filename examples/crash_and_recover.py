"""Pull the plug on a journalled database and watch it recover.

A helper program (a real child process) opens a journalled database,
records three job runs -- each one committed, so each one is written in
the journal "diary" first -- then starts a fourth inside a transaction
and dies with ``os._exit`` before committing, just like a power cut.

This program then checks the helper really crashed where planned (it
exits with an unusual code, 9, because an ordinary error exits with 1),
reopens the same folder, and shows that recovery read the diary, put back
every committed run, and left out the one that was never finished. If the
helper failed some other way, it says so and shows the helper's last
error lines instead.

Run it with ``uv run python examples/crash_and_recover.py``.
"""

import os
import subprocess
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from pydb.database import Database
from pydb.record import Value
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType, to_iso

TABLE = "job_runs"
CHILD_TIMEOUT = 20
CRASH_EXIT_CODE = 9  # unusual on purpose: a program that hits an error exits with 1
ERROR_LINES = 5
COMMITTED_RUNS = (
    ("nightly-backup", date(2026, 10, 15), datetime(2026, 10, 15, 2, 0, tzinfo=UTC)),
    ("tidy-logs", date(2026, 10, 16), datetime(2026, 10, 16, 3, 15, tzinfo=UTC)),
    ("send-report", date(2026, 10, 17), datetime(2026, 10, 17, 7, 30, tzinfo=UTC)),
)
UNFINISHED_RUN = ("rebuild-index", date(2026, 10, 17), datetime(2026, 10, 17, 8, 0, tzinfo=UTC))


def runs_schema() -> Schema:
    """Return the job-run table's columns: a name, a run date and a start time."""
    return Schema(
        columns=[
            Column("job", DataType.TEXT),
            Column("run_date", DataType.DATE),
            Column("started", DataType.TIMESTAMP),
        ]
    )


def show(value: Value) -> str:
    """Return *value* as text, with dates and timestamps in their ISO form."""
    return to_iso(value) if isinstance(value, date) else str(value)


def child(folder: Path) -> None:
    """Commit three job runs, start a fourth, then crash without warning."""
    db = Database(folder, journal=True)
    runs = db.create_table(TABLE, runs_schema())
    for job, run_date, started in COMMITTED_RUNS:
        runs.insert({"job": job, "run_date": run_date, "started": started})
    txn = Transaction(db)
    job, run_date, started = UNFINISHED_RUN
    txn.get_table(TABLE).insert({"job": job, "run_date": run_date, "started": started})
    os._exit(CRASH_EXIT_CODE)  # the power cut: no commit, no close, no clean-up


def child_command(folder: Path) -> list[str]:
    """Return the command that runs the crashing helper on *folder*."""
    return [sys.executable, __file__, "child", str(folder)]


def show_recovery(folder: Path) -> None:
    """Reopen *folder* and print every job run that recovery brought back."""
    print("Reopening the folder -- recovery reads the journal diary:")
    with Database(folder, journal=True) as db:
        runs = db.get_table(TABLE)
        records = runs.select()
        for record in records:
            print(
                f"  job {record.record_id}: {record['job']} ran on {show(record['run_date'])}, "
                f"started {show(record['started'])}"
            )
        if all(record["job"] != UNFINISHED_RUN[0] for record in records):
            print("job 4: never committed, so it's gone")
        else:
            print("job 4: survived -- that should never happen!")
        print(f"The next new job run gets ID {runs.next_id}.")


def main() -> None:
    """Run the crashing helper, then reopen its folder and show what survived."""
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp) / "jobs-db"
        print("Starting a helper program that writes job runs to a journalled database...")
        result = subprocess.run(
            child_command(folder),
            check=False,
            timeout=CHILD_TIMEOUT,
            capture_output=True,
            text=True,
        )
        if result.returncode != CRASH_EXIT_CODE:
            print(
                f"The helper program stopped unexpectedly (exit code {result.returncode}) "
                "instead of crashing in the middle of job 4, so there is nothing to recover."
            )
            for line in result.stderr.strip().splitlines()[-ERROR_LINES:]:
                print(f"  {line}")
            return
        print(f"The helper program crashed (exit code {result.returncode}) in the middle of job 4.")
        print()
        show_recovery(folder)


if __name__ == "__main__":
    if sys.argv[1:2] == ["child"]:
        child(Path(sys.argv[2]))
    else:
        main()
