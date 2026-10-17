"""Tests for "one SQL statement, one commit".

An ``UPDATE`` or ``DELETE`` that touches many rows is a single commit: one
journal line, one ``fsync``, one batch for the commit listeners -- and if
it fails part-way, none of its rows change. Inside an open transaction the
statement simply joins that transaction.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from pydb.changes import Change
from pydb.database import Database
from pydb.durable import JOURNAL_FILE
from pydb.executor import QueryError, execute
from pydb.journal import Journal
from pydb.sql_parser import parse_sql
from pydb.transaction import Transaction

_ROWS = 10
_NEW_POWER = 99  # the power the crash helper's UPDATE sets
_CHILD_TIMEOUT = 5
_CRASH_EXIT = 1
_HELPER = Path(__file__).with_name("crash_helpers.py")


def _sql(db: Database, sql: str) -> None:
    """Run one SQL statement."""
    execute(parse_sql(sql), db)


def _cards(db: Database, rows: int = _ROWS) -> None:
    """Create a cards table with *rows* rows (power 0, 1, 2, ...)."""
    _sql(db, "CREATE TABLE cards (name TEXT UNIQUE, power INTEGER)")
    for number in range(rows):
        _sql(db, f"INSERT INTO cards VALUES ('card{number}', {number})")


def _entries(folder: Path) -> int:
    """Return how many commits the folder's journal holds."""
    return len(Journal(folder / JOURNAL_FILE).read())


def _powers(db: Database) -> list[object]:
    """Return every card's power, in ID order."""
    return [r["power"] for r in db.get_table("cards").select()]


class TestOneJournalEntry:
    """A many-row statement is one line in the journal."""

    @pytest.mark.parametrize(
        "sql", ["UPDATE cards SET power = 7", "DELETE FROM cards WHERE power >= 0"]
    )
    def test_many_rows_one_entry(self, tmp_path: Path, sql: str) -> None:
        """Write a single journal entry for a ten-row UPDATE or DELETE."""
        db = Database(tmp_path, journal=True)
        _cards(db)
        before = _entries(tmp_path)
        _sql(db, sql)
        assert _entries(tmp_path) == before + 1
        assert len(Journal(tmp_path / JOURNAL_FILE).read()[-1][1]) == _ROWS

    def test_many_rows_one_fsync(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Force the disk once for a ten-row UPDATE, not once per row."""
        db = Database(tmp_path, journal=True)
        _cards(db)
        real_fsync = os.fsync
        calls: list[int] = []

        def counting_fsync(fd: int) -> None:
            """Count each fsync, then do it."""
            calls.append(fd)
            real_fsync(fd)

        monkeypatch.setattr(os, "fsync", counting_fsync)
        _sql(db, "UPDATE cards SET power = 7")
        assert len(calls) == 1

    def test_listener_sees_one_batch(self, tmp_path: Path) -> None:
        """Hand the commit listeners the whole statement as one batch."""
        db = Database(tmp_path)
        _cards(db)
        batches: list[list[Change]] = []
        db.add_commit_listener(batches.append)
        _sql(db, "UPDATE cards SET power = 7")
        assert [len(batch) for batch in batches] == [_ROWS]


class TestAllOrNothing:
    """A statement that fails part-way changes nothing."""

    @pytest.mark.parametrize("journal", [False, True])
    def test_failure_part_way_undoes_the_whole_statement(
        self, tmp_path: Path, *, journal: bool
    ) -> None:
        """Leave every row as it was when the second row breaks UNIQUE."""
        db = Database(tmp_path, journal=journal)
        _cards(db)
        before = [r.data for r in db.get_table("cards").select()]
        with pytest.raises(QueryError, match="UNIQUE"):
            _sql(db, "UPDATE cards SET name = 'same'")
        assert [r.data for r in db.get_table("cards").select()] == before
        assert not db.in_transaction

    def test_statement_inside_a_transaction_joins_it(self, tmp_path: Path) -> None:
        """Hold a statement's rows with the open transaction's other changes."""
        db = Database(tmp_path, journal=True)
        _cards(db)
        before = _entries(tmp_path)
        with Transaction(db):
            _sql(db, "UPDATE cards SET power = 7")
            _sql(db, "DELETE FROM cards WHERE power = 7")
        assert _entries(tmp_path) == before + 1
        txn = Transaction(db)
        _sql(db, "INSERT INTO cards VALUES ('new', 1)")
        _sql(db, "UPDATE cards SET power = 2")
        txn.rollback()
        assert db.get_table("cards").row_count == 0
        assert _entries(tmp_path) == before + 1


class TestCrashMidStatement:
    """A child process dies halfway through a ten-row UPDATE."""

    def test_no_row_of_the_statement_survives(self, tmp_path: Path) -> None:
        """Reopen with every row as it was before the statement."""
        result = subprocess.run(
            [sys.executable, str(_HELPER), "mid_statement", str(tmp_path), str(_ROWS)],
            check=False,
            timeout=_CHILD_TIMEOUT,
            capture_output=True,
            text=True,
        )
        assert result.returncode == _CRASH_EXIT, result.stderr
        with Database(tmp_path, journal=True) as db:
            assert _powers(db) == list(range(_ROWS))
            assert _NEW_POWER not in _powers(db)
