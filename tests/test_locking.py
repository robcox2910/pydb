"""Tests for the database lock (threads) and the LOCK file (processes).

One pen, many writers, taking turns: a ``Database`` owns one re-entrant
lock that every table it hands out shares, and a ``Transaction`` holds it
from start to finish -- so other threads wait their turn and nobody ever
reads a half-finished transaction. A journalled database also leaves a
``LOCK`` file with its process ID, so a second program can't open it.
"""

import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pydb import locking
from pydb.database import Database, DatabaseError
from pydb.durable import RecoveryError
from pydb.executor import execute
from pydb.locking import LOCK_FILE, DatabaseLockedError, LockType, lock_public_methods
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.table import Table
from pydb.transaction import Transaction, TransactionError
from pydb.types import DataType

START_BALANCE = 1000
ACCOUNTS = 2
TOTAL = START_BALANCE * ACCOUNTS
WRITERS = 4
TRANSFERS = 50
AMOUNT = 3
PAUSE = 0.0001  # seconds a transfer pauses half-way, inviting others in
PYTHON_READS = 5  # Python-API sums per SQL sum in the reader
SMALL_CHECKPOINT = 2048  # bytes: makes automatic checkpoints happen mid-test
WAIT = 5.0  # seconds a thread may wait before the test calls it stuck
SHORT_WAIT = 0.2  # seconds to wait to show that something is *still* blocked
CHILD_TIMEOUT = 30
FIRST_ID = 1
SECOND_ID = 2
POWER = 55
ROLLED_BACK = 999


def _accounts_schema() -> Schema:
    """Return a schema with a single INTEGER balance column."""
    return Schema(columns=[Column("balance", DataType.INTEGER)])


def _bank(path: Path, *, journal: bool = True) -> Database:
    """Open a database holding two accounts of START_BALANCE each."""
    db = Database(path, journal=journal, sync=False, checkpoint_bytes=SMALL_CHECKPOINT)
    if "accounts" not in db.table_names():
        table = db.create_table("accounts", _accounts_schema())
        for _ in range(ACCOUNTS):
            table.insert({"balance": START_BALANCE})
    return db


def _transfer(db: Database, source: int, target: int) -> None:
    """Move AMOUNT from *source* to *target* in one transaction."""
    with Transaction(db) as txn:
        table = txn.get_table("accounts")
        table.update(source, {"balance": _balance(table, source) - AMOUNT})
        time.sleep(PAUSE)  # let other threads run: without the lock they'd see half a transfer
        table.update(target, {"balance": _balance(table, target) + AMOUNT})


def _balance(table: Table, record_id: int) -> int:
    """Return one account's balance."""
    value = table.get(record_id)["balance"]
    assert isinstance(value, int)
    return value


def _sum(rows: Iterable[Mapping[str, Value]] | Iterable[Record]) -> int:
    """Return the total of the balance column."""
    total = 0
    for row in rows:
        value = row["balance"]
        assert isinstance(value, int)
        total += value
    return total


class TestConcurrentTransactions:
    """Review Focus 4: threads take turns, readers see whole transactions only."""

    def test_concurrent_transactions_serialise(self, tmp_path: Path) -> None:
        """4 writers x 50 transfers and a reader: no errors, the sum never changes."""
        db = _bank(tmp_path)
        bad_sums: list[int] = []
        done = threading.Event()

        def writer(number: int) -> None:
            """Run TRANSFERS transfers, odd threads one way, even the other."""
            source, target = (FIRST_ID, SECOND_ID) if number % 2 else (SECOND_ID, FIRST_ID)
            for _ in range(TRANSFERS):
                _transfer(db, source, target)

        def reader() -> int:
            """Check the sum through Python and SQL until the writers finish."""
            checks = 0
            while not done.is_set() or checks == 0:
                sums = [_sum(db.get_table("accounts").select()) for _ in range(PYTHON_READS)]
                sums.append(_sum(execute(parse_sql("SELECT balance FROM accounts"), db)))
                bad_sums.extend(s for s in sums if s != TOTAL)
                checks += 1
            return checks

        with ThreadPoolExecutor(max_workers=WRITERS + 1) as pool:
            watcher = pool.submit(reader)
            writers = [pool.submit(writer, n) for n in range(WRITERS)]
            try:
                for future in writers:
                    future.result(WAIT)  # re-raises anything a writer raised
            finally:
                done.set()
            assert watcher.result(WAIT) > 0

        assert bad_sums == []
        # Half the threads push one way and half the other, so it evens out.
        balances = [r["balance"] for r in db.get_table("accounts").select()]
        assert balances == [START_BALANCE, START_BALANCE]
        db.close()
        with Database(tmp_path, journal=True) as again:
            assert [r["balance"] for r in again.get_table("accounts").select()] == balances

    def test_reader_waits_for_open_transaction(self, tmp_path: Path) -> None:
        """A SELECT from another thread waits until the transaction commits."""
        db = _bank(tmp_path, journal=False)
        results: list[int] = []
        txn = Transaction(db)
        txn.get_table("accounts").update(FIRST_ID, {"balance": 0})

        def read() -> None:
            """Sum the balances with SQL."""
            rows = execute(parse_sql("SELECT balance FROM accounts"), db)
            results.append(_sum(rows))

        thread = threading.Thread(target=read)
        thread.start()
        thread.join(SHORT_WAIT)
        assert thread.is_alive()  # still waiting for the pen
        txn.get_table("accounts").update(SECOND_ID, {"balance": TOTAL})
        txn.commit()
        thread.join(WAIT)
        assert results == [TOTAL]

    def test_second_transaction_in_another_thread_waits(self, tmp_path: Path) -> None:
        """Another thread's Transaction waits for the first instead of raising."""
        db = _bank(tmp_path, journal=False)
        started = threading.Event()
        first = Transaction(db)

        def second() -> None:
            """Open and commit a transaction."""
            with Transaction(db) as txn:
                started.set()
                txn.get_table("accounts").update(FIRST_ID, {"balance": 0})

        thread = threading.Thread(target=second)
        thread.start()
        assert not started.wait(SHORT_WAIT)
        first.commit()
        assert started.wait(WAIT)
        thread.join(WAIT)
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == 0

    def test_held_table_reference_never_sees_rolled_back_work(self, tmp_path: Path) -> None:
        """A table fetched before a transaction shows its rollback, not its writes."""
        db = _bank(tmp_path, journal=False)
        table = db.get_table("accounts")
        txn = Transaction(db)
        txn.get_table("accounts").update(FIRST_ID, {"balance": ROLLED_BACK})
        txn.rollback()
        assert table.get(FIRST_ID)["balance"] == START_BALANCE
        assert db.get_table("accounts") is table


def _lock_is_free(lock: LockType) -> bool:
    """Return whether another thread can take *lock* right now."""
    got: list[bool] = []

    def take() -> None:
        """Try the lock briefly from a fresh thread."""
        acquired = lock.acquire(timeout=SHORT_WAIT)
        got.append(acquired)
        if acquired:
            lock.release()

    thread = threading.Thread(target=take)
    thread.start()
    thread.join(WAIT)
    return got == [True]


class TestSameThread:
    """The same thread can nest calls; only a second Transaction is refused."""

    def test_plain_calls_inside_transaction(self, tmp_path: Path) -> None:
        """Database and table calls inside a transaction don't deadlock."""
        db = _bank(tmp_path, journal=False)
        with Transaction(db), db.lock:
            db.create_table("extra", _accounts_schema()).insert({"balance": 1})
            execute(parse_sql("INSERT INTO accounts VALUES (5)"), db)
        assert db.get_table("extra").row_count == 1
        assert _lock_is_free(db.lock)

    def test_second_transaction_same_thread_raises(self, tmp_path: Path) -> None:
        """Opening a second Transaction in the same thread raises; the first survives."""
        db = _bank(tmp_path, journal=False)
        first = Transaction(db)
        with pytest.raises(TransactionError):
            Transaction(db)
        first.get_table("accounts").update(FIRST_ID, {"balance": 0})
        first.commit()
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == 0
        assert _lock_is_free(db.lock)

    def test_lock_released_after_rollback_and_failed_commit(self, tmp_path: Path) -> None:
        """Rollback and a failing commit both hand the lock back."""
        db = _bank(tmp_path, journal=False)
        txn = Transaction(db)
        txn.rollback()
        assert _lock_is_free(db.lock)

        def refuse(_changes: object) -> None:
            """Refuse every commit."""
            raise RuntimeError

        db.add_commit_listener(refuse)
        txn = Transaction(db)
        txn.get_table("accounts").update(FIRST_ID, {"balance": 0})
        with pytest.raises(RuntimeError):
            txn.commit()
        txn.rollback()  # a harmless no-op, and no double release
        assert _lock_is_free(db.lock)

    def test_property_setters_are_kept_and_locked(self) -> None:
        """``lock_public_methods`` keeps a property's setter and deleter, locked too."""

        @lock_public_methods
        class Box:
            """A tiny Lockable with a settable, deletable property."""

            def __init__(self) -> None:
                """Start empty."""
                self.lock = threading.RLock()
                self.seen: list[bool] = []
                self._value = 0

            @property
            def value(self) -> int:
                """Return the value."""
                return self._value

            @value.setter
            def value(self, new: int) -> None:
                """Set the value, noting whether the lock was held."""
                self.seen.append(self.lock._is_owned())  # type: ignore[attr-defined]
                self._value = new

            @value.deleter
            def value(self) -> None:
                """Reset the value, noting whether the lock was held."""
                self.seen.append(self.lock._is_owned())  # type: ignore[attr-defined]
                self._value = 0

        box = Box()
        box.value = POWER
        assert box.value == POWER
        del box.value
        assert box.value == 0
        assert box.seen == [True, True]

    def test_tables_share_the_database_lock(self, tmp_path: Path) -> None:
        """Adopted tables use the database's lock; a free-standing table has its own."""
        db = _bank(tmp_path, journal=False)
        assert db.get_table("accounts").lock is db.lock
        loose = Table("loose", _accounts_schema())
        assert loose.lock is not db.lock
        with loose.lock:
            assert loose.insert({"balance": 1}).record_id == FIRST_ID


def _fail_inside_transaction(db: Database) -> None:
    """Change a balance in a ``with Transaction`` block, then fail."""
    with Transaction(db) as txn:
        txn.get_table("accounts").update(FIRST_ID, {"balance": 0})
        msg = "boom"
        raise ValueError(msg)


class TestOtherThreadFinish:
    """A transaction must be finished by the thread that opened it."""

    def test_commit_or_rollback_from_another_thread_raises(self, tmp_path: Path) -> None:
        """Another thread gets TransactionError, and the owner can still finish."""
        db = _bank(tmp_path, journal=False)
        txn = Transaction(db)
        txn.get_table("accounts").update(FIRST_ID, {"balance": 0})
        with ThreadPoolExecutor(max_workers=1) as pool:
            for finish in (txn.commit, txn.rollback):
                with pytest.raises(TransactionError, match="thread"):
                    pool.submit(finish).result(WAIT)
        assert txn.is_active
        txn.commit()
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == 0
        assert _lock_is_free(db.lock)


class TestTransactionContextManager:
    """``with Transaction(db) as txn:`` commits, or rolls back and re-raises."""

    def test_commit_on_success(self, tmp_path: Path) -> None:
        """A clean block commits."""
        db = _bank(tmp_path, journal=False)
        with Transaction(db) as txn:
            txn.get_table("accounts").update(FIRST_ID, {"balance": 0})
        assert not txn.is_active
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == 0

    def test_rollback_and_reraise_on_error(self, tmp_path: Path) -> None:
        """A failing block rolls back and the error comes out."""
        db = _bank(tmp_path, journal=False)
        with pytest.raises(ValueError, match="boom"):
            _fail_inside_transaction(db)
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == START_BALANCE
        assert _lock_is_free(db.lock)

    def test_manual_commit_inside_block(self, tmp_path: Path) -> None:
        """Committing inside the block leaves nothing for the exit to do."""
        db = _bank(tmp_path, journal=False)
        with Transaction(db) as txn:
            txn.get_table("accounts").update(FIRST_ID, {"balance": 0})
            txn.commit()
        assert db.get_table("accounts").get(FIRST_ID)["balance"] == 0
        assert _lock_is_free(db.lock)


_OPEN_IN_CHILD = textwrap.dedent(
    """
    import sys
    from pydb.database import Database
    from pydb.locking import DatabaseLockedError
    try:
        Database(sys.argv[1], journal=True).close()
    except DatabaseLockedError as exc:
        print("locked:", exc)
    else:
        print("opened")
    """
)


def _open_in_child(folder: Path) -> str:
    """Try to open *folder* in another process; return what it printed."""
    result = subprocess.run(
        [sys.executable, "-c", _OPEN_IN_CHILD, str(folder)],
        check=True,
        timeout=CHILD_TIMEOUT,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _dead_pid() -> int:
    """Return the PID of a child process that has finished and been reaped."""
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait(CHILD_TIMEOUT)
    return child.pid


class TestLockFile:
    """Review Focus 5: one process per journalled folder; stale locks are taken over."""

    def test_second_process_is_refused(self, tmp_path: Path) -> None:
        """Another process opening the folder gets DatabaseLockedError."""
        with _bank(tmp_path):
            assert (tmp_path / LOCK_FILE).read_text().strip() == str(os.getpid())
            assert _open_in_child(tmp_path).startswith("locked:")
        assert not (tmp_path / LOCK_FILE).exists()
        assert _open_in_child(tmp_path) == "opened\n"

    def test_stale_lock_from_dead_pid_is_taken_over(self, tmp_path: Path) -> None:
        """A LOCK left by a process that has died is replaced."""
        _bank(tmp_path).close()
        (tmp_path / LOCK_FILE).write_text(f"{_dead_pid()}\n")
        with Database(tmp_path, journal=True) as db:
            assert db.get_table("accounts").row_count == ACCOUNTS
            assert (tmp_path / LOCK_FILE).read_text().strip() == str(os.getpid())

    def test_lock_of_living_process_we_cannot_signal(self, tmp_path: Path) -> None:
        """A PID we may not signal (PermissionError) is alive: refuse."""
        (tmp_path / LOCK_FILE).write_text("1\n")  # init/launchd: alive, not ours
        if os.getuid() == 0:
            pytest.skip("root may signal every process")
        with pytest.raises(DatabaseLockedError, match="process 1"):
            Database(tmp_path, journal=True)

    def test_lock_of_living_child_process(self, tmp_path: Path) -> None:
        """A LOCK naming a process that is still running is refused."""
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            (tmp_path / LOCK_FILE).write_text(f"{child.pid}\n")
            with pytest.raises(DatabaseLockedError, match=f"process {child.pid}"):
                Database(tmp_path, journal=True)
        finally:
            child.kill()
            child.wait(CHILD_TIMEOUT)

    def test_failed_lock_write_leaves_no_lock(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If making the LOCK durable fails, the half-made LOCK is removed."""

        def full_disk(*_args: object) -> None:
            """Fail like a full disk."""
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(locking, "sync_folder", full_disk)
        with pytest.raises(OSError, match="No space"):
            Database(tmp_path, journal=True)
        monkeypatch.undo()
        assert not (tmp_path / LOCK_FILE).exists()

    def test_forgotten_database_lets_go(self, tmp_path: Path) -> None:
        """A Database nobody can reach any more releases the folder."""
        db = _bank(tmp_path)
        del db
        with Database(tmp_path, journal=True) as again:
            assert again.get_table("accounts").row_count == ACCOUNTS

    def test_garbage_lock_is_refused_with_advice(self, tmp_path: Path) -> None:
        """An unreadable LOCK is not guessed at: the message says what to do."""
        (tmp_path / LOCK_FILE).write_text("not a pid")
        with pytest.raises(DatabaseLockedError, match="delete"):
            Database(tmp_path, journal=True)
        assert (tmp_path / LOCK_FILE).exists()

    def test_second_open_in_same_process_is_refused(self, tmp_path: Path) -> None:
        """Two Database objects on one journalled folder would clash: refuse."""
        with _bank(tmp_path), pytest.raises(DatabaseLockedError, match="this process"):
            Database(tmp_path, journal=True)

    def test_own_pid_left_over_is_taken_over(self, tmp_path: Path) -> None:
        """A LOCK with our PID that no open Database holds is stale."""
        (tmp_path / LOCK_FILE).write_text(f"{os.getpid()}\n")
        with Database(tmp_path, journal=True):
            assert (tmp_path / LOCK_FILE).exists()
        assert not (tmp_path / LOCK_FILE).exists()

    def test_failed_open_releases_lock(self, tmp_path: Path) -> None:
        """If recovery fails, the LOCK is not left behind."""
        tmp_path.joinpath("checkpoint.json").write_text('{"lsn": "x"}')
        with pytest.raises(RecoveryError):
            Database(tmp_path, journal=True)
        assert not (tmp_path / LOCK_FILE).exists()

    def test_plain_database_makes_no_lock(self, tmp_path: Path) -> None:
        """Without a journal there is no LOCK file."""
        with _bank(tmp_path, journal=False):
            assert not (tmp_path / LOCK_FILE).exists()


_GC_INSIDE_GUARD = textwrap.dedent(
    """
    import faulthandler, gc, pathlib, sys
    faulthandler.dump_traceback_later(5, exit=True)  # a deadlock fails fast
    from pydb import locking
    from pydb.database import Database
    from pydb.schema import Column, Schema
    from pydb.types import DataType
    root = pathlib.Path(sys.argv[1])
    gc.disable()
    db = Database(root / "a", journal=True)
    db.create_table("t", Schema(columns=[Column("x", DataType.INTEGER)]))
    db = None  # forgotten: only the cyclic garbage collector can free it
    real_mkdir = pathlib.Path.mkdir
    def mkdir(self, *args, **kwargs):
        if locking._open_here_guard.locked():  # collect while the guard is held
            gc.collect()
        return real_mkdir(self, *args, **kwargs)
    pathlib.Path.mkdir = mkdir
    Database(root / "b", journal=True).close()
    print("ok", (root / "a" / "LOCK").exists())
    """
)


class TestGarbageCollection:
    """A forgotten Database's finalizer may run anywhere -- even mid-acquire."""

    def test_collection_inside_the_guard_does_not_deadlock(self, tmp_path: Path) -> None:
        """Freeing a forgotten Database while the open-folder guard is held is safe."""
        result = subprocess.run(
            [sys.executable, "-c", _GC_INSIDE_GUARD, str(tmp_path)],
            check=False,
            timeout=CHILD_TIMEOUT,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout == "ok False\n"


class TestClose:
    """Closing twice is harmless; a closed journalled database refuses changes."""

    def test_close_is_idempotent(self, tmp_path: Path) -> None:
        """A second close (or ``__exit__``) does nothing."""
        db = _bank(tmp_path)
        with db:
            pass
        db.close()
        db.close()
        assert not (tmp_path / LOCK_FILE).exists()
        assert not db.journalled

    def test_close_inside_transaction_then_rollback(self, tmp_path: Path) -> None:
        """Rolling back after closing mid-transaction still undoes everything."""
        db = _bank(tmp_path)
        txn = Transaction(db)
        db.create_table("extra", _accounts_schema())
        db.close()
        txn.rollback()
        assert db.table_names() == ["accounts"]
        assert _lock_is_free(db.lock)
        with Database(tmp_path, journal=True) as again:
            assert again.table_names() == ["accounts"]

    def test_closed_database_refuses_changes(self, tmp_path: Path) -> None:
        """After close a change is undone and refused, not silently lost."""
        db = _bank(tmp_path)
        table = db.get_table("accounts")
        db.close()
        with pytest.raises(DatabaseError, match="closed"):
            table.insert({"balance": POWER})
        assert table.row_count == ACCOUNTS
        with pytest.raises(DatabaseError, match="closed"):
            db.save()
        assert table.get(FIRST_ID)["balance"] == START_BALANCE
