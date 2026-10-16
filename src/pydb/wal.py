"""Write-Ahead Log (WAL) -- crash recovery for the database.

Before making any change to a table, the WAL writes a record of the
operation to a log file. After a crash, ``recover()`` reads the log and
hands back the operations of every *committed* transaction, in order, so
they can be re-applied; anything never committed (or rolled back) is
left out.

(PyDB keeps this as a standalone teaching module -- its tables don't log
through it yet. See the README.)

Think of it like writing each change on a sticky note before applying
it to the actual card binder.
"""

import json
import os
from pathlib import Path

from pydb.errors import PyDBError


class WALError(PyDBError):
    """Raise when a WAL operation fails."""


# Log entry types.
_BEGIN = "BEGIN"
_COMMIT = "COMMIT"
_ROLLBACK = "ROLLBACK"
_OPERATION = "OP"


class WriteAheadLog:
    """Append-only log for crash recovery.

    Each transaction gets a sequence of log entries:
    BEGIN → OP → OP → ... → COMMIT (or ROLLBACK).

    On recovery, transactions without a COMMIT are discarded.

    Args:
        log_path: Path to the WAL file.

    """

    __slots__ = ("_log_path",)

    def __init__(self, log_path: Path) -> None:
        """Create or open a WAL at the given path."""
        self._log_path = log_path

    @property
    def path(self) -> Path:
        """Return the WAL file path."""
        return self._log_path

    def log_begin(self, txn_id: str) -> None:
        """Log the start of a transaction."""
        self._append({"type": _BEGIN, "txn": txn_id})

    def log_operation(self, txn_id: str, table: str, operation: str, data: dict[str, str]) -> None:
        """Log a single operation within a transaction.

        Args:
            txn_id: The transaction identifier.
            table: The table being modified.
            operation: The operation type (INSERT, UPDATE, DELETE).
            data: Key-value data describing the operation.

        """
        self._append(
            {"type": _OPERATION, "txn": txn_id, "table": table, "op": operation, "data": data}
        )

    def log_commit(self, txn_id: str) -> None:
        """Log that a transaction has been committed -- and force it to disk.

        Writing to a file normally only reaches the computer's memory; the
        disk gets it a little later. A commit is a promise ("this is saved"),
        so we call ``fsync`` to make sure it really is on the disk before we
        return.
        """
        self._append({"type": _COMMIT, "txn": txn_id}, sync=True)

    def log_rollback(self, txn_id: str) -> None:
        """Log that a transaction has been rolled back."""
        self._append({"type": _ROLLBACK, "txn": txn_id})

    def _append(self, entry: dict[str, str | dict[str, str]], *, sync: bool = False) -> None:
        """Append a JSON entry to the log file (forcing it to disk if ``sync``)."""
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
            if sync:
                f.flush()
                os.fsync(f.fileno())

    def read_entries(self) -> list[dict[str, str | dict[str, str]]]:
        """Read all log entries from the WAL file.

        Returns:
            A list of log entry dicts, in order.

        Raises:
            WALError: If an entry *before* the last line is damaged. A
                damaged last line is expected after a crash mid-write (a
                "torn" write) and is skipped; damage anywhere else means
                the log can't be trusted.

        """
        if not self._log_path.exists():
            return []
        lines = self._log_path.read_text(encoding="utf-8").splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        entries: list[dict[str, str | dict[str, str]]] = []
        for number, raw_line in enumerate(lines, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                entries.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                if number == len(lines):
                    break  # torn final write from a crash -- safe to drop
                msg = f"WAL is corrupt at line {number}"
                raise WALError(msg) from exc
        return entries

    def get_committed_txns(self) -> set[str]:
        """Return the set of transaction IDs that were committed.

        Useful for recovery: only replay operations from committed
        transactions.

        """
        entries = self.read_entries()
        committed: set[str] = set()
        for entry in entries:
            if entry.get("type") == _COMMIT:
                txn_id = entry.get("txn", "")
                if isinstance(txn_id, str):
                    committed.add(txn_id)
        return committed

    def get_operations(self, txn_id: str) -> list[dict[str, str | dict[str, str]]]:
        """Return all operations for a specific transaction.

        Args:
            txn_id: The transaction to retrieve operations for.

        Returns:
            A list of operation entries.

        """
        return [
            e for e in self.read_entries() if e.get("txn") == txn_id and e.get("type") == _OPERATION
        ]

    def recover(self) -> list[dict[str, str | dict[str, str]]]:
        """Return every committed operation, in the order it was logged.

        This is what to replay after a crash: operations from transactions
        that reached COMMIT. Transactions that were rolled back, or that
        never finished, are left out -- as if they never happened.

        Raises:
            WALError: If the log is damaged before its final line.

        """
        entries = self.read_entries()
        committed = {
            txn
            for entry in entries
            if entry.get("type") == _COMMIT and isinstance(txn := entry.get("txn"), str)
        }
        return [
            entry
            for entry in entries
            if entry.get("type") == _OPERATION and entry.get("txn") in committed
        ]

    def clear(self) -> None:
        """Clear the WAL file (called after successful checkpoint)."""
        if self._log_path.exists():
            self._log_path.write_text("", encoding="utf-8")
