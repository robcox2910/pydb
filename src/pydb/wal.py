"""Write-Ahead Log (WAL) -- crash recovery for the database.

Before making any change to a table, the WAL writes a record of the
operation to a log file. If the database crashes mid-write, it reads
the log on startup and either replays committed operations or discards
uncommitted ones.

Think of it like writing each change on a sticky note before applying
it to the actual card binder.
"""

import json
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
        """Log that a transaction has been committed."""
        self._append({"type": _COMMIT, "txn": txn_id})

    def log_rollback(self, txn_id: str) -> None:
        """Log that a transaction has been rolled back."""
        self._append({"type": _ROLLBACK, "txn": txn_id})

    def _append(self, entry: dict[str, str | dict[str, str]]) -> None:
        """Append a JSON entry to the log file."""
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    def read_entries(self) -> list[dict[str, str | dict[str, str]]]:
        """Read all log entries from the WAL file.

        Returns:
            A list of log entry dicts, in order.

        """
        if not self._log_path.exists():
            return []
        entries: list[dict[str, str | dict[str, str]]] = []
        with self._log_path.open(encoding="utf-8") as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if stripped:
                    try:
                        entries.append(json.loads(stripped))
                    except json.JSONDecodeError:
                        continue  # Skip corrupted entries during recovery.
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

    def clear(self) -> None:
        """Clear the WAL file (called after successful checkpoint)."""
        if self._log_path.exists():
            self._log_path.write_text("", encoding="utf-8")
