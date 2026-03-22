"""The outbox pattern -- all-or-nothing operations across DB and MQ.

When you need to save data AND send a message, the outbox pattern
ensures both happen or neither does. Think of it like buying a toy:
the payment and the receipt are recorded together in one action.
Later, a relay process delivers the receipts to the message queue.

This prevents the nightmare scenario where you save data but the
message is lost (or vice versa).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from pydb.database import Database
from pydb.errors import PyDBError
from pydb.executor import execute
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.transaction import Transaction
from pydb.types import DataType

OUTBOX_TABLE = "_outbox"
STATUS_PENDING = "pending"
STATUS_SENT = "sent"


class OutboxError(PyDBError):
    """Raise when an outbox operation fails."""


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """A message stored in the outbox, waiting to be relayed.

    Args:
        queue: The target queue name.
        body: The message body.
        status: "pending" or "sent".
        created_at: When the message was created.

    """

    queue: str
    body: str
    status: str = STATUS_PENDING
    created_at: str = ""


class Outbox:
    """Coordinate database writes with message queue delivery.

    The outbox stores messages in a database table alongside your
    data changes. Both are in the same transaction -- if one fails,
    both are rolled back. A relay process later reads pending
    messages and sends them to the actual message queue.

    Args:
        database: The PyDB database to use.

    """

    __slots__ = ("_database",)

    def __init__(self, database: Database) -> None:
        """Create an outbox backed by the given database."""
        self._database = database
        self._ensure_outbox_table()

    def _ensure_outbox_table(self) -> None:
        """Create the outbox table if it doesn't exist."""
        if OUTBOX_TABLE in self._database.table_names():
            return
        schema = Schema(
            columns=[
                Column(name="queue", data_type=DataType.TEXT, not_null=True),
                Column(name="body", data_type=DataType.TEXT, not_null=True),
                Column(name="status", data_type=DataType.TEXT, not_null=True),
                Column(name="created_at", data_type=DataType.TEXT, not_null=True),
            ]
        )
        self._database.create_table(OUTBOX_TABLE, schema)

    def execute(self, sql: str, message: dict[str, str]) -> None:
        """Execute a SQL statement and save a message atomically.

        Both the SQL and the message are committed together. If
        either fails, neither is saved.

        Args:
            sql: The SQL statement to execute.
            message: A dict with "queue" and "body" keys.

        Raises:
            OutboxError: If the operation fails.

        """
        if "queue" not in message or "body" not in message:
            msg = "Message must have 'queue' and 'body' keys"
            raise OutboxError(msg)

        txn = Transaction(self._database)
        try:
            # Execute the user's SQL.
            parsed = parse_sql(sql)
            execute(parsed, self._database)

            # Save the message to the outbox table.
            outbox_table = self._database.get_table(OUTBOX_TABLE)
            outbox_table.insert(
                {
                    "queue": message["queue"],
                    "body": message["body"],
                    "status": STATUS_PENDING,
                    "created_at": datetime.now(tz=UTC).isoformat(),
                }
            )

            txn.commit()
        except Exception as exc:
            txn.rollback()
            msg = f"Outbox operation failed: {exc}"
            raise OutboxError(msg) from exc

    def execute_multi(self, statements: list[str], message: dict[str, str]) -> None:
        """Execute multiple SQL statements and save a message atomically.

        All statements and the message are committed together.

        Args:
            statements: SQL statements to execute.
            message: A dict with "queue" and "body" keys.

        Raises:
            OutboxError: If any operation fails.

        """
        if "queue" not in message or "body" not in message:
            msg = "Message must have 'queue' and 'body' keys"
            raise OutboxError(msg)

        txn = Transaction(self._database)
        try:
            for sql in statements:
                parsed = parse_sql(sql)
                execute(parsed, self._database)

            outbox_table = self._database.get_table(OUTBOX_TABLE)
            outbox_table.insert(
                {
                    "queue": message["queue"],
                    "body": message["body"],
                    "status": STATUS_PENDING,
                    "created_at": datetime.now(tz=UTC).isoformat(),
                }
            )

            txn.commit()
        except Exception as exc:
            txn.rollback()
            msg = f"Outbox operation failed: {exc}"
            raise OutboxError(msg) from exc

    def pending_messages(self) -> list[OutboxMessage]:
        """Return all pending (unsent) messages.

        Returns:
            A list of OutboxMessage objects waiting to be relayed.

        """
        sql = f"SELECT * FROM {OUTBOX_TABLE} WHERE status = '{STATUS_PENDING}'"
        results = execute(parse_sql(sql), self._database)
        return [
            OutboxMessage(
                queue=str(row["queue"]),
                body=str(row["body"]),
                status=str(row["status"]),
                created_at=str(row["created_at"]),
            )
            for row in results
        ]

    def relay(self, put_fn: Callable[[str, str], None]) -> int:
        """Relay pending messages to a message queue.

        Reads all pending messages, calls put_fn for each, and marks
        them as sent. Safe to call multiple times (idempotent -- sent
        messages are skipped).

        Args:
            put_fn: A callable that takes (queue_name, message_body)
                    and delivers the message.

        Returns:
            The number of messages relayed.

        """
        pending = self.pending_messages()
        count = 0

        for msg in pending:
            try:
                put_fn(msg.queue, msg.body)
                # Mark as sent.
                update_sql = (
                    f"UPDATE {OUTBOX_TABLE} SET status = '{STATUS_SENT}' "
                    f"WHERE queue = '{msg.queue}' AND body = '{msg.body}' "
                    f"AND status = '{STATUS_PENDING}'"
                )
                execute(parse_sql(update_sql), self._database)
                count += 1
            except Exception:  # noqa: BLE001, S112
                continue  # Skip failed messages, try next.

        return count

    def sent_count(self) -> int:
        """Return the number of messages that have been successfully sent."""
        count_sql = f"SELECT COUNT(*) FROM {OUTBOX_TABLE} WHERE status = '{STATUS_SENT}'"
        results = execute(parse_sql(count_sql), self._database)
        return int(results[0]["COUNT(*)"])
