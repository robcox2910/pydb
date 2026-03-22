"""Tests for the outbox pattern -- all-or-nothing DB + MQ operations.

The outbox ensures data saves and message sends happen together.
If one fails, both are rolled back. These tests verify that
guarantee.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import execute
from pydb.outbox import Outbox, OutboxError
from pydb.sql_parser import parse_sql

ONE_MESSAGE = 1
ZERO_MESSAGES = 0
BALANCE_90 = 90


def _make_db(tmp_path: Path) -> Database:
    """Create a database with an accounts table."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE accounts (name TEXT, balance INTEGER)"), db)
    execute(parse_sql("INSERT INTO accounts VALUES ('Alice', 100)"), db)
    execute(parse_sql("INSERT INTO accounts VALUES ('Bob', 50)"), db)
    return db


class TestOutboxExecute:
    """Verify that SQL and messages are saved atomically."""

    def test_both_succeed(self, tmp_path: Path) -> None:
        """When SQL succeeds, both data and message are saved."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        outbox.execute(
            sql="UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
            message={"queue": "receipts", "body": "Alice paid $10"},
        )

        # Data was saved.
        rows = execute(parse_sql("SELECT balance FROM accounts WHERE name = 'Alice'"), db)
        assert rows[0]["balance"] == BALANCE_90

        # Message was saved.
        pending = outbox.pending_messages()
        assert len(pending) == ONE_MESSAGE
        assert pending[0].queue == "receipts"
        assert pending[0].body == "Alice paid $10"

    def test_sql_fails_rolls_back_message(self, tmp_path: Path) -> None:
        """When SQL fails, the message is NOT saved."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        with pytest.raises(OutboxError):
            outbox.execute(
                sql="UPDATE nonexistent SET x = 1",
                message={"queue": "receipts", "body": "Should not be saved"},
            )

        # No message should be pending.
        assert len(outbox.pending_messages()) == ZERO_MESSAGES

    def test_missing_queue_key_raises(self, tmp_path: Path) -> None:
        """A message without 'queue' key should raise OutboxError."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        with pytest.raises(OutboxError, match="queue"):
            outbox.execute(
                sql="SELECT 1",
                message={"body": "no queue"},
            )

    def test_missing_body_key_raises(self, tmp_path: Path) -> None:
        """A message without 'body' key should raise OutboxError."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        with pytest.raises(OutboxError, match="body"):
            outbox.execute(
                sql="SELECT 1",
                message={"queue": "test"},
            )


class TestOutboxMulti:
    """Verify execute_multi with multiple SQL statements."""

    def test_multi_all_succeed(self, tmp_path: Path) -> None:
        """Multiple SQL statements and message all succeed together."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        outbox.execute_multi(
            statements=[
                "UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
                "UPDATE accounts SET balance = 60 WHERE name = 'Bob'",
            ],
            message={"queue": "transfers", "body": "Alice sent $10 to Bob"},
        )

        alice = execute(parse_sql("SELECT balance FROM accounts WHERE name = 'Alice'"), db)
        bob = execute(parse_sql("SELECT balance FROM accounts WHERE name = 'Bob'"), db)
        assert alice[0]["balance"] == BALANCE_90
        assert bob[0]["balance"] == 60  # noqa: PLR2004

        assert len(outbox.pending_messages()) == ONE_MESSAGE

    def test_multi_one_fails_rolls_back_all(self, tmp_path: Path) -> None:
        """If any SQL fails, everything is rolled back."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        with pytest.raises(OutboxError):
            outbox.execute_multi(
                statements=[
                    "UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
                    "UPDATE nonexistent SET x = 1",  # This will fail.
                ],
                message={"queue": "transfers", "body": "Should not be saved"},
            )

        # Alice's balance should be unchanged (rolled back).
        alice = execute(parse_sql("SELECT balance FROM accounts WHERE name = 'Alice'"), db)
        assert alice[0]["balance"] == 100  # noqa: PLR2004

        # No message should be pending.
        assert len(outbox.pending_messages()) == ZERO_MESSAGES


class TestOutboxRelay:
    """Verify the relay delivers messages and marks them sent."""

    def test_relay_delivers_messages(self, tmp_path: Path) -> None:
        """The relay should call put_fn for each pending message."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)
        delivered: list[tuple[str, str]] = []

        outbox.execute(
            sql="UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
            message={"queue": "receipts", "body": "Alice paid $10"},
        )

        def mock_put(queue: str, body: str) -> None:
            delivered.append((queue, body))

        count = outbox.relay(mock_put)
        assert count == ONE_MESSAGE
        assert delivered == [("receipts", "Alice paid $10")]

    def test_relay_marks_sent(self, tmp_path: Path) -> None:
        """After relay, messages should be marked as sent."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        outbox.execute(
            sql="UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
            message={"queue": "receipts", "body": "Alice paid $10"},
        )

        outbox.relay(lambda _q, _b: None)

        # No pending messages remain.
        assert len(outbox.pending_messages()) == ZERO_MESSAGES
        # One sent message.
        assert outbox.sent_count() == ONE_MESSAGE

    def test_relay_is_idempotent(self, tmp_path: Path) -> None:
        """Running relay twice should not deliver messages twice."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)
        call_count = 0

        outbox.execute(
            sql="UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
            message={"queue": "receipts", "body": "Alice paid $10"},
        )

        def counting_put(_queue: str, _body: str) -> None:
            nonlocal call_count
            call_count += 1

        outbox.relay(counting_put)
        outbox.relay(counting_put)  # Second relay should find nothing pending.

        assert call_count == ONE_MESSAGE

    def test_relay_skips_failed_delivery(self, tmp_path: Path) -> None:
        """If put_fn raises, the message stays pending for retry."""
        db = _make_db(tmp_path)
        outbox = Outbox(db)

        outbox.execute(
            sql="UPDATE accounts SET balance = 90 WHERE name = 'Alice'",
            message={"queue": "receipts", "body": "Alice paid $10"},
        )

        def failing_put(_queue: str, _body: str) -> None:
            msg = "Delivery failed"
            raise RuntimeError(msg)

        count = outbox.relay(failing_put)
        assert count == ZERO_MESSAGES

        # Message should still be pending.
        assert len(outbox.pending_messages()) == ONE_MESSAGE


class TestOutboxTable:
    """Verify outbox table management."""

    def test_outbox_table_created_automatically(self, tmp_path: Path) -> None:
        """The outbox table should be created on first use."""
        db = _make_db(tmp_path)
        Outbox(db)
        assert "_outbox" in db.table_names()

    def test_outbox_table_not_duplicated(self, tmp_path: Path) -> None:
        """Creating multiple Outbox instances should not duplicate the table."""
        db = _make_db(tmp_path)
        Outbox(db)
        Outbox(db)  # Should not raise.
        assert db.table_names().count("_outbox") == ONE_MESSAGE
