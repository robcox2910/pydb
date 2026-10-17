"""Child-process scenarios for the crash tests in ``test_durable.py``.

Each scenario opens a journalled database, does some work and then dies
with ``os._exit(1)`` -- no ``save()``, no ``close()``, no clean-up -- just
like a program whose power was cut. The parent test reopens the folder
and checks what survived.

Run as ``python tests/crash_helpers.py <scenario> <folder> [count]``.
"""

import os
import sys
from pathlib import Path

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType

_ARGS_WITH_COUNT = 4
_DEFAULT_COUNT = 10


def _cards_schema() -> Schema:
    """Return the schema the crash scenarios use."""
    return Schema(
        columns=[
            Column("name", DataType.TEXT),
            Column("power", DataType.INTEGER),
        ]
    )


def commit_rows(folder: Path, count: int) -> None:
    """Commit *count* rows one by one, then crash."""
    db = Database(folder, journal=True)
    table = db.create_table("cards", _cards_schema())
    for number in range(count):
        table.insert({"name": f"card{number}", "power": number})
    os._exit(1)


def crash_mid_transaction(folder: Path, count: int) -> None:
    """Commit *count* rows, then crash halfway through a transaction."""
    db = Database(folder, journal=True)
    table = db.create_table("cards", _cards_schema())
    for number in range(count):
        table.insert({"name": f"card{number}", "power": number})
    txn = Transaction(db)
    cards = txn.get_table("cards")
    for number in range(count):
        cards.insert({"name": f"lost{number}", "power": -number})
    cards.delete(1)
    os._exit(1)


_SCENARIOS = {
    "commit": commit_rows,
    "mid_txn": crash_mid_transaction,
}


def main(argv: list[str]) -> None:
    """Run the scenario named on the command line."""
    count = int(argv[3]) if len(argv) >= _ARGS_WITH_COUNT else _DEFAULT_COUNT
    _SCENARIOS[argv[1]](Path(argv[2]), count)


if __name__ == "__main__":
    main(sys.argv)
