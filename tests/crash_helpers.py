"""Child-process scenarios for the crash tests in ``test_durable.py``.

Each scenario opens a journalled database, does some work and then dies
with ``os._exit(1)`` -- no ``save()``, no ``close()``, no clean-up -- just
like a program whose power was cut. The parent test reopens the folder
and checks what survived.

Run as ``python tests/crash_helpers.py <scenario> <folder> [count]``, or
``python tests/crash_helpers.py checkpoint <folder> <step> <first|second>``.
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
_NO_CRASH_EXIT = 3  # not 1, so the parent can tell it apart from the crash
_UPDATED_ID = 2
_DELETED_ID = 3
_NEW_POWER = 99
_TXN_POWER = 7


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


def checkpoint_workload(db: Database, *, prior_checkpoint: bool) -> None:
    """Do the work the checkpoint crash test checks (also run by the parent).

    With *prior_checkpoint* the work is split by one finished checkpoint, so
    the crashing checkpoint has an older one to fall back on.
    """
    cards = db.create_table("cards", _cards_schema())
    old = db.create_table("old", _cards_schema())
    for number in range(_DEFAULT_COUNT):
        cards.insert({"name": f"card{number}", "power": number})
    old.insert({"name": "gone", "power": 0})
    db.create_table("quiet", _cards_schema()).insert({"name": "still", "power": 1})
    if prior_checkpoint:
        db.checkpoint()
    cards.update(_UPDATED_ID, {"power": _NEW_POWER})
    cards.delete(_DELETED_ID)
    cards.create_index("by_power", "power")
    db.drop_table("old")
    txn = Transaction(db)
    txn.get_table("cards").insert({"name": "txn", "power": _TXN_POWER})
    txn.commit()


def crash_during_checkpoint(folder: Path, step: str, when: str) -> None:
    """Do the checkpoint workload, then crash right after checkpoint *step*."""
    db = Database(folder, journal=True)
    checkpoint_workload(db, prior_checkpoint=when == "second")
    db._crash_after = step  # the private test hook
    db.checkpoint()
    sys.stderr.write("the checkpoint did not crash\n")
    sys.exit(_NO_CRASH_EXIT)


_SCENARIOS = {
    "commit": commit_rows,
    "mid_txn": crash_mid_transaction,
}


def main(argv: list[str]) -> None:
    """Run the scenario named on the command line."""
    if argv[1] == "checkpoint":
        crash_during_checkpoint(Path(argv[2]), argv[3], argv[4])
        return
    count = int(argv[3]) if len(argv) >= _ARGS_WITH_COUNT else _DEFAULT_COUNT
    _SCENARIOS[argv[1]](Path(argv[2]), count)


if __name__ == "__main__":
    main(sys.argv)
