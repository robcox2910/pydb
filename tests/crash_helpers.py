"""Child-process scenarios for the crash tests in ``test_durable.py``.

Each scenario opens a journalled database, does some work and then dies
with ``os._exit(1)`` -- no ``save()``, no ``close()``, no clean-up -- just
like a program whose power was cut. The parent test reopens the folder
and checks what survived.

Run as ``python tests/crash_helpers.py <scenario> <folder> [count]``,
``python tests/crash_helpers.py checkpoint <folder> <step> <first|second>``, or
``python tests/crash_helpers.py random <folder> <seed>``.

The ``random`` scenario is the recovery property test's workload: a seeded
random script of work (see ``plan_for``) with a random crash point. The
parent works out the committed state from the same seed (``expected_state``)
without looking at what the child did.
"""

import os
import random
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from pydb.database import Database
from pydb.record import Value
from pydb.schema import Column, Schema
from pydb.table import Table
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


def abandon(db: Database) -> None:
    """Let go of a journalled database without closing it, as if its program died.

    Nothing is checkpointed: the journal and the ``LOCK`` file are simply
    released, so the test can reopen the folder in this same process.
    """
    if db._release_store is not None:
        db._release_store()


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


PROPERTY_TABLE = "cards"
CHECKPOINT_STEPS = ("snapshots", "checkpoint_file", "rotate")
_TINY_CHECKPOINT = 1500  # bytes: small enough that automatic checkpoints happen
_DEFAULT_CHECKPOINT = 4 * 1024 * 1024
_MIN_STEPS = 12
_MAX_STEPS = 28
_MAX_TXN_OPS = 4
_CHECKPOINT_CHANCE = 0.1
_AUTO_CHANCE = 0.45
_COMMIT_CHANCE = 0.65
_FIRST_DAY = date(2026, 10, 1)
_POWER_RANGE = 200
_DAY_RANGE = 60

type Rows = dict[int, dict[str, Value]]


def property_schema() -> Schema:
    """Return the schema the random scripts use (text, number and date)."""
    return Schema(
        columns=[
            Column("name", DataType.TEXT),
            Column("power", DataType.INTEGER),
            Column("day", DataType.DATE),
        ]
    )


@dataclass(frozen=True)
class Op:
    """One row change: ``insert``, ``update`` or ``delete`` of *record_id*."""

    kind: str
    record_id: int
    values: dict[str, Value] = field(default_factory=lambda: {})


@dataclass(frozen=True)
class Step:
    """One step of a script: an autocommit op, a transaction, or a checkpoint."""

    kind: str  # "auto", "txn" or "checkpoint"
    ops: tuple[Op, ...] = ()
    commit: bool = True


@dataclass(frozen=True)
class Crash:
    """Where the plug is pulled.

    Attributes:
        after_steps: How many steps finish before the crash.
        txn_ops: If set, the next step is a transaction and the crash comes
            after this many of its ops, before it commits.
        checkpoint_step: If set, the next step is a checkpoint and the crash
            comes right after this checkpoint step.

    """

    after_steps: int
    txn_ops: int | None = None
    checkpoint_step: str | None = None


@dataclass(frozen=True)
class CrashPlan:
    """A whole random script: its steps, its crash and the database options."""

    steps: tuple[Step, ...]
    crash: Crash
    checkpoint_bytes: int


def _random_values(rng: random.Random) -> dict[str, Value]:
    """Return a random row (or the new values for an update)."""
    return {
        "name": f"card{rng.randrange(1000)}",
        "power": rng.randrange(_POWER_RANGE),
        "day": _FIRST_DAY + timedelta(days=rng.randrange(_DAY_RANGE)),
    }


def _random_op(rng: random.Random, rows: Rows, next_id: int) -> Op:
    """Pick a random op that makes sense for *rows* (insert if there are none)."""
    kind = rng.choice(("insert", "insert", "update", "delete")) if rows else "insert"
    if kind == "insert":
        return Op("insert", next_id, _random_values(rng))
    target = rng.choice(sorted(rows))
    if kind == "update":
        return Op("update", target, {"power": rng.randrange(_POWER_RANGE)})
    return Op("delete", target)


def apply_op(rows: Rows, next_id: int, op: Op) -> int:
    """Apply *op* to the model *rows* in place and return the new next ID."""
    if op.kind == "insert":
        rows[op.record_id] = dict(op.values)
        return max(next_id, op.record_id + 1)
    if op.kind == "update":
        rows[op.record_id] = {**rows[op.record_id], **op.values}
    else:
        del rows[op.record_id]
    return next_id


def _random_step(rng: random.Random, rows: Rows, next_id: int) -> Step:
    """Pick a random step that makes sense for the committed *rows*."""
    roll = rng.random()
    if roll < _CHECKPOINT_CHANCE:
        return Step("checkpoint")
    if roll < _CHECKPOINT_CHANCE + _AUTO_CHANCE:
        return Step("auto", (_random_op(rng, rows, next_id),))
    scratch, scratch_next = dict(rows), next_id
    ops: list[Op] = []
    for _ in range(rng.randint(1, _MAX_TXN_OPS)):
        op = _random_op(rng, scratch, scratch_next)
        scratch_next = apply_op(scratch, scratch_next, op)
        ops.append(op)
    return Step("txn", tuple(ops), commit=rng.random() < _COMMIT_CHANCE)


def _commit_step(rows: Rows, next_id: int, step: Step) -> int:
    """Apply a step's *committed* effect to the model and return the next ID."""
    if step.kind == "auto" or (step.kind == "txn" and step.commit):
        for op in step.ops:
            next_id = apply_op(rows, next_id, op)
    return next_id


_CRASH_KINDS = ("between", "txn", "checkpoint")


def _random_crash(rng: random.Random, steps: tuple[Step, ...], seed: int) -> Crash:
    """Pick a crash point: between steps, inside a transaction, or in a checkpoint.

    The kind of crash (and, for a checkpoint, which step) takes turns with
    the seed, so a handful of seeds is enough to cover all of them; where
    exactly it happens is random.
    """
    kind = _CRASH_KINDS[seed % len(_CRASH_KINDS)]
    places = [i for i, step in enumerate(steps) if step.kind == kind]
    if kind == "between" or not places:
        return Crash(rng.randint(0, len(steps)))
    after = rng.choice(places)
    if kind == "txn":
        return Crash(after, txn_ops=rng.randint(0, len(steps[after].ops)))
    step_name = CHECKPOINT_STEPS[seed // len(_CRASH_KINDS) % len(CHECKPOINT_STEPS)]
    return Crash(after, checkpoint_step=step_name)


def plan_for(seed: int) -> CrashPlan:
    """Build the random script for *seed* (the same seed always gives the same plan)."""
    rng = random.Random(seed)  # noqa: S311 -- seeded test data, not crypto
    rows: Rows = {}
    next_id = 1
    steps: list[Step] = []
    for _ in range(rng.randint(_MIN_STEPS, _MAX_STEPS)):
        step = _random_step(rng, rows, next_id)
        next_id = _commit_step(rows, next_id, step)
        steps.append(step)
    frozen = tuple(steps)
    size = _TINY_CHECKPOINT if seed % 2 else _DEFAULT_CHECKPOINT
    return CrashPlan(frozen, _random_crash(rng, frozen, seed), size)


def expected_state(plan: CrashPlan) -> tuple[Rows, int]:
    """Return the committed ``(rows, next_id)`` the crash must leave behind."""
    rows: Rows = {}
    next_id = 1
    for step in plan.steps[: plan.crash.after_steps]:
        next_id = _commit_step(rows, next_id, step)
    return rows, next_id


def _run_ops(table_get: Callable[[], Table], ops: Sequence[Op]) -> None:
    """Carry out *ops* on the table *table_get* returns."""
    for op in ops:
        table = table_get()
        if op.kind == "insert":
            table.insert(op.values)
        elif op.kind == "update":
            table.update(op.record_id, op.values)
        else:
            table.delete(op.record_id)


def _run_step(db: Database, step: Step) -> None:
    """Carry out one whole step of a script."""
    if step.kind == "checkpoint":
        db.checkpoint()
        return
    if step.kind == "auto":
        _run_ops(lambda: db.get_table(PROPERTY_TABLE), step.ops)
        return
    txn = Transaction(db)
    _run_ops(lambda: txn.get_table(PROPERTY_TABLE), step.ops)
    if step.commit:
        txn.commit()
    else:
        txn.rollback()


def crash_randomly(folder: Path, seed: int) -> None:
    """Follow the seed's script and crash where its plan says."""
    plan = plan_for(seed)
    db = Database(folder, journal=True, checkpoint_bytes=plan.checkpoint_bytes)
    db.create_table(PROPERTY_TABLE, property_schema())
    for step in plan.steps[: plan.crash.after_steps]:
        _run_step(db, step)
    crash = plan.crash
    if crash.txn_ops is not None:
        txn = Transaction(db)
        upcoming = plan.steps[crash.after_steps].ops[: crash.txn_ops]
        _run_ops(lambda: txn.get_table(PROPERTY_TABLE), upcoming)
    if crash.checkpoint_step is not None:
        db._crash_after = crash.checkpoint_step  # the private test hook
        # If an automatic checkpoint already wrote everything, this one has
        # nothing to do and returns -- then we simply crash just after it.
        db.checkpoint()
    os._exit(1)


_SCENARIOS = {
    "commit": commit_rows,
    "mid_txn": crash_mid_transaction,
}


def main(argv: list[str]) -> None:
    """Run the scenario named on the command line."""
    if argv[1] == "random":
        crash_randomly(Path(argv[2]), int(argv[3]))
        return
    if argv[1] == "checkpoint":
        crash_during_checkpoint(Path(argv[2]), argv[3], argv[4])
        return
    count = int(argv[3]) if len(argv) >= _ARGS_WITH_COUNT else _DEFAULT_COUNT
    _SCENARIOS[argv[1]](Path(argv[2]), count)


if __name__ == "__main__":
    main(sys.argv)
