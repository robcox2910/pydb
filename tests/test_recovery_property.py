"""A randomised crash test: whatever happens, recovery gives back exactly the commits.

Each seed builds a random "script" of work -- single inserts, updates and
deletes, small transactions that are committed or rolled back, and the odd
checkpoint -- plus a random moment to pull the plug. A real child process
follows the script and dies with ``os._exit`` at that moment (sometimes in
the middle of a transaction, sometimes in the middle of a checkpoint).

The parent never looks at what the child did. It works out, from the seed
alone, what the committed state *must* be (a plain dict of rows plus the
next ID), reopens the folder and checks the database agrees -- then checks
it keeps working and that reopening twice changes nothing.
"""

import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from pydb.database import Database
from pydb.record import Value
from tests.crash_helpers import (
    PROPERTY_TABLE,
    CrashPlan,
    expected_state,
    plan_for,
    property_schema,
)

_HELPER = Path(__file__).with_name("crash_helpers.py")
_CHILD_TIMEOUT = 20
_CRASH_EXIT = 1
_SEEDS = tuple(range(12))
_EXTRA_ROW: dict[str, Value] = {"name": "after", "power": -1, "day": date(2026, 12, 25)}


def _run_child(folder: Path, seed: int) -> None:
    """Run the seed's script in a real child process and check that it crashed."""
    result = subprocess.run(
        [sys.executable, str(_HELPER), "random", str(folder), str(seed)],
        check=False,
        timeout=_CHILD_TIMEOUT,
        capture_output=True,
    )
    assert result.returncode == _CRASH_EXIT, result.stderr.decode()


def _rows(db: Database) -> dict[int, dict[str, Value]]:
    """Return every row of the property table as ``{id: data}``."""
    return {r.record_id: dict(r.data) for r in db.get_table(PROPERTY_TABLE).select()}


class TestPlans:
    """The scripts themselves are deterministic and cover the tricky cases."""

    def test_same_seed_same_plan(self) -> None:
        """Build the same script twice from one seed (the child relies on it)."""
        assert plan_for(_SEEDS[-1]) == plan_for(_SEEDS[-1])

    def test_seeds_cover_every_kind_of_crash(self) -> None:
        """Across the seeds, crash between steps, mid-transaction and mid-checkpoint."""
        plans: list[CrashPlan] = [plan_for(seed) for seed in _SEEDS]
        assert any(p.crash.txn_ops is not None for p in plans)
        assert any(p.crash.checkpoint_step is not None for p in plans)
        assert any(p.crash.txn_ops is None and p.crash.checkpoint_step is None for p in plans)

    def test_seeds_cover_every_kind_of_step(self) -> None:
        """Across the seeds, see autocommits, commits, rollbacks and checkpoints."""
        steps = [step for seed in _SEEDS for step in plan_for(seed).steps]
        kinds = {step.kind for step in steps}
        assert kinds == {"auto", "txn", "checkpoint"}
        assert any(s.kind == "txn" and s.commit for s in steps)
        assert any(s.kind == "txn" and not s.commit for s in steps)
        op_kinds = {op.kind for step in steps for op in step.ops}
        assert op_kinds == {"insert", "update", "delete"}

    def test_some_seeds_checkpoint_automatically(self) -> None:
        """Use a tiny checkpoint size for some seeds and the default for others."""
        sizes = {plan_for(seed).checkpoint_bytes for seed in _SEEDS}
        assert len(sizes) > 1


class TestRecoveryProperty:
    """After any crash, the reopened database equals the committed model."""

    @pytest.mark.parametrize("seed", _SEEDS)
    def test_reopen_matches_committed_model(self, tmp_path: Path, seed: int) -> None:
        """Crash at the seed's moment, reopen, and compare with the model."""
        plan = plan_for(seed)
        rows, next_id = expected_state(plan)
        _run_child(tmp_path, seed)

        with Database(tmp_path, journal=True) as db:
            assert _rows(db) == rows
            table = db.get_table(PROPERTY_TABLE)
            assert table.next_id == next_id
            assert table.insert(_EXTRA_ROW).record_id == next_id

        with Database(tmp_path, journal=True) as again:
            assert _rows(again) == {**rows, next_id: _EXTRA_ROW}

    def test_schema_matches_the_child(self, tmp_path: Path) -> None:
        """Recover the property table with the schema the scripts use."""
        _run_child(tmp_path, _SEEDS[0])
        with Database(tmp_path, journal=True) as db:
            schema = db.get_table(PROPERTY_TABLE).schema
            assert schema.column_names == property_schema().column_names
