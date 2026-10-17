"""Check that every example imports cleanly, exposes ``main()`` and runs."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.py"))
CRASH_EXAMPLE = EXAMPLES_DIR / "crash_and_recover.py"
BROKEN_CHILD = "import sys; sys.stderr.write('ModuleNotFoundError: no pydb here\\n'); sys.exit(1)"
KEY_LINES: dict[str, tuple[str, ...]] = {
    "crash_and_recover": (
        "The helper program crashed (exit code 9) in the middle of job 4.",
        "  job 1: nightly-backup ran on 2026-10-15, started 2026-10-15T02:00:00Z",
        "  job 3: send-report ran on 2026-10-17, started 2026-10-17T07:30:00Z",
        "job 4: never committed, so it's gone",
        "The next new job run gets ID 4.",
    ),
    "run_dates": (
        "Runs on 2026-10-16: 2",
        "Runs from 2026-10-15 to 2026-10-16: 4",
        "Runs that started between 06:00 and 11:00 UTC on 2026-10-17: 2",
        "The 12:15 run in Paris (+02:00) is stored in UTC as 2026-10-17T10:15:00Z",
        "First run date: 2026-10-15, latest start: 2026-10-17T22:45:00Z",
    ),
}


def load_example(path: Path) -> ModuleType:
    """Import the example at ``path`` (examples/ is not a package)."""
    spec = importlib.util.spec_from_file_location(f"example_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestExamples:
    """Test the runnable examples."""

    def test_examples_exist(self) -> None:
        """Find every example file."""
        assert {p.stem for p in EXAMPLE_FILES} == set(KEY_LINES)

    @pytest.mark.parametrize("path", EXAMPLE_FILES, ids=lambda p: p.stem)
    def test_exposes_main(self, path: Path) -> None:
        """Import each example and expect a callable ``main``."""
        assert callable(getattr(load_example(path), "main", None))

    @pytest.mark.parametrize("path", EXAMPLE_FILES, ids=lambda p: p.stem)
    def test_main_prints_key_lines(self, path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Run each example to completion and see its key output lines, in order."""
        load_example(path).main()
        lines = capsys.readouterr().out.splitlines()
        positions = [lines.index(line) for line in KEY_LINES[path.stem]]
        assert positions == sorted(positions)


class TestCrashAndRecover:
    """The crash demo only tells its story when the helper really crashed."""

    def test_unexpected_child_failure_is_reported(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Say the helper failed, show what it said, and skip the recovery story."""
        example = load_example(CRASH_EXAMPLE)

        def broken_command(_folder: Path) -> list[str]:
            """Return a helper command that fails the ordinary way (exit code 1)."""
            return [sys.executable, "-c", BROKEN_CHILD]

        monkeypatch.setattr(example, "child_command", broken_command)
        example.main()
        out = capsys.readouterr().out
        assert "stopped unexpectedly (exit code 1)" in out
        assert "  ModuleNotFoundError: no pydb here" in out.splitlines()
        assert "crashed (exit code" not in out
        assert "Reopening" not in out

    def test_a_surviving_job_4_is_called_out(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Flag it loudly if the unfinished job somehow survived."""
        example = load_example(CRASH_EXAMPLE)
        with example.Database(tmp_path, journal=True) as db:
            runs = db.create_table(example.TABLE, example.runs_schema())
            for job, run_date, started in (*example.COMMITTED_RUNS, example.UNFINISHED_RUN):
                runs.insert({"job": job, "run_date": run_date, "started": started})
        example.show_recovery(tmp_path)
        assert "job 4: survived -- that should never happen!" in capsys.readouterr().out
