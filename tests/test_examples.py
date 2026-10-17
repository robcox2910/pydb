"""Check that every example imports cleanly, exposes ``main()`` and runs."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.py"))
KEY_LINES: dict[str, tuple[str, ...]] = {
    "crash_and_recover": (
        "The helper program crashed (exit code 1) in the middle of job 4.",
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
