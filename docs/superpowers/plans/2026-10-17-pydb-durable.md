# PyDB Durability, Dates and Locking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give PyDB `DATE`/`TIMESTAMP` types, a checksummed journal that makes every commit durable with crash recovery and checkpoints, persisted index definitions and a per-database lock — opt-in via `Database(path, journal=True)`, old behaviour unchanged — with kid-friendly docs and a PyStack crash demo.

**Architecture:** Typed values flow through schema/serializer/CSV/SQL. Tables report every change to a per-database change recorder; commits (single ops or whole transactions) become one journal entry written with one fsync. Opening a journalled database loads the last checkpoint and replays newer entries; checkpoints write snapshots atomically and rotate the journal. One RLock serialises access.

**Tech Stack:** Python 3.14 stdlib only (`datetime`, `zlib`, `os`, `threading`, `subprocess` in tests), uv, ruff, pyright strict, pytest + pytest-cov + pytest-timeout.

**Spec:** `docs/superpowers/specs/2026-10-17-pydb-durable-design.md`

## Global Constraints

- Python `>=3.14`; never `TYPE_CHECKING`, never `from __future__ import annotations`.
- Google-style imperative docstrings on every public module/class/function/method and every test class/method (repo CLAUDE.md).
- `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright src tests` (strict) clean; `uv run pytest` ≥ 80 % coverage; named constants in tests; `# noqa` only where tools genuinely conflict (repo CLAUDE.md) — never for PLR2004.
- Per-test timeout 10 s: add `pytest-timeout>=2.3.1` to dev deps and `"--timeout=10"` to addopts.
- Conventional commits ending with a `Co-Authored-By:` trailer naming the model that wrote them.
- No new runtime dependencies.
- Default behaviour unchanged: `Database(path)` without `journal=True` must behave exactly as before (all existing tests stay green unmodified, except where a test asserted the absence of a now-supported type).
- `wal.py` and `mvcc.py` (standalone teaching modules) unchanged.
- Files under ~400 lines for new modules; don't grow `executor.py` (687) or `sql_parser.py` (624) by more than ~60 lines each — put new logic in new modules.
- Every docs snippet actually run; output shown matches. Kid-friendly pages: analogy, runnable snippet, "What We Test".
- Defaults (spec): journal `sync=True`, `checkpoint_bytes` 4 MiB, `repair=False`; TIMESTAMP stored UTC with `Z`; naive datetimes rejected.

## Review Focus

1. **Power cut mid-append** (journal ends in a partial line): the database opens with every earlier commit and the torn tail truncated — Task 4 test `test_torn_tail_is_ignored_and_truncated`.
2. **Crash during a checkpoint** at each step (after some snapshots, after `checkpoint.json`, before journal rotation): reopening yields exactly the last committed state, nothing doubled — Task 6 test `test_crash_at_every_checkpoint_step`.
3. **`True` into an INTEGER column, a `datetime` into a DATE column, a naive datetime into TIMESTAMP** → `SchemaError`, never silently stored — Task 1 tests.
4. **Two threads running transactions on the same table** → no exceptions, final state equals some serial order, and a reader never sees a half-applied transaction — Task 7 test `test_concurrent_transactions_serialise`.
5. **A second process opening the same journalled directory** → `DatabaseLockedError`; a stale `LOCK` from a dead PID is taken over — Task 7 tests.

---

### Task 1: DATE and TIMESTAMP values

**Files:** Modify `src/pydb/types.py`, `record.py` (`Value`), `schema.py` (validation), `serializer.py`, `csv_io.py`, `formatter.py`, `btree.py` (`BTreeKey`), `pyproject.toml` (pytest-timeout); Tests `tests/test_dates.py` (+ update `tests/test_types.py` if it asserts the old member list).

**Interfaces — Produces:**
- `DataType.DATE = "DATE"`, `DataType.TIMESTAMP = "TIMESTAMP"`; `PYTHON_TYPES` gains `date`, `datetime`.
- `Value = str | int | float | bool | date | datetime`; `BTreeKey` same set.
- `pydb.types.check_value(value: object, data_type: DataType) -> Value` — the single place that validates/normalises: exact-type rules (`bool` is not INTEGER/FLOAT, `int` accepted for FLOAT? keep today's rule — read schema.py and preserve it), `datetime` is not DATE, TIMESTAMP requires `tzinfo` and is converted to UTC; raises `SchemaError` (or the existing validation error type — reuse what schema.py raises).
- `to_iso(value: date | datetime) -> str` / `from_iso(text: str, data_type: DataType) -> date | datetime` in `types.py` — DATE `YYYY-MM-DD`; TIMESTAMP `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (accept `+00:00` and other offsets on input, always emit `Z`).
- Serializer, CSV import/export and formatter use these.

- [ ] Steps: failing tests (type table incl. Review Focus 3; ISO round trips incl. microseconds and `+02:00` → UTC; JSON table round trip with both types; CSV round trip; formatter shows ISO; an index on a DATE column finds ranges) → implement → gates → **Commit** `feat: add DATE and TIMESTAMP column types`.

### Task 2: Dates in SQL

**Files:** Modify `src/pydb/sql_tokenizer.py`, `sql_parser.py` (type names + literal parsing, minimal), `executor.py` (comparisons) — or create `src/pydb/sql_literals.py` for typed-literal parsing; Tests `tests/test_sql_dates.py`.

**Interfaces — Produces:** `CREATE TABLE runs (job TEXT, run_date DATE, started TIMESTAMP)`; literals `DATE '2026-10-17'` and `TIMESTAMP '2026-10-17T09:30:00Z'` produce `date`/`datetime` values (bad literal → the parser's existing error type with a clear message); `INSERT … VALUES (…, DATE '…', TIMESTAMP '…')`; `WHERE run_date = DATE '…'`, `<`, `>=`, `BETWEEN … AND …`, `ORDER BY started DESC`, `MIN`/`MAX`; comparing a DATE column with a bare string → the executor's existing type error (no implicit conversion).

- [ ] Steps: failing tests for each statement form → implement → gates → **Commit** `feat: support DATE and TIMESTAMP in SQL`.

### Task 3: Change recording and persisted index definitions

**Files:** Modify `src/pydb/table.py`, `database.py`, `transaction.py`, `serializer.py`/`storage.py` (index list in snapshots); Create `src/pydb/changes.py`; Tests `tests/test_changes.py`, `tests/test_index_persistence.py`.

**Interfaces — Produces:**
- `changes.py`: `@dataclass(frozen=True) Change(kind: ChangeKind, table: str, record_id: int | None = None, data: dict[str, Value] | None = None, schema: Schema | None = None, index: tuple[str, str] | None = None)`; `class ChangeKind(StrEnum)`: `CREATE_TABLE`, `DROP_TABLE`, `INSERT`, `UPDATE`, `DELETE`, `CREATE_INDEX`, `DROP_INDEX`. `type ChangeSink = Callable[[Change], None]`.
- `Table` gets an optional `_sink: ChangeSink | None` (set by its `Database`); `insert` (with the assigned id and full data), `update` (id + full new data), `delete` (id), `create_index`, `drop_index` call it after the in-memory change succeeds.
- `Database` records `CREATE_TABLE`/`DROP_TABLE`; exposes `_on_change(change)` that, when no transaction is active, treats the change as its own commit (`_commit([change])`), and inside a transaction appends it to the transaction's buffer. `_commit(changes: list[Change]) -> None` is the hook Task 5 fills (here: a no-op list kept for tests via `Database._committed_for_tests` — or better, a pluggable `commit_listener` callable; pick the cleaner and document it).
- `Transaction`: buffers changes; `commit()` passes the buffer to `db._commit`; `rollback()` discards it (in addition to today's snapshot restore).
- Snapshot format: table JSON gains `"indexes": [{"name": …, "column": …}]`; loading rebuilds indexes; old files without the key load fine. This applies with or without the journal.

- [ ] Steps: failing tests (each mutation emits the right `Change`; transaction commit delivers one batch, rollback none; index definitions survive `save()`/`load()`; old-format file loads) → implement → gates → **Commit** `feat: record table changes and persist index definitions`.

### Task 4: The journal file

**Files:** Create `src/pydb/journal.py`; Tests `tests/test_journal.py`.

**Interfaces — Produces:**
- `class JournalCorruptError(PyDBError)` (attribute `line: int`).
- `encode_entry(lsn: int, changes: Sequence[Change]) -> bytes` → `b"<len> <crc32 hex8> <json>\n"` where `<len>` is the byte length of `<json>`; JSON shape `{"lsn": int, "changes": [...]}` with values serialised via Task 1's ISO helpers (`{"t": "date", "v": "…"}`-style tagging for non-JSON values — choose a tagging scheme and document it).
- `decode_entries(data: bytes) -> tuple[list[tuple[int, list[Change]]], int]` → (entries, valid_byte_length). Torn/invalid **final** entry → stops, valid length excludes it; an invalid entry followed by a valid one → `JournalCorruptError(line=n)`.
- `class Journal(path: Path, *, sync: bool = True)`: `append(changes) -> int` (assigns the next LSN, writes one entry, `flush` + `fsync` when `sync`), `size_bytes`, `last_lsn`, `read(*, after: int = 0, repair: bool = False) -> list[tuple[int, list[Change]]]` (truncates a torn tail on disk), `rotate(start_after: int) -> None` (atomically replace with an empty journal, fsync the directory), `close()`.

- [ ] Steps: failing tests (round trip all change kinds incl. dates; `test_torn_tail_is_ignored_and_truncated` (Review Focus 1: cut the file at every byte offset of the last entry); flipped byte in a middle entry → `JournalCorruptError` with the right line; `repair=True` stops there; LSNs continue after reopen; `sync=False` skips fsync (monkeypatch `os.fsync` to count)) → implement → gates → **Commit** `feat: add checksummed journal`.

### Task 5: Journalled databases — durable commits and recovery

**Files:** Modify `src/pydb/database.py` (or create `src/pydb/durable.py` for the recovery/replay logic to keep database.py small); Tests `tests/test_durable.py`, `tests/crash_helpers.py`.

**Interfaces — Produces:** `Database(path, *, journal: bool = False, sync: bool = True, checkpoint_bytes: int = 4 * 1024 * 1024, repair: bool = False)`. With `journal=True`: on construction, load `checkpoint.json` (if any) + listed snapshots, then replay `Journal.read(after=checkpoint_lsn)` applying each change **without** re-journalling it; every `_commit(changes)` appends one entry (durable before the mutating call returns). `load()` on a journalled database is a no-op (already loaded) — document. `close()` and context-manager support. With `journal=False`, nothing changes.

- [ ] Steps: failing tests — in-process: commit, drop the object without `save()`, reopen → data present; transaction rolled back → absent; replay of every change kind incl. index create/drop and table drop; real crash tests with `subprocess` running a helper script (`tests/crash_helpers.py`, invoked with `sys.executable`) that commits N rows and `os._exit(1)`s (no save) → parent reopens and sees N; a child that dies mid-transaction → none of that transaction → implement → gates → **Commit** `feat: make journalled commits durable and recover on open`.

### Task 6: Checkpoints

**Files:** Modify `src/pydb/database.py`/`durable.py`; Tests `tests/test_checkpoint.py`.

**Interfaces — Produces:** `Database.checkpoint() -> None` (journalled only; writes snapshots of tables changed since the last checkpoint + removes files of dropped tables, then `checkpoint.json` atomically `{"lsn": N, "tables": [...]}`, then `journal.rotate`); `save()` on a journalled database calls `checkpoint()`; auto-checkpoint after a commit that makes `size_bytes > checkpoint_bytes`; `close()` checkpoints. A private test hook `_crash_after: str | None` (step names `"snapshots"`, `"checkpoint_file"`, `"rotate"`) lets the crash helper `os._exit` between steps.

- [ ] Steps: failing tests — checkpoint shrinks the journal; reopen after checkpoint + more commits; `test_crash_at_every_checkpoint_step` (Review Focus 2, real child processes, each step) → exact last committed state; auto-checkpoint with a tiny threshold → implement → gates → **Commit** `feat: add checkpoints with crash-safe ordering`.

### Task 7: Locking — threads and processes

**Files:** Modify `src/pydb/database.py`, `table.py`, `transaction.py`; Create `src/pydb/locking.py` (lock file); Tests `tests/test_locking.py`.

**Interfaces — Produces:** `Database` owns `threading.RLock` (`db.lock`), handed to each `Table` it creates/loads (free-standing tables get their own); every public `Database` and `Table` method acquires it. `Transaction` acquires at construction, releases at `commit`/`rollback`; usable as `with Transaction(db) as txn:` (commit on success, rollback on exception, re-raise). `class DatabaseLockedError(PyDBError)`; journalled databases create `LOCK` with `O_CREAT | O_EXCL` containing the PID, removed on `close()`; an existing `LOCK` whose PID is not alive (`os.kill(pid, 0)` → `ProcessLookupError`) is replaced.

- [ ] Steps: failing tests — `test_concurrent_transactions_serialise` (Review Focus 4: 4 threads × 50 transfers between two rows inside transactions; sum invariant holds at every read by a 5th reader thread; no exceptions); same-thread re-entrancy; context manager commit/rollback; Review Focus 5 (second process via subprocess → `DatabaseLockedError`; stale lock with a dead PID taken over) → implement → gates → **Commit** `feat: add database lock for threads and a lock file for processes`.

### Task 8: Recovery property test, docs, examples, README

**Files:** Create `tests/test_recovery_property.py`, `examples/crash_and_recover.py`, `examples/run_dates.py`, `docs/concepts/dates-and-times.md`, `journal.md`, `crash-recovery.md`, `checkpoints.md`, `torn-writes.md`, `locks-and-threads.md`; Modify `docs/concepts/wal.md`, `docs/index.md`, `README.md`; `tests/test_examples.py` (create if absent).

- Property test: seeded random sequences of inserts/updates/deletes/transactions (some rolled back)/checkpoints, with a crash (child process `os._exit`) at a random point; reopen and compare with an in-memory model of the committed state. Keep it < 5 s (fewer, well-chosen seeds).
- Examples expose `main()` (< 3 s, deterministic, friendly output; crash example uses a temp directory and a child process).
- Docs per spec; `wal.md` points to the real journal; index learning path; README section with a runnable `journal=True` snippet and a "Developing alongside a sibling" note if absent (keep a single Releasing section).
- [ ] Steps: failing tests → implement → run every snippet/example → gates → **Commit** `docs: journal, recovery and dates learning path with examples`.

### Task 9: PyStack `dbcrash`

**Files (PyStack worktree on a new branch — never the user's checkout):** add the command to the plugin that owns database commands (find where PyStack registers its `pydb`-backed shell commands — `src/pystack/cli.py` / `environment.py` / an adapter — and follow that pattern), its tests, `README.md`, `docs/index.md`. Develop against the new pydb with `uv pip install -e ../pydb` + `uv run --no-sync`; the controller bumps PyStack's pydb tag after release.

**Produces:** shell command `dbcrash` per the spec (child process commits three job-run rows with DATE + TIMESTAMP columns, dies mid-transaction on the fourth; parent reopens, prints the three rows and "job 4: never committed, so it's gone"). Deterministic, traceback-free, < 3 s.

- [ ] Steps: failing test asserting the key lines → implement → 5 identical runs → gates → **Commit** `feat(db): add dbcrash demo for PyDB's journal`.
