# PyDB Durability, Dates and Thread Safety — Design

**Date:** 2026-10-17
**Repo:** pydb (library), pystack (integration)

## Why

The job scheduler will keep its real records — every job run, its state
changes, start/end times, exit codes, alerts — in PyDB. If the scheduler
crashes or the machine loses power, those records must still be there.

Today PyDB keeps tables in memory and writes whole tables to JSON only on
an explicit `save()` (atomically, thanks to recent fixes). A change made
since the last save is lost on a crash. The write-ahead log (`wal.py`) is a
standalone teaching module that tables don't use. There are no date/time
types (the scheduler is all about *run dates* and *timestamps*), index
definitions aren't saved, and nothing is safe to use from more than one
thread (the web UI's blocking handlers run on worker threads).

So PyDB gains: **date/time types, a real journal that makes every commit
durable, crash recovery, checkpoints, persisted indexes, and a lock**. The
existing behaviour stays the default; the new durability is switched on per
database (`Database(path, journal=True)`), so PyStack's adapters keep
working unchanged.

The user handed the design choices to Claude ("you decide", "move on");
everything below is Claude's call.

## Goals

1. `DATE` and `TIMESTAMP` column types end to end: Python API, schema
   validation, JSON storage, CSV, the formatter, B-tree index keys, SQL
   (`CREATE TABLE … run_date DATE`, literals `DATE '2026-10-17'`,
   `TIMESTAMP '2026-10-17T09:30:00Z'`, comparisons, `BETWEEN`, `ORDER BY`).
2. A journal (`journal.py`): every committed change is appended and
   `fsync`ed before the call returns; on open, the last checkpoint is
   loaded and later committed changes are replayed.
3. Checkpoints: write every changed table atomically, record the journal
   position, start a fresh journal. Automatic when the journal passes a
   size threshold; manual via `checkpoint()`; `save()` becomes a
   checkpoint when the journal is on.
4. Torn-write safety: each journal entry has a length and a CRC32; a torn
   final entry (power cut mid-write) is ignored; corruption *before* the
   end refuses to open (`JournalCorruptError`) unless `repair=True`.
5. Index definitions persisted with the table and rebuilt on load; index
   create/drop journalled.
6. Thread safety: one re-entrant lock per database guards every public
   database and table operation; a `Transaction` holds it from begin to
   commit/rollback.
7. Kid-friendly docs for every concept, runnable examples, PyStack demo.

## Non-goals

- Multi-process access to one database directory (one process only; a
  lock file refuses a second opener).
- Concurrent writers (one writer at a time, by design — the lock).
- Async I/O inside PyDB (the scheduler calls it via `run_in_thread` when it
  must not block its loop).
- Time zones other than UTC for `TIMESTAMP`; `TIME` and `INTERVAL` types;
  SQL date functions (`NOW()` etc.).
- Changing the standalone teaching modules `wal.py` and `mvcc.py`.

## Anti-goals

- A committed change that disappears after a crash.
- A database that silently opens with half its history missing because a
  journal entry in the middle was corrupt.
- Breaking PyStack's existing use of `Database(path)` / `save()` / `load()`.
- Naive datetimes sneaking into a `TIMESTAMP` column.

## Constraints

- Python 3.14, uv, ruff (repo rules), pyright strict, pytest ≥ 80 %,
  per-test timeout 10 s, conventional commits, PR into protected `main`;
  no `TYPE_CHECKING`, no `from __future__ import annotations`; files under
  ~400 lines (the existing `executor.py` and `sql_parser.py` are larger —
  don't grow them much; put new SQL pieces in new modules where natural).
- No new runtime dependencies (stdlib `zlib.crc32`, `os.fsync`,
  `datetime`, `threading`).
- `Value` grows to `str | int | float | bool | date | datetime`; careful:
  `datetime` is a subclass of `date`, and `bool` of `int` — validation must
  check the exact intended type.

## Behaviour

### Dates and timestamps

- `DataType.DATE` ↔ `datetime.date` (not a `datetime`), stored as ISO
  `YYYY-MM-DD`.
- `DataType.TIMESTAMP` ↔ timezone-aware `datetime` in UTC; aware values in
  other zones are converted to UTC on insert/update; **naive datetimes are
  rejected** (`SchemaError`). Stored as ISO 8601 with `Z`, microseconds
  kept.
- Index keys: dates/timestamps are comparable within their own type; the
  B-tree key type grows accordingly; mixing types in one index cannot happen
  because columns are typed.
- CSV import parses the ISO forms; export writes them. The formatter shows
  ISO strings.
- SQL: column types `DATE`, `TIMESTAMP`; literals `DATE '…'` and
  `TIMESTAMP '…'` (a bad literal is a parse error); a plain string compared
  with a DATE/TIMESTAMP column is *not* auto-converted (type error, so mistakes
  are loud). `WHERE run_date BETWEEN DATE '…' AND DATE '…'`, `ORDER BY`,
  `MIN/MAX` work.

### The journal

- Files in the database directory: table snapshots `<table>.json` (as
  today, now including index definitions), `checkpoint.json` (atomic:
  `{"lsn": N, "tables": [...]}`), `journal.log` (entries after that LSN),
  and `LOCK`.
- Entry format: one line per entry, `<length> <crc32-hex> <json>\n`, where
  `json` holds `{"lsn", "kind", ...}`. Kinds: `commit` (a list of ops),
  where ops are `create_table` (name, schema, indexes), `drop_table`,
  `insert` (table, id, data), `update` (table, id, data), `delete` (table,
  id), `create_index`, `drop_index`, `create_view`/`drop_view` (stored as
  SQL text if views come from SQL; otherwise views stay in-memory and are
  documented as not durable — Claude's call: views are not journalled).
- **One entry per commit**: a transaction's ops become one `commit` entry
  written with one `fsync` — so a transaction is all-or-nothing on disk.
  An operation outside a transaction is its own commit.
- `fsync` policy: `sync=True` (default) fsyncs every commit; `sync=False`
  skips fsync (fast, for tests and demos; documented as unsafe).
- Recovery on open: read `checkpoint.json` (or start empty), load the
  listed snapshots, then replay journal entries with `lsn > checkpoint lsn`
  in order. Replay is idempotent with respect to the checkpoint boundary.
  The last entry may be torn (bad length/CRC/JSON at EOF) → ignored and
  truncated away. A bad entry followed by good ones → `JournalCorruptError`
  naming the line, unless `repair=True` (then stop at the bad entry and log
  a warning).
- Checkpoint: under the lock, write every table changed since the last
  checkpoint (atomic writes), then `checkpoint.json` with the current LSN,
  then rotate the journal (new empty file, fsync dir). A crash at any point
  leaves a state that recovers correctly (old checkpoint + full journal, or
  new checkpoint + journal entries ≤ new LSN skipped).
- Auto-checkpoint when `journal.log` exceeds `checkpoint_bytes` (default
  4 MiB), after the commit that crossed it. `close()` checkpoints and
  releases the lock file; `Database` supports `with`.
- `journal=False` (default): behaviour exactly as today.

### Transactions and the lock

- `Database` owns a `threading.RLock`; every public method of `Database`
  and of the `Table` objects it hands out acquires it (tables get a
  reference to their database's lock; a free-standing `Table` gets its own).
- `Transaction(db)` acquires the lock at creation and releases it on
  commit/rollback (also as a context manager: commit on success, rollback
  on exception). Other threads wait; the same thread can nest plain calls.
  Rollback keeps today's snapshot semantics; ops are buffered and written
  to the journal only at commit.
- A lock file (`LOCK`, created with `O_EXCL`, holding the PID) stops a
  second process opening the same journalled database
  (`DatabaseLockedError`); a stale lock from a dead PID is taken over.

### Indexes

- Table snapshots store index definitions (name, column); load rebuilds
  them. `create_index`/`drop_index` are journalled.

### Compatibility

- Old snapshot files (no index list, no checkpoint) load fine.
- Existing public APIs keep their signatures; new parameters are keyword
  with defaults that preserve today's behaviour.

## Docs (kid-friendly, every concept)

New `docs/concepts/` pages: `dates-and-times` (calendars and clocks; why
"UTC"), `journal` (the diary you write in *before* you do something),
`crash-recovery` (waking up and reading yesterday's diary), `checkpoints`
(tidying the diary into the photo album), `torn-writes` (a half-written
diary line, and the checksum that spots it), `locks-and-threads` (one
pen, many writers, taking turns). Update `wal.md` to point at the real
journal. `docs/index.md` learning path extended; README section.

Examples: `examples/crash_and_recover.py` (writes, kills a child process
mid-way with `os._exit`, reopens, prints what survived),
`examples/run_dates.py` (DATE/TIMESTAMP queries).

## PyStack integration

PyStack command `dbcrash`: in a temp directory, starts a child Python
process that opens a journalled PyDB, commits three job-run rows, starts a
fourth inside a transaction and dies without committing; the parent
reopens and prints the three committed rows (with DATE and TIMESTAMP
columns) and "job 4: never committed, so it's gone". Deterministic, < 3 s.
PyStack pins pydb's new tag.

## Testing approach

- Type validation tables (incl. `bool`-is-`int`, `datetime`-is-`date`,
  naive vs aware), round trips through JSON, CSV, SQL.
- Journal unit tests on bytes: framing, CRC, torn tail, mid-file corruption,
  repair.
- Crash tests with real child processes (`subprocess` + `os._exit`) at
  chosen points: after commit, mid-transaction, during checkpoint (via a
  test hook that exits between steps).
- Recovery property test: random ops with random crash points; after
  reopen the database equals the last committed state.
- Thread test: several threads doing transactions; final state equals a
  serial execution; no exceptions.

## Left to the builder

Internal module split, exact log wording, how the plan divides tasks.
