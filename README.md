# PyDB

An educational database engine built from scratch in Python.

PyDB is a fully functional relational database that stores data in tables,
supports SQL queries, and manages transactions -- all written from the ground up
as a learning project. Built incrementally using TDD, every concept is explained
with real-world analogies a 12-year-old can follow.

## Features

These are wired into the database engine -- run them through SQL and the
executor uses them automatically:

- **Full SQL support** -- SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE
- **JOINs** -- cross-reference rows from two tables (aggregates supported)
- **Aggregations** -- COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- **Subqueries** -- scalar and IN subqueries in WHERE clauses
- **Views** -- saved queries as virtual tables *(in-memory; not persisted)*
- **B-Tree indexes** -- CREATE/DROP INDEX; equality lookups use the index
  automatically; index definitions are saved and rebuilt on load
- **Query planner** -- EXPLAIN shows scan vs. index lookup decisions
- **Constraints** -- PRIMARY KEY, NOT NULL, UNIQUE (duplicates are found
  with one look-up in a register of values, not by reading every row)
- **Dates and times** -- `DATE` and `TIMESTAMP` columns (stored in UTC),
  with `DATE '…'` / `TIMESTAMP '…'` literals in SQL
- **Transactions** -- commit/rollback via table snapshots, also as
  `with Transaction(db):`; a table is copied only when the transaction first
  changes it, so untouched (big) tables cost nothing
- **Durable databases** -- `Database(path, journal=True)` writes every commit
  to a checksummed journal before it counts, recovers after a crash, and
  checkpoints to keep the journal short
- **Locks** -- threads take turns on one database lock; a `LOCK` file stops
  a second program opening the same journalled folder
- **Storage engine** -- JSON persistence for table schemas and rows
- **Interactive REPL** -- type SQL and see pretty-printed results

### Standalone teaching modules

These live in their own modules and work *with* a `Database`, but they
are **not** yet wired into the Database/executor pipeline -- import and
drive them directly to see how each idea works:

- **Write-Ahead Log** (`pydb.wal`) -- crash recovery via operation logging
  (the real database uses its own journal: see "Durable databases" below)
- **MVCC** (`pydb.mvcc`) -- multi-version concurrency for snapshot isolation
- **CSV import/export** (`pydb.csv_io`) -- load and save spreadsheet files
- **Outbox pattern** (`pydb.outbox`) -- atomic DB writes + message queue delivery

> **Note on persistence:** `save()`/`load()` persist table schemas, rows
> and index definitions. Views live in memory only, so they are recreated
> rather than restored on restart. Without `journal=True`, nothing is safe
> on disk until you call `save()`.

> **Note on updates:** `Table.update()` is *copy-on-write*: it returns a new
> `Record`, and a `Record` you fetched earlier keeps its old values, so rows
> you are reading never change under you. Fetch the row again (`get()` or
> `select()`) to see the new values. See
> [docs/concepts/journal.md](docs/concepts/journal.md) for the other ways
> every database now differs from before.

## Example

```sql
pydb> CREATE TABLE cards (name TEXT NOT NULL, type TEXT, power INTEGER)
Table 'cards' created

pydb> INSERT INTO cards VALUES ('Pikachu', 'Electric', 55)
1 row inserted

pydb> SELECT type, COUNT(*), AVG(power) FROM cards GROUP BY type
┌──────────┬──────────┬────────────┐
│ type     │ COUNT(*) │ AVG(power) │
├──────────┼──────────┼────────────┤
│ Electric │        1 │       55.0 │
└──────────┴──────────┴────────────┘

pydb> EXPLAIN SELECT * FROM cards WHERE name = 'Pikachu'
Full table scan on cards
```

## Durable databases

Open a database with `journal=True` and every commit is written to
`journal.log` -- and forced onto the disk -- before it returns. Reopening the
folder (even after a crash) replays the journal, so committed work is
never lost and unfinished transactions leave no trace:

```python
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "jobs"
    schema = Schema(columns=[
        Column("job", DataType.TEXT),
        Column("run_date", DataType.DATE),
        Column("started", DataType.TIMESTAMP),
    ])
    with Database(folder, journal=True) as db:      # every commit is fsynced
        runs = db.create_table("runs", schema)
        runs.insert({"job": "backup", "run_date": date(2026, 10, 17),
                     "started": datetime(2026, 10, 17, 2, 0, tzinfo=UTC)})
        with Transaction(db) as txn:                 # all-or-nothing, one commit
            txn.get_table("runs").insert({"job": "report", "run_date": date(2026, 10, 17),
                                          "started": datetime(2026, 10, 17, 7, 30, tzinfo=UTC)})

    with Database(folder, journal=True) as db:      # reopen: recovered from disk
        print([row["job"] for row in db.get_table("runs").select()])
```

Output:

```
['backup', 'report']
```

Options: `sync=False` skips the fsync (fast, but a power cut can lose the
last commits), `checkpoint_bytes` sets when the journal is checkpointed
automatically (4 MiB by default), and `repair=True` opens a journal damaged
in the middle by keeping the commits before the damage. `save()` and
`close()` take a checkpoint. See it survive a real crash with
`uv run python examples/crash_and_recover.py`, and read the
"Durable Databases" learning path in [docs/index.md](docs/index.md).

## Quick Start

```bash
# Install dependencies
uv sync --all-extras

# Launch the interactive REPL
uv run pydb

# Run tests
uv run pytest

# Lint and type check
uv run ruff check .
uv run pyright src tests
```

<!-- docs:start -->
## Documentation

Full docs (with kid-friendly explanations of every idea): https://robcox2910.github.io/pydb/
<!-- docs:end -->

## Developing alongside a sibling

PyDB has no sibling dependencies of its own, but
[PyStack](https://github.com/robcox2910/pystack) pins PyDB to a release tag
(in its `[tool.uv.sources]`), and its `uv.lock` records the exact commit. A
change merged here can't affect PyStack until PyStack chooses to upgrade.

To try an unreleased PyDB change from a PyStack checkout next to this one,
install it over the pinned release:

```bash
cd ../pystack
uv pip install -e ../pydb
uv run --no-sync pytest
```

Use `uv run --no-sync` while you do this: a plain `uv run` (like `uv sync`)
puts the pinned release back, which is how you undo the override.

## Releasing

Other projects in the series ([PyStack](https://github.com/robcox2910/pystack)) use a pinned release of this one,
not whatever is on `main`. To publish a change for them:

1. Merge the change to `main`.
2. Tag it: `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`.
   A change to the public API bumps the minor version (`v0.1.0` → `v0.2.0`);
   anything else bumps the patch (`v0.1.0` → `v0.1.1`).
3. In each project that uses it, edit the tag in `[tool.uv.sources]`, run
   `uv lock --upgrade-package <name>`, and open a pull request so CI checks
   the pair together.

<!-- series:start -->
## Related Projects

This project is one of a series that builds real systems "from scratch in Python" to teach how they work. See the [series home](https://robcox2910.github.io/) for the full story.

| Project | What it teaches | Repository | Docs |
| --- | --- | --- | --- |
| PyOS | Operating systems | [py-os](https://github.com/robcox2910/py-os) | [Docs](https://robcox2910.github.io/py-os/) |
| Pebble | Compilers and programming languages | [pebble-lang](https://github.com/robcox2910/pebble-lang) | [Docs](https://robcox2910.github.io/pebble-lang/) |
| **PyDB** | **Relational databases** | [pydb](https://github.com/robcox2910/pydb) | [Docs](https://robcox2910.github.io/pydb/) |
| PyKV | Key-value stores | [pykv](https://github.com/robcox2910/pykv) | [Docs](https://robcox2910.github.io/pykv/) |
| PyDocDB | Document databases | [pydoc-db](https://github.com/robcox2910/pydoc-db) | [Docs](https://robcox2910.github.io/pydoc-db/) |
| PyGraphDB | Graph databases | [pygraphdb](https://github.com/robcox2910/pygraphdb) | [Docs](https://robcox2910.github.io/pygraphdb/) |
| PyTSDB | Time-series databases | [pytsdb](https://github.com/robcox2910/pytsdb) | [Docs](https://robcox2910.github.io/pytsdb/) |
| PyVecDB | Vector databases | [pyvecdb](https://github.com/robcox2910/pyvecdb) | [Docs](https://robcox2910.github.io/pyvecdb/) |
| PyColDB | Column-family databases | [pycoldb](https://github.com/robcox2910/pycoldb) | [Docs](https://robcox2910.github.io/pycoldb/) |
| PyCrypt | Cryptography | [pycrypt](https://github.com/robcox2910/pycrypt) | [Docs](https://robcox2910.github.io/pycrypt/) |
| PyNet | Computer networking | [pynet](https://github.com/robcox2910/pynet) | [Docs](https://robcox2910.github.io/pynet/) |
| PyWeb | HTTP web servers | [pyweb](https://github.com/robcox2910/pyweb) | [Docs](https://robcox2910.github.io/pyweb/) |
| PyGit | Version control | [pygit](https://github.com/robcox2910/pygit) | [Docs](https://robcox2910.github.io/pygit/) |
| PySearch | Search engines | [pysearch](https://github.com/robcox2910/pysearch) | [Docs](https://robcox2910.github.io/pysearch/) |
| PyMQ | Message queues | [pymq](https://github.com/robcox2910/pymq) | [Docs](https://robcox2910.github.io/pymq/) |
| PyLLM | Language models (the brain) | [pyllm](https://github.com/robcox2910/pyllm) | [Docs](https://robcox2910.github.io/pyllm/) |
| PyStack | Full-stack integration | [pystack](https://github.com/robcox2910/pystack) | [Docs](https://robcox2910.github.io/pystack/) |
<!-- series:end -->

## License

MIT
