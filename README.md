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
- **NULL** -- nullable columns, `IS NULL` / `IS NOT NULL`, and
  three-valued logic (TRUE / FALSE / UNKNOWN) in WHERE, ON and HAVING
- **Transactions** -- commit/rollback via an undo log (each change notes
  how to undo itself, so no table is ever copied), also as
  `with Transaction(db):`
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

## Install

[![PyPI](https://img.shields.io/pypi/v/fromscratch-pydb)](https://pypi.org/project/fromscratch-pydb/)

```bash
pip install fromscratch-pydb
```

Or get the whole series at once: `pip install fromscratch-pystack`.
<!-- docs:end -->

## Developing alongside a sibling

PyDB has no sibling dependencies of its own, but
[PyStack](https://github.com/robcox2910/pystack) depends on a PyDB release
from PyPI (`fromscratch-pydb`), and its `uv.lock` records the exact version. A
change merged here can't affect PyStack until PyDB publishes a release and
PyStack chooses to upgrade.

To try an unreleased PyDB change from a PyStack checkout next to this one,
install it over the released version:

```bash
cd ../pystack
uv pip install -e ../pydb
uv run --no-sync pytest
```

Use `uv run --no-sync` while you do this: a plain `uv run` (like `uv sync`)
puts the released version back, which is how you undo the override.

## Releasing

Other projects in the series ([PyStack](https://github.com/robcox2910/pystack)) use a released version of this one from PyPI,
not whatever is on `main`. To publish a change for them:

1. Merge the change to `main`.
2. Release it: `gh release create vX.Y.Z --target main --generate-notes`
   (or push a `vX.Y.Z` tag); the release workflow publishes it to PyPI.
   A change to the public API bumps the minor version (`v0.1.0` → `v0.2.0`);
   anything else bumps the patch (`v0.1.0` → `v0.1.1`).
3. In each project that uses it, raise the `fromscratch-pydb>=X.Y.Z` floor in
   `pyproject.toml`, run `uv lock --upgrade-package fromscratch-pydb`, and open
   a pull request so CI checks the pair together.

<!-- series:start -->
## Related Projects

This project is one of a series that builds real systems "from scratch in Python" to teach how they work. See the [series home](https://robcox2910.github.io/) for the full story.

| Project | What it teaches | Repository | Docs | PyPI |
| --- | --- | --- | --- | --- |
| PyOS | Operating systems | [py-os](https://github.com/robcox2910/py-os) | [Docs](https://robcox2910.github.io/py-os/) | [fromscratch-pyos](https://pypi.org/project/fromscratch-pyos/) |
| Pebble | Compilers and programming languages | [pebble-lang](https://github.com/robcox2910/pebble-lang) | [Docs](https://robcox2910.github.io/pebble-lang/) | [fromscratch-pebble](https://pypi.org/project/fromscratch-pebble/) |
| **PyDB** | **Relational databases** | [pydb](https://github.com/robcox2910/pydb) | [Docs](https://robcox2910.github.io/pydb/) | [fromscratch-pydb](https://pypi.org/project/fromscratch-pydb/) |
| PyKV | Key-value stores | [pykv](https://github.com/robcox2910/pykv) | [Docs](https://robcox2910.github.io/pykv/) | [fromscratch-pykv](https://pypi.org/project/fromscratch-pykv/) |
| PyDocDB | Document databases | [pydoc-db](https://github.com/robcox2910/pydoc-db) | [Docs](https://robcox2910.github.io/pydoc-db/) | [fromscratch-pydocdb](https://pypi.org/project/fromscratch-pydocdb/) |
| PyGraphDB | Graph databases | [pygraphdb](https://github.com/robcox2910/pygraphdb) | [Docs](https://robcox2910.github.io/pygraphdb/) | [fromscratch-pygraphdb](https://pypi.org/project/fromscratch-pygraphdb/) |
| PyTSDB | Time-series databases | [pytsdb](https://github.com/robcox2910/pytsdb) | [Docs](https://robcox2910.github.io/pytsdb/) | [fromscratch-pytsdb](https://pypi.org/project/fromscratch-pytsdb/) |
| PyVecDB | Vector databases | [pyvecdb](https://github.com/robcox2910/pyvecdb) | [Docs](https://robcox2910.github.io/pyvecdb/) | [fromscratch-pyvecdb](https://pypi.org/project/fromscratch-pyvecdb/) |
| PyColDB | Column-family databases | [pycoldb](https://github.com/robcox2910/pycoldb) | [Docs](https://robcox2910.github.io/pycoldb/) | [fromscratch-pycoldb](https://pypi.org/project/fromscratch-pycoldb/) |
| PyCrypt | Cryptography | [pycrypt](https://github.com/robcox2910/pycrypt) | [Docs](https://robcox2910.github.io/pycrypt/) | [fromscratch-pycrypt](https://pypi.org/project/fromscratch-pycrypt/) |
| PyNet | Computer networking | [pynet](https://github.com/robcox2910/pynet) | [Docs](https://robcox2910.github.io/pynet/) | [fromscratch-pynet](https://pypi.org/project/fromscratch-pynet/) |
| PyWeb | HTTP web servers | [pyweb](https://github.com/robcox2910/pyweb) | [Docs](https://robcox2910.github.io/pyweb/) | [fromscratch-pyweb](https://pypi.org/project/fromscratch-pyweb/) |
| PyGit | Version control | [pygit](https://github.com/robcox2910/pygit) | [Docs](https://robcox2910.github.io/pygit/) | [fromscratch-pygit](https://pypi.org/project/fromscratch-pygit/) |
| PySearch | Search engines | [pysearch](https://github.com/robcox2910/pysearch) | [Docs](https://robcox2910.github.io/pysearch/) | [fromscratch-pysearch](https://pypi.org/project/fromscratch-pysearch/) |
| PyMQ | Message queues | [pymq](https://github.com/robcox2910/pymq) | [Docs](https://robcox2910.github.io/pymq/) | [fromscratch-pymq](https://pypi.org/project/fromscratch-pymq/) |
| PyLLM | Language models (the brain) | [pyllm](https://github.com/robcox2910/pyllm) | [Docs](https://robcox2910.github.io/pyllm/) | [fromscratch-pyllm](https://pypi.org/project/fromscratch-pyllm/) |
| PyScheduler | Job scheduling | [pyscheduler](https://github.com/robcox2910/pyscheduler) | [Docs](https://robcox2910.github.io/pyscheduler/) | [fromscratch-pyscheduler](https://pypi.org/project/fromscratch-pyscheduler/) |
| PyStack | Full-stack integration | [pystack](https://github.com/robcox2910/pystack) | [Docs](https://robcox2910.github.io/pystack/) | [fromscratch-pystack](https://pypi.org/project/fromscratch-pystack/) |
<!-- series:end -->

## License

MIT
