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
  automatically *(in-memory; not persisted)*
- **Query planner** -- EXPLAIN shows scan vs. index lookup decisions
- **Constraints** -- PRIMARY KEY, NOT NULL, UNIQUE
- **Storage engine** -- JSON persistence for table schemas and rows
- **Interactive REPL** -- type SQL and see pretty-printed results

### Standalone teaching modules

These live in their own modules and work *with* a `Database`, but they
are **not** yet wired into the Database/executor pipeline -- import and
drive them directly to see how each idea works:

- **Transactions** (`pydb.transaction`) -- commit/rollback via table snapshots
- **Write-Ahead Log** (`pydb.wal`) -- crash recovery via operation logging
- **MVCC** (`pydb.mvcc`) -- multi-version concurrency for snapshot isolation
- **CSV import/export** (`pydb.csv_io`) -- load and save spreadsheet files
- **Outbox pattern** (`pydb.outbox`) -- atomic DB writes + message queue delivery

> **Note on persistence:** `save()`/`load()` persist table schemas and
> rows. Views and indexes currently live in memory only, so they are
> rebuilt (or recreated) rather than restored on restart.

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

## Documentation

Full docs at [robcox2910.github.io/pydb](https://robcox2910.github.io/pydb/)

## Related Projects

PyDB is part of an educational series where every layer of the
computing stack is built from scratch:

| Project | What It Teaches |
|---------|----------------|
| [PyOS](https://github.com/robcox2910/py-os) | Operating systems |
| [Pebble](https://github.com/robcox2910/pebble-lang) | Compilers and programming languages |
| [PyStack](https://github.com/robcox2910/pystack) | Full-stack integration |
| [PyWeb](https://github.com/robcox2910/pyweb) | HTTP web servers |
| [PyGit](https://github.com/robcox2910/pygit) | Version control |
| [PyCrypt](https://github.com/robcox2910/pycrypt) | Cryptography |
| [PyNet](https://github.com/robcox2910/pynet) | Networking |
| [PySearch](https://github.com/robcox2910/pysearch) | Full-text search |
| [PyMQ](https://github.com/robcox2910/pymq) | Message queues |

All projects use TDD, comprehensive documentation with real-world
analogies, and are designed for learners aged 12+.

## License

MIT
