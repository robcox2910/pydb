# PyDB

An educational database engine built from scratch in Python.

PyDB is a fully functional relational database that stores data in tables,
supports SQL queries, and manages transactions -- all written from the ground up
as a learning project. Built incrementally using TDD, every concept is explained
with real-world analogies a 12-year-old can follow.

## Features

- **Full SQL support** -- SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE
- **JOINs** -- cross-reference rows from two tables
- **Aggregations** -- COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- **Subqueries** -- scalar and IN subqueries in WHERE clauses
- **Views** -- saved queries as virtual tables
- **B-Tree indexes** -- fast lookups with CREATE/DROP INDEX
- **Query planner** -- EXPLAIN shows scan vs. index lookup decisions
- **Constraints** -- PRIMARY KEY, NOT NULL, UNIQUE
- **Transactions** -- commit/rollback with snapshot isolation
- **Storage engine** -- JSON persistence with crash-safe writes
- **Write-Ahead Log** -- crash recovery via operation logging
- **MVCC** -- multi-version concurrency for snapshot isolation
- **CSV import/export** -- load and save data from spreadsheet files
- **Outbox pattern** -- atomic DB writes + message queue delivery
- **Interactive REPL** -- type SQL and see pretty-printed results

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
