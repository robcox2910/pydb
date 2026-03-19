# PyDB

An educational database engine built from scratch in Python.

PyDB is a fully functional relational database that stores data in tables,
supports SQL queries, and manages transactions -- all written from the ground up
as a learning project. Built incrementally using TDD, every concept is explained
with real-world analogies a 12-year-old can follow.

## Features

- Tables with typed columns (schema enforcement)
- Insert, select, update, and delete operations
- Storage engine (persist data to disk)

## Example

```python
from pydb.table import Table
from pydb.schema import Schema, Column
from pydb.types import DataType

# Define a schema -- like designing a form
schema = Schema(columns=[
    Column(name="name", data_type=DataType.TEXT),
    Column(name="age", data_type=DataType.INTEGER),
])

# Create a table and insert data
table = Table(name="friends", schema=schema)
table.insert({"name": "Alice", "age": 12})
table.insert({"name": "Bob", "age": 13})

# Find all friends older than 12
results = table.select(where=lambda row: row["age"] > 12)
```

## Quick Start

```bash
# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint and type check
uv run ruff check .
uv run pyright src tests
```

## Documentation

Full docs at [robcox2910.github.io/pydb](https://robcox2910.github.io/pydb/)

## License

MIT
