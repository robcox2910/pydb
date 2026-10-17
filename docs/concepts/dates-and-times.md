# Dates and Times

## Calendars and Clocks

A job scheduler keeps a diary of every job it runs. For each run it wants
two different things:

- **Which day was it?** That's a page on a **calendar**: "17 October 2026".
  There's no time of day on it -- just the day.
- **Exactly when did it start?** That's a reading on a **clock**:
  "17 October 2026 at 07:30".

PyDB has a column type for each:

| Type | Like a... | Python value | Stored as |
|------|-----------|--------------|-----------|
| `DATE` | calendar page | `datetime.date` | `2026-10-17` |
| `TIMESTAMP` | clock reading | a `datetime` **with a time zone** | `2026-10-17T07:30:00Z` |

## Why "UTC"?

Clocks in different countries show different times. When it's 12:15 in
Paris, it's 10:15 in London (in summer). If one program writes down
"12:15" and another writes down "10:15", are they the same moment? You
can't tell -- unless everyone agrees on **one shared clock**.

That shared clock is called **UTC** (Coordinated Universal Time). PyDB
turns every `TIMESTAMP` into UTC before it stores it, and writes it with a
`Z` on the end ("Zulu", the radio name for UTC). So 12:15 in Paris
(two hours ahead of UTC) is stored as `10:15:00Z`.

A time with **no** time zone at all (Python calls it a *naive* datetime) is
refused: PyDB can't know which clock it was read from, and guessing would
quietly put your job runs in the wrong order.

## Try It

```python
from datetime import UTC, date, datetime, timedelta, timezone

from pydb.errors import SchemaError
from pydb.schema import Column, Schema
from pydb.table import Table
from pydb.types import DataType, to_iso

runs = Table(
    name="runs",
    schema=Schema(columns=[
        Column("job", DataType.TEXT),
        Column("run_date", DataType.DATE),
        Column("started", DataType.TIMESTAMP),
    ]),
)

paris = timezone(timedelta(hours=2))
row = runs.insert({
    "job": "backup",
    "run_date": date(2026, 10, 17),
    "started": datetime(2026, 10, 17, 12, 15, tzinfo=paris),  # 12:15 in Paris
})
print(to_iso(row["run_date"]), to_iso(row["started"]))

try:  # a time with no time zone: which clock is it on?
    runs.insert({"job": "oops", "run_date": date(2026, 10, 17),
                 "started": datetime(2026, 10, 17, 9, 0)})
except SchemaError as error:
    print("Refused:", error)

try:  # a moment is not a calendar day
    runs.insert({"job": "oops", "run_date": datetime(2026, 10, 17, 9, 0, tzinfo=UTC),
                 "started": datetime(2026, 10, 17, 9, 0, tzinfo=UTC)})
except SchemaError as error:
    print("Refused:", error)
```

Output:

```
2026-10-17 2026-10-17T10:15:00Z
Refused: Column 'started' TIMESTAMP values must have a timezone (naive datetime rejected)
Refused: Column 'run_date' expects DATE, got datetime
```

(Sneaky detail: in Python a `datetime` *is a kind of* `date`, just as
`True` is a kind of `int`. PyDB checks the exact type, so neither slips
into the wrong column.)

## Dates in SQL

In SQL you write a date as `DATE '2026-10-17'` and a moment as
`TIMESTAMP '2026-10-17T07:30:00Z'`. Comparisons, `BETWEEN`, `ORDER BY`,
`MIN` and `MAX` all work. A plain string is **not** turned into a date for
you -- that mistake is loud, not silent:

```python
import tempfile
from pathlib import Path

from pydb.database import Database
from pydb.executor import execute
from pydb.sql_parser import parse_sql

with tempfile.TemporaryDirectory() as tmp:
    db = Database(Path(tmp) / "runs")
    for sql in [
        "CREATE TABLE runs (job TEXT, run_date DATE, started TIMESTAMP)",
        "INSERT INTO runs VALUES ('backup', DATE '2026-10-16', TIMESTAMP '2026-10-16T02:00:00Z')",
        "INSERT INTO runs VALUES ('report', DATE '2026-10-17', TIMESTAMP '2026-10-17T07:30:00Z')",
    ]:
        execute(parse_sql(sql), db)
    rows = execute(parse_sql("SELECT job FROM runs WHERE run_date >= DATE '2026-10-17'"), db)
    print([row["job"] for row in rows])
    try:
        execute(parse_sql("SELECT job FROM runs WHERE run_date = '2026-10-17'"), db)
    except Exception as error:
        print(type(error).__name__ + ":", error)
```

Output:

```
['report']
QueryError: Cannot compare DATE column 'run_date' with str (write DATE '...' -- no implicit conversion)
```

For a bigger tour -- runs on a day, across a few days, and inside a time
window -- run `uv run python examples/run_dates.py`.

## What We Test

- `DATE` accepts a `date` and refuses a `datetime`; `INTEGER` refuses `True`.
- `TIMESTAMP` refuses a naive datetime and turns other time zones into UTC.
- Dates and timestamps survive the trip through JSON, CSV, the journal and SQL.
- `DATE '…'` and `TIMESTAMP '…'` literals parse; a bad one is a parse error.
- `WHERE`, `BETWEEN`, `ORDER BY`, `MIN` and `MAX` work on both types.
- Comparing a date column with a plain string is an error.
- Indexes on date columns find the right rows.

## Next Up

Head to [The Journal](journal.md) to see how PyDB keeps every change safe
the moment you make it.
