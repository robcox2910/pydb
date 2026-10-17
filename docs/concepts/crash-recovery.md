# Crash Recovery

## Waking Up and Reading Yesterday's Diary

You fell asleep halfway through sorting your card collection. In the
morning the cards are everywhere and you can't remember what you'd done.
But you'd written every finished swap in your diary! So you:

1. Get out your **photo album** -- the last tidy photo of the collection.
2. Read the diary, starting just after the day of that photo.
3. Redo every finished swap you wrote down, in order.

The swap you were in the middle of when you fell asleep? You never wrote
"done" for it, so you simply don't redo it.

That's exactly what a journalled PyDB database does every time you open
it -- whether the last program closed it nicely or crashed:

1. Read `checkpoint.json` (or start empty if there isn't one).
2. Load the table snapshots it lists -- the photo album.
3. **Replay** every [journal](journal.md) line with an LSN *bigger* than the
   checkpoint's LSN, in order.

A transaction that hadn't committed was never written to the journal, so
there's nothing to replay. A line the checkpoint already includes is
skipped, so nothing ever happens twice.

## Try It: Pull the Plug

Here a child program commits two cards, starts a third inside a
transaction, and then dies with `os._exit` -- no `save()`, no `close()`,
no goodbye. That's as close to a power cut as Python gets.

```python
import subprocess
import sys
import tempfile
from pathlib import Path

from pydb.database import Database

CHILD = """
import os, sys
from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType

db = Database(sys.argv[1], journal=True)
cards = db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
cards.insert({"name": "Pikachu"})            # committed
cards.insert({"name": "Eevee"})              # committed
txn = Transaction(db)
txn.get_table("cards").insert({"name": "Mew"})   # not committed yet...
os._exit(1)                                  # ...the power goes out!
"""

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "cards"
    subprocess.run([sys.executable, "-c", CHILD, str(folder)], check=False)
    with Database(folder, journal=True) as db:   # waking up: read the diary
        print([row["name"] for row in db.get_table("cards").select()])
        print("next ID:", db.get_table("cards").next_id)
```

Output:

```
['Pikachu', 'Eevee']
next ID: 3
```

Both committed cards came back; Mew, who was never committed, is gone.
The next new card still gets a fresh ID.

There's a friendlier version with job runs, dates and times in
`examples/crash_and_recover.py`:

```
$ uv run python examples/crash_and_recover.py
Starting a helper program that writes job runs to a journalled database...
The helper program crashed (exit code 1) in the middle of job 4.

Reopening the folder -- recovery reads the journal diary:
  job 1: nightly-backup ran on 2026-10-15, started 2026-10-15T02:00:00Z
  job 2: tidy-logs ran on 2026-10-16, started 2026-10-16T03:15:00Z
  job 3: send-report ran on 2026-10-17, started 2026-10-17T07:30:00Z
job 4: never committed, so it's gone
The next new job run gets ID 4.
```

## When the Diary Itself Is Damaged

- A **half-written last line** is normal after a power cut: it's ignored
  and trimmed away (see [Torn Writes](torn-writes.md)).
- A damaged line **in the middle** is not normal. PyDB refuses to open with
  a `JournalCorruptError` rather than quietly forgetting commits.
  `Database(path, journal=True, repair=True)` keeps everything before the
  damage and logs a warning.

## Testing It With Randomness

Picking crash moments by hand can miss the awkward ones, so PyDB also has
a **property test**. For each seed it builds a random script -- inserts,
updates, deletes, small transactions that commit or roll back, the odd
checkpoint -- and a random moment to pull the plug (between steps, in the
middle of a transaction, or part-way through a checkpoint). A child process
follows the script and crashes. The test works out, from the seed alone,
what the committed rows *must* be, reopens the folder and checks they
match exactly.

## Binders That Were Thrown Away

What if you're still holding a binder after it was thrown away -- a table
you dropped, a table made in a transaction that rolled back, or an old one
whose name now belongs to a brand-new table? If PyDB let you write in it,
the diary would say "add a card to *cards*" for a binder that no longer
exists, and the next recovery would get confused.

So on a journalled database, the old `Table` object is **detached**: every
change through it is refused with a `DatabaseError` (and undone), and
nothing reaches the diary. Ask the database for the table again with
`get_table()`.

```python
import tempfile
from pathlib import Path

from pydb.database import Database, DatabaseError
from pydb.schema import Column, Schema
from pydb.types import DataType

with tempfile.TemporaryDirectory() as tmp:
    db = Database(Path(tmp) / "cards", journal=True)
    old = db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
    db.drop_table("cards")
    db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
    try:
        old.insert({"name": "Ghost"})
    except DatabaseError as error:
        print(error)
    print(db.get_table("cards").row_count)
    db.close()
```

Output:

```
Table 'cards' is no longer part of the database (it was dropped, rolled back or replaced); get it again with get_table()
0
```

(On a plain database, without `journal=True`, a detached table just
becomes a binder of its own, as before: you can still change it in memory,
but nothing is recorded and `save()` never writes it.)

## What We Test

- Committed rows survive a real crash (a child process and `os._exit`).
- Nothing from an unfinished transaction survives.
- Recovery loads the checkpoint's snapshots, then replays only newer lines.
- Replaying never writes to the journal again or calls commit listeners.
- The next ID and the next LSN carry on correctly after reopening.
- A random crash anywhere in a random script gives back exactly the
  committed state (the property test), and reopening twice changes nothing.
- Writing through a dropped, rolled-back or replaced table is refused, so
  a crash right after it still reopens cleanly (the property test also
  drops and recreates the table now and then).
- A damaged middle line raises `JournalCorruptError`; `repair=True` keeps
  the commits before it.

## Next Up

Head to [Checkpoints](checkpoints.md) to see how the diary stays short.
