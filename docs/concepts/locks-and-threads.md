# Locks and Threads

## One Pen, Many Writers, Taking Turns

Four friends want to write in the same diary at once. If they all
scribble at the same time, the lines get jumbled: half of Ann's sentence,
then a bit of Bob's... So they agree a rule: **there's only one pen**.
Whoever holds the pen writes; everyone else waits. When you finish, you
hand the pen on.

Inside a program, the "friends" are **threads** -- several jobs running at
the same time. The "pen" is a **lock**.

## The Database Lock

Every `Database` has one lock (`db.lock`), and every table it hands out
shares it. Every public method -- `insert`, `select`, `update`, running a
SQL statement -- picks up the pen first and puts it down when it's done.
So another thread never sees one SQL `UPDATE` that changes ten rows
half-finished. (It is also a single commit -- one line in the
[journal](journal.md), all ten rows or none.)

A [transaction](transactions.md) holds the pen for its *whole* life:
`Transaction(db)` picks it up and `commit()` or `rollback()` hands it back.
Other threads simply wait their turn, so nobody can ever see a transaction
half-done. The thread holding the pen can still call ordinary methods --
it already has the pen.

Use it as a `with` block: it commits if everything went well and rolls
back if an error escaped.

## Try It

```python
import tempfile
import threading
from pathlib import Path

from pydb.database import Database
from pydb.locking import DatabaseLockedError
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "bank"
    db = Database(folder, journal=True)
    jars = db.create_table("jars", Schema(columns=[
        Column("owner", DataType.TEXT),
        Column("coins", DataType.INTEGER),
    ]))
    jars.insert({"owner": "Ann", "coins": 100})   # ID 1
    jars.insert({"owner": "Bob", "coins": 100})   # ID 2

    def move_coins(times: int) -> None:
        for _ in range(times):
            with Transaction(db) as txn:          # take the pen...
                table = txn.get_table("jars")
                ann, bob = table.get(1), table.get(2)
                table.update(1, {"coins": ann["coins"] - 1})
                table.update(2, {"coins": bob["coins"] + 1})
                                                  # ...and hand it back

    writers = [threading.Thread(target=move_coins, args=(10,)) for _ in range(4)]
    for writer in writers:
        writer.start()
    for writer in writers:
        writer.join()
    print({row["owner"]: row["coins"] for row in jars.select()})

    print(sorted(p.name for p in folder.iterdir()))
    try:
        Database(folder, journal=True)            # a second "program" tries to open it
    except DatabaseLockedError as error:
        print("Refused:", type(error).__name__)
    db.close()
    print("LOCK" in [p.name for p in folder.iterdir()])
```

Output:

```
{'Ann': 60, 'Bob': 140}
['LOCK', 'journal.log']
Refused: DatabaseLockedError
False
```

Four threads moved 40 coins and not one went missing: the total is still
200, exactly as if the transfers had happened one after another.

## Photos, Not the Card Itself

When you `update` a row, PyDB doesn't rub out the old card and write on
it. It makes a **new** card and swaps it in -- *copy-on-write*. Anyone who
already fetched the old card keeps an unchanged "photo" of it, so a reader
never sees a row change under its feet halfway through reading it.

## The LOCK File: One Program at a Time

The pen only works inside one program. Two *programs* writing the same
journal would number their commits on top of each other. So a journalled
database also puts a `LOCK` file in its folder, holding its process ID:

- Opening a folder whose `LOCK` belongs to a running program raises
  `DatabaseLockedError`. (Opening it twice in the *same* program is refused
  too -- `close()` the first one.)
- If the program named in `LOCK` has died (a crash never gets to tidy up),
  the stale `LOCK` is taken over automatically.
- `close()` -- or leaving the `with` block -- removes it.

## What We Test

- Several threads running transfer transactions never lose a coin, never
  raise, and end in the same state as running them one by one.
- A reader in another thread never sees a half-applied transaction.
- Another thread's `Transaction` waits instead of failing.
- The `with Transaction(db)` block commits on success and rolls back on error.
- A transaction must be finished by the thread that opened it.
- A second process opening the folder gets `DatabaseLockedError`; a stale
  `LOCK` from a dead process is taken over; `close()` removes `LOCK`.
- `update()` hands back a new record and earlier ones keep their values.

## Next Up

Head to [Write-Ahead Log](wal.md) to see the small, standalone version of
the journal idea, or [MVCC](mvcc.md) for another way to let many users
read and write at once.
