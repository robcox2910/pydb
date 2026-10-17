# The Journal

## The Diary You Write In *Before* You Do Something

Imagine you're in charge of the class pet-feeding rota. Every time you
change the rota you **first** write the change in a diary -- "Tuesday:
Sam feeds the hamster instead of Priya" -- and only *then* tell everyone
it's done. If you lose the rota sheet, you can rebuild it from the diary.

A **journalled** database works the same way. Open it with
`journal=True` and every commit is written to a file called
`journal.log` -- and forced onto the disk -- *before* the change counts as
done. That's what makes the "D" in ACID (durability) real: once a commit
returns, it survives a crash, even if you never call `save()`.

(This is the grown-up version of the [Write-Ahead Log](wal.md) lesson --
the same idea, now wired into the real database.)

## One Line per Commit

Each line in the diary is one **commit**:

```
<length> <checksum> {"lsn": 7, "changes": [...]}
```

- **`lsn`** (log sequence number) counts commits: 1, 2, 3, ... Each commit
  gets the next number.
- **`changes`** lists everything that commit did: create a table, insert a
  row, update one, delete one, add an index...
- **`length`** and **`checksum`** let PyDB spot a line that was only half
  written (see [Torn Writes](torn-writes.md)).

A change outside a transaction is its own commit. One SQL `UPDATE` or
`DELETE` is one commit too, however many rows it touches: if a row fails
part-way (say it breaks a `UNIQUE` rule), the whole statement is undone. A whole
[transaction](transactions.md) is **one** line, written in one go -- so on
disk it's all-or-nothing too. A rolled-back transaction never reaches the
diary at all.

## Try It

```python
import tempfile
from pathlib import Path

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "cards"
    db = Database(folder, journal=True)  # sync=True: every commit is fsynced
    cards = db.create_table("cards", Schema(columns=[
        Column("name", DataType.TEXT),
        Column("power", DataType.INTEGER),
    ]))
    cards.insert({"name": "Pikachu", "power": 55})  # one commit

    with Transaction(db) as txn:                    # one commit for both rows
        txn.get_table("cards").insert({"name": "Eevee", "power": 40})
        txn.get_table("cards").insert({"name": "Snorlax", "power": 70})

    for line in (folder / "journal.log").read_text().splitlines():
        print(line[:60] + "...")
    db.close()
```

Output:

```
151 bc328180 {"lsn":1,"changes":[{"k":"create_table","t":"ca...
92 5148313f {"lsn":2,"changes":[{"k":"insert","t":"cards","i...
161 a1e02a6e {"lsn":3,"changes":[{"k":"insert","t":"cards","...
```

Three commits, three lines -- and the transaction's two inserts share
line 3.

## Forcing It Onto the Disk

When a program writes to a file, the computer usually keeps the bytes in
memory for a while and writes them to the disk later. That's fast, but a
power cut would lose them. So after writing each commit, PyDB calls
`os.fsync`, which means "put it on the disk *now*, and don't come back
until you have".

`Database(path, journal=True, sync=False)` skips the `fsync`. It's much
faster, which is handy for tests and demos, but it is **not safe**: a power
cut can lose the last few commits.

If writing the diary fails (say the disk is full), the commit is undone in
memory and you get a `JournalWriteError`. After that the database refuses
further commits until you reopen it or take a [checkpoint](checkpoints.md)
-- better a loud stop than a diary with a hole in it.

## Who Else Is in the Folder?

| File | What it is |
|------|------------|
| `journal.log` | The diary of commits since the last checkpoint |
| `checkpoint.json` | Which photo-album pages are current (see [Checkpoints](checkpoints.md)) |
| `snapshots/` | The photo album: one snapshot file per table |
| `LOCK` | "Someone's using this!" (see [Locks and Threads](locks-and-threads.md)) |

Without `journal=True`, PyDB behaves exactly as before: nothing is safe on
disk until you call `save()`.

## What We Test

- A commit survives without `save()`.
- Each autocommit is one diary line; a transaction is one line.
- A ten-row SQL `UPDATE` or `DELETE` is one line and one `fsync`; a crash
  halfway through it leaves none of its rows changed.
- A rolled-back transaction leaves no trace in the diary.
- Every kind of change (tables, rows, indexes) is written and read back.
- Each line's length and CRC-32 checksum are checked when it is read.
- Every commit is fsynced when `sync=True`; a failed write undoes the commit.
- `journal=False` databases behave exactly as before.

## Next Up

Head to [Crash Recovery](crash-recovery.md) to see the diary save the day.
