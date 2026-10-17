# Checkpoints

## Tidying the Diary Into the Photo Album

If you wrote every card swap in your diary for a whole year, rebuilding
your collection after a sleepy morning would take ages -- you'd have to
redo every swap since January! So every now and then you:

1. Take a **photo** of the whole collection as it is right now.
2. Stick it in your photo album, with a note: "this photo includes
   everything up to diary entry 5".
3. Start a fresh diary page.

Next time you wake up confused, you start from the latest photo and only
redo the few swaps written since. That's a **checkpoint**.

## Try It

```python
import json
import tempfile
from pathlib import Path

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.types import DataType

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "cards"
    db = Database(folder, journal=True)
    cards = db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
    for name in ["Pikachu", "Eevee", "Snorlax", "Mew"]:
        cards.insert({"name": name})
    journal = folder / "journal.log"
    print("diary before:", journal.stat().st_size, "bytes")

    db.checkpoint()          # save() does the same on a journalled database
    print("diary after: ", journal.read_text().strip())
    print("album:       ", sorted(p.name for p in (folder / "snapshots").iterdir()))
    card = (folder / "checkpoint.json").read_text()
    print("card:        ", card.replace("\n", "").replace(" ", ""))

    cards.insert({"name": "Ditto"})  # lands in the fresh diary as LSN 6
    db.close()               # close() takes a final checkpoint too

with tempfile.TemporaryDirectory() as tmp:  # an automatic checkpoint
    folder = Path(tmp) / "cards"
    db = Database(folder, journal=True, checkpoint_bytes=300)
    cards = db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
    for number in range(10):
        cards.insert({"name": f"card{number}"})
    card = json.loads((folder / "checkpoint.json").read_text())
    size = (folder / "journal.log").stat().st_size
    print("auto: diary is", size, "bytes; last checkpoint at LSN", card["lsn"])
    db.close()
```

Output:

```
diary before: 496 bytes
diary after:  PYDBJ1 5
album:        ['cards.5.json']
card:         {"lsn":5,"tables":[{"name":"cards","file":"cards.5.json"}]}
auto: diary is 10 bytes; last checkpoint at LSN 11
```

After the checkpoint the diary is just a header -- `PYDBJ1 5` means "this
diary carries on after LSN 5" -- and the photo `cards.5.json` holds all
four cards.

## When Does It Happen?

- When you call `db.checkpoint()` -- or `save()` / `save_table()`, which do
  the same thing on a journalled database.
- When you `close()` the database (or leave a `with Database(...)` block).
- **Automatically**, after any commit that makes `journal.log` bigger than
  `checkpoint_bytes` (4 MiB unless you choose otherwise).

A checkpoint is never taken in the middle of a transaction -- that would
photograph half-done work. Only tables that changed since the last
checkpoint get a new photo.

## The Careful Order

A checkpoint is three steps, always in this order:

1. **Write the new photos** -- each with a brand-new file name (`cards.5.json`),
   so the photos the *old* card points at are never touched.
2. **Write `checkpoint.json`** -- atomically: it's either the old card or the
   new one, never half of each.
3. **Start a fresh diary** (rotate `journal.log`), then tidy away photos
   nobody points at any more.

What if the power goes out in between? Every moment is safe:

| Crash after... | On reopening |
|----------------|--------------|
| some new photos | The old card still rules: old photos + the full diary. The new photos are ignored. |
| the new card | New photos; diary lines up to the new LSN are skipped. Nothing happens twice. |
| the fresh diary | New photos, empty diary. Leftover old photos are tidied next time. |

## What We Test

- A checkpoint shrinks the journal to its header and writes `checkpoint.json`.
- Reopening after a checkpoint plus more commits gives the right rows and LSN.
- Only changed tables are rewritten; dropped tables' files are removed.
- `save()`, `save_table()` and `close()` checkpoint; a transaction blocks it.
- Automatic checkpoints happen once the journal passes `checkpoint_bytes`,
  and a failed one never undoes a commit.
- A real crash after *each* step recovers exactly, with nothing doubled.
- A missing snapshot, a bad card or a gap in the journal is a `RecoveryError`.

## Next Up

Head to [Torn Writes](torn-writes.md) to see how PyDB spots a half-written
diary line.
