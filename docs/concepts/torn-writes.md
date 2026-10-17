# Torn Writes

## A Half-Written Diary Line

You're writing in your diary when the lights go out. In the morning the
last line says "Tuesday: Sam feeds the ham" -- and stops. You didn't
finish it, so you don't trust it: you cross it out and carry on.

Computers have the same problem. A power cut can stop a write part-way
through, leaving the end of `journal.log` **torn**. PyDB needs a way to
tell a complete line from a torn or scribbled-on one.

## The Checksum That Spots It

Every journal line starts with two numbers:

```
<length> <checksum> <json>
```

- **Length** -- how many bytes the JSON part should be.
- **Checksum** -- a **CRC-32**: a short fingerprint calculated from every
  byte of the JSON. Change even one letter and the fingerprint comes out
  completely different.

When PyDB reads a line, it recalculates the fingerprint. If the length is
wrong, the fingerprint doesn't match, or the JSON won't parse, the line is
bad.

What happens next depends on **where** the bad line is:

- **The very last line** -- that's a torn write from a crash. It's normal.
  The line was never a finished commit (the commit only counts once its
  line is safely on disk), so PyDB ignores it and trims it off the file.
- **A line in the middle** with good lines after it -- that's *not* what a
  crash looks like. Something damaged the file. PyDB refuses to open it
  with a `JournalCorruptError` naming the line, because quietly skipping
  it could lose real commits. If you'd rather keep what you can, open with
  `repair=True`: PyDB keeps every commit before the damage and logs a
  warning on the `pydb.durable` logger.

## Try It

```python
import shutil
import subprocess
import sys
import tempfile
import zlib
from pathlib import Path

from pydb.database import Database
from pydb.journal import JournalCorruptError

# A diary line is "<length> <checksum> <json>". The checksum is a CRC-32:
print(format(zlib.crc32(b'{"hello":"world"}'), "08x"))
print(format(zlib.crc32(b'{"hello":"wurld"}'), "08x"))  # one letter changed

CHILD = """
import os, sys
from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.types import DataType

db = Database(sys.argv[1], journal=True)
cards = db.create_table("cards", Schema(columns=[Column("name", DataType.TEXT)]))
for name in ["Pikachu", "Eevee", "Snorlax"]:
    cards.insert({"name": name})
os._exit(1)  # crash: no checkpoint, so everything is still in the diary
"""


def names(db: Database) -> list[str]:
    return [str(row["name"]) for row in db.get_table("cards").select()]


with tempfile.TemporaryDirectory() as tmp:
    original = Path(tmp) / "original"
    subprocess.run([sys.executable, "-c", CHILD, str(original)], check=False)

    torn = Path(tmp) / "torn"  # 1. the power cut half-way through the last line
    shutil.copytree(original, torn)
    diary = torn / "journal.log"
    diary.write_bytes(diary.read_bytes()[:-20])
    with Database(torn, journal=True) as db:
        print("torn tail:", names(db))

    damaged = Path(tmp) / "damaged"  # 2. a scribble in the middle of the diary
    shutil.copytree(original, damaged)
    diary = damaged / "journal.log"
    diary.write_bytes(diary.read_bytes().replace(b"Eevee", b"Eeeee"))
    try:
        Database(damaged, journal=True)
    except JournalCorruptError as error:
        print("damaged:", error)
    with Database(damaged, journal=True, repair=True) as db:
        print("repaired:", names(db))
```

Output:

```
d80941d1
7a44602c
torn tail: ['Pikachu', 'Eevee']
damaged: journal is corrupt at line 3
repaired: ['Pikachu']
```

(The repaired open also logs the warning `journal damaged (journal is
corrupt at line 3); repair keeps the entries before it`.)

Line 1 creates the table and lines 2–4 insert the three cards. Chopping
the end off line 4 loses only Snorlax, which never finished being written.
Scribbling on line 3 (Eevee) damages the *middle*, so PyDB stops -- and
`repair=True` keeps just the commits before it.

## Copying Files Safely Too

The same worry applies to the snapshot files and `checkpoint.json`. They
are never edited in place: PyDB writes a temporary file, forces it to the
disk, then *renames* it over the old one. A rename is all-or-nothing, so
you see either the old file or the new one -- never a torn mix.

## What We Test

- Each line's length and CRC-32 are checked when the journal is read.
- A torn last line (cut short, bad checksum or bad JSON) is ignored and
  trimmed, and every earlier commit is kept.
- A bad line in the middle raises `JournalCorruptError` naming the line.
- `repair=True` keeps the commits before the damage and logs a warning.
- Atomic writes never leave a half-written snapshot or checkpoint file.

## Next Up

Head to [Locks and Threads](locks-and-threads.md) to see how several
writers share one database.
