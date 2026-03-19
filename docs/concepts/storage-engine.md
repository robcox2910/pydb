# Storage Engine

## The Notebook on the Shelf

So far, our tables live only in memory -- like holding trading cards in your
hands. The moment you close Python, *poof*, they're gone. That's like building
an amazing sandcastle right at the edge of the tide.

A **storage engine** is the notebook you write your cards into and place on a
shelf. Tomorrow, next week, or next year, you can grab that notebook, read it,
and get all your cards back exactly as they were.

```
Memory (fast, temporary)          Disk (slower, permanent)
┌──────────────┐                  ┌──────────────────────┐
│  Table       │  ──  save  ──▶  │  cards.json          │
│  (in Python) │                  │  (on your hard drive)│
│              │  ◀── load  ──   │                      │
└──────────────┘                  └──────────────────────┘
```

## How It Works

The storage engine has two jobs:

1. **Save** -- take a table from memory, serialize it to JSON, and write it
   to a file.
2. **Load** -- read a JSON file from disk, deserialize it, and reconstruct the
   table in memory.

Each table gets its own file inside a **data directory**. If your database has
three tables (`cards`, `players`, `scores`), you'll see three files:

```
data/
├── cards.json
├── players.json
└── scores.json
```

## Safety First

What if the power goes out halfway through a save? You'd end up with a
half-written file -- corrupted data! Our storage engine uses a simple trick
called **write-then-rename**:

1. Write the new data to a **temporary file** (e.g., `cards.json.tmp`).
2. Only when the write is 100% complete, **rename** the temp file to the
   real name.

Renaming is an **atomic** operation on most file systems -- it either happens
completely or not at all. So you never end up with a half-written file.

## What We Test

- Saving a table creates a file on disk.
- Loading a saved table reconstructs it exactly (same schema, records, IDs).
- The round-trip preserves all data types (text, integer, float, boolean).
- Saving uses a temp file (write-then-rename) for safety.
- Loading a file that doesn't exist raises a clear error.
- Loading a corrupted file raises a clear error.
- Deleting a table removes its file.

## Next Up

One table is great, but a real database manages *many* tables. Head to
[Database](database.md) to see how we tie everything together.
