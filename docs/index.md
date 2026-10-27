---
title: "Build your own SQL database in Python"
description: "A relational database built from scratch in Python: SQL parsing, tables, B-tree indexes, a query planner, transactions and a write-ahead log."
---

# PyDB: Build your own SQL database in Python

## What Is a Database?

Imagine you have a **notebook full of trading cards**. Each page lists one card:
its name, its rarity, how many copies you own, and what you'd trade it for.

That notebook *is* a database.

A **database** is just an organised collection of information. Your phone's
contacts app is a database. The high-score table in a video game is a database.
The register your teacher uses to take attendance -- database.

But wait -- couldn't you keep all that in a plain text file? Sure, the same way
you *could* pile every book you own on the floor. You'd eventually find the one
you want, but it would take ages. A database is the bookshelf: it keeps things
tidy so you can find, add, change, and remove information quickly and reliably.

## Why Build One from Scratch?

Every app you use -- from Instagram to Minecraft servers -- has a database
hiding underneath. By building one yourself, you'll learn:

- **How data is stored** -- from Python dicts in memory to bytes on disk.
- **How queries work** -- how a computer finds the needle in the haystack.
- **How data stays safe** -- what happens when the power goes out mid-write.
- **How SQL works** -- the language almost every database speaks.

## The Big Picture

Here's the journey we'll take, layer by layer:

```
You  →  SQL  →  Query Engine  →  Table  →  Storage Engine  →  Disk
         ↑          ↑               ↑            ↑
       Parser    Planner         Schema       Pages & Files
```

Don't worry if that looks complicated -- we'll build one piece at a time,
starting with the simplest possible thing: a **record**.

## Our Building Blocks

| Concept | Real-World Analogy | What It Does |
|---------|-------------------|--------------|
| **Record** | One trading card | A single row of data |
| **Schema** | The card template | Defines what columns exist and their types |
| **Table** | A binder of cards | Holds many records, all following the same schema |
| **Data Types** | "name is text, count is a number" | Rules about what kind of value each column holds |
| **Storage Engine** | Writing the binder to a notebook | Saves tables to disk so they survive a restart |
| **Index** | The card catalog at a library | Makes finding specific records lightning-fast |
| **Query Engine** | Asking the librarian | Understands requests like "find all rare cards" |
| **SQL Parser** | Translating English to librarian-speak | Turns SQL text into instructions the engine understands |
| **Transactions** | The "no take-backs" rule | Groups of changes that either all happen or none do |
| **Outbox** | All-or-nothing shopping | Atomic DB + MQ operations |
| **Dates and Times** | Calendars and one shared clock | `DATE` and `TIMESTAMP` columns, stored in UTC |
| **NULL** | An empty box with a question mark | A value that is unknown -- not zero, not blank |
| **Journal** | A diary you write in *before* you act | Makes every commit survive a crash |
| **Checkpoint** | Tidying the diary into a photo album | Keeps the journal short |
| **Lock** | One pen, many writers | Lets threads and programs take turns safely |

## Install

```bash
pip install fromscratch-pydb
```

Or get the whole series at once with `pip install fromscratch-pystack`. To read the code or run the tests, clone it from [GitHub](https://github.com/robcox2910/pydb).

## Let's Start!

Head to [Records](concepts/records.md) to build our first piece.

## Learning Path: Durable Databases

Once you've met [Transactions](concepts/transactions.md) and the
[Write-Ahead Log](concepts/wal.md), this path shows how PyDB keeps your
data safe when the power goes out -- the kind of database a job scheduler
can trust. Read the pages in order:

1. [Dates and Times](concepts/dates-and-times.md) -- calendars, clocks, and
   why every timestamp is stored in UTC.
2. [NULL](concepts/null.md) -- the empty box with a question mark: a run
   that hasn't ended yet has no end time.
3. [The Journal](concepts/journal.md) -- the diary you write in *before* you
   do something: `Database(path, journal=True)`.
4. [Crash Recovery](concepts/crash-recovery.md) -- waking up and reading
   yesterday's diary.
5. [Checkpoints](concepts/checkpoints.md) -- tidying the diary into the
   photo album.
6. [Torn Writes](concepts/torn-writes.md) -- a half-written diary line, and
   the checksum that spots it.
7. [Locks and Threads](concepts/locks-and-threads.md) -- one pen, many
   writers, taking turns.

Then try the examples:

```bash
uv run python examples/crash_and_recover.py   # pull the plug, then recover
uv run python examples/run_dates.py           # DATE and TIMESTAMP queries
```
