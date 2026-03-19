# Indexes

## The Back of the Textbook

Open any school textbook and flip to the very last pages. You'll find an
**index** -- an alphabetical list of topics with page numbers:

```
Dinosaurs ............ 42
Earthquakes .......... 78
Fossils .............. 55
Volcanoes ............ 91
```

Want to learn about fossils? You don't read the whole book -- you look up
"Fossils" in the index, see "page 55", and jump straight there.

A **database index** works the same way. Instead of checking every single
record to find the one you want (reading every page), the index tells you
exactly where to look.

## Why Do We Need Indexes?

Without an index, finding a record means scanning the entire table:

```
"Find the card named Pikachu"

Check record 1... no.
Check record 2... no.
Check record 3... yes!
```

With 10 records, that's fine. With 10,000 records, it's painfully slow.
With an index on the "name" column, you jump straight to Pikachu in just
a few steps -- even if the table has millions of rows.

## The B-Tree: A Magical Filing Cabinet

The data structure behind most database indexes is called a **B-Tree**.
Think of it as a magical filing cabinet with a special rule: **it always
stays perfectly balanced**.

Imagine a filing cabinet where:

- Each **drawer** can hold a few cards (say, 3 cards max).
- Cards in each drawer are always in **alphabetical order**.
- When a drawer gets too full, it **splits** into two drawers, and the
  middle card moves up to a label on the cabinet.
- The labels tell you which drawer to open: "A-F in drawer 1, G-M in
  drawer 2, N-Z in drawer 3."

```
            ┌─────────┐
            │  [Gio]  │         ← Root: "Go left for A-G, right for G-Z"
            └────┬────┘
           ╱           ╲
    ┌──────────┐  ┌──────────┐
    │ Ada, Cho │  │ Kai, Zoe │  ← Leaves: actual data lives here
    └──────────┘  └──────────┘
```

To find "Kai":
1. Start at the root: "Kai" > "Gio" → go right.
2. Check the right leaf: found "Kai"!

That's just **2 steps** instead of scanning all 4 names. With millions of
records, a B-Tree only needs about 3-4 steps. That's the magic of
logarithmic search.

## B-Tree Rules

A B-Tree of **order** *m* follows these rules:

1. Each node holds at most *m - 1* keys (cards in the drawer).
2. Each node has at most *m* children (sub-drawers).
3. All leaves are at the **same depth** (the cabinet is balanced).
4. Keys within each node are **sorted**.
5. When a node overflows, it **splits** and pushes the middle key up.

We use order 4 by default -- small enough to see the splits happening in
tests, big enough to be useful.

## Indexes in PyDB

```python
from pydb.index import Index

# Create an index on the "name" column
idx = Index(name="idx_name", column="name")

# Add entries (column value → record ID)
idx.insert("Pikachu", record_id=1)
idx.insert("Charmander", record_id=2)
idx.insert("Squirtle", record_id=3)

# Find a record by value -- instant!
idx.find("Pikachu")       # [1]
idx.find("Charmander")    # [2]

# Range search: all names from "A" to "M"
idx.find_range("A", "M")  # [2]  (Charmander)
```

## What We Test

- Inserting keys keeps the tree sorted and balanced.
- Searching finds the correct record IDs.
- Searching for a missing key returns an empty list.
- The tree splits nodes correctly when they overflow.
- Range searches return all matching keys.
- Deleting a key removes it from the index.
- The tree stays balanced after many inserts and deletes.

## Next Up

Now that we can find records fast, we need a way to *ask* for them in
plain English (well, plain SQL). Head to the
[Query Engine](query-engine.md) to learn how SELECT, WHERE, and more work.
