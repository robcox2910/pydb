# Query Planner

## The Smart Librarian

Imagine two ways to find a book called "Dinosaurs" in a library:

1. **Walk every shelf** and check every book until you find it (slow).
2. **Check the card catalog**, find "Dinosaurs → Shelf 7, Position 3",
   and walk straight there (fast).

A **query planner** is the librarian who decides which approach to use.
If there's a card catalog (an index) for the column you're searching,
the planner uses it. If there isn't, it walks every shelf (full table
scan).

## EXPLAIN: Watching the Librarian Think

The `EXPLAIN` command shows you the plan *without* running the query:

```sql
EXPLAIN SELECT * FROM cards WHERE name = 'Pikachu'
```

If there's an index on `name`:

```
┌────────────────────────────────────────────┐
│ plan                                       │
├────────────────────────────────────────────┤
│ Index lookup on idx_cards_name (name = ...) │
└────────────────────────────────────────────┘
```

If there's no index:

```
┌──────────────────────────┐
│ plan                     │
├──────────────────────────┤
│ Full table scan on cards │
└──────────────────────────┘
```

Now kids can *see* why indexes matter!

## CREATE INDEX and DROP INDEX

```sql
-- Create an index on the name column
CREATE INDEX idx_name ON cards (name)

-- Remove an index
DROP INDEX idx_name ON cards
```

Once an index exists, the table automatically keeps it up to date --
every INSERT, UPDATE, and DELETE updates the index too.

## How the Planner Decides

The planner follows simple rules:

1. Look at the WHERE clause.
2. If WHERE uses `=` on a column that has an index → **index lookup**.
3. Otherwise → **full table scan**.

Real databases have much fancier planners that consider statistics,
multiple indexes, join ordering, and more. But our simple version
teaches the core idea: **choosing the right strategy makes queries
faster**.

## What We Test

- EXPLAIN shows "Full table scan" when no index exists.
- EXPLAIN shows "Index lookup" when an index covers the WHERE column.
- CREATE INDEX adds an index to a table.
- DROP INDEX removes an index.
- Queries with an available index use it (same results, faster path).
- Indexes are maintained on INSERT, UPDATE, and DELETE.

## Next Up

Head to [Constraints](constraints.md) to learn about PRIMARY KEY,
NOT NULL, UNIQUE, and FOREIGN KEY -- rules that keep your data honest.
