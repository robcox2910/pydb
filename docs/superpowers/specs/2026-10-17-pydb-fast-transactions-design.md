# PyDB Fast Transactions and Constraint Checks — Design

**Date:** 2026-10-17
**Repo:** pydb (release v0.3.0)

## Why

The job scheduler (pyscheduler) group-commits every run state change
through a journalled PyDB `Transaction`. Today:

- `Transaction.__init__` copies every record of **every** table
  (`transaction.py`, `_take_snapshots`). A commit that touches one row of
  `runs` pays for copying the whole `events` history.
- `Schema.check_constraints` scans every record of the table for each
  PRIMARY KEY / UNIQUE column on every insert and update (`schema.py`).

So both costs grow with the size of the database. These are pure
performance changes: behaviour, error messages and public APIs stay as
they are.

## Goals

1. **Undo-log transactions.** *(Revised after review: the first cut was
   copy-on-first-write, but the scheduler touches `runs` and `events` on
   every commit, so copying each touched table still made every commit
   O(rows).)* Rollback no longer copies anything. Each row or index change
   made while a transaction is open first records its inverse in O(1):
   insert -> remove that record ID and restore `next_id`; update -> put
   back the previous `Record` object (records are copy-on-write, so the old
   reference is kept, not copied); delete -> reinsert the old `Record` with
   the same ID; index create -> drop it; index drop -> put the same `Index`
   object back in its place; a `next_id` change -> restore it. Table-level
   changes are undone from the catalog the transaction remembers at its
   start: tables created inside it are dropped, dropped or replaced ones
   are put back (the same `Table` object). Rollback replays the log newest
   first, keeping indexes and the unique register in step; commit discards
   it. This must cover changes made through a `Table` object obtained
   before the transaction began, SQL statements run inside it, and every
   other path that mutates a table while it is open.
2. **Hash-indexed uniqueness.** Each table keeps, for every PRIMARY KEY or
   UNIQUE column, a mapping from value to record id. The mapping is kept
   in step on insert, update, delete, rollback restore, load, recovery
   replay and any other path that changes records. The constraint check
   becomes a dictionary lookup. Same errors, same messages. NULLs are not
   indexed (several NULLs in a UNIQUE column stay allowed, as today).
   Value equality must match the old `==` semantics exactly, including
   `1 == 1.0 == True`. If the old code treated those as duplicates, the
   new code must too; pin it with a test.
3. **Benchmarks as tests.** Tests (not timing-flaky) that prove the
   complexity:
   - A transaction that inserts one row into a 50 000-row table and rolls
     back does O(1) bookkeeping: one undo note and no copied records.
     Check with a counter or spy, not with wall-clock time.
   - Inserting into a table with a UNIQUE column must not scan the
     records. Check with a spy or counter, or by asserting that 20 000
     inserts finish in well under the 10 s test timeout, with a generous
     margin.

## Non-goals

- Changing the journal format, durability rules, the lock, or the public
  API. `Transaction`'s docstring is updated to describe the undo log.

## Constraints

Repo rules apply: Python 3.14, ruff, pyright strict, ≥ 80 % coverage
(today about 96.7 %; keep it), docstrings, named constants, files under
about 400 lines (`database.py` is already 576 lines: do not grow it; put
new code in new or smaller modules), no `TYPE_CHECKING`, no
`from __future__ import annotations`, conventional commits.

## Docs

- Update `docs/concepts/transactions*.md` (whichever covers rollback) with
  a kid-friendly paragraph: write down how to undo each scribble,
  instead of photocopying the page.
- Update the concept page that covers constraints or indexes: a
  register of names, so you don't have to read the whole class list to
  check for a duplicate.
- Add a CHANGELOG or README note if the repo keeps one.
