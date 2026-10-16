# Write-Ahead Log (WAL)

## Sticky Notes Before Changes

Imagine you're rearranging your card binder. Before you move any card,
you write a sticky note: "Moved Pikachu from page 3 to page 7." If
someone bumps the table and cards go flying, you can read your sticky
notes to put everything back.

A **Write-Ahead Log** works the same way. Before the database changes
any data, it writes the change to a log file first. If the power goes
out mid-write, the database reads the log on startup and either:

- **Replays** committed changes (they were meant to stick)
- **Discards** uncommitted changes (they were never finished)

## How It Works

```
Transaction starts   →  Log: "BEGIN txn_001"
Insert a record      →  Log: "OP txn_001 INSERT cards {name: Pikachu}"
Insert another       →  Log: "OP txn_001 INSERT cards {name: Charmander}"
Transaction commits  →  Log: "COMMIT txn_001"
```

The log file is **append-only** -- we only add lines, never change
existing ones. This makes it very fast and very safe.

## Recovery After a Crash

After a crash, `recover()` reads the log:

1. Find all transactions that have a COMMIT entry → hand back their
   operations, in order, to **replay**.
2. Find all transactions without a COMMIT (or that rolled back) →
   **discard** them.
3. Once the replay is done, `clear()` the log and continue normally.

Two details make this trustworthy:

- **A commit really reaches the disk.** Writing to a file usually only
  lands in the computer's memory first. A commit is a promise, so we call
  `fsync` to force it onto the disk before saying "done".
- **Only the last sticky note can be torn.** If the power cut out while we
  were writing, the *last* line might be half-written -- that's expected,
  so we skip it. But a damaged line in the *middle* means something else
  went wrong, so `recover()` stops with a `WALError` instead of quietly
  skipping work that may have mattered.

(PyDB keeps the WAL as a standalone lesson: its tables don't log through
it yet.)

## What We Test

- BEGIN, COMMIT, and ROLLBACK are logged correctly.
- Operations include table name, operation type, and data.
- `get_committed_txns()` identifies only committed transactions.
- `get_operations()` retrieves ops for a specific transaction.
- `recover()` returns only committed operations, in log order.
- A torn final line is skipped; damage in the middle raises `WALError`.
- COMMIT is forced to disk with `fsync`.
- `clear()` empties the log after recovery.

## Next Up

Head to [MVCC](mvcc.md) to learn how databases handle multiple
users at the same time.
