# MVCC (Multi-Version Concurrency Control)

## Timestamped Cards

Imagine every trading card has a tiny timestamp: "created at version 3"
and "deleted at version 7." When you start browsing the binder at
version 5, you only see cards created at version 5 or earlier that
haven't been deleted yet. Your friend, browsing at version 8, sees a
different set of cards -- including ones added after you started.

That's **MVCC**: each reader gets a consistent **snapshot** of the
data based on when they started reading, even if someone else is
writing at the same time.

## Why It Matters

Without MVCC, two people using the database at the same time can
see inconsistent data -- like seeing half of a trade (Pikachu removed
but Charmander not yet added). MVCC prevents this by giving each
reader their own frozen-in-time view.

## How It Works

Each row has two version numbers:

| Field | Meaning |
|-------|---------|
| `created_at` | The version (transaction) that created this row |
| `deleted_at` | The version that deleted this row (None if active) |

A row is **visible** at version V if:
- `created_at <= V` (the row existed when you started), AND
- `deleted_at is None` or `deleted_at > V` (it hadn't been deleted yet)

## Vacuum: Cleaning Up Old Versions

Over time, deleted rows pile up. **Vacuum** removes old versions that
no active reader can ever see -- like throwing away sticky notes once
everyone has finished looking at the binder.

```python
store.vacuum(oldest_active_version=5)
# Removes rows deleted before version 5
```

## What We Test

- Inserted rows are visible at their creation version.
- Rows are not visible before they were created.
- Deleted rows are not visible at or after the deletion version.
- Snapshot isolation: two versions see different data.
- Vacuum removes old deleted rows.
- Vacuum preserves rows still visible to active readers.
