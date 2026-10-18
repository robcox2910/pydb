# PyDB NULL Values — Design

**Date:** 2026-10-18
**Repo:** pydb (release v0.4.0)

## Why

PyDB has a `NOT NULL` constraint but no NULL. Every column must be given a
value, and `None` fails the type check. So a value that is genuinely
missing — a run that hasn't ended, an exit code for a job that never
started — has to be faked with a sentinel such as `""`. The scheduler
(pyscheduler) is the first real user to need this. NULL is also a core
database idea worth teaching: "we don't know" is different from "zero" or
"empty".

## Goals

1. **Nullable columns.** A column without `NOT NULL` (and not the PRIMARY
   KEY) accepts `None`.
   - On insert, a missing nullable column defaults to `None`.
   - A missing NOT NULL / PRIMARY KEY column is still an error, with the
     existing message.
   - `update` may set a nullable column to `None`.
   - `None` passes the type check of every type for a nullable column, and
     fails it for a NOT NULL one, with the existing NOT NULL message.
   - The `Value` type alias includes `None`.
2. **Storage and journal.** NULL is stored as JSON `null` in snapshots and
   journal entries, and round-trips through recovery, checkpoints and
   tagged DATE/TIMESTAMP values. Old files without NULLs load unchanged.
3. **Indexes and uniqueness.**
   - B-tree indexes do not store NULL keys, so `select_by_index(index,
     None)` returns `[]`. Rollback, update to or from NULL, and delete keep
     indexes correct.
   - The UNIQUE register already ignores NULL, so several NULLs remain
     allowed.
4. **SQL.**
   - A `NULL` literal in `INSERT ... VALUES`, `UPDATE ... SET`, and
     comparisons.
   - `IS NULL` and `IS NOT NULL` predicates.
   - Three-valued logic in WHERE / ON / HAVING. A comparison involving NULL
     is UNKNOWN; `AND` / `OR` / `NOT` follow Kleene logic; a row is kept
     only when the condition is TRUE. `BETWEEN` and `IN` follow the same
     rules as their expansions.
   - ORDER BY puts NULLs first when ascending and last when descending
     (SQLite's rule).
   - Aggregates: `COUNT(*)` counts rows; `COUNT(col)`, `SUM`, `AVG`, `MIN`
     and `MAX` skip NULLs; over no non-NULL values they return NULL
     (`COUNT` returns 0).
   - GROUP BY and DISTINCT treat NULLs as one group / equal.
   - A JOIN condition comparing NULLs never matches.
   - The planner must not use an index for `col = NULL` (which is always
     UNKNOWN). It may serve `IS NULL` with a scan.
5. **CSV and formatter.**
   - The formatter shows `NULL`.
   - CSV export writes an empty field for NULL.
   - CSV import reads an empty field as NULL for nullable non-TEXT columns.
     A NOT NULL non-TEXT column given an empty field is an error.
   - *Revised after review:* TEXT NULLs must round-trip too (two NULLs in
     a UNIQUE TEXT column must not come back as clashing `''`). CSV files
     are written and read with `csv.QUOTE_NOTNULL`: every real value is
     quoted (`""` for the empty string) and NULL is a bare empty field. On
     import a bare empty field is NULL for every type; a quoted `""` is
     `""` in TEXT and NULL elsewhere. A one-column row holding NULL cannot
     be written (it would be a blank line) and is a `CSVError`.
6. **Docs:** a kid-friendly `docs/concepts/null.md` ("the empty box with a
   question mark": not zero, not blank, *unknown*; why `NULL = NULL` isn't
   true; the truth table). Update the schema, constraints, aggregations and
   joins pages where NULL changes what they say. Add it to the nav and the
   learning path.

## Non-goals

- Column DEFAULT values other than NULL.
- `COALESCE` and other NULL functions (they may come later). Add
  `COALESCE` only if trivial.
- `NULLS FIRST/LAST` syntax.

## Constraints

- Repo rules apply: Python 3.14, ruff, pyright strict, coverage ≥ the
  current level (about 97 %), docstrings, named constants, files under
  about 400 lines.
- `database.py` (576 lines), `executor.py` and `sql_parser.py` must not
  grow much. Put NULL logic in new modules where natural, for example
  `nulls.py` for three-valued helpers.
- No `TYPE_CHECKING`, no `from __future__ import annotations`.
- Conventional commits.
- Existing behaviour for non-NULL data is unchanged. All existing tests
  stay green.

## Testing

- Table-driven three-valued logic tests: the full AND/OR/NOT truth table
  with UNKNOWN, and WHERE filtering.
- Round trips:
  - insert NULL → save/load
  - journal → crash recovery
  - checkpoint → reopen
  - CSV export/import
- Index tests: NULL is never indexed; update NULL → value → NULL keeps
  `find()` right; rollback restores index contents.
- SQL tests for every Goal 4 bullet, including aggregates over all-NULL
  columns and ORDER BY placement.
