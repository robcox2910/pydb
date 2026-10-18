# NULL: The Empty Box with a Question Mark

## Not Zero, Not Blank -- Unknown

Imagine a class register for a spelling test. Most children have a mark:
7, 9, 10. But Sam was off sick. What is Sam's mark?

- It isn't **0** -- Sam didn't get every word wrong.
- It isn't **blank text** (`""`) -- that would be a mark written as nothing.
- It's **unknown**. Nobody knows what Sam would have scored.

A database writes "unknown" as **NULL**. Picture an empty box with a big
question mark on the lid. You don't know what's inside -- maybe nothing
was ever put in, maybe someone just hasn't told you yet.

PyDB's job scheduler needs NULL all the time: a run that hasn't finished
has no end time yet, and a job that never started has no exit code.

## Making a Column That Can Be NULL

A column can hold NULL unless you say otherwise. `NOT NULL` and
`PRIMARY KEY` columns must always have a real value.

```sql
CREATE TABLE runs (
    name  TEXT NOT NULL,     -- must always be filled in
    code  INTEGER,           -- may be NULL
    ended TIMESTAMP          -- may be NULL
)

INSERT INTO runs VALUES ('build', NULL, NULL)   -- an explicit NULL
INSERT INTO runs (name) VALUES ('test')         -- left out, so NULL
INSERT INTO runs (code) VALUES (1)              -- Error: name is required
UPDATE runs SET code = 0 WHERE name = 'build'   -- fill a box in later
UPDATE runs SET code = NULL                     -- ...or empty it again
```

In Python, NULL is `None`:

```python
runs = db.get_table("runs")
runs.insert({"name": "lint"})        # code and ended are None
runs.get(3)["code"]                  # None
```

`None` fits a column of any type -- TEXT, INTEGER, DATE and the rest --
as long as the column allows NULL. A NOT NULL column refuses it with
`Column 'name' cannot be NULL`.

## Why `NULL = NULL` Isn't True

Here's the strange part. Sam and Priya were both off sick. Did they get
the **same** mark?

You can't say yes -- maybe Sam would have got 8 and Priya 10. You can't
say no either. The honest answer is **"we don't know"**.

So in SQL a question about a NULL has a *third* answer, **UNKNOWN**, as
well as TRUE and FALSE:

| Question | Answer |
|----------|--------|
| `5 = 5` | TRUE |
| `5 = 7` | FALSE |
| `NULL = 5` | UNKNOWN |
| `NULL = NULL` | UNKNOWN |
| `NULL > 3` | UNKNOWN |

This is called **three-valued logic**. A `WHERE` clause keeps a row only
when the answer is TRUE -- UNKNOWN rows are left out, just like FALSE
ones. That's why this finds nothing at all:

```sql
SELECT * FROM runs WHERE code = NULL   -- always UNKNOWN: no rows!
```

To ask "is the box empty?", use `IS NULL` -- it always answers TRUE or
FALSE:

```sql
SELECT * FROM runs WHERE code IS NULL       -- runs with no exit code yet
SELECT * FROM runs WHERE code IS NOT NULL   -- runs that have one
```

## The Truth Tables

How do AND, OR and NOT work with UNKNOWN? Think of UNKNOWN as "could be
either". (These rules are called Kleene logic.)

**AND** needs both sides to be TRUE. One FALSE is enough to make it FALSE,
whatever the other side is.

| AND | TRUE | FALSE | UNKNOWN |
|-----|------|-------|---------|
| **TRUE** | TRUE | FALSE | UNKNOWN |
| **FALSE** | FALSE | FALSE | FALSE |
| **UNKNOWN** | UNKNOWN | FALSE | UNKNOWN |

**OR** needs just one side to be TRUE. One TRUE is enough to make it
TRUE, whatever the other side is.

| OR | TRUE | FALSE | UNKNOWN |
|----|------|-------|---------|
| **TRUE** | TRUE | TRUE | TRUE |
| **FALSE** | TRUE | FALSE | UNKNOWN |
| **UNKNOWN** | TRUE | UNKNOWN | UNKNOWN |

**NOT** flips TRUE and FALSE -- but if you don't know whether the box
holds 5, you don't know whether it *doesn't*, either:

| NOT TRUE | NOT FALSE | NOT UNKNOWN |
|----------|-----------|-------------|
| FALSE | TRUE | UNKNOWN |

So `NOT code = 3` does **not** find the rows where `code` is NULL. You
can group conditions with brackets: `NOT (code = 3 OR code = 7)`.

`BETWEEN` and `IN` follow the same rules as what they stand for:
`code BETWEEN 1 AND 5` is `code >= 1 AND code <= 5`, and
`code IN (SELECT ...)` is "`code = ` the first answer `OR code = ` the
second...". You can put the NOT in front or in the middle -- these pairs
mean exactly the same:

```sql
NOT code BETWEEN 1 AND 5        code NOT BETWEEN 1 AND 5
NOT code IN (SELECT ...)        code NOT IN (SELECT ...)
```

A list that holds a NULL makes `code NOT IN (...)` UNKNOWN for every row
that isn't in it -- a famous SQL surprise!

> **Heads up:** `IS` is now a SQL keyword (for `IS NULL`), so a column or
> table can no longer be called `is`.

PyDB keeps these rules in one small module, `pydb.nulls` (`and3`, `or3`,
`not3`, `in3`), written as `True`, `False` and `None` for UNKNOWN.

## Sorting

Where do the question-mark boxes go when you sort? PyDB follows SQLite:
**NULLs come first** with `ORDER BY col` (ascending) and **last** with
`ORDER BY col DESC`.

## Counting and Adding Up

You can't add an unknown mark to a total, so aggregates skip NULLs --
like working out the class average from the children who sat the test:

| Aggregate | With NULLs | With no known values at all |
|-----------|------------|------------------------------|
| `COUNT(*)` | counts every row | 0 if there are no rows |
| `COUNT(col)` | counts the non-NULL values | 0 |
| `SUM`, `AVG`, `MIN`, `MAX` | skip the NULLs | NULL |

`GROUP BY` puts all the NULLs into one "unknown" pile.

## JOINs

A JOIN staples rows together when their ON condition is TRUE. A NULL
key gives UNKNOWN, so a row with a NULL key never joins -- not even to
another row with a NULL key.

## Indexes

An index is a sorted list of values. "Unknown" has no place in a sorted
list (is it bigger or smaller than 5?), and `col = NULL` can never be
TRUE anyway -- so **NULLs are never put in an index**. The planner never
uses an index for `col = NULL`, and answers `col IS NULL` by looking at
every row.

## UNIQUE

Two NULLs are not "the same value" (we don't know what either one is),
so a UNIQUE column may hold as many NULLs as you like.

## On Disk, in the Journal and in CSV Files

- Table files and journal entries store NULL as JSON `null`.
- The formatter prints `NULL`.
- In a CSV file every real value is written in quotes, so the empty
  string is `""`, and a NULL is a **bare empty field** with no quotes.
  That way both come back exactly when the file is read in again (see
  [CSV Import/Export](csv.md)).

## What We Test

- The full AND / OR / NOT truth tables with UNKNOWN, and every
  comparison with a NULL on one or both sides.
- Missing nullable columns become NULL; NOT NULL and PRIMARY KEY columns
  still refuse NULL.
- NULLs survive save and load, the journal and crash recovery,
  checkpoints, and CSV export and import.
- Indexes never hold a NULL, and stay right through updates to and from
  NULL, deletes and rollback.
- `IS NULL`, `IS NOT NULL`, `NOT`, `BETWEEN` and `IN` in WHERE, HAVING
  and on views and JOINs; ORDER BY placement; aggregates over all-NULL
  columns; GROUP BY with a NULL group.

## Next Up

Now that a column can say "I don't know", see how it changes the rules
in [Constraints](constraints.md) and [Aggregations](aggregations.md).
