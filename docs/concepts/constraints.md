# Constraints

## The Rules of the Collection

Every good card collection has rules:

- "Every card must have a serial number." → **PRIMARY KEY**
- "You can't leave the name blank." → **NOT NULL**
- "No two cards can have the same name." → **UNIQUE**

**Constraints** are rules you attach to columns to keep your data
honest. The database enforces them automatically -- if someone tries
to break a rule, the operation is rejected with a clear error.

## PRIMARY KEY

A primary key is a column that **uniquely identifies** each row. It's
like the serial number stamped on each trading card -- no two cards can
have the same number, and every card must have one.

```sql
CREATE TABLE cards (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    power INTEGER NOT NULL
)
```

A PRIMARY KEY is automatically **NOT NULL** and **UNIQUE**.

## NOT NULL

A NOT NULL column cannot be left empty. Every row must have a value
for that column.

```sql
CREATE TABLE players (
    name TEXT NOT NULL,
    score INTEGER
)

-- This works: every column has a value.
INSERT INTO players (name, score) VALUES ('Alice', 100)
```

Think of it like a form where certain fields are marked with a red
asterisk (*) -- you must fill them in.

### An honest note about NULL

Real databases let a column hold **NULL** -- a special "no value here"
marker -- and NOT NULL is the rule that forbids it. **PyDB has no NULL
yet.** Every column always needs a value, so this:

```sql
-- This fails in PyDB: 'score' is missing.
INSERT INTO players (name) VALUES ('Alice')
-- Error: Missing required column(s): score
```

is rejected whether or not the column is NOT NULL. In other words,
right now *every* column behaves as if it were required. The `NOT NULL`
keyword is parsed and remembered, but because nothing can ever be NULL,
it has no extra work to do.

> **Try it yourself!** Adding real NULL support is a great exercise:
> teach the tokenizer/parser a `NULL` literal, let a value be `None`,
> allow the schema to skip missing optional columns, and *then* NOT NULL
> becomes the rule that stops NULLs sneaking in. See if you can wire it
> all the way through.

## UNIQUE

A UNIQUE column means no two rows can have the same value in that
column. It's like a rule that says "no duplicate names in the binder."

```sql
CREATE TABLE users (
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
)

INSERT INTO users VALUES ('alice@example.com', 'Alice')  -- OK
INSERT INTO users VALUES ('alice@example.com', 'Bob')    -- FAILS! Email taken.
```

## How PyDB Spots a Duplicate Fast

Picture a teacher checking that no two children in her class share a name.
The slow way: each time a new child arrives, walk past *every* desk and
read *every* name tag. With 30 children that's fine; with 20 000 it takes
all day.

The fast way is a **register** -- a list of names, each pointing at a desk.
A new child says "I'm Sam", the teacher looks up "Sam" in the register
once, and she knows straight away whether that name is taken. When a child
changes their name or leaves, she updates the register.

Each PyDB table keeps a register like that (`pydb.unique.UniqueRegister`)
for every PRIMARY KEY and UNIQUE column: a dictionary from value to row.
Checking a new value is one look-up, however big the table is. The table
keeps the register up to date on every insert, update and delete, and
rebuilds it when a table is loaded from disk, replayed after a crash, or
rolled back by a transaction.

Two values count as the same exactly when Python's `==` says so. That's
why, in a FLOAT column, `1` and `1.0` clash (`1` is widened to `1.0`
anyway). A missing value is never registered, and neither is NaN ("not a
number") -- `NaN == NaN` is False, so NaNs never clash.

## What We Test

- PRIMARY KEY rejects duplicate values.
- UNIQUE rejects duplicate values (on INSERT and UPDATE).
- Updating a row without changing its unique value is allowed.
- Every column requires a value (PyDB has no NULL yet).
- Constraints are parsed correctly in CREATE TABLE.
- Clear error messages explain which constraint was violated.
- The register agrees with the old "compare with every row" check for
  every pair of tricky values (`1`, `1.0`, `True`, NaN, dates...).
- The register stays right after updates, deletes, rollback, load and
  crash recovery -- and an insert reads only a handful of rows, not all.

## Next Up

Now that our data has rules, we can ask more powerful questions.
Head to [Subqueries](subqueries.md) to learn about questions inside
questions.
