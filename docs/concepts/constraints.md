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

-- This works:
INSERT INTO players (name, score) VALUES ('Alice', 100)

-- This fails:
INSERT INTO players (name) VALUES (NULL)  -- name is NOT NULL!
```

Think of it like a form where certain fields are marked with a red
asterisk (*) -- you must fill them in.

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

## What We Test

- PRIMARY KEY rejects duplicate values.
- PRIMARY KEY rejects NULL values.
- NOT NULL rejects missing values.
- UNIQUE rejects duplicate values.
- UNIQUE allows NULL values (unless also NOT NULL).
- Constraints are parsed correctly in CREATE TABLE.
- Clear error messages explain which constraint was violated.

## What's Next?

You've built a complete database engine from scratch! It has tables,
schemas, types, a storage engine, B-tree indexes, a query engine with
JOINs and aggregations, an SQL parser, transactions, a query planner,
constraints, and an interactive REPL. That's an incredible achievement!
