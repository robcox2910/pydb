# SQL Parser

## Translating English to Librarian-Speak

So far, we've been building queries in Python:

```python
Query(table="cards", columns=["name"], where=Condition("power", Operator.GT, 50))
```

That works, but wouldn't it be nicer to just type:

```sql
SELECT name FROM cards WHERE power > 50
```

That's **SQL** (Structured Query Language) -- the language almost every
database in the world speaks. Our SQL parser's job is to **translate**
that text into the Python `Query` objects our executor already
understands.

## The Two-Step Translation

Just like translating a sentence from French to English, we do it in
two steps:

### Step 1: Tokenizer (splitting into words)

First, we break the SQL text into **tokens** -- individual meaningful
pieces:

```
SELECT  name  FROM  cards  WHERE  power  >  50
  ↓      ↓     ↓     ↓      ↓      ↓    ↓   ↓
KEYWORD IDENT KEYWORD IDENT KEYWORD IDENT OP  NUM
```

Each token knows its type (keyword, identifier, number, operator) and
its value.

### Step 2: Parser (understanding the sentence)

Then, we read the tokens in order and build a `Query`:

- See `SELECT` → we're building a select query.
- See `name` → that's a column to include.
- See `FROM` → the next word is the table name.
- See `cards` → the table is "cards".
- See `WHERE` → a filter condition follows.
- See `power > 50` → that's `Condition("power", Operator.GT, 50)`.

## What SQL Can We Parse?

We support a useful subset of SQL:

```sql
-- Select all columns
SELECT * FROM cards

-- Select specific columns
SELECT name, power FROM cards

-- Filter with WHERE
SELECT * FROM cards WHERE power > 50

-- Multiple conditions
SELECT * FROM cards WHERE power > 50 AND type = 'Electric'
SELECT * FROM cards WHERE type = 'Fire' OR type = 'Water'

-- Ordering
SELECT * FROM cards ORDER BY name
SELECT * FROM cards ORDER BY power DESC

-- Limiting results
SELECT * FROM cards LIMIT 10

-- All together
SELECT name, power FROM cards WHERE power >= 50 ORDER BY name DESC LIMIT 5
```

## Token Types

| Token | Examples | Meaning |
|-------|----------|---------|
| `KEYWORD` | SELECT, FROM, WHERE | SQL reserved words |
| `IDENTIFIER` | name, cards, power | Table/column names |
| `INTEGER` | 42, 100, 0 | Whole numbers |
| `FLOAT` | 3.14, 0.5 | Decimal numbers |
| `STRING` | 'hello', 'Pikachu' | Text in quotes |
| `OPERATOR` | =, !=, >, <, >=, <= | Comparison operators |
| `STAR` | * | "All columns" |
| `COMMA` | , | Separates column names |
| `EOF` | (end) | No more input |

## What We Test

- Tokenizing breaks SQL into the correct token types.
- Keywords are case-insensitive (SELECT = select = SeLeCt).
- String literals with single quotes are parsed correctly.
- The parser produces correct Query objects for simple SELECTs.
- WHERE with all six operators works.
- AND and OR produce correct compound conditions.
- ORDER BY with ASC/DESC works.
- LIMIT produces the correct cap.
- Invalid SQL raises clear error messages.

## Next Up

With the SQL parser in place, we can build a **REPL** -- an interactive
prompt where you type SQL and see results immediately, just like a real
database!
