# Query Engine

## Asking the Librarian

You've got your card binder (table), your card catalog (index), and your
notebook on the shelf (storage engine). Now you need a way to **ask
questions** about your cards.

Imagine walking up to a librarian and saying:

> "Show me all the Pokémon cards with power greater than 50, sorted by
> name, but only the first 3."

The librarian doesn't just guess -- they follow a clear plan:

1. **What** do you want? → The cards (which table).
2. **Which ones** match? → Power > 50 (the filter).
3. **What order?** → Sorted by name.
4. **How many?** → Only the first 3.

A **query engine** does the same thing. It takes a structured question
(called a **query**), breaks it into steps, and executes them against
your tables.

## The Two Halves

### 1. The Query (the question)

A query is a Python object that describes what you want:

```python
from pydb.query import Query, Condition, OrderBy

query = Query(
    table="cards",
    columns=["name", "power"],           # What columns to show
    where=Condition("power", ">", 50),   # Which rows match
    order_by=OrderBy("name"),            # What order
    limit=3,                             # How many
)
```

### 2. The Executor (the librarian)

The executor takes a query and a database, then returns results:

```python
from pydb.executor import execute

results = execute(query, database)
# [{"name": "Charmander", "power": 52},
#  {"name": "Pikachu", "power": 55}]
```

## Conditions (the WHERE clause)

A condition is a simple rule: **column**, **operator**, **value**.

| Operator | Meaning | Example |
|----------|---------|---------|
| `=` | Equals | `name = "Pikachu"` |
| `!=` | Not equals | `type != "Fire"` |
| `>` | Greater than | `power > 50` |
| `>=` | Greater or equal | `power >= 50` |
| `<` | Less than | `power < 30` |
| `<=` | Less or equal | `power <= 30` |

You can also combine conditions:

```python
# Power > 50 AND type = "Electric"
where = And(
    Condition("power", ">", 50),
    Condition("type", "=", "Electric"),
)
```

## Projections (the SELECT clause)

**Projection** is a fancy word for "which columns do you want to see?"
If you only care about names and powers, you don't need to see every
column:

```
All columns:    name, type, power, rarity
Projected:      name, power
```

It's like covering up columns on a spreadsheet -- the data is still
there, you just choose which parts to show.

## Ordering and Limits

- **ORDER BY** sorts the results (ascending by default, or descending).
- **LIMIT** caps how many results you get back -- useful when you only
  want the "top 3" or "first 10".

## What We Test

- A query with no WHERE returns all rows.
- A query with a WHERE condition filters correctly for each operator.
- AND conditions require both sides to be true.
- OR conditions require at least one side to be true.
- Column projection returns only the requested columns.
- ORDER BY sorts results correctly (ascending and descending).
- LIMIT caps the number of results.
- Querying a non-existent table raises an error.
- Using an unknown column in WHERE raises an error.

## Next Up

Writing queries as Python objects works, but wouldn't it be nicer to
just type `SELECT name, power FROM cards WHERE power > 50`? That's what
the **SQL parser** does -- head there next to learn how we turn text
into query objects.
