# Schema

## The Card Template

Before you start collecting trading cards, someone has to **design** the card.
They decide: "Every card will have a Name (text), a Type (text), and a Power
(number)." That design is the **template**.

A **schema** is the template for a database table. It says:

- What columns exist (Name, Type, Power).
- What kind of data each column holds (text, number, etc.).
- Any rules (e.g., "Name can't be empty" -- `NOT NULL`).

```
┌─────────────────────────────────────┐
│  CARD TEMPLATE (Schema)             │
│                                     │
│  Name:   [text, required]           │
│  Type:   [text, required]           │
│  Power:  [integer, required]        │
└─────────────────────────────────────┘
```

## Why Do We Need a Schema?

Imagine if someone slipped a card into your binder that said
`Power: "banana"`. That makes no sense! A schema acts as a **bouncer at the
door** -- it checks every new record before letting it in:

- "Is `name` actually text? ✓ Come in."
- "Is `power` a number? You wrote 'banana'? ✗ Rejected."

Without a schema, your data would be a mess -- some rows might have three
columns, others might have five, and you'd never be sure what you're looking at.

## Schemas in PyDB

```python
from pydb.schema import Schema, Column
from pydb.types import DataType

schema = Schema(columns=[
    Column(name="name", data_type=DataType.TEXT),
    Column(name="type", data_type=DataType.TEXT),
    Column(name="power", data_type=DataType.INTEGER),
])

# Check if a set of values is valid
schema.validate({"name": "Pikachu", "type": "Electric", "power": 55})  # ✓ OK
schema.validate({"name": "Pikachu", "type": "Electric", "power": "banana"})  # ✗ SchemaError!
```

## Data Types

Every column has a **data type** that tells the database what kind of values
are allowed:

| DataType | Python Type | Example |
|----------|------------|---------|
| `TEXT` | `str` | `"Pikachu"` |
| `INTEGER` | `int` | `55` |
| `FLOAT` | `float` | `3.14` |
| `BOOLEAN` | `bool` | `True` |

Think of data types like labelled drawers. You wouldn't put socks in the
cutlery drawer -- and you shouldn't put text in an integer column.

`DATE` and `TIMESTAMP` columns hold calendar days and moments in time (see
[Dates and Times](dates-and-times.md)).

## Empty Boxes: NULL

Any column can hold **NULL** (`None` in Python) -- "we don't know" -- unless
it is `NOT NULL` or the `PRIMARY KEY`. NULL fits every data type. When you
insert a row, a nullable column you leave out is filled with NULL; a
required one you leave out is an error. See [NULL](null.md).

```python
schema = Schema(columns=[
    Column(name="name", data_type=DataType.TEXT, not_null=True),
    Column(name="power", data_type=DataType.INTEGER),
])
schema.validate({"name": "Pikachu", "power": None})   # ✓ OK -- power is unknown
schema.validate({"name": None, "power": 55})          # ✗ 'name' cannot be NULL
```

## What We Test

- A schema knows its column names and types.
- Validation passes when all values match the expected types.
- Validation fails (with a clear error) when a value has the wrong type.
- Validation fails when a required column is missing.
- NULL passes for a nullable column of any type and fails for a NOT NULL one.
- Validation fails when an unknown column is provided.

## Next Up

Now that we have a template (schema) and a card (record), let's build the
**binder** that holds them all together. Head to [Tables](tables.md).
