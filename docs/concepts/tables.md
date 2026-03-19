# Tables

## The Card Binder

You've got your card template (schema) and you know what a single card looks
like (record). Now you need somewhere to keep them all -- a **binder**.

A **table** is that binder. It:

- Holds a collection of records that all follow the same schema.
- Assigns each new record a unique ID (like numbering pages).
- Lets you add, find, update, and remove records.

```
┌──────────────────────────────────────────┐
│  TABLE: pokemon_cards                     │
│  Schema: name(TEXT), type(TEXT), power(INT)│
│                                          │
│  ID │ name      │ type     │ power       │
│  ───┼───────────┼──────────┼─────────    │
│   1 │ Pikachu   │ Electric │ 55          │
│   2 │ Charmander│ Fire     │ 52          │
│   3 │ Squirtle  │ Water    │ 48          │
└──────────────────────────────────────────┘
```

## Tables in PyDB

```python
from pydb.table import Table
from pydb.schema import Schema, Column
from pydb.types import DataType

schema = Schema(columns=[
    Column(name="name", data_type=DataType.TEXT),
    Column(name="power", data_type=DataType.INTEGER),
])

table = Table(name="cards", schema=schema)

# Insert -- the table assigns an ID automatically
table.insert({"name": "Pikachu", "power": 55})
table.insert({"name": "Charmander", "power": 52})

# Select -- get all records
all_cards = table.select()  # returns [Record(...), Record(...)]

# Select with a filter
strong = table.select(where=lambda row: row["power"] > 50)

# Update -- change Pikachu's power
table.update(record_id=1, values={"power": 60})

# Delete -- remove a record
table.delete(record_id=2)
```

## How IDs Work

Every time you insert a record, the table gives it the next available number.
The first record gets ID 1, the next gets ID 2, and so on. IDs are never
reused -- if you delete record 2, the next insert gets ID 3, not 2.

This is just like a library giving every book an accession number. Even if a
book is lost, its number is retired.

## What We Test

- A new table starts empty.
- Inserting a record increases the row count.
- Each inserted record gets a unique, increasing ID.
- Selecting with no filter returns all records.
- Selecting with a filter returns only matching records.
- Updating changes a record's values.
- Deleting removes a record.
- Inserting invalid data (wrong types) raises a SchemaError.

## Next Up

Right now our table lives only in memory -- if you close Python, your data
vanishes like a sandcastle at high tide. Next we'll build a
**serializer** to convert tables to text. Head to
[Serializer](serializer.md).
