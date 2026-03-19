# Database

## The Collection Room

You've got your card binder (table), you know how to write cards down
(serializer), and you've got a shelf to store them (storage engine). Now
imagine a whole **room** full of shelves, each holding a different binder --
one for Pokémon cards, one for football stickers, one for high scores.

That room is the **database**. It:

- Keeps track of which tables exist (the **catalog**).
- Creates new tables and drops old ones.
- Saves and loads tables through the storage engine.
- Makes sure no two tables share the same name.

```
┌───────────────────────────────────────────┐
│  DATABASE: my_game_db                      │
│                                           │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  │
│  │ players │  │ scores  │  │ items    │  │
│  │  table  │  │  table  │  │  table   │  │
│  └────┬────┘  └────┬────┘  └────┬─────┘  │
│       │            │            │         │
│  ─────┴────────────┴────────────┴─────    │
│              Storage Engine               │
│  ─────────────────────────────────────    │
│                 Disk                      │
└───────────────────────────────────────────┘
```

## Using the Database

```python
from pydb.database import Database
from pydb.schema import Schema, Column
from pydb.types import DataType

# Open (or create) a database in a directory
db = Database(path="my_game_db")

# Create a table
schema = Schema(columns=[
    Column(name="name", data_type=DataType.TEXT),
    Column(name="score", data_type=DataType.INTEGER),
])
db.create_table("high_scores", schema)

# Use the table
table = db.get_table("high_scores")
table.insert({"name": "Alice", "score": 9001})

# Save everything to disk
db.save()

# Later... load it all back
db2 = Database(path="my_game_db")
db2.load()
scores = db2.get_table("high_scores")
```

## The Catalog

The database keeps a **catalog** in memory -- a list of which tables exist.
When you call `save()`, each table gets its own `.json` file:

```
my_game_db/
├── high_scores.json    ← table data
└── players.json        ← table data
```

When you call `load()`, the database scans the directory, reads every
`.json` file, and rebuilds its catalog automatically. Think of it like
walking into the collection room and reading the labels on each binder.

## What We Test

- Creating a database makes the data directory.
- Creating a table adds it to the catalog.
- Getting a table by name returns the correct table.
- Getting a non-existent table raises an error.
- Creating a duplicate table raises an error.
- Dropping a table removes it from the catalog and deletes its file.
- Saving and loading round-trips all tables correctly.
- The table list shows all current table names.

## Next Up

We now have a fully persistent database! The next big frontier is making it
*fast*. Head to [Indexes](indexes.md) to learn about B-trees.
