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

The database maintains a **catalog** -- a list of all table names and their
schemas. This is saved as a special file called `catalog.json` in the data
directory:

```
my_game_db/
├── catalog.json        ← knows which tables exist
├── high_scores.json    ← table data
└── players.json        ← table data
```

Think of the catalog as the **index card pinned to the door** of the
collection room. Before you walk in, you can check what's inside.

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
*fast* -- head to the index concepts to learn about B-trees, or jump to the
query engine to learn how SQL works.
