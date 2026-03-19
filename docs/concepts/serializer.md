# Serializer

## Writing Cards in Secret Code

Imagine you want to mail your trading cards to a friend. You can't just shove
the actual cards through the internet -- you need to **write them down** in a
way your friend can read and recreate them on their end.

That's what a **serializer** does. It takes a Python object (like a `Record` or
a `Schema`) and converts it into a string of text (JSON) that can be saved to a
file or sent over a network. The reverse process -- turning that text back into
a Python object -- is called **deserialization**.

```
Python object  →  serialize  →  JSON text  →  save to file
                                                    ↓
Python object  ←  deserialize ←  JSON text ←  read from file
```

## Why JSON?

There are lots of formats we could use (binary, XML, CSV...), but **JSON** is
perfect for learning because:

1. **It's human-readable** -- you can open the file and *see* your data.
2. **Python has built-in support** -- the `json` module does the heavy lifting.
3. **It's everywhere** -- web APIs, config files, and databases all use it.

Real production databases use compact binary formats for speed, but JSON lets
us *see* what's happening at every step.

## What Gets Serialized?

We need to save two things for each table:

1. **The schema** -- the column names and their types (the card template).
2. **The records** -- the actual data rows (the cards themselves).

```json
{
    "name": "cards",
    "schema": {
        "columns": [
            {"name": "name", "data_type": "TEXT"},
            {"name": "power", "data_type": "INTEGER"}
        ]
    },
    "next_id": 3,
    "records": [
        {"record_id": 1, "data": {"name": "Pikachu", "power": 55}},
        {"record_id": 2, "data": {"name": "Charmander", "power": 52}}
    ]
}
```

## What We Test

- A schema round-trips through serialize/deserialize without losing information.
- A record round-trips correctly.
- A full table (schema + records + next ID) round-trips correctly.
- Invalid JSON raises a clear error.
- JSON with missing fields raises a clear error.

## Next Up

Now that we can convert tables to text, we need somewhere to **put** that text.
Head to [Storage Engine](storage-engine.md) to learn how we save and load
files on disk.
