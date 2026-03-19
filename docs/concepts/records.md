# Records

## The Trading Card

Pick up any trading card. It has facts printed on it: a name, a type, a power
level, maybe a picture. Each fact sits in its own spot on the card.

A **record** is exactly that -- one card. In database language, people also call
it a **row**. Each fact on the card is a **field** (or a **column value**).

```
┌─────────────────────────┐
│  Name:   Pikachu        │  ← field
│  Type:   Electric       │  ← field
│  Power:  55             │  ← field
└─────────────────────────┘
        One record
```

## Records in PyDB

In PyDB, a record is a Python object that holds:

1. An **ID** -- a unique number so we can tell records apart, like a serial
   number stamped on each card.
2. **Data** -- a dictionary mapping column names to values.

```python
from pydb.record import Record

card = Record(record_id=1, data={"name": "Pikachu", "type": "Electric", "power": 55})
card["name"]   # "Pikachu"
card["power"]  # 55
```

## Why Not Just Use a Dictionary?

Good question! A plain `dict` would work for a while, but a `Record` gives us
two superpowers:

1. **Identity** -- every record has an `id`, so you can say "update record 7"
   instead of hunting through a list.
2. **Structure** -- later, when we add schemas, the `Record` will know how to
   validate itself ("power must be a number, not a word").

## What We Test

Following our TDD workflow, we wrote tests *first*:

- A record stores data and gives it back.
- A record knows its own ID.
- You can read fields using `record["column_name"]`.
- Two records with the same ID are considered equal.
- A record can list all its field names.

Only after those tests were written (and failing!) did we write the `Record`
class to make them pass.

## Next Up

A single card is nice, but a *collection* is where things get interesting.
Head to [Schema](schema.md) to learn how we define the template that every
card in a collection must follow.
