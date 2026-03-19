# JOINs

## Stapling Two Binders Together

You have two card binders. One lists **trainers** (their name and town).
The other lists **Pokémon** (their name, type, and which trainer owns
them).

```
TRAINERS                    POKEMON
┌───────┬──────────┐        ┌────────────┬──────────┬─────────┐
│ name  │ town     │        │ name       │ type     │ trainer │
├───────┼──────────┤        ├────────────┼──────────┼─────────┤
│ Ash   │ Pallet   │        │ Pikachu    │ Electric │ Ash     │
│ Misty │ Cerulean │        │ Starmie    │ Water    │ Misty   │
│ Brock │ Pewter   │        │ Charmander │ Fire     │ Ash     │
└───────┴──────────┘        └────────────┴──────────┴─────────┘
```

Now someone asks: "Show me each Pokémon with its trainer's town."

That information isn't in either binder alone. You need to **cross-
reference** them -- look up the trainer name in the Pokémon binder, find
that trainer in the Trainers binder, and staple the matching rows
together.

That's a **JOIN**.

```sql
SELECT pokemon.name, trainers.town
FROM pokemon
JOIN trainers ON pokemon.trainer = trainers.name
```

Result:

```
┌────────────┬──────────┐
│ name       │ town     │
├────────────┼──────────┤
│ Pikachu    │ Pallet   │
│ Starmie    │ Cerulean │
│ Charmander │ Pallet   │
└────────────┴──────────┘
```

## How It Works

The JOIN checks every combination of rows from the two tables. For each
pair, it tests the **ON condition**. If the condition is true, it
staples those two rows together into one result row.

```
For each row in pokemon:
    For each row in trainers:
        If pokemon.trainer == trainers.name:
            → Include this combined row in the result
```

This is called a **nested loop join** -- the simplest way to do it. Real
databases have faster methods, but the idea is always the same: find
matching rows across tables.

## Dot Notation

When two tables have columns with the same name, you need to say *which*
table you mean. That's what **dot notation** is for:

- `trainers.name` → the "name" column from the trainers table
- `pokemon.name` → the "name" column from the pokemon table

It's like saying "Ash's name" vs. "Pikachu's name" -- the dot tells you
whose name you're talking about.

## What We Test

- A JOIN with matching rows produces combined results.
- A JOIN with no matches returns an empty result.
- Dot notation selects columns from the correct table.
- WHERE clauses work after a JOIN.
- ORDER BY works on joined results.
- LIMIT works on joined results.
- Missing table in JOIN raises an error.

## Next Up

Now that we can combine tables, we need a way to *summarise* them.
Head to [Aggregations](aggregations.md) to learn about COUNT, SUM,
AVG, and GROUP BY.
