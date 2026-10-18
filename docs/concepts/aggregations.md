# Aggregations

## Sorting Cards into Piles

Imagine you dump all your trading cards on the table and sort them into
piles by type: Electric pile, Fire pile, Water pile. Then you count how
many cards are in each pile, add up their power levels, and find the
strongest card in each group.

That's exactly what **aggregations** do in a database.

```sql
SELECT type, COUNT(*), SUM(power), MAX(power)
FROM cards
GROUP BY type
```

```
┌──────────┬──────────┬────────────┬────────────┐
│ type     │ COUNT(*) │ SUM(power) │ MAX(power) │
├──────────┼──────────┼────────────┼────────────┤
│ Electric │        2 │        110 │         60 │
│ Fire     │        1 │         52 │         52 │
│ Water    │        1 │         48 │         48 │
└──────────┴──────────┴────────────┴────────────┘
```

## The Five Aggregate Functions

| Function | What it does | Analogy |
|----------|-------------|---------|
| `COUNT(*)` | Count the rows in each group | How many cards in each pile? |
| `SUM(col)` | Add up all values | Total power of each pile? |
| `AVG(col)` | Calculate the average | Average power per pile? |
| `MIN(col)` | Find the smallest value | Weakest card in each pile? |
| `MAX(col)` | Find the largest value | Strongest card in each pile? |

## Skipping the Unknowns

What if some cards have no power written on them -- their power is
[NULL](null.md)? You can't add "unknown" to a total, so:

- `COUNT(*)` counts every row (every card on the table).
- `COUNT(power)` counts only the cards whose power is known.
- `SUM`, `AVG`, `MIN` and `MAX` skip the NULLs.
- If there is no known value at all -- every power is NULL, or the pile is
  empty -- `COUNT` gives 0 and the others give NULL. The total of nothing
  we know is itself unknown.

```sql
SELECT COUNT(*), COUNT(power), AVG(power) FROM cards
-- AVG is worked out from the known powers only.
```

## GROUP BY: Making the Piles

Without `GROUP BY`, an aggregate function works on the **entire table**:

```sql
SELECT COUNT(*) FROM cards     -- How many cards total?
SELECT AVG(power) FROM cards   -- Average power across all cards?
```

With `GROUP BY`, the table is split into groups first, and the aggregate
runs on each group separately:

```sql
SELECT type, COUNT(*) FROM cards GROUP BY type
-- How many cards of each type?
```

Cards whose type is NULL all go into one pile of their own.

## HAVING: Throwing Away Small Piles

`HAVING` is like `WHERE`, but for groups instead of individual rows.
It filters *after* grouping:

```sql
SELECT type, COUNT(*) FROM cards
GROUP BY type
HAVING COUNT(*) > 1
-- Only show types that have more than one card
```

Think of it this way:
- **WHERE** filters individual cards *before* sorting into piles.
- **HAVING** filters entire piles *after* counting.

HAVING uses the same three-valued logic as WHERE: `HAVING SUM(power) > 100`
leaves out a pile whose sum is NULL. Ask `HAVING SUM(power) IS NULL` to
find those piles.

## Aggregates Without GROUP BY

You can use aggregate functions without `GROUP BY` to summarise the
whole table:

```sql
SELECT COUNT(*), AVG(power), MAX(power) FROM cards
```

This treats the entire table as one big pile and gives you a single
result row.

## What We Test

- COUNT(*) counts all rows.
- SUM, AVG, MIN, MAX compute correctly.
- GROUP BY creates separate groups.
- Multiple aggregate functions in one query work.
- HAVING filters groups after aggregation.
- Aggregates without GROUP BY work on the whole table.
- Mixing aggregate functions with regular columns (via GROUP BY) works.
- NULLs are skipped, and an aggregate over no known values is NULL
  (`COUNT` is 0).

## Next Up

We now have a powerful query language! Next, head to the
[Query Planner](query-planner.md) to learn how the database decides
the fastest way to answer your questions.
