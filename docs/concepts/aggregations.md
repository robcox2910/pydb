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

## Next Up

We now have a powerful query language! Next, head to the
[Query Planner](query-planner.md) to learn how the database decides
the fastest way to answer your questions.
