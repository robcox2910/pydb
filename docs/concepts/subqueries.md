# Subqueries

## A Question Inside a Question

Sometimes you need the answer to one question before you can ask another:

> "Show me all cards that are stronger than average."

You can't answer that without first knowing the average. A **subquery**
is a SELECT inside another SELECT -- the inner query runs first, and
its result feeds into the outer query.

```sql
SELECT * FROM cards
WHERE power > (SELECT AVG(power) FROM cards)
```

The database runs `(SELECT AVG(power) FROM cards)` first, gets (say)
`53.75`, then runs the outer query as `WHERE power > 53.75`.

## Scalar Subqueries

A **scalar subquery** returns a single value. You can use it anywhere
you'd use a number or string:

```sql
-- Cards stronger than average
SELECT * FROM cards WHERE power > (SELECT AVG(power) FROM cards)

-- Cards with the maximum power
SELECT * FROM cards WHERE power = (SELECT MAX(power) FROM cards)
```

If the subquery returns more than one row or column, it's an error.

## IN Subqueries

An **IN subquery** returns a list of values. You use it with the `IN`
keyword to check if a value appears in that list:

```sql
-- Trainers who own Electric Pokémon
SELECT * FROM trainers
WHERE name IN (SELECT trainer FROM pokemon WHERE type = 'Electric')
```

This is like asking: "First, find all trainers of Electric Pokémon.
Then, show me the details for those trainers."

## What We Test

- Scalar subqueries return a single value.
- IN subqueries return a list of values.
- Subqueries can reference different tables than the outer query.
- Nested subqueries work (a subquery inside a subquery).
- Subqueries with WHERE clauses filter correctly.

## Next Up

Head to [Views](views.md) to learn about saved queries that act like
virtual tables.
