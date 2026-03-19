# Views

## A Label on the Binder

Instead of physically reorganizing your cards, you stick a label on the
binder: "strong cards = everything with power > 50." Every time you
open to that label, it automatically shows you the matching cards.

A **view** is a saved query that acts like a virtual table. It doesn't
store data -- it re-runs the query every time you access it.

```sql
-- Save the query as a view
CREATE VIEW strong_cards AS
  SELECT name, power FROM cards WHERE power > 50

-- Now use it like a table
SELECT * FROM strong_cards
```

Views are useful for:
- **Simplifying complex queries** -- give a short name to a long query.
- **Hiding complexity** -- other users see "strong_cards" without
  knowing the WHERE clause behind it.
- **Security** -- show only certain columns or rows.
