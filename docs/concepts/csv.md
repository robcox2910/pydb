# CSV Import & Export

## Trading Cards with the Outside World

So far, all our data lives inside PyDB. But what if you have a
spreadsheet full of Pokémon stats, or you want to share your database
with a friend who uses Excel?

**CSV** (Comma-Separated Values) is the universal language of
spreadsheets. It's a plain text file where each line is a row and
values are separated by commas:

```
name,type,power
Pikachu,Electric,55
Charmander,Fire,52
Squirtle,Water,48
```

Almost every tool can read and write CSV -- Excel, Google Sheets,
Python, and now PyDB.

## Exporting: Database → Spreadsheet

```python
from pydb.csv_io import export_table

table = db.get_table("cards")
export_table(table, "cards.csv")
# Creates a CSV file with headers and all rows
```

The first line of the file is the **header** (column names), and each
line after that is one record from the table.

## Importing: Spreadsheet → Database

```python
from pydb.csv_io import import_csv

table = db.get_table("cards")
import_csv(table, "new_cards.csv")
# Adds all rows from the CSV into the table
```

The CSV header must match the table's column names. Values are
automatically converted to the right type -- "55" becomes the integer
`55`, "true" becomes the boolean `True`.

## Empty Fields and NULL

A [NULL](null.md) and an empty piece of text (`""`) are different things,
but in a CSV file both look like nothing between two commas. PyDB tells
them apart with **quotes**: it writes every real value in quotes, and
NULL as a *bare* empty field with no quotes at all.

```
"name","note"
"Pikachu",          <- note is NULL (unknown)
"Raichu",""         <- note is the empty string
```

Importing, PyDB reads the file the same way:

- A bare empty field is NULL, whatever the column type.
- A quoted `""` is the empty string in a TEXT column, and NULL in any
  other column (an empty number is no number at all).
- Unquoted values, as in a hand-written file, still work.
- An empty field in a NOT NULL column is an error (a quoted `""` is fine
  in a NOT NULL TEXT column), and a nullable column missing from the
  header is NULL in every imported row.

One corner case: a table with a **single** column can't export a row
whose value is NULL -- that row would be a completely blank line, which
CSV readers skip -- so `export_table` raises a `CSVError` instead.

## What We Test

- Exporting creates a valid CSV file with headers.
- Importing adds rows with correct type conversion.
- Round-trip (export → import) preserves all data.
- Missing files raise a clear error.
- Wrong types raise a clear error with the row number.
- NULL is exported as a bare empty field and `""` as a quoted one, and
  both come back exactly -- even several NULLs in a UNIQUE TEXT column.

## Next Up

Head to [Write-Ahead Log](wal.md) to learn how databases survive crashes.
