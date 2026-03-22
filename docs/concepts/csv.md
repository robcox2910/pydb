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

## What We Test

- Exporting creates a valid CSV file with headers.
- Importing adds rows with correct type conversion.
- Round-trip (export → import) preserves all data.
- Missing files raise a clear error.
- Wrong types raise a clear error with the row number.

## Next Up

Head to [Write-Ahead Log](wal.md) to learn how databases survive crashes.
