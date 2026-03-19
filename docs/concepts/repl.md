# REPL

## Your Own Database Prompt

A **REPL** stands for **Read-Eval-Print Loop**. It's an interactive
prompt where you type commands and see results immediately -- just like
the Python `>>>` prompt, but for SQL.

```
pydb> SELECT * FROM cards
┌────┬────────────┬──────────┬───────┐
│ id │ name       │ type     │ power │
├────┼────────────┼──────────┼───────┤
│  1 │ Pikachu    │ Electric │    55 │
│  2 │ Charmander │ Fire     │    52 │
│  3 │ Squirtle   │ Water    │    48 │
└────┴────────────┴──────────┴───────┘
3 rows returned

pydb> SELECT name FROM cards WHERE power > 50 ORDER BY name
┌────────────┐
│ name       │
├────────────┤
│ Charmander │
│ Pikachu    │
└────────────┘
2 rows returned

pydb> .quit
Goodbye!
```

## How to Use It

```bash
# Start the REPL with a database directory
pydb my_database

# Or just start with an in-memory database
pydb
```

## Special Commands

Besides SQL, the REPL understands a few **dot commands**:

| Command | What it does |
|---------|-------------|
| `.quit` or `.exit` | Leave the REPL |
| `.tables` | List all tables |
| `.schema <table>` | Show a table's column definitions |
| `.save` | Save all tables to disk |
| `.help` | Show available commands |

## What We Test

- Dot commands return the correct output (`.tables`, `.schema`, `.help`).
- `.quit` and `.exit` signal the REPL to stop.
- SQL queries are parsed, executed, and formatted as pretty tables.
- Invalid SQL shows a clear parse error.
- Querying a missing table shows a clear query error.
- Empty results display a helpful "(empty result set)" message.

## How It Works

The REPL is just a loop:

1. **Read** -- show the `pydb>` prompt and wait for input.
2. **Eval** -- parse the SQL and execute it against the database.
3. **Print** -- format the results as a nice table.
4. **Loop** -- go back to step 1.

That's it! The REPL ties together everything we've built: the SQL
parser, the query executor, the database, and the storage engine.

## Next Up

The REPL is the front door to your database. Now explore more advanced
features like [Transactions](transactions.md) for the "no take-backs"
rule.
