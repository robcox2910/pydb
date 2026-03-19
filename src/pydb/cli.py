"""PyDB command-line interface -- an interactive SQL REPL.

The REPL (Read-Eval-Print Loop) is the front door to your database.
Type SQL, see results. Type dot commands for meta-operations.
"""

import sys

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.formatter import format_results
from pydb.sql_parser import ParseError, parse_sql
from pydb.sql_tokenizer import TokenizerError

PROMPT = "pydb> "
CONTINUE_PROMPT = "   ...> "

HELP_TEXT = """
PyDB -- an educational database engine

SQL commands:
  SELECT columns FROM table [WHERE ...] [ORDER BY ...] [LIMIT n]

Dot commands:
  .tables          List all tables in the database
  .schema <table>  Show a table's column definitions
  .save            Save all tables to disk
  .help            Show this help message
  .quit / .exit    Exit the REPL
""".strip()


def _handle_dot_command(line: str, database: Database) -> str | None:
    """Handle a dot command and return output text, or None to quit.

    Args:
        line: The dot command (e.g., ".tables").
        database: The active database.

    Returns:
        Output text to display, or None to signal exit.

    """
    parts = line.strip().split(maxsplit=1)
    command = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    match command:
        case ".quit" | ".exit":
            return None
        case ".help":
            return HELP_TEXT
        case ".tables":
            names = database.table_names()
            return "\n".join(names) if names else "(no tables)"
        case ".schema":
            return _handle_schema(arg, database)
        case ".save":
            database.save()
            return "All tables saved to disk."
        case _:
            return f"Unknown command: {command!r}. Type .help for help."


def _handle_schema(arg: str, database: Database) -> str:
    """Handle the .schema dot command.

    Args:
        arg: The table name argument.
        database: The active database.

    Returns:
        Schema description or an error message.

    """
    if not arg:
        return "Usage: .schema <table_name>"
    try:
        table = database.get_table(arg)
    except Exception:  # noqa: BLE001
        return f"Table {arg!r} not found"
    lines = [
        f"Table: {table.name}",
        *[f"  {col.name}: {col.data_type.value}" for col in table.schema.columns],
    ]
    return "\n".join(lines)


def _execute_sql(sql: str, database: Database) -> str:
    """Parse and execute a SQL statement, returning formatted output.

    Args:
        sql: The SQL text to execute.
        database: The active database.

    Returns:
        Formatted result text or error message.

    """
    try:
        query = parse_sql(sql)
    except (ParseError, TokenizerError) as exc:
        return f"Parse error: {exc}"

    try:
        results = execute(query, database)
    except QueryError as exc:
        return f"Query error: {exc}"

    return format_results(results, columns=query.columns or None)


def repl(database: Database) -> None:
    """Run the interactive REPL loop.

    Args:
        database: The database to query against.

    """
    _write("Welcome to PyDB! Type .help for help, .quit to exit.\n")

    while True:
        try:
            line = input(PROMPT).strip()
        except EOFError, KeyboardInterrupt:
            _write("\nGoodbye!")
            break

        if not line:
            continue

        if line.startswith("."):
            result = _handle_dot_command(line, database)
            if result is None:
                _write("Goodbye!")
                break
            _write(result)
        else:
            output = _execute_sql(line, database)
            _write(output)


def _write(text: str) -> None:
    """Write text to stdout followed by a newline."""
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def main() -> None:
    """Entry point for the pydb CLI."""
    path = sys.argv[1] if len(sys.argv) > 1 else "pydb_data"
    database = Database(path=path)

    # Load any existing tables from disk.
    database.load()

    repl(database)
