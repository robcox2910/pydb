"""Shared test fixtures and helpers for PyDB tests."""

from pathlib import Path

from pydb.database import Database
from pydb.executor import execute
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.types import DataType

# ── Shared named constants ──────────────────────────────────────────

POWER_55 = 55
POWER_52 = 52
POWER_48 = 48
POWER_60 = 60
RECORD_ID_1 = 1
RECORD_ID_2 = 2


def make_name_power_schema() -> Schema:
    """Create a standard test schema with name (TEXT) and power (INTEGER)."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def make_cards_db(tmp_path: Path) -> Database:
    """Create a database with a cards table containing 3 rows."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, type TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 'Electric', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 'Fire', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 'Water', 48)"), db)
    return db
