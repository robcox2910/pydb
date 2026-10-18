"""Model checks -- PyDB must behave exactly like things we already trust.

A property test checks one rule for many inputs. A *model check* goes
further: Hypothesis invents a random *story* -- insert this, delete that,
look up the other -- and plays it on two things at once:

- the real PyDB code;
- a **model**: something simple that we already trust to be right.

After every step the two must agree. If they ever disagree, Hypothesis
shrinks the story to the shortest list of steps that still goes wrong.

There are two machines here:

- ``CardTableMachine`` types SQL into PyDB and does the same thing to
  SQLite, the database built into Python (``sqlite3``), which millions of
  programs rely on.
- ``BTreeMachine`` drives a B-Tree and a plain dict of sets side by side.

Neither model is a copy of PyDB's code, so when the two disagree, PyDB is
the one to suspect.

See ``docs/property-testing.md`` for the walk-through.
"""

import sqlite3
import tempfile
import unittest
from typing import cast

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, invariant, rule

from pydb.btree import BTree
from pydb.database import Database
from pydb.errors import QueryError
from pydb.executor import ExecuteResult, execute
from pydb.sql_parser import parse_sql
from tests.strategies import btree_keys, btree_orders, card_names

# The thorough profile runs 2,000 examples of each test, and shrinking a
# failure takes a few seconds more, so these get longer than the 10 seconds
# pyproject.toml gives every other test.
pytestmark = pytest.mark.timeout(300)

# --- Strategies and helpers for the card table ---------------------------------

# SQLite keeps whole numbers in 64 bits, so this is the biggest one it can
# hold (https://www.sqlite.org/datatype3.html). PyDB's SQL has no minus
# sign (``_read_number`` in pydb/sql_tokenizer.py reads digits only), so
# numbers start at 0.
LARGEST_SQLITE_INTEGER = 2**63 - 1

CREATE_CARDS = "CREATE TABLE cards (num INTEGER PRIMARY KEY, name TEXT)"


def card_numbers() -> st.SearchStrategy[int]:
    """Make a card number: any whole number both PyDB's SQL and SQLite can hold."""
    return st.integers(min_value=0, max_value=LARGEST_SQLITE_INTEGER)


def sql_text(name: str | None) -> str:
    """Write a name as a SQL literal: NULL, or quoted with each ``'`` doubled."""
    if name is None:
        return "NULL"
    return "'" + name.replace("'", "''") + "'"


def rows_word(count: int) -> str:
    """Return "row" for one and "rows" for any other count, as PyDB writes it."""
    return "row" if count == 1 else "rows"


# --- Machine 1: SQL typed into PyDB and into SQLite ----------------------------


# why: 20 steps find the same bugs, and 50 (the default) is most of the run time.
@settings(stateful_step_count=20)
class CardTableMachine(RuleBasedStateMachine):
    """Play random INSERTs, UPDATEs, DELETEs and SELECTs on PyDB and on SQLite."""

    # Card numbers we've used before. Two random numbers almost never match,
    # so rules also pick from here to insert a number twice, or to find,
    # rename or delete a card that really is (or was) in the table.
    numbers = Bundle("numbers")

    def __init__(self) -> None:
        """Start with an empty cards table in PyDB and in SQLite."""
        super().__init__()
        self.folder = tempfile.TemporaryDirectory()
        self.db = Database(self.folder.name)
        self.pydb(CREATE_CARDS)
        self.model = sqlite3.connect(":memory:")
        self.model.execute(CREATE_CARDS)

    def pydb(self, sql: str) -> ExecuteResult:
        """Run one SQL statement on PyDB."""
        return execute(parse_sql(sql), self.db)

    @rule(target=numbers, num=st.one_of(numbers, card_numbers()), name=card_names())
    def insert_card(self, num: int, name: str | None) -> int:
        """Insert a card in both: PyDB refuses it exactly when SQLite does."""
        sql = f"INSERT INTO cards VALUES ({num}, {sql_text(name)})"
        try:
            self.model.execute("INSERT INTO cards VALUES (?, ?)", (num, name))
        except sqlite3.IntegrityError:  # that number is taken
            with pytest.raises(QueryError):
                self.pydb(sql)
        else:
            assert self.pydb(sql) == [{"result": "1 row inserted"}]
        return num

    @rule(num=st.one_of(numbers, card_numbers()), name=card_names())
    def rename_card(self, num: int, name: str | None) -> None:
        """Rename a card in both: PyDB says it changed as many rows as SQLite did."""
        sql = f"UPDATE cards SET name = {sql_text(name)} WHERE num = {num}"
        count = self.model.execute("UPDATE cards SET name = ? WHERE num = ?", (name, num)).rowcount
        assert self.pydb(sql) == [{"result": f"{count} {rows_word(count)} updated"}]

    @rule(num=st.one_of(numbers, card_numbers()))
    def delete_card(self, num: int) -> None:
        """Delete a card from both: PyDB says it removed as many rows as SQLite did."""
        count = self.model.execute("DELETE FROM cards WHERE num = ?", (num,)).rowcount
        result = self.pydb(f"DELETE FROM cards WHERE num = {num}")
        assert result == [{"result": f"{count} {rows_word(count)} deleted"}]

    @rule(num=st.one_of(numbers, card_numbers()))
    def find_card(self, num: int) -> None:
        """Look a card up by number: PyDB finds the same card as SQLite, or none."""
        found = self.pydb(f"SELECT * FROM cards WHERE num = {num}")
        expected = self.model.execute("SELECT * FROM cards WHERE num = ?", (num,)).fetchall()
        assert [(row["num"], row["name"]) for row in found] == expected

    @invariant()
    def same_cards_in_the_same_order(self) -> None:
        """After every step, both tables hold the same cards, sorted by number and by name."""
        by_num = self.pydb("SELECT * FROM cards ORDER BY num")
        expected = self.model.execute("SELECT * FROM cards ORDER BY num").fetchall()
        assert [(row["num"], row["name"]) for row in by_num] == expected
        # Both databases sort NULL before every name (see pydb/nulls.py).
        by_name = self.pydb("SELECT name FROM cards ORDER BY name")
        names = self.model.execute("SELECT name FROM cards ORDER BY name").fetchall()
        assert [(row["name"],) for row in by_name] == names

    def teardown(self) -> None:
        """Close SQLite and delete PyDB's folder."""
        self.model.close()
        self.folder.cleanup()


# pytest runs this like any other test class: one run plays 100 random stories.
# (Hypothesis doesn't say what type ``TestCase`` is, so ``cast`` tells pyright.)
TestCardTableModel = cast("type[unittest.TestCase]", CardTableMachine.TestCase)


# --- Machine 2: a B-Tree beside a dict of sets ---------------------------------


# why: 20 steps find the same bugs, and 50 (the default) is most of the run time.
@settings(stateful_step_count=20)
class BTreeMachine(RuleBasedStateMachine):
    """Play random inserts, deletes and searches on a B-Tree and on a dict of sets."""

    # (key, record ID) pairs we've inserted before, so deletes can pick one
    # that really is (or was) in the tree.
    entries = Bundle("entries")

    def __init__(self) -> None:
        """Start with an empty dict; the tree is made once the order is chosen."""
        super().__init__()
        self.tree = BTree()
        self.model: dict[int, set[int]] = {}

    @initialize(order=btree_orders())
    def choose_order(self, order: int) -> None:
        """Make an empty tree whose drawers hold at most ``order - 1`` keys."""
        self.tree = BTree(order=order)

    @rule(target=entries, key=btree_keys(), record_id=st.integers(min_value=1))
    def insert(self, key: int, record_id: int) -> tuple[int, int]:
        """Add a record ID under a key, in both."""
        self.tree.insert(key, record_id)
        self.model.setdefault(key, set()).add(record_id)
        return (key, record_id)

    @rule(entry=st.one_of(entries, st.tuples(btree_keys(), st.integers(min_value=1))))
    def delete(self, entry: tuple[int, int]) -> None:
        """Remove a record ID from a key; the tree says True exactly when it was there."""
        key, record_id = entry
        was_there = record_id in self.model.get(key, set())
        if was_there:
            self.model[key].discard(record_id)
            if not self.model[key]:
                del self.model[key]
        assert self.tree.delete(key, record_id) == was_there

    @rule(key=btree_keys())
    def search(self, key: int) -> None:
        """Look a key up: the tree gives the dict's record IDs, smallest first."""
        assert self.tree.search(key) == sorted(self.model.get(key, set()))

    @rule(low=btree_keys(), high=btree_keys())
    def find_range(self, low: int, high: int) -> None:
        """Find every record ID with a key from ``low`` to ``high``, just like the dict."""
        expected = {rid for key, ids in self.model.items() if low <= key <= high for rid in ids}
        assert self.tree.find_range(low, high) == sorted(expected)

    @invariant()
    def same_keys_and_record_ids(self) -> None:
        """After every step, the tree holds the dict's keys in order, each with the dict's IDs."""
        assert self.tree.all_keys() == sorted(self.model)
        for key, record_ids in self.model.items():
            assert self.tree.search(key) == sorted(record_ids)


# pytest runs this like any other test class: one run plays 100 random stories.
# (Hypothesis doesn't say what type ``TestCase`` is, so ``cast`` tells pyright.)
TestBTreeModel = cast("type[unittest.TestCase]", BTreeMachine.TestCase)
