"""Property-based tests -- rules that must hold for *every* input.

An ordinary test checks one example someone thought of: "save a table
holding Pikachu with power 55, load it back, and Pikachu still has 55". A
property test states a rule instead, such as "whatever is in the table,
loading the saved file gives back exactly that", and lets Hypothesis invent
hundreds of tables to try to break it. If one breaks the rule, Hypothesis
*shrinks* it down to the smallest input that still fails, and shows you that.

Each test below says its rule in plain English in its docstring. The
strategies at the top are recipes for making random inputs; each one is
named for what it makes.

The step-by-step checks of SQL against SQLite, and of the B-Tree against a
plain dict, live in ``tests/test_model.py``. See
``docs/property-testing.md`` for the walk-through.
"""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pydb.btree import BTree, BTreeNode
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.serializer import StoredTable, deserialize_table, serialize_table_data
from pydb.storage import StorageEngine, StorageError
from pydb.types import DataType
from tests.strategies import btree_keys, btree_orders

# The thorough profile runs 2,000 examples of each test, and shrinking a
# failure takes a few seconds more, so these get longer than the 10 seconds
# pyproject.toml gives every other test.
pytestmark = pytest.mark.timeout(300)

# --- Strategies: recipes for random inputs -----------------------------------
# btree_keys() and btree_orders() live in tests/strategies.py, shared with
# the model check.

# A table here has at most this many columns and rows. Bigger tables find
# the same bugs, take longer to make, and a failure shrinks to a small one.
MOST_COLUMNS = 4
MOST_ROWS = 5

# A junk JSON document is built from at most this many plain pieces.
MOST_PIECES_OF_JUNK = 10


def values_of(data_type: DataType) -> st.SearchStrategy[Value]:
    """Make any value a column of this type can hold, or NULL."""
    match data_type:
        case DataType.TEXT:
            kind: st.SearchStrategy[Value] = st.text()
        case DataType.INTEGER:
            kind = st.integers()
        case DataType.FLOAT:
            kind = st.floats()  # NaN and infinity too: they are floats
        case DataType.BOOLEAN:
            kind = st.booleans()
        case DataType.DATE:
            kind = st.dates()
        case DataType.TIMESTAMP:
            # A table stores every TIMESTAMP in UTC (see check_value in pydb/types.py).
            kind = st.datetimes(timezones=st.just(UTC)).map(_without_fold)
    return st.one_of(st.none(), kind)


def _without_fold(moment: datetime) -> datetime:
    """Return the same moment with ``fold`` set to 0."""
    # ``fold`` tells apart the two 1:30s you get when the clocks go back an
    # hour. UTC never changes its clocks, so in UTC it means nothing: both
    # versions are the same moment, ``==`` says so, and JSON has nowhere to
    # keep it. Only ``repr`` would show the difference.
    return moment.replace(fold=0)


def columns() -> st.SearchStrategy[Column]:
    """Make one column: any name, any type, and any mix of constraints."""
    return st.builds(
        Column,
        name=st.text(),
        data_type=st.sampled_from(DataType),
        primary_key=st.booleans(),
        not_null=st.booleans(),
        unique=st.booleans(),
    )


@st.composite
def filled_columns(draw: st.DrawFn, row_count: int) -> tuple[Column, list[Value]]:
    """Make one column and the ``row_count`` values the rows hold in it."""
    column = draw(columns())
    values = draw(st.lists(values_of(column.data_type), min_size=row_count, max_size=row_count))
    return column, values


def _name_of(filled: tuple[Column, list[Value]]) -> str:
    """Return a filled column's name (so no two columns share one)."""
    return filled[0].name


@st.composite
def stored_tables(draw: st.DrawFn) -> StoredTable:
    """Make everything a table file holds: name, schema, rows, next ID and indexes."""
    row_count = draw(st.integers(min_value=0, max_value=MOST_ROWS))
    # Each column comes with its own values, so when Hypothesis shrinks a
    # failing table it can drop a column and its values in one go.
    filled = draw(
        st.lists(filled_columns(row_count), min_size=1, max_size=MOST_COLUMNS, unique_by=_name_of)
    )
    schema = Schema([column for column, _ in filled])
    records = [
        Record(row + 1, {column.name: values[row] for column, values in filled})
        for row in range(row_count)
    ]
    # The next ID must be past every record ID already handed out.
    next_id = draw(st.integers(min_value=row_count + 1))
    indexes = draw(st.lists(st.tuples(st.text(), st.sampled_from(schema.column_names))))
    return StoredTable(draw(st.text()), schema, records, next_id, indexes)


def any_bytes() -> st.SearchStrategy[bytes]:
    """Make any run of raw bytes, including the empty one and ones that aren't text."""
    return st.binary()


def any_json() -> st.SearchStrategy[object]:
    """Make any JSON value at all: null, true, numbers, text, lists and objects."""
    return st.recursive(
        st.one_of(st.none(), st.booleans(), st.integers(), st.floats(), st.text()),
        _lists_and_objects_of,
        max_leaves=MOST_PIECES_OF_JUNK,
    )


def _lists_and_objects_of(
    children: st.SearchStrategy[object],
) -> st.SearchStrategy[list[object] | dict[str, object]]:
    """Make a JSON list, or a JSON object, holding values made by ``children``."""
    return st.one_of(st.lists(children), st.dictionaries(st.text(), children))


def table_shaped_json() -> st.SearchStrategy[dict[str, object]]:
    """Make a JSON object with a table file's labels, but anything at all under them."""
    return st.fixed_dictionaries(
        {
            "name": any_json(),
            "schema": st.one_of(any_json(), st.fixed_dictionaries({"columns": any_json()})),
            "next_id": any_json(),
            "records": any_json(),
        },
        optional={"indexes": any_json()},
    )


# --- Helpers -----------------------------------------------------------------


def as_text(table: StoredTable) -> str:
    """Write out every part of a stored table, with each value's type showing."""
    # Record's ``==`` only compares record IDs, and ``==`` thinks True == 1
    # == 1.0, so we compare ``repr`` text instead: it shows every value, and
    # writes True as ``True``, 1 as ``1`` and 1.0 as ``1.0``.
    rows = [(r.record_id, r.data) for r in table.records]
    return repr((table.name, table.schema.columns, rows, table.next_id, table.indexes))


def load_or_refuse(folder: Path) -> StoredTable | None:
    """Load table ``cards`` from ``folder``, or return None if PyDB refuses it with StorageError.

    Any *other* exception is not caught, so it escapes and fails the test.
    """
    try:
        return StorageEngine(folder).load_stored_table("cards")
    except StorageError:
        return None


def leaf_depths(node: BTreeNode, depth: int = 0) -> set[int]:
    """Return how far down each bottom drawer (leaf) sits below ``node``."""
    if node.leaf:
        return {depth}
    return {d for child in node.children for d in leaf_depths(child, depth + 1)}


def drawers_below(node: BTreeNode) -> list[BTreeNode]:
    """Return every drawer (node) under ``node``, not counting ``node`` itself."""
    drawers: list[BTreeNode] = []
    for child in node.children:
        drawers.append(child)
        drawers.extend(drawers_below(child))
    return drawers


# --- Save and load: whatever you save comes back -------------------------------


@given(table=stored_tables())
def test_save_then_load_gives_back_the_same_table(table: StoredTable) -> None:
    """Loading a saved table gives back exactly the same name, schema, rows and types."""
    saved = serialize_table_data(
        table.name, table.schema, table.records, table.next_id, indexes=table.indexes
    )
    assert as_text(deserialize_table(saved)) == as_text(table)


# --- Loading junk: a bad table file is refused, never a crash -------------------


@given(junk=any_bytes())
def test_any_bytes_on_disk_load_or_raise_storage_error(junk: bytes) -> None:
    """Whatever bytes are in a table file, loading gives a table or a StorageError, never another error."""
    with tempfile.TemporaryDirectory() as folder:
        (Path(folder) / "cards.json").write_bytes(junk)
        load_or_refuse(Path(folder))


@given(document=table_shaped_json())
def test_any_table_shaped_json_loads_or_raises_storage_error(document: object) -> None:
    """Whatever is under a table file's labels, loading gives a table or a StorageError, never another error."""
    with tempfile.TemporaryDirectory() as folder:
        (Path(folder) / "cards.json").write_text(json.dumps(document), encoding="utf-8")
        load_or_refuse(Path(folder))


# --- The B-Tree: sorted, and balanced, whatever you do to it -------------------


@given(order=btree_orders(), added=st.lists(btree_keys()), removed=st.lists(btree_keys()))
def test_btree_stays_sorted_and_balanced(order: int, added: list[int], removed: list[int]) -> None:
    """After any inserts and deletes, the B-Tree holds exactly the right keys, in order, and stays balanced."""
    tree = BTree(order=order)
    for record_id, key in enumerate(added):
        tree.insert(key, record_id)
    for key in removed:
        for record_id in tree.search(key):
            tree.delete(key, record_id)

    assert tree.all_keys() == sorted(set(added) - set(removed))
    # Balanced: every bottom drawer is the same distance from the top.
    assert len(leaf_depths(tree.root)) == 1
    # No drawer is over-full, and none but the top one is less than half full.
    assert len(tree.root.keys) <= order - 1
    # The fewest keys a drawer may hold: half the order, rounded up, minus one.
    half_full = (order + 1) // 2 - 1
    assert all(half_full <= len(node.keys) <= order - 1 for node in drawers_below(tree.root))
