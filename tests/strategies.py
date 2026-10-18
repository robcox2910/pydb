"""Strategies -- recipes for random keys and card names, shared by the property tests.

A strategy tells Hypothesis how to invent one kind of input. Each one here
is a named function, named for what it makes, with a one-line docstring.
``tests/test_properties.py`` and ``tests/test_model.py`` both use them.
"""

from hypothesis import strategies as st

from pydb.btree import MIN_ORDER

# B-Tree keys are drawn from 0 to this. A small range means the same key
# turns up again and again, so stories really do add a second record to a
# key, and really do delete keys that are in the tree.
LARGEST_BTREE_KEY = 30

# The biggest B-Tree order tried. Small orders split and merge drawers
# after only a few cards, which is where the tricky code lives.
LARGEST_BTREE_ORDER = 6


def btree_orders() -> st.SearchStrategy[int]:
    """Make a B-Tree order (how many children a drawer may have), from 3 to 6."""
    # BTree refuses an order below MIN_ORDER (see BTree.__init__).
    return st.integers(min_value=MIN_ORDER, max_value=LARGEST_BTREE_ORDER)


def btree_keys() -> st.SearchStrategy[int]:
    """Make a small whole number to use as a B-Tree key."""
    return st.integers(min_value=0, max_value=LARGEST_BTREE_KEY)


def card_names() -> st.SearchStrategy[str | None]:
    """Make a card name: any text at all (quotes, emoji, empty), or NULL."""
    return st.one_of(st.none(), st.text())
