"""Three-valued logic -- TRUE, FALSE and UNKNOWN.

A NULL is an empty box with a question mark on it. It is not zero and it
is not blank: it means *we don't know*. So when a question involves a
NULL -- "is the unknown number bigger than 5?" -- the honest answer is
"we don't know" too. That third answer is called **UNKNOWN**.

PyDB writes the three answers as ``True``, ``False`` and ``None``
(``UNKNOWN``). The rules for combining them are Kleene's:

* ``AND`` is FALSE if either side is FALSE (one "no" is enough), else
  UNKNOWN if either side is UNKNOWN, else TRUE.
* ``OR`` is TRUE if either side is TRUE (one "yes" is enough), else
  UNKNOWN if either side is UNKNOWN, else FALSE.
* ``NOT`` swaps TRUE and FALSE, and leaves UNKNOWN alone.

A WHERE, ON or HAVING clause keeps a row only when its answer is TRUE --
UNKNOWN rows are left out, just like FALSE ones (see ``is_true``).
"""

from collections.abc import Iterable
from typing import Any

# A three-valued answer: True, False, or None for UNKNOWN.
type Truth = bool | None

# The "we don't know" answer.
UNKNOWN: Truth = None


def and3(left: Truth, right: Truth) -> Truth:
    """Return ``left AND right`` in three-valued logic."""
    if left is False or right is False:
        return False
    if left is None or right is None:
        return UNKNOWN
    return True


def or3(left: Truth, right: Truth) -> Truth:
    """Return ``left OR right`` in three-valued logic."""
    if left is True or right is True:
        return True
    if left is None or right is None:
        return UNKNOWN
    return False


def not3(value: Truth) -> Truth:
    """Return ``NOT value`` in three-valued logic (NOT UNKNOWN is UNKNOWN)."""
    if value is None:
        return UNKNOWN
    return not value


def is_true(value: Truth) -> bool:
    """Return whether a filter keeps the row: only TRUE does, not UNKNOWN."""
    return value is True


def in3(value: Any, candidates: Iterable[Any]) -> Truth:
    """Return ``value IN (candidates)`` in three-valued logic.

    It behaves like ``value = a OR value = b OR ...``: TRUE on a match;
    otherwise UNKNOWN if *value* is NULL or the list holds a NULL (that
    unknown entry might have matched); otherwise FALSE. Nothing is in an
    empty list, so that is always FALSE.
    """
    answer: Truth = False
    for candidate in candidates:
        if value is None or candidate is None:
            answer = UNKNOWN
        elif value == candidate:
            return True
    return answer


def null_sort_key(value: Any) -> tuple[bool, Any]:
    """Return a sort key that puts NULLs before every real value.

    Sorting ascending puts NULLs first and sorting with ``reverse=True``
    puts them last -- SQLite's rule. A NULL is never compared with ``<``
    (which would raise ``TypeError``): the first item of the key already
    tells the two apart.
    """
    return (value is not None, value)
