"""Tests for three-valued logic -- TRUE, FALSE and UNKNOWN.

NULL means "we don't know". A question about something unknown has the
answer "unknown" too, so PyDB's logic has three answers instead of two.
These tables check every combination (Kleene's rules).
"""

from typing import cast

import pytest

from pydb.nulls import UNKNOWN, Truth, and3, in3, is_true, not3, null_sort_key, or3
from pydb.query import Operator, compare3, compare_values

T, F, U = True, False, UNKNOWN

AND_TABLE: list[tuple[Truth, Truth, Truth]] = [
    (T, T, T),
    (T, F, F),
    (T, U, U),
    (F, T, F),
    (F, F, F),
    (F, U, F),
    (U, T, U),
    (U, F, F),
    (U, U, U),
]

OR_TABLE: list[tuple[Truth, Truth, Truth]] = [
    (T, T, T),
    (T, F, T),
    (T, U, T),
    (F, T, T),
    (F, F, F),
    (F, U, U),
    (U, T, T),
    (U, F, U),
    (U, U, U),
]

NOT_TABLE: list[tuple[Truth, Truth]] = [(T, F), (F, T), (U, U)]

FIVE = 5
SEVEN = 7


class TestTruthTables:
    """AND, OR and NOT follow Kleene's three-valued logic."""

    @pytest.mark.parametrize(("left", "right", "expected"), AND_TABLE)
    def test_and(self, left: Truth, right: Truth, expected: Truth) -> None:
        """AND is FALSE if either side is FALSE, else UNKNOWN if either is UNKNOWN."""
        assert and3(left, right) is expected

    @pytest.mark.parametrize(("left", "right", "expected"), OR_TABLE)
    def test_or(self, left: Truth, right: Truth, expected: Truth) -> None:
        """OR is TRUE if either side is TRUE, else UNKNOWN if either is UNKNOWN."""
        assert or3(left, right) is expected

    @pytest.mark.parametrize(("value", "expected"), NOT_TABLE)
    def test_not(self, value: Truth, expected: Truth) -> None:
        """NOT UNKNOWN is still UNKNOWN."""
        assert not3(value) is expected

    @pytest.mark.parametrize(("value", "expected"), [(T, True), (F, False), (U, False)])
    def test_only_true_keeps_a_row(self, value: Truth, *, expected: bool) -> None:
        """A WHERE clause keeps a row only when the answer is TRUE."""
        assert is_true(value) is expected


class TestCompare3:
    """Comparisons involving NULL are UNKNOWN."""

    @pytest.mark.parametrize(
        "op", [Operator.EQ, Operator.NE, Operator.GT, Operator.GE, Operator.LT, Operator.LE]
    )
    @pytest.mark.parametrize(("left", "right"), [(None, FIVE), (FIVE, None), (None, None)])
    def test_null_comparisons_are_unknown(
        self, op: Operator, left: int | None, right: int | None
    ) -> None:
        """Even NULL = NULL is UNKNOWN, not TRUE."""
        assert compare3(left, op, right) is UNKNOWN
        assert compare_values(left, op, right) is False

    def test_known_values_compare_normally(self) -> None:
        """Without NULLs the answer is TRUE or FALSE as before."""
        assert compare3(FIVE, Operator.LT, SEVEN) is True
        assert compare3(FIVE, Operator.EQ, SEVEN) is False

    def test_is_null(self) -> None:
        """IS NULL and IS NOT NULL always answer TRUE or FALSE."""
        assert compare3(None, Operator.IS_NULL, None) is True
        assert compare3(FIVE, Operator.IS_NULL, None) is False
        assert compare3(None, Operator.IS_NOT_NULL, None) is False
        assert compare3(FIVE, Operator.IS_NOT_NULL, None) is True

    def test_unknown_operator_is_an_error(self) -> None:
        """An operator PyDB does not know is refused, not silently FALSE."""
        with pytest.raises(ValueError, match="Unknown operator"):
            compare3(FIVE, cast(Operator, "??"), SEVEN)

    def test_in_uses_in3(self) -> None:
        """IN goes through the three-valued membership test."""
        assert compare3(FIVE, Operator.IN, [FIVE]) is True
        assert compare3(None, Operator.IN, [FIVE]) is UNKNOWN


class TestIn3:
    """``x IN (a, b)`` behaves like ``x = a OR x = b``."""

    def test_found(self) -> None:
        """A match is TRUE even if the list also holds a NULL."""
        assert in3(FIVE, [None, FIVE]) is True

    def test_not_found_without_nulls(self) -> None:
        """No match and no NULL in the list is FALSE."""
        assert in3(FIVE, [SEVEN]) is False

    def test_not_found_with_a_null(self) -> None:
        """No match but a NULL in the list is UNKNOWN (it might have been 5)."""
        assert in3(FIVE, [SEVEN, None]) is UNKNOWN

    def test_null_left_side(self) -> None:
        """NULL IN (...) is UNKNOWN."""
        assert in3(None, [FIVE]) is UNKNOWN

    def test_empty_list(self) -> None:
        """Nothing is in an empty list -- not even NULL."""
        assert in3(None, []) is False


class TestNullSortKey:
    """NULLs sort first ascending and last descending."""

    def test_ascending(self) -> None:
        """Sorting with the key puts NULLs at the front."""
        values = [SEVEN, None, FIVE, None]
        assert sorted(values, key=null_sort_key) == [None, None, FIVE, SEVEN]

    def test_descending(self) -> None:
        """Reversing puts NULLs at the back."""
        values = [SEVEN, None, FIVE]
        assert sorted(values, key=null_sort_key, reverse=True) == [SEVEN, FIVE, None]
