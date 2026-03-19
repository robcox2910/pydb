"""Tests for the query model.

A query is the question you write on a slip of paper before handing it
to the librarian. These tests verify that conditions, AND/OR logic,
ordering, and the query structure all work correctly.
"""

from pydb.query import And, Condition, Operator, Or, OrderBy, Query, SortDirection
from pydb.record import Record

# Named constants.
RECORD_ID_1 = 1
POWER_55 = 55
POWER_52 = 52
POWER_30 = 30
LIMIT_3 = 3


def _pikachu() -> Record:
    """Create a Pikachu test record."""
    return Record(
        record_id=RECORD_ID_1, data={"name": "Pikachu", "type": "Electric", "power": POWER_55}
    )


def _charmander() -> Record:
    """Create a Charmander test record."""
    return Record(
        record_id=RECORD_ID_1, data={"name": "Charmander", "type": "Fire", "power": POWER_52}
    )


def _magikarp() -> Record:
    """Create a Magikarp test record."""
    return Record(
        record_id=RECORD_ID_1, data={"name": "Magikarp", "type": "Water", "power": POWER_30}
    )


class TestConditionOperators:
    """Verify each comparison operator."""

    def test_eq_matches(self) -> None:
        """EQ should match when values are equal."""
        cond = Condition("name", Operator.EQ, "Pikachu")
        assert cond.matches(_pikachu())

    def test_eq_rejects(self) -> None:
        """EQ should reject when values differ."""
        cond = Condition("name", Operator.EQ, "Pikachu")
        assert not cond.matches(_charmander())

    def test_ne_matches(self) -> None:
        """NE should match when values differ."""
        cond = Condition("name", Operator.NE, "Pikachu")
        assert cond.matches(_charmander())

    def test_ne_rejects(self) -> None:
        """NE should reject when values are equal."""
        cond = Condition("name", Operator.NE, "Pikachu")
        assert not cond.matches(_pikachu())

    def test_gt_matches(self) -> None:
        """GT should match when record value is greater."""
        cond = Condition("power", Operator.GT, POWER_52)
        assert cond.matches(_pikachu())

    def test_gt_rejects_equal(self) -> None:
        """GT should reject when values are equal."""
        cond = Condition("power", Operator.GT, POWER_55)
        assert not cond.matches(_pikachu())

    def test_ge_matches_equal(self) -> None:
        """GE should match when values are equal."""
        cond = Condition("power", Operator.GE, POWER_55)
        assert cond.matches(_pikachu())

    def test_ge_matches_greater(self) -> None:
        """GE should match when record value is greater."""
        cond = Condition("power", Operator.GE, POWER_30)
        assert cond.matches(_pikachu())

    def test_lt_matches(self) -> None:
        """LT should match when record value is less."""
        cond = Condition("power", Operator.LT, POWER_55)
        assert cond.matches(_charmander())

    def test_lt_rejects_equal(self) -> None:
        """LT should reject when values are equal."""
        cond = Condition("power", Operator.LT, POWER_52)
        assert not cond.matches(_charmander())

    def test_le_matches_equal(self) -> None:
        """LE should match when values are equal."""
        cond = Condition("power", Operator.LE, POWER_52)
        assert cond.matches(_charmander())

    def test_le_matches_less(self) -> None:
        """LE should match when record value is less."""
        cond = Condition("power", Operator.LE, POWER_55)
        assert cond.matches(_charmander())


class TestAndOr:
    """Verify AND and OR combinators."""

    def test_and_both_true(self) -> None:
        """AND should match when both conditions are true."""
        cond = And(
            Condition("type", Operator.EQ, "Electric"),
            Condition("power", Operator.GT, POWER_30),
        )
        assert cond.matches(_pikachu())

    def test_and_one_false(self) -> None:
        """AND should reject when one condition is false."""
        cond = And(
            Condition("type", Operator.EQ, "Fire"),
            Condition("power", Operator.GT, POWER_30),
        )
        assert not cond.matches(_pikachu())

    def test_or_one_true(self) -> None:
        """OR should match when at least one condition is true."""
        cond = Or(
            Condition("type", Operator.EQ, "Fire"),
            Condition("type", Operator.EQ, "Electric"),
        )
        assert cond.matches(_pikachu())

    def test_or_both_false(self) -> None:
        """OR should reject when both conditions are false."""
        cond = Or(
            Condition("type", Operator.EQ, "Fire"),
            Condition("type", Operator.EQ, "Grass"),
        )
        assert not cond.matches(_pikachu())

    def test_nested_and_or(self) -> None:
        """Nested AND/OR should evaluate correctly."""
        cond = Or(
            And(
                Condition("type", Operator.EQ, "Water"),
                Condition("power", Operator.GT, POWER_55),
            ),
            Condition("name", Operator.EQ, "Pikachu"),
        )
        assert cond.matches(_pikachu())
        assert not cond.matches(_magikarp())


class TestOrderBy:
    """Verify OrderBy defaults."""

    def test_default_direction_is_asc(self) -> None:
        """OrderBy should default to ascending."""
        ob = OrderBy(column="name")
        assert ob.direction == SortDirection.ASC

    def test_explicit_desc(self) -> None:
        """OrderBy should accept DESC direction."""
        ob = OrderBy(column="name", direction=SortDirection.DESC)
        assert ob.direction == SortDirection.DESC


class TestQuery:
    """Verify Query structure."""

    def test_defaults(self) -> None:
        """A minimal query should have sensible defaults."""
        q = Query(table="cards")
        assert q.table == "cards"
        assert q.columns == []
        assert q.where is None
        assert q.order_by is None
        assert q.limit is None

    def test_full_query(self) -> None:
        """A fully specified query should store all parts."""
        q = Query(
            table="cards",
            columns=["name", "power"],
            where=Condition("power", Operator.GT, POWER_30),
            order_by=OrderBy("name"),
            limit=LIMIT_3,
        )
        assert q.table == "cards"
        assert q.columns == ["name", "power"]
        assert q.where is not None
        assert q.order_by is not None
        assert q.limit == LIMIT_3
