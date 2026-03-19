"""Tests for the SQL parser.

The parser reads tokens and builds Query objects. These tests verify
that SQL text is correctly translated into the structured queries the
executor understands.
"""

import pytest

from pydb.query import Operator, SortDirection
from pydb.sql_parser import ParseError, parse_sql

POWER_50 = 50
POWER_55 = 55
LIMIT_5 = 5
LIMIT_10 = 10
PI = 3.14


class TestSelectStar:
    """Verify SELECT * queries."""

    def test_select_star(self) -> None:
        """SELECT * FROM cards should return all columns."""
        q = parse_sql("SELECT * FROM cards")
        assert q.table == "cards"
        assert q.columns == []
        assert q.where is None
        assert q.order_by is None
        assert q.limit is None

    def test_case_insensitive_keywords(self) -> None:
        """SQL keywords should be case-insensitive."""
        q = parse_sql("select * from cards")
        assert q.table == "cards"


class TestSelectColumns:
    """Verify column projection in SELECT."""

    def test_single_column(self) -> None:
        """SELECT name FROM cards should project one column."""
        q = parse_sql("SELECT name FROM cards")
        assert q.columns == ["name"]

    def test_multiple_columns(self) -> None:
        """SELECT name, power FROM cards should project two columns."""
        q = parse_sql("SELECT name, power FROM cards")
        assert q.columns == ["name", "power"]

    def test_three_columns(self) -> None:
        """Three columns should all be captured."""
        q = parse_sql("SELECT name, type, power FROM cards")
        assert q.columns == ["name", "type", "power"]


class TestWhereClause:
    """Verify WHERE conditions with all operators."""

    def test_equals(self) -> None:
        """WHERE name = 'Pikachu' should produce an EQ condition."""
        q = parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'")
        assert q.where is not None
        assert q.where.column == "name"  # type: ignore[union-attr]
        assert q.where.operator == Operator.EQ  # type: ignore[union-attr]
        assert q.where.value == "Pikachu"  # type: ignore[union-attr]

    def test_not_equals(self) -> None:
        """WHERE type != 'Fire' should produce a NE condition."""
        q = parse_sql("SELECT * FROM cards WHERE type != 'Fire'")
        assert q.where is not None
        assert q.where.operator == Operator.NE  # type: ignore[union-attr]

    def test_greater_than(self) -> None:
        """WHERE power > 50 should produce a GT condition."""
        q = parse_sql("SELECT * FROM cards WHERE power > 50")
        assert q.where is not None
        assert q.where.operator == Operator.GT  # type: ignore[union-attr]
        assert q.where.value == POWER_50  # type: ignore[union-attr]

    def test_greater_or_equal(self) -> None:
        """WHERE power >= 50 should produce a GE condition."""
        q = parse_sql("SELECT * FROM cards WHERE power >= 50")
        assert q.where is not None
        assert q.where.operator == Operator.GE  # type: ignore[union-attr]

    def test_less_than(self) -> None:
        """WHERE power < 55 should produce a LT condition."""
        q = parse_sql("SELECT * FROM cards WHERE power < 55")
        assert q.where is not None
        assert q.where.operator == Operator.LT  # type: ignore[union-attr]
        assert q.where.value == POWER_55  # type: ignore[union-attr]

    def test_less_or_equal(self) -> None:
        """WHERE power <= 50 should produce a LE condition."""
        q = parse_sql("SELECT * FROM cards WHERE power <= 50")
        assert q.where is not None
        assert q.where.operator == Operator.LE  # type: ignore[union-attr]

    def test_string_value(self) -> None:
        """WHERE with a string value should parse correctly."""
        q = parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'")
        assert q.where is not None
        assert q.where.value == "Pikachu"  # type: ignore[union-attr]

    def test_float_value(self) -> None:
        """WHERE with a float value should parse correctly."""
        q = parse_sql("SELECT * FROM cards WHERE rating > 3.14")
        assert q.where is not None
        assert q.where.value == PI  # type: ignore[union-attr]

    def test_boolean_true(self) -> None:
        """WHERE active = TRUE should parse the boolean."""
        q = parse_sql("SELECT * FROM cards WHERE active = TRUE")
        assert q.where is not None
        assert q.where.value is True  # type: ignore[union-attr]

    def test_boolean_false(self) -> None:
        """WHERE active = FALSE should parse the boolean."""
        q = parse_sql("SELECT * FROM cards WHERE active = FALSE")
        assert q.where is not None
        assert q.where.value is False  # type: ignore[union-attr]


class TestAndOr:
    """Verify AND and OR in WHERE clauses."""

    def test_and(self) -> None:
        """AND should produce an And combinator."""
        q = parse_sql("SELECT * FROM cards WHERE power > 50 AND type = 'Electric'")
        assert q.where is not None
        assert hasattr(q.where, "left")
        assert hasattr(q.where, "right")

    def test_or(self) -> None:
        """OR should produce an Or combinator."""
        q = parse_sql("SELECT * FROM cards WHERE type = 'Fire' OR type = 'Water'")
        assert q.where is not None
        assert hasattr(q.where, "left")

    def test_multiple_and(self) -> None:
        """Multiple ANDs should chain left-to-right."""
        q = parse_sql("SELECT * FROM cards WHERE a = 1 AND b = 2 AND c = 3")
        assert q.where is not None
        # The outer node should be an And with a left that is also an And.
        assert hasattr(q.where, "left")
        assert hasattr(q.where.left, "left")  # type: ignore[union-attr]


class TestOrderBy:
    """Verify ORDER BY clause."""

    def test_order_by_default_asc(self) -> None:
        """ORDER BY without direction should default to ASC."""
        q = parse_sql("SELECT * FROM cards ORDER BY name")
        assert q.order_by is not None
        assert q.order_by.column == "name"
        assert q.order_by.direction == SortDirection.ASC

    def test_order_by_asc(self) -> None:
        """ORDER BY name ASC should be ascending."""
        q = parse_sql("SELECT * FROM cards ORDER BY name ASC")
        assert q.order_by is not None
        assert q.order_by.direction == SortDirection.ASC

    def test_order_by_desc(self) -> None:
        """ORDER BY power DESC should be descending."""
        q = parse_sql("SELECT * FROM cards ORDER BY power DESC")
        assert q.order_by is not None
        assert q.order_by.column == "power"
        assert q.order_by.direction == SortDirection.DESC


class TestLimit:
    """Verify LIMIT clause."""

    def test_limit(self) -> None:
        """LIMIT 10 should cap results."""
        q = parse_sql("SELECT * FROM cards LIMIT 10")
        assert q.limit == LIMIT_10

    def test_limit_5(self) -> None:
        """LIMIT 5 should work."""
        q = parse_sql("SELECT * FROM cards LIMIT 5")
        assert q.limit == LIMIT_5


class TestCombined:
    """Verify full SQL statements with all clauses."""

    def test_full_query(self) -> None:
        """A query with all clauses should parse completely."""
        q = parse_sql("SELECT name, power FROM cards WHERE power >= 50 ORDER BY name DESC LIMIT 5")
        assert q.table == "cards"
        assert q.columns == ["name", "power"]
        assert q.where is not None
        assert q.order_by is not None
        assert q.order_by.column == "name"
        assert q.order_by.direction == SortDirection.DESC
        assert q.limit == LIMIT_5

    def test_where_and_order(self) -> None:
        """WHERE + ORDER BY without LIMIT should work."""
        q = parse_sql("SELECT * FROM cards WHERE power > 50 ORDER BY name")
        assert q.where is not None
        assert q.order_by is not None
        assert q.limit is None

    def test_where_and_limit(self) -> None:
        """WHERE + LIMIT without ORDER BY should work."""
        q = parse_sql("SELECT * FROM cards WHERE power > 50 LIMIT 5")
        assert q.where is not None
        assert q.order_by is None
        assert q.limit == LIMIT_5


class TestErrors:
    """Verify error handling for invalid SQL."""

    def test_missing_from(self) -> None:
        """Missing FROM should raise ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * cards")

    def test_missing_table_name(self) -> None:
        """Missing table name after FROM should raise ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * FROM")

    def test_invalid_where_value(self) -> None:
        """A WHERE with no value should raise ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * FROM cards WHERE power >")

    def test_unexpected_token(self) -> None:
        """Unexpected tokens should raise ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * FROM cards BOGUS")
