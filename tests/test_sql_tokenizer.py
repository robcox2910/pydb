"""Tests for the SQL tokenizer.

The tokenizer is the postal worker who sorts SQL text into bins of
tokens. These tests verify every token type is recognised correctly.
"""

import pytest

from pydb.sql_tokenizer import Token, TokenizerError, TokenType, tokenize

EXPECTED_TOKEN_COUNT_3 = 3
EXPECTED_TOKEN_COUNT_4 = 4
EXPECTED_TOKEN_COUNT_6 = 6


class TestKeywords:
    """Verify SQL keyword recognition."""

    def test_select_keyword(self) -> None:
        """SELECT should be recognised as a keyword."""
        tokens = tokenize("SELECT")
        assert tokens[0] == Token(TokenType.KEYWORD, "SELECT")

    def test_case_insensitive(self) -> None:
        """Keywords should be case-insensitive."""
        tokens = tokenize("select")
        assert tokens[0] == Token(TokenType.KEYWORD, "SELECT")

    def test_mixed_case(self) -> None:
        """Mixed-case keywords should work."""
        tokens = tokenize("SeLeCt")
        assert tokens[0] == Token(TokenType.KEYWORD, "SELECT")

    def test_all_keywords(self) -> None:
        """All SQL keywords should be recognised."""
        for kw in ("FROM", "WHERE", "AND", "OR", "ORDER", "BY", "ASC", "DESC", "LIMIT"):
            tokens = tokenize(kw)
            assert tokens[0].token_type == TokenType.KEYWORD


class TestIdentifiers:
    """Verify identifier tokenization."""

    def test_simple_identifier(self) -> None:
        """A simple word should be an identifier."""
        tokens = tokenize("cards")
        assert tokens[0] == Token(TokenType.IDENTIFIER, "cards")

    def test_identifier_with_underscore(self) -> None:
        """Identifiers with underscores should work."""
        tokens = tokenize("my_table")
        assert tokens[0] == Token(TokenType.IDENTIFIER, "my_table")

    def test_identifier_with_digits(self) -> None:
        """Identifiers can contain digits (but not start with them)."""
        tokens = tokenize("table2")
        assert tokens[0] == Token(TokenType.IDENTIFIER, "table2")


class TestNumbers:
    """Verify number tokenization."""

    def test_integer(self) -> None:
        """Whole numbers should be INTEGER tokens."""
        tokens = tokenize("42")
        assert tokens[0] == Token(TokenType.INTEGER, "42")

    def test_float(self) -> None:
        """Decimal numbers should be FLOAT tokens."""
        tokens = tokenize("3.14")
        assert tokens[0] == Token(TokenType.FLOAT, "3.14")

    def test_zero(self) -> None:
        """Zero should be a valid integer."""
        tokens = tokenize("0")
        assert tokens[0] == Token(TokenType.INTEGER, "0")


class TestStrings:
    """Verify string literal tokenization."""

    def test_single_quoted_string(self) -> None:
        """Single-quoted text should be a STRING token."""
        tokens = tokenize("'Pikachu'")
        assert tokens[0] == Token(TokenType.STRING, "Pikachu")

    def test_string_with_spaces(self) -> None:
        """Strings can contain spaces."""
        tokens = tokenize("'hello world'")
        assert tokens[0] == Token(TokenType.STRING, "hello world")

    def test_empty_string(self) -> None:
        """Empty strings should work."""
        tokens = tokenize("''")
        assert tokens[0] == Token(TokenType.STRING, "")

    def test_unterminated_string_raises(self) -> None:
        """An unterminated string should raise TokenizerError."""
        with pytest.raises(TokenizerError, match="Unterminated"):
            tokenize("'hello")


class TestOperators:
    """Verify operator tokenization."""

    def test_equals(self) -> None:
        """= should be an operator."""
        tokens = tokenize("=")
        assert tokens[0] == Token(TokenType.OPERATOR, "=")

    def test_not_equals(self) -> None:
        """!= should be a single operator."""
        tokens = tokenize("!=")
        assert tokens[0] == Token(TokenType.OPERATOR, "!=")

    def test_greater_than(self) -> None:
        """> should be an operator."""
        tokens = tokenize(">")
        assert tokens[0] == Token(TokenType.OPERATOR, ">")

    def test_greater_or_equal(self) -> None:
        """>= should be a single operator."""
        tokens = tokenize(">=")
        assert tokens[0] == Token(TokenType.OPERATOR, ">=")

    def test_less_than(self) -> None:
        """< should be an operator."""
        tokens = tokenize("<")
        assert tokens[0] == Token(TokenType.OPERATOR, "<")

    def test_less_or_equal(self) -> None:
        """<= should be a single operator."""
        tokens = tokenize("<=")
        assert tokens[0] == Token(TokenType.OPERATOR, "<=")


class TestSpecialTokens:
    """Verify special single-character tokens."""

    def test_star(self) -> None:
        """* should be a STAR token."""
        tokens = tokenize("*")
        assert tokens[0] == Token(TokenType.STAR, "*")

    def test_comma(self) -> None:
        """, should be a COMMA token."""
        tokens = tokenize(",")
        assert tokens[0] == Token(TokenType.COMMA, ",")

    def test_lparen(self) -> None:
        """( should be a LPAREN token."""
        tokens = tokenize("(")
        assert tokens[0] == Token(TokenType.LPAREN, "(")

    def test_rparen(self) -> None:
        """) should be a RPAREN token."""
        tokens = tokenize(")")
        assert tokens[0] == Token(TokenType.RPAREN, ")")


class TestFullStatements:
    """Verify tokenization of complete SQL statements."""

    def test_select_star(self) -> None:
        """SELECT * FROM cards should produce 4 tokens + EOF."""
        tokens = tokenize("SELECT * FROM cards")
        assert len(tokens) == EXPECTED_TOKEN_COUNT_4 + 1
        assert tokens[0].value == "SELECT"
        assert tokens[1].value == "*"
        assert tokens[2].value == "FROM"
        assert tokens[3].value == "cards"
        assert tokens[4].token_type == TokenType.EOF

    def test_select_with_where(self) -> None:
        """A WHERE clause should tokenize correctly."""
        tokens = tokenize("SELECT * FROM cards WHERE power > 50")
        types = [t.token_type for t in tokens[:-1]]
        assert types == [
            TokenType.KEYWORD,
            TokenType.STAR,
            TokenType.KEYWORD,
            TokenType.IDENTIFIER,
            TokenType.KEYWORD,
            TokenType.IDENTIFIER,
            TokenType.OPERATOR,
            TokenType.INTEGER,
        ]

    def test_whitespace_handling(self) -> None:
        """Extra whitespace should be ignored."""
        tokens = tokenize("  SELECT  *   FROM   cards  ")
        values = [t.value for t in tokens[:-1]]
        assert values == ["SELECT", "*", "FROM", "cards"]


class TestErrors:
    """Verify error handling."""

    def test_invalid_character(self) -> None:
        """An invalid character should raise TokenizerError."""
        with pytest.raises(TokenizerError, match="Unexpected character"):
            tokenize("SELECT @ FROM cards")

    def test_eof_always_last(self) -> None:
        """The last token should always be EOF."""
        tokens = tokenize("")
        assert len(tokens) == 1
        assert tokens[0].token_type == TokenType.EOF


class TestBooleans:
    """Verify boolean keyword tokenization."""

    def test_true(self) -> None:
        """TRUE should be a keyword."""
        tokens = tokenize("TRUE")
        assert tokens[0] == Token(TokenType.KEYWORD, "TRUE")

    def test_false(self) -> None:
        """FALSE should be a keyword."""
        tokens = tokenize("FALSE")
        assert tokens[0] == Token(TokenType.KEYWORD, "FALSE")
