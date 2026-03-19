"""SQL parser -- turn tokens into Query objects.

The parser reads tokens left-to-right and assembles them into the Query,
Condition, And, Or, and OrderBy objects that the executor already
understands. It's like reading a sentence word by word and building up
the meaning as you go.
"""

from pydb.errors import PyDBError
from pydb.query import And, Condition, Operator, Or, OrderBy, Query, SortDirection, WhereClause
from pydb.sql_tokenizer import Token, TokenType, tokenize


class ParseError(PyDBError):
    """Raise when the SQL text cannot be parsed."""


# Mapping from SQL operator text to our Operator enum.
_OPERATOR_MAP: dict[str, Operator] = {
    "=": Operator.EQ,
    "!=": Operator.NE,
    ">": Operator.GT,
    ">=": Operator.GE,
    "<": Operator.LT,
    "<=": Operator.LE,
}


class _Parser:
    """A recursive-descent parser for a subset of SQL.

    Consumes tokens one at a time, building up a Query object.

    Args:
        tokens: The token list from the tokenizer.

    """

    __slots__ = ("_pos", "_tokens")

    def __init__(self, tokens: list[Token]) -> None:
        """Create a parser for the given token list."""
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> Token:
        """Return the current token without consuming it."""
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        """Return the current token and move to the next."""
        token = self._tokens[self._pos]
        self._pos += 1
        return token

    def _expect(self, token_type: TokenType, value: str | None = None) -> Token:
        """Consume a token, raising if it doesn't match expectations.

        Args:
            token_type: The expected token type.
            value: The expected value (optional, case-insensitive for keywords).

        Returns:
            The consumed token.

        Raises:
            ParseError: If the token doesn't match.

        """
        token = self._advance()
        if token.token_type != token_type:
            msg = f"Expected {token_type.value}, got {token.token_type.value} ({token.value!r})"
            raise ParseError(msg)
        if value is not None and token.value.upper() != value.upper():
            msg = f"Expected {value!r}, got {token.value!r}"
            raise ParseError(msg)
        return token

    def parse_select(self) -> Query:
        """Parse a SELECT statement into a Query object.

        Grammar::

            SELECT columns FROM table [WHERE condition] [ORDER BY col [ASC|DESC]] [LIMIT n]

        Returns:
            A fully populated Query.

        Raises:
            ParseError: If the SQL is malformed.

        """
        self._expect(TokenType.KEYWORD, "SELECT")

        # Parse columns.
        columns = self._parse_columns()

        # FROM table.
        self._expect(TokenType.KEYWORD, "FROM")
        table_name = self._expect(TokenType.IDENTIFIER).value

        # Optional clauses.
        where: WhereClause | None = None
        order_by: OrderBy | None = None
        limit: int | None = None

        while self._peek().token_type != TokenType.EOF:
            kw = self._peek()
            if kw.token_type == TokenType.KEYWORD and kw.value == "WHERE":
                self._advance()
                where = self._parse_where()
            elif kw.token_type == TokenType.KEYWORD and kw.value == "ORDER":
                order_by = self._parse_order_by()
            elif kw.token_type == TokenType.KEYWORD and kw.value == "LIMIT":
                self._advance()
                limit_token = self._expect(TokenType.INTEGER)
                limit = int(limit_token.value)
            else:
                msg = f"Unexpected token: {kw.value!r}"
                raise ParseError(msg)

        return Query(
            table=table_name,
            columns=columns,
            where=where,
            order_by=order_by,
            limit=limit,
        )

    def _parse_columns(self) -> list[str]:
        """Parse the column list after SELECT.

        Returns:
            A list of column names, or an empty list for SELECT *.

        """
        if self._peek().token_type == TokenType.STAR:
            self._advance()
            return []

        columns: list[str] = []
        columns.append(self._expect(TokenType.IDENTIFIER).value)

        while self._peek().token_type == TokenType.COMMA:
            self._advance()  # Skip comma.
            columns.append(self._expect(TokenType.IDENTIFIER).value)

        return columns

    def _parse_where(self) -> WhereClause:
        """Parse a WHERE clause with AND/OR support.

        Grammar::

            where = condition ((AND | OR) condition)*

        Left-to-right associativity, AND binds tighter than OR.
        For simplicity we parse left-to-right without precedence.

        Returns:
            A Condition, And, or Or object.

        """
        left = self._parse_condition()

        while self._peek().token_type == TokenType.KEYWORD and self._peek().value in ("AND", "OR"):
            combinator = self._advance().value
            right = self._parse_condition()
            left = And(left, right) if combinator == "AND" else Or(left, right)

        return left

    def _parse_condition(self) -> Condition:
        """Parse a single comparison condition.

        Grammar::

            condition = identifier operator value

        Returns:
            A Condition object.

        """
        column = self._expect(TokenType.IDENTIFIER).value
        op_token = self._expect(TokenType.OPERATOR)

        if op_token.value not in _OPERATOR_MAP:
            msg = f"Unknown operator: {op_token.value!r}"
            raise ParseError(msg)

        operator = _OPERATOR_MAP[op_token.value]
        value = self._parse_value()

        return Condition(column=column, operator=operator, value=value)

    def _parse_value(self) -> str | int | float | bool:
        """Parse a literal value (string, number, or boolean).

        Returns:
            The parsed Python value.

        Raises:
            ParseError: If the token is not a valid literal.

        """
        token = self._advance()

        match token.token_type:
            case TokenType.STRING:
                return token.value
            case TokenType.INTEGER:
                return int(token.value)
            case TokenType.FLOAT:
                return float(token.value)
            case TokenType.KEYWORD if token.value == "TRUE":
                return True
            case TokenType.KEYWORD if token.value == "FALSE":
                return False
            case _:
                msg = f"Expected a value, got {token.token_type.value} ({token.value!r})"
                raise ParseError(msg)

    def _parse_order_by(self) -> OrderBy:
        """Parse an ORDER BY clause.

        Grammar::

            ORDER BY identifier [ASC | DESC]

        Returns:
            An OrderBy object.

        """
        self._expect(TokenType.KEYWORD, "ORDER")
        self._expect(TokenType.KEYWORD, "BY")
        column = self._expect(TokenType.IDENTIFIER).value

        direction = SortDirection.ASC
        if self._peek().token_type == TokenType.KEYWORD and self._peek().value in ("ASC", "DESC"):
            dir_token = self._advance()
            direction = SortDirection.DESC if dir_token.value == "DESC" else SortDirection.ASC

        return OrderBy(column=column, direction=direction)


def parse_sql(sql: str) -> Query:
    """Parse a SQL SELECT statement into a Query object.

    This is the main entry point -- give it SQL text, get back a Query.

    Args:
        sql: The SQL text to parse.

    Returns:
        A Query object ready for the executor.

    Raises:
        ParseError: If the SQL is invalid.
        TokenizerError: If the SQL contains invalid characters.

    """
    tokens = tokenize(sql)
    parser = _Parser(tokens)
    return parser.parse_select()
