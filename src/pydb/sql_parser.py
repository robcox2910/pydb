"""SQL parser -- turn tokens into Query and Statement objects.

The parser reads tokens left-to-right and assembles them into the Query,
Condition, And, Or, OrderBy, and Statement objects that the executor
understands. It's like reading a sentence word by word and building up
the meaning as you go.
"""

from pydb.errors import PyDBError
from pydb.query import And, Condition, Operator, Or, OrderBy, Query, SortDirection, WhereClause
from pydb.record import Value
from pydb.sql_tokenizer import Token, TokenType, tokenize
from pydb.statements import (
    CreateTableStatement,
    DeleteStatement,
    DropTableStatement,
    InsertStatement,
    Statement,
    UpdateStatement,
)
from pydb.types import DataType


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

# Mapping from SQL type names to DataType.
_TYPE_MAP: dict[str, DataType] = {
    "TEXT": DataType.TEXT,
    "INTEGER": DataType.INTEGER,
    "INT": DataType.INTEGER,
    "FLOAT": DataType.FLOAT,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
}


class _Parser:
    """A recursive-descent parser for a subset of SQL.

    Consumes tokens one at a time, building Query or Statement objects.

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

    def parse(self) -> Query | Statement:
        """Parse any supported SQL statement.

        Returns:
            A Query (for SELECT) or Statement (for write operations).

        Raises:
            ParseError: If the SQL is malformed.

        """
        kw = self._peek()
        if kw.token_type != TokenType.KEYWORD:
            msg = f"Expected a SQL keyword, got {kw.token_type.value} ({kw.value!r})"
            raise ParseError(msg)

        match kw.value:
            case "SELECT":
                return self._parse_select()
            case "CREATE":
                return self._parse_create_table()
            case "DROP":
                return self._parse_drop_table()
            case "INSERT":
                return self._parse_insert()
            case "UPDATE":
                return self._parse_update()
            case "DELETE":
                return self._parse_delete()
            case _:
                msg = f"Unsupported statement: {kw.value}"
                raise ParseError(msg)

    def _parse_select(self) -> Query:
        """Parse a SELECT statement into a Query object.

        Grammar::

            SELECT columns FROM table [WHERE condition] [ORDER BY col [ASC|DESC]] [LIMIT n]

        """
        self._expect(TokenType.KEYWORD, "SELECT")
        columns = self._parse_select_columns()

        self._expect(TokenType.KEYWORD, "FROM")
        table_name = self._expect(TokenType.IDENTIFIER).value

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
                limit = int(self._expect(TokenType.INTEGER).value)
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

    def _parse_create_table(self) -> CreateTableStatement:
        """Parse a CREATE TABLE statement.

        Grammar::

            CREATE TABLE name (col_name TYPE, ...)

        """
        self._expect(TokenType.KEYWORD, "CREATE")
        self._expect(TokenType.KEYWORD, "TABLE")
        table_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.LPAREN)
        columns: list[tuple[str, DataType]] = []
        columns.append(self._parse_column_def())

        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            columns.append(self._parse_column_def())

        self._expect(TokenType.RPAREN)
        return CreateTableStatement(table=table_name, columns=columns)

    def _parse_column_def(self) -> tuple[str, DataType]:
        """Parse a single column definition: name TYPE."""
        col_name = self._expect(TokenType.IDENTIFIER).value
        type_token = self._advance()

        # Type names can be identifiers (INT, TEXT) or keywords (BOOLEAN).
        type_name = type_token.value.upper()
        if type_name not in _TYPE_MAP:
            msg = f"Unknown column type: {type_token.value!r}"
            raise ParseError(msg)

        return col_name, _TYPE_MAP[type_name]

    def _parse_drop_table(self) -> DropTableStatement:
        """Parse a DROP TABLE statement.

        Grammar::

            DROP TABLE name

        """
        self._expect(TokenType.KEYWORD, "DROP")
        self._expect(TokenType.KEYWORD, "TABLE")
        table_name = self._expect(TokenType.IDENTIFIER).value
        return DropTableStatement(table=table_name)

    def _parse_insert(self) -> InsertStatement:
        """Parse an INSERT INTO statement.

        Grammar::

            INSERT INTO table (col, ...) VALUES (val, ...)
            INSERT INTO table VALUES (val, ...)

        """
        self._expect(TokenType.KEYWORD, "INSERT")
        self._expect(TokenType.KEYWORD, "INTO")
        table_name = self._expect(TokenType.IDENTIFIER).value

        # Optional column list.
        columns: list[str] = []
        if self._peek().token_type == TokenType.LPAREN:
            self._advance()
            columns.append(self._expect(TokenType.IDENTIFIER).value)
            while self._peek().token_type == TokenType.COMMA:
                self._advance()
                columns.append(self._expect(TokenType.IDENTIFIER).value)
            self._expect(TokenType.RPAREN)

        self._expect(TokenType.KEYWORD, "VALUES")
        self._expect(TokenType.LPAREN)

        values: list[Value] = [self._parse_value()]
        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            values.append(self._parse_value())

        self._expect(TokenType.RPAREN)
        return InsertStatement(table=table_name, columns=columns, values=values)

    def _parse_update(self) -> UpdateStatement:
        """Parse an UPDATE statement.

        Grammar::

            UPDATE table SET col = val, ... [WHERE condition]

        """
        self._expect(TokenType.KEYWORD, "UPDATE")
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.KEYWORD, "SET")

        assignments: dict[str, Value] = {}
        col = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.OPERATOR, "=")
        assignments[col] = self._parse_value()

        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            col = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.OPERATOR, "=")
            assignments[col] = self._parse_value()

        where: WhereClause | None = None
        if self._peek().token_type == TokenType.KEYWORD and self._peek().value == "WHERE":
            self._advance()
            where = self._parse_where()

        return UpdateStatement(table=table_name, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteStatement:
        """Parse a DELETE FROM statement.

        Grammar::

            DELETE FROM table [WHERE condition]

        """
        self._expect(TokenType.KEYWORD, "DELETE")
        self._expect(TokenType.KEYWORD, "FROM")
        table_name = self._expect(TokenType.IDENTIFIER).value

        where: WhereClause | None = None
        if self._peek().token_type == TokenType.KEYWORD and self._peek().value == "WHERE":
            self._advance()
            where = self._parse_where()

        return DeleteStatement(table=table_name, where=where)

    def _parse_select_columns(self) -> list[str]:
        """Parse the column list after SELECT."""
        if self._peek().token_type == TokenType.STAR:
            self._advance()
            return []

        columns: list[str] = [self._expect(TokenType.IDENTIFIER).value]
        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            columns.append(self._expect(TokenType.IDENTIFIER).value)
        return columns

    def _parse_where(self) -> WhereClause:
        """Parse a WHERE clause with AND/OR support."""
        left = self._parse_condition()

        while self._peek().token_type == TokenType.KEYWORD and self._peek().value in ("AND", "OR"):
            combinator = self._advance().value
            right = self._parse_condition()
            left = And(left, right) if combinator == "AND" else Or(left, right)

        return left

    def _parse_condition(self) -> Condition:
        """Parse a single comparison condition: column op value."""
        column = self._expect(TokenType.IDENTIFIER).value
        op_token = self._expect(TokenType.OPERATOR)

        if op_token.value not in _OPERATOR_MAP:
            msg = f"Unknown operator: {op_token.value!r}"
            raise ParseError(msg)

        return Condition(
            column=column, operator=_OPERATOR_MAP[op_token.value], value=self._parse_value()
        )

    def _parse_value(self) -> Value:
        """Parse a literal value (string, number, or boolean)."""
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
        """Parse an ORDER BY clause."""
        self._expect(TokenType.KEYWORD, "ORDER")
        self._expect(TokenType.KEYWORD, "BY")
        column = self._expect(TokenType.IDENTIFIER).value

        direction = SortDirection.ASC
        if self._peek().token_type == TokenType.KEYWORD and self._peek().value in ("ASC", "DESC"):
            dir_token = self._advance()
            direction = SortDirection.DESC if dir_token.value == "DESC" else SortDirection.ASC

        return OrderBy(column=column, direction=direction)


def parse_sql(sql: str) -> Query | Statement:
    """Parse a SQL statement into a Query or Statement object.

    This is the main entry point -- give it SQL text, get back a
    structured representation.

    Args:
        sql: The SQL text to parse.

    Returns:
        A Query (for SELECT) or Statement (for write operations).

    Raises:
        ParseError: If the SQL is invalid.
        TokenizerError: If the SQL contains invalid characters.

    """
    tokens = tokenize(sql)
    parser = _Parser(tokens)
    return parser.parse()
