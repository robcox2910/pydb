"""SQL parser -- turn tokens into Query and Statement objects.

The parser reads tokens left-to-right and assembles them into the Query,
Condition, And, Or, OrderBy, and Statement objects that the executor
understands. It's like reading a sentence word by word and building up
the meaning as you go.
"""

from pydb.errors import PyDBError
from pydb.query import (
    AggFunc,
    AggregateColumn,
    And,
    Condition,
    JoinClause,
    Operator,
    Or,
    OrderBy,
    Query,
    SortDirection,
    Subquery,
    WhereClause,
)
from pydb.record import Value
from pydb.schema import Column
from pydb.sql_tokenizer import Token, TokenType, tokenize
from pydb.statements import (
    CreateIndexStatement,
    CreateTableStatement,
    DeleteStatement,
    DropIndexStatement,
    DropTableStatement,
    ExplainStatement,
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

# Aggregate function names.
_AGG_FUNCS: dict[str, AggFunc] = {
    "COUNT": AggFunc.COUNT,
    "SUM": AggFunc.SUM,
    "AVG": AggFunc.AVG,
    "MIN": AggFunc.MIN,
    "MAX": AggFunc.MAX,
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
                return self._parse_create()
            case "DROP":
                return self._parse_drop()
            case "INSERT":
                return self._parse_insert()
            case "UPDATE":
                return self._parse_update()
            case "DELETE":
                return self._parse_delete()
            case "EXPLAIN":
                return self._parse_explain()
            case _:
                msg = f"Unsupported statement: {kw.value}"
                raise ParseError(msg)

    def _parse_select(self) -> Query:
        """Parse a SELECT statement into a Query object.

        Grammar::

            SELECT columns FROM table [JOIN table ON col = col]
                [WHERE condition] [GROUP BY col, ...] [HAVING condition]
                [ORDER BY col [ASC|DESC]] [LIMIT n]

        """
        self._expect(TokenType.KEYWORD, "SELECT")
        columns, aggregates = self._parse_select_list()

        self._expect(TokenType.KEYWORD, "FROM")
        table_name = self._expect(TokenType.IDENTIFIER).value

        join: JoinClause | None = None
        where: WhereClause | None = None
        group_by: list[str] = []
        having: WhereClause | None = None
        order_by: OrderBy | None = None
        limit: int | None = None

        while self._peek().token_type not in (TokenType.EOF, TokenType.RPAREN):
            kw = self._peek()
            if kw.token_type == TokenType.KEYWORD and kw.value == "JOIN":
                join = self._parse_join()
            elif kw.token_type == TokenType.KEYWORD and kw.value == "WHERE":
                self._advance()
                where = self._parse_where()
            elif kw.token_type == TokenType.KEYWORD and kw.value == "GROUP":
                group_by = self._parse_group_by()
            elif kw.token_type == TokenType.KEYWORD and kw.value == "HAVING":
                self._advance()
                having = self._parse_where()
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
            aggregates=aggregates,
            join=join,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
        )

    def _parse_create(self) -> CreateTableStatement | CreateIndexStatement:
        """Parse a CREATE TABLE or CREATE INDEX statement."""
        self._expect(TokenType.KEYWORD, "CREATE")
        next_kw = self._peek()
        if next_kw.token_type == TokenType.KEYWORD and next_kw.value == "INDEX":
            return self._parse_create_index()
        self._expect(TokenType.KEYWORD, "TABLE")
        return self._parse_create_table_body()

    def _parse_create_table_body(self) -> CreateTableStatement:
        """Parse the body of a CREATE TABLE (after CREATE TABLE)."""
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.LPAREN)
        columns: list[Column] = [self._parse_column_def()]
        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            columns.append(self._parse_column_def())
        self._expect(TokenType.RPAREN)
        return CreateTableStatement(table=table_name, columns=columns)

    def _parse_create_index(self) -> CreateIndexStatement:
        """Parse CREATE INDEX name ON table (column)."""
        self._expect(TokenType.KEYWORD, "INDEX")
        index_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.KEYWORD, "ON")
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.LPAREN)
        column = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.RPAREN)
        return CreateIndexStatement(index_name=index_name, table=table_name, column=column)

    def _parse_column_def(self) -> Column:
        """Parse a single column definition: name TYPE [constraints].

        Constraints: PRIMARY KEY, NOT NULL, UNIQUE (in any order).
        """
        col_name = self._expect(TokenType.IDENTIFIER).value
        type_token = self._advance()

        type_name = type_token.value.upper()
        if type_name not in _TYPE_MAP:
            msg = f"Unknown column type: {type_token.value!r}"
            raise ParseError(msg)

        data_type = _TYPE_MAP[type_name]
        primary_key = False
        not_null = False
        unique = False

        # Parse optional constraints after the type.
        while self._peek().token_type == TokenType.KEYWORD and self._peek().value in (
            "PRIMARY",
            "NOT",
            "UNIQUE",
        ):
            kw = self._advance().value
            if kw == "PRIMARY":
                self._expect(TokenType.KEYWORD, "KEY")
                primary_key = True
                not_null = True  # PRIMARY KEY implies NOT NULL.
            elif kw == "NOT":
                self._expect(TokenType.KEYWORD, "NULL")
                not_null = True
            elif kw == "UNIQUE":
                unique = True

        return Column(
            name=col_name,
            data_type=data_type,
            primary_key=primary_key,
            not_null=not_null,
            unique=unique,
        )

    def _parse_drop(self) -> DropTableStatement | DropIndexStatement:
        """Parse a DROP TABLE or DROP INDEX statement."""
        self._expect(TokenType.KEYWORD, "DROP")
        next_kw = self._peek()
        if next_kw.token_type == TokenType.KEYWORD and next_kw.value == "INDEX":
            self._advance()
            index_name = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.KEYWORD, "ON")
            table_name = self._expect(TokenType.IDENTIFIER).value
            return DropIndexStatement(index_name=index_name, table=table_name)
        self._expect(TokenType.KEYWORD, "TABLE")
        table_name = self._expect(TokenType.IDENTIFIER).value
        return DropTableStatement(table=table_name)

    def _parse_explain(self) -> ExplainStatement:
        """Parse EXPLAIN SELECT ... into an ExplainStatement."""
        self._expect(TokenType.KEYWORD, "EXPLAIN")
        query = self._parse_select()
        return ExplainStatement(query=query)

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

    def _parse_join(self) -> JoinClause:
        """Parse a JOIN ... ON clause.

        Grammar::

            JOIN table ON col = col

        Column references may use dot notation (table.column).

        """
        self._expect(TokenType.KEYWORD, "JOIN")
        join_table = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.KEYWORD, "ON")
        left_col = self._parse_qualified_name()
        self._expect(TokenType.OPERATOR, "=")
        right_col = self._parse_qualified_name()
        return JoinClause(table=join_table, left_column=left_col, right_column=right_col)

    def _parse_qualified_name(self) -> str:
        """Parse an identifier, optionally qualified with dot notation.

        Returns:
            A name like "column" or "table.column".

        """
        name = self._expect(TokenType.IDENTIFIER).value
        if self._peek().token_type == TokenType.DOT:
            self._advance()
            col = self._expect(TokenType.IDENTIFIER).value
            return f"{name}.{col}"
        return name

    def _parse_select_list(self) -> tuple[list[str], list[AggregateColumn]]:
        """Parse the SELECT list (columns and/or aggregate functions).

        Returns:
            A tuple of (plain column names, aggregate columns).

        """
        if self._peek().token_type == TokenType.STAR:
            self._advance()
            return [], []

        columns: list[str] = []
        aggregates: list[AggregateColumn] = []
        self._parse_select_item(columns, aggregates)

        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            self._parse_select_item(columns, aggregates)

        return columns, aggregates

    def _parse_select_item(
        self,
        columns: list[str],
        aggregates: list[AggregateColumn],
    ) -> None:
        """Parse one item in the SELECT list (column or aggregate)."""
        token = self._peek()

        # Check for aggregate function: KEYWORD followed by LPAREN.
        if token.token_type == TokenType.KEYWORD and token.value in _AGG_FUNCS:
            agg = self._parse_aggregate()
            aggregates.append(agg)
        else:
            columns.append(self._parse_qualified_name())

    def _parse_aggregate(self) -> AggregateColumn:
        """Parse an aggregate function call like COUNT(*) or SUM(power)."""
        func_token = self._advance()
        func = _AGG_FUNCS[func_token.value]
        self._expect(TokenType.LPAREN)

        if self._peek().token_type == TokenType.STAR:
            self._advance()
            col = "*"
        else:
            col = self._parse_qualified_name()

        self._expect(TokenType.RPAREN)
        alias = f"{func_token.value}({col})"
        return AggregateColumn(function=func, column=col, alias=alias)

    def _parse_group_by(self) -> list[str]:
        """Parse a GROUP BY clause.

        Grammar::

            GROUP BY col, col, ...

        """
        self._expect(TokenType.KEYWORD, "GROUP")
        self._expect(TokenType.KEYWORD, "BY")
        columns: list[str] = [self._parse_qualified_name()]
        while self._peek().token_type == TokenType.COMMA:
            self._advance()
            columns.append(self._parse_qualified_name())
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
        """Parse a single comparison condition: column op value.

        Supports:
        - Regular: column > 50
        - Aggregate in HAVING: COUNT(*) > 1
        - IN subquery: column IN (SELECT ...)
        - Scalar subquery: column > (SELECT AVG(...) FROM ...)
        """
        token = self._peek()
        if token.token_type == TokenType.KEYWORD and token.value in _AGG_FUNCS:
            agg = self._parse_aggregate()
            column = agg.alias
        else:
            column = self._parse_qualified_name()

        # Handle IN (SELECT ...) or IN (val, val, ...).
        if self._peek().token_type == TokenType.KEYWORD and self._peek().value == "IN":
            self._advance()
            self._expect(TokenType.LPAREN)
            if self._peek().token_type == TokenType.KEYWORD and self._peek().value == "SELECT":
                inner = self._parse_select()
                self._expect(TokenType.RPAREN)
                return Condition(column=column, operator=Operator.IN, value=Subquery(query=inner))
            msg = "IN requires a subquery: IN (SELECT ...)"
            raise ParseError(msg)

        op_token = self._expect(TokenType.OPERATOR)
        if op_token.value not in _OPERATOR_MAP:
            msg = f"Unknown operator: {op_token.value!r}"
            raise ParseError(msg)

        # Check for scalar subquery: op (SELECT ...).
        if self._peek().token_type == TokenType.LPAREN:
            next_after = self._tokens[self._pos + 1] if self._pos + 1 < len(self._tokens) else None
            if (
                next_after
                and next_after.token_type == TokenType.KEYWORD
                and next_after.value == "SELECT"
            ):
                self._expect(TokenType.LPAREN)
                inner = self._parse_select()
                self._expect(TokenType.RPAREN)
                return Condition(
                    column=column,
                    operator=_OPERATOR_MAP[op_token.value],
                    value=Subquery(query=inner),
                )

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
        column = self._parse_qualified_name()

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
