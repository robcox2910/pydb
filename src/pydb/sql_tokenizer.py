"""SQL tokenizer -- break SQL text into meaningful tokens.

Just like a postal worker sorting letters into bins, the tokenizer reads
SQL text character by character and groups them into tokens: keywords,
identifiers, numbers, strings, and operators.
"""

from dataclasses import dataclass
from enum import StrEnum

from pydb.errors import PyDBError


class TokenizerError(PyDBError):
    """Raise when the tokenizer encounters invalid input."""


class TokenType(StrEnum):
    """Types of tokens the tokenizer can produce."""

    KEYWORD = "KEYWORD"
    IDENTIFIER = "IDENTIFIER"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    OPERATOR = "OPERATOR"
    STAR = "STAR"
    COMMA = "COMMA"
    DOT = "DOT"
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    EOF = "EOF"


# SQL keywords we recognise (always stored uppercase).
KEYWORDS = frozenset(
    {
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "LIMIT",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "TABLE",
        "DROP",
        "JOIN",
        "ON",
        "GROUP",
        "HAVING",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "TRUE",
        "FALSE",
    }
)


@dataclass(frozen=True, slots=True)
class Token:
    """A single token from the SQL input.

    Args:
        token_type: What kind of token this is.
        value: The text content of the token.

    """

    token_type: TokenType
    value: str


# Map single characters to their token types.
_SINGLE_CHAR_TOKENS: dict[str, TokenType] = {
    "*": TokenType.STAR,
    ",": TokenType.COMMA,
    ".": TokenType.DOT,
    "(": TokenType.LPAREN,
    ")": TokenType.RPAREN,
}


def tokenize(sql: str) -> list[Token]:
    """Break a SQL string into a list of tokens.

    Args:
        sql: The SQL text to tokenize.

    Returns:
        A list of tokens ending with an EOF token.

    Raises:
        TokenizerError: If the input contains invalid characters.

    """
    tokens: list[Token] = []
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]

        # Skip whitespace.
        if ch.isspace():
            i += 1
        elif ch in _SINGLE_CHAR_TOKENS:
            tokens.append(Token(_SINGLE_CHAR_TOKENS[ch], ch))
            i += 1
        elif ch in ("!", ">", "<") and i + 1 < length and sql[i + 1] == "=":
            tokens.append(Token(TokenType.OPERATOR, sql[i : i + 2]))
            i += 2
        elif ch in ("=", ">", "<"):
            tokens.append(Token(TokenType.OPERATOR, ch))
            i += 1
        elif ch == "'":
            i, string_val = _read_string(sql, i, length)
            tokens.append(Token(TokenType.STRING, string_val))
        elif ch.isdigit():
            i, num_token = _read_number(sql, i, length)
            tokens.append(num_token)
        elif ch.isalpha() or ch == "_":
            i = _read_word_token(sql, i, length, tokens)
        else:
            msg = f"Unexpected character: {ch!r}"
            raise TokenizerError(msg)

    tokens.append(Token(TokenType.EOF, ""))
    return tokens


def _read_string(sql: str, start: int, length: int) -> tuple[int, str]:
    """Read a single-quoted string literal starting at *start*.

    Returns:
        A tuple of (new position, string value without quotes).

    """
    i = start + 1  # Skip the opening quote.
    chars: list[str] = []
    while i < length:
        if sql[i] == "'":
            return i + 1, "".join(chars)
        chars.append(sql[i])
        i += 1
    msg = "Unterminated string literal"
    raise TokenizerError(msg)


def _read_number(sql: str, start: int, length: int) -> tuple[int, Token]:
    """Read an integer or float literal starting at *start*.

    Returns:
        A tuple of (new position, Token).

    """
    i = start
    has_dot = False
    while i < length and (sql[i].isdigit() or sql[i] == "."):
        if sql[i] == ".":
            if has_dot:
                break
            has_dot = True
        i += 1
    text = sql[start:i]
    token_type = TokenType.FLOAT if has_dot else TokenType.INTEGER
    return i, Token(token_type, text)


def _read_word(sql: str, start: int, length: int) -> tuple[int, str]:
    """Read an identifier or keyword starting at *start*.

    Returns:
        A tuple of (new position, word text).

    """
    i = start
    while i < length and (sql[i].isalnum() or sql[i] == "_"):
        i += 1
    return i, sql[start:i]


def _read_word_token(sql: str, start: int, length: int, tokens: list[Token]) -> int:
    """Read a word and append the appropriate token (keyword or identifier).

    Returns:
        The new position after the word.

    """
    i, word = _read_word(sql, start, length)
    upper = word.upper()
    if upper in KEYWORDS:
        tokens.append(Token(TokenType.KEYWORD, upper))
    else:
        tokens.append(Token(TokenType.IDENTIFIER, word))
    return i
