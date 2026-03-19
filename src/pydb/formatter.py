"""Result formatter -- display query results as pretty tables.

Turns a list of row dictionaries into a human-readable ASCII table,
just like the output you see in real database tools.
"""

from collections.abc import Sequence

from pydb.record import Value


def format_results(rows: Sequence[dict[str, Value]], columns: list[str] | None = None) -> str:
    """Format query results as a pretty ASCII table.

    Args:
        rows: The result rows from the executor.
        columns: Column names to display. If None, uses keys from first row.

    Returns:
        A formatted string with borders, headers, and aligned columns.

    """
    if not rows:
        return "(empty result set)"

    # Determine column order.
    cols = columns or list(rows[0].keys())

    # Calculate column widths (header vs data).
    widths: dict[str, int] = {}
    for col in cols:
        max_data = max(len(_format_value(row.get(col, ""))) for row in rows)
        widths[col] = max(len(col), max_data)

    # Build the table.
    parts: list[str] = [
        _border_line(cols, widths, top=True),
        _header_line(cols, widths),
        _border_line(cols, widths, top=False),
        *[_data_line(cols, widths, row) for row in rows],
        _border_line(cols, widths, bottom=True),
    ]

    row_count = len(rows)
    row_word = "row" if row_count == 1 else "rows"
    parts.append(f"{row_count} {row_word} returned")

    return "\n".join(parts)


def _format_value(value: Value | str) -> str:
    """Convert a value to its display string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _border_line(
    cols: list[str],
    widths: dict[str, int],
    *,
    top: bool = False,
    bottom: bool = False,
) -> str:
    """Build a horizontal border line."""
    if top:
        left, mid, right = "┌", "┬", "┐"
    elif bottom:
        left, mid, right = "└", "┴", "┘"
    else:
        left, mid, right = "├", "┼", "┤"
    segments = [f"{'─' * (widths[c] + 2)}" for c in cols]
    return f"{left}{mid.join(segments)}{right}"


def _header_line(cols: list[str], widths: dict[str, int]) -> str:
    """Build the header row with column names."""
    cells = [f" {col:<{widths[col]}} " for col in cols]
    return f"│{'│'.join(cells)}│"


def _data_line(cols: list[str], widths: dict[str, int], row: dict[str, Value]) -> str:
    """Build a data row."""
    cells: list[str] = []
    for col in cols:
        val = _format_value(row.get(col, ""))
        # Right-align numbers, left-align everything else.
        if isinstance(row.get(col), int | float):
            cells.append(f" {val:>{widths[col]}} ")
        else:
            cells.append(f" {val:<{widths[col]}} ")
    return f"│{'│'.join(cells)}│"
