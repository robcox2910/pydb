"""Tests for the DATE and TIMESTAMP column types.

Dates are like calendar pages: a DATE is just the day, while a TIMESTAMP is
the day plus the exact moment, always written in one shared clock (UTC).
"""

from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pydb.btree import BTree
from pydb.csv_io import CSVError, export_table, import_csv
from pydb.errors import SchemaError
from pydb.formatter import format_results
from pydb.schema import Column, Schema
from pydb.serializer import deserialize_table_data, serialize_table_data
from pydb.table import Table
from pydb.types import DataType, check_value, from_iso, to_iso

PLUS_TWO = timezone(timedelta(hours=2))
DAY = date(2026, 10, 17)
MOMENT = datetime(2026, 10, 17, 12, 30, 45, 123456, tzinfo=UTC)
FIVE = 5
THREE = 3
YEAR = 2026
HOUR_UTC = 10
HALF = 1.5


def _naive() -> datetime:
    """Build a deliberately timezone-less datetime without a lint suppression."""
    return datetime.fromisoformat("2026-01-01T00:00:00")


def _table() -> Table:
    """Build a table with a DATE and a TIMESTAMP column."""
    schema = Schema(
        [
            Column(name="day", data_type=DataType.DATE),
            Column(name="at", data_type=DataType.TIMESTAMP),
        ]
    )
    return Table(name="events", schema=schema)


class TestCheckValue:
    """Verify the single type-checking gate."""

    @pytest.mark.parametrize(
        ("value", "data_type"),
        [
            (True, DataType.INTEGER),
            (1, DataType.BOOLEAN),
            (1, DataType.FLOAT),
            (True, DataType.FLOAT),
            (datetime(2026, 1, 1, tzinfo=UTC), DataType.DATE),
            (_naive(), DataType.TIMESTAMP),
            (DAY, DataType.TIMESTAMP),
            ("2026-10-17", DataType.DATE),
            (DAY, DataType.TEXT),
            (None, DataType.TEXT),
        ],
    )
    def test_rejects_wrong_types(self, value: object, data_type: DataType) -> None:
        """Wrong or sloppy types raise SchemaError, never stored silently."""
        with pytest.raises(SchemaError):
            check_value(value, data_type)

    def test_accepts_exact_types(self) -> None:
        """Exact types pass through unchanged."""
        assert check_value("x", DataType.TEXT) == "x"
        assert check_value(FIVE, DataType.INTEGER) == FIVE
        assert check_value(HALF, DataType.FLOAT) == HALF
        assert check_value(True, DataType.BOOLEAN) is True
        assert check_value(DAY, DataType.DATE) == DAY

    def test_timestamp_converted_to_utc(self) -> None:
        """An aware timestamp is converted to UTC."""
        local = datetime(2026, 10, 17, 12, 0, tzinfo=PLUS_TWO)
        result = check_value(local, DataType.TIMESTAMP)
        assert isinstance(result, datetime)
        assert result.utcoffset() == timedelta(0)
        assert result.hour == HOUR_UTC

    def test_schema_validate_rejects(self) -> None:
        """Schema.validate uses the same rules."""
        schema = Schema([Column(name="n", data_type=DataType.INTEGER)])
        with pytest.raises(SchemaError):
            schema.validate({"n": True})

    def test_table_normalises_timestamp(self) -> None:
        """Inserting a +02:00 timestamp stores it in UTC."""
        table = _table()
        rec = table.insert({"day": DAY, "at": datetime(2026, 10, 17, 12, tzinfo=PLUS_TWO)})
        stored = rec["at"]
        assert isinstance(stored, datetime)
        assert stored.hour == HOUR_UTC
        assert stored.utcoffset() == timedelta(0)

    def test_table_rejects_naive(self) -> None:
        """A naive datetime is refused by the table."""
        table = _table()
        with pytest.raises(SchemaError):
            table.insert({"day": DAY, "at": _naive()})


class TestIso:
    """Verify ISO conversion."""

    def test_date_round_trip(self) -> None:
        """A date survives to_iso then from_iso."""
        assert to_iso(DAY) == "2026-10-17"
        assert from_iso("2026-10-17", DataType.DATE) == DAY

    def test_timestamp_round_trip_microseconds(self) -> None:
        """Microseconds are kept and the suffix is Z."""
        text = to_iso(MOMENT)
        assert text == "2026-10-17T12:30:45.123456Z"
        assert from_iso(text, DataType.TIMESTAMP) == MOMENT

    def test_timestamp_without_microseconds(self) -> None:
        """Zero microseconds are omitted."""
        assert to_iso(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)) == "2026-01-02T03:04:05Z"

    def test_offset_input_becomes_utc(self) -> None:
        """A +02:00 input is converted to UTC."""
        result = from_iso("2026-10-17T12:00:00+02:00", DataType.TIMESTAMP)
        assert isinstance(result, datetime)
        assert to_iso(result) == "2026-10-17T10:00:00Z"

    def test_offset_utc_spelling(self) -> None:
        """+00:00 is accepted."""
        result = from_iso("2026-10-17T10:00:00+00:00", DataType.TIMESTAMP)
        assert to_iso(result) == "2026-10-17T10:00:00Z"

    def test_to_iso_converts_offsets(self) -> None:
        """Non-UTC aware datetimes are emitted as UTC."""
        assert to_iso(datetime(2026, 10, 17, 12, tzinfo=PLUS_TWO)) == "2026-10-17T10:00:00Z"

    def test_to_iso_rejects_naive(self) -> None:
        """Naive datetimes cannot be written."""
        with pytest.raises(SchemaError):
            to_iso(_naive())

    @pytest.mark.parametrize(
        ("text", "data_type"),
        [
            ("nonsense", DataType.DATE),
            ("2026-10-17T10:00:00Z", DataType.DATE),
            ("nonsense", DataType.TIMESTAMP),
            ("2026-10-17T10:00:00", DataType.TIMESTAMP),
            ("2026-10-17", DataType.TIMESTAMP),
            ("x", DataType.TEXT),
        ],
    )
    def test_bad_text_rejected(self, text: str, data_type: DataType) -> None:
        """Malformed text raises SchemaError."""
        with pytest.raises(SchemaError):
            from_iso(text, data_type)


class TestPersistence:
    """Verify JSON, CSV, formatter and index behaviour."""

    def test_json_round_trip(self) -> None:
        """Both types survive the JSON table format."""
        table = _table()
        table.insert({"day": DAY, "at": MOMENT})
        text = serialize_table_data("events", table.schema, table.select(), THREE)
        assert "2026-10-17T12:30:45.123456Z" in text
        _, _, records, _ = deserialize_table_data(text)
        assert records[0]["day"] == DAY
        assert records[0]["at"] == MOMENT

    def test_csv_round_trip(self, tmp_path: Path) -> None:
        """Both types survive CSV export then import."""
        table = _table()
        table.insert({"day": DAY, "at": MOMENT})
        path = tmp_path / "e.csv"
        export_table(table, path)
        assert "2026-10-17T12:30:45.123456Z" in path.read_text(encoding="utf-8")
        other = _table()
        import_csv(other, path)
        assert other.select()[0]["at"] == MOMENT
        assert other.select()[0]["day"] == DAY

    def test_csv_bad_date(self, tmp_path: Path) -> None:
        """A bad date in CSV raises CSVError."""
        path = tmp_path / "bad.csv"
        path.write_text("day,at\nnope,2026-10-17T10:00:00Z\n", encoding="utf-8")
        with pytest.raises(CSVError):
            import_csv(_table(), path)

    def test_formatter_shows_iso(self) -> None:
        """The pretty table prints ISO text."""
        out = format_results([{"day": DAY, "at": MOMENT}])
        assert "2026-10-17" in out
        assert "2026-10-17T12:30:45.123456Z" in out

    def test_index_range_on_date(self) -> None:
        """An index on a DATE column finds ranges."""
        table = _table()
        for offset in range(10):
            table.insert({"day": DAY + timedelta(days=offset), "at": MOMENT})
        index = table.create_index("by_day", "day")
        found = index.find_range(DAY + timedelta(days=2), DAY + timedelta(days=4))
        assert len(found) == THREE

    def test_btree_with_timestamps(self) -> None:
        """A B-tree can hold timestamps."""
        tree = BTree()
        tree.insert(MOMENT, 1)
        tree.insert(MOMENT + timedelta(hours=1), 2)
        assert tree.search(MOMENT) == [1]
