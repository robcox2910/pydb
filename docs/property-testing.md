# Property Testing (Every Card Comes Back)

## The Mystery Shopper

Picture a card shop that keeps every customer's trading cards in binders.
You can hand cards in, swap them, take them out, and ask "which cards have
I got, in number order?" PyDB is that shop, made of Python.

How would you check the shop works? The usual way: hand in one card, ask
for it back, and smile. That's an **example test**. It's useful, but it
only tells you about the one card you thought of.

Now imagine a mystery shopper. All day long they hand in thousands of
odd things -- a card with no name, a card whose name is a single space, a
card numbered zero, a name full of quotes and emoji -- and for each one
they check a single rule:

> Whatever you hand in, you get exactly that back.

That rule is a **property**. It doesn't say what any one card looks like.
It says something that must be true for *every* card.

When the shopper finds something that breaks the rule, they don't just
report "something went wrong around lunchtime, with a pile of cards".
They try again with less and less -- fewer cards, shorter names, smaller
numbers -- checking it still goes wrong each time, until it's as simple
as it can be. That boiling-down is called **shrinking**, and it's the
best part: the smallest failing input usually points straight at the bug.

In Python, the mystery shopper is a library called
[Hypothesis](https://hypothesis.readthedocs.io/). PyDB's property tests
live in `tests/test_properties.py`, next to the ordinary example tests,
and the model checks (more on those below) are in `tests/test_model.py`.

## A Property from PyDB

PyDB's [serializer](concepts/serializer.md) writes a whole table down as
JSON so the [storage engine](concepts/storage-engine.md) can keep it in a
file, then reads it back the next day. Whatever goes into the file should
come back out. Here's the property:

```python
@st.composite
def filled_columns(draw: st.DrawFn, row_count: int) -> tuple[Column, list[Value]]:
    """Make one column and the ``row_count`` values the rows hold in it."""
    column = draw(columns())
    values = draw(st.lists(values_of(column.data_type), min_size=row_count, max_size=row_count))
    return column, values


@st.composite
def stored_tables(draw: st.DrawFn) -> StoredTable:
    """Make everything a table file holds: name, schema, rows, next ID and indexes."""
    row_count = draw(st.integers(min_value=0, max_value=MOST_ROWS))
    filled = draw(
        st.lists(filled_columns(row_count), min_size=1, max_size=MOST_COLUMNS, unique_by=_name_of)
    )
    schema = Schema([column for column, _ in filled])
    records = [
        Record(row + 1, {column.name: values[row] for column, values in filled})
        for row in range(row_count)
    ]
    next_id = draw(st.integers(min_value=row_count + 1))
    indexes = draw(st.lists(st.tuples(st.text(), st.sampled_from(schema.column_names))))
    return StoredTable(draw(st.text()), schema, records, next_id, indexes)


@given(table=stored_tables())
def test_save_then_load_gives_back_the_same_table(table: StoredTable) -> None:
    """Loading a saved table gives back exactly the same name, schema, rows and types."""
    saved = serialize_table_data(
        table.name, table.schema, table.records, table.next_id, indexes=table.indexes
    )
    assert as_text(deserialize_table(saved)) == as_text(table)
```

Line by line:

- **`filled_columns` and `stored_tables`** are **strategies**: recipes
  for making random inputs. `@st.composite` lets a recipe use other
  recipes, one `draw` at a time. `filled_columns` makes one column (any
  name, any of PyDB's six types, any mix of PRIMARY KEY, NOT NULL and
  UNIQUE) plus a value for every row: text, whole numbers, decimals
  (even infinity and NaN, "not a number"), `True`/`False`, dates,
  timestamps, or NULL.
- **`stored_tables`** glues the columns into a schema, cuts the values
  into rows, and adds a table name, a next ID and some indexes. Each
  column brings its own values, so when Hypothesis shrinks a failing
  table it can throw away a whole column in one go.
- **`@given(table=stored_tables())`** tells Hypothesis to run the test
  many times (100 in a normal run), each time with a fresh `table`.
- **The docstring** says the rule in plain English. If you can't say the
  rule in one sentence, it probably isn't a good property yet.
- **`serialize_table_data`** writes the table out as JSON text, and
  **`deserialize_table`** reads it back. This kind of property is called
  a **round-trip**.
- **`as_text`** is a small helper that writes out every part of a table
  with `repr()`, the text you'd type to make it. Why not just `==`?
  Because a `Record`'s `==` only compares record IDs, so two cards with
  the same number but different names would count as "equal". And
  Python thinks `True == 1` and `1 == 1.0`, so a BOOLEAN that came back
  as a number would sneak through too. A good property says exactly what
  "the same" means.

Notice what the test *doesn't* say. It never says "save Pikachu with
power 55 and you get Pikachu with power 55". It doesn't need to know any
answers. It only needs a rule that's always true.

PyDB has three more properties:

| Rule | Test |
|---|---|
| Whatever bytes are in a table file, loading gives a table or a `StorageError`, never another error | `test_any_bytes_on_disk_load_or_raise_storage_error` |
| Whatever is under a table file's labels, loading gives a table or a `StorageError`, never another error | `test_any_table_shaped_json_loads_or_raises_storage_error` |
| After any inserts and deletes, the B-Tree holds exactly the right keys, in order, and stays balanced | `test_btree_stays_sorted_and_balanced` |

The two "junk" properties matter because a program using PyDB writes
`except StorageError:` to handle a broken file. If some other error
sneaks out, that program crashes instead. But **a loader that refuses
every file passes both junk properties**, because refusing is always
allowed, so the round-trip sits beside them: it fails straight away if
loading never gives anything back.

## Checking Against a Model

A property checks one action. But a database is something you change
step by step: insert, insert again, delete, look up. Bugs often hide in
the *order* of steps -- deleting a card that was never there, or reusing
a card number after its card was thrown away.

So the shopper has a second trick. They take a notebook and write down
every card they hand in, and after every step they compare the shop's
binders with the notebook. If the two ever disagree, something's wrong
with the shop.

That notebook is a **model**: something simple that we already trust.
For PyDB's SQL, the notebook is a whole second shop: **SQLite**, the
database that comes free with Python (`import sqlite3`) and runs inside
millions of phones and browsers. The shopper types the same request
into both and expects the same answer. Here's part of the check in
`tests/test_model.py`:

```python
class CardTableMachine(RuleBasedStateMachine):
    numbers = Bundle("numbers")

    def __init__(self) -> None:
        """Start with an empty cards table in PyDB and in SQLite."""
        super().__init__()
        self.folder = tempfile.TemporaryDirectory()
        self.db = Database(self.folder.name)
        self.pydb(CREATE_CARDS)
        self.model = sqlite3.connect(":memory:")
        self.model.execute(CREATE_CARDS)

    @rule(target=numbers, num=st.one_of(numbers, card_numbers()), name=card_names())
    def insert_card(self, num: int, name: str | None) -> int:
        """Insert a card in both: PyDB refuses it exactly when SQLite does."""
        sql = f"INSERT INTO cards VALUES ({num}, {sql_text(name)})"
        try:
            self.model.execute("INSERT INTO cards VALUES (?, ?)", (num, name))
        except sqlite3.IntegrityError:  # that number is taken
            with pytest.raises(QueryError):
                self.pydb(sql)
        else:
            assert self.pydb(sql) == [{"result": "1 row inserted"}]
        return num

    @rule(num=st.one_of(numbers, card_numbers()))
    def delete_card(self, num: int) -> None:
        """Delete a card from both: PyDB says it removed as many rows as SQLite did."""
        count = self.model.execute("DELETE FROM cards WHERE num = ?", (num,)).rowcount
        result = self.pydb(f"DELETE FROM cards WHERE num = {num}")
        assert result == [{"result": f"{count} {rows_word(count)} deleted"}]

    @invariant()
    def same_cards_in_the_same_order(self) -> None:
        by_num = self.pydb("SELECT * FROM cards ORDER BY num")
        expected = self.model.execute("SELECT * FROM cards ORDER BY num").fetchall()
        assert [(row["num"], row["name"]) for row in by_num] == expected


TestCardTableModel = cast("type[unittest.TestCase]", CardTableMachine.TestCase)
```

Piece by piece:

- **`RuleBasedStateMachine`** is Hypothesis's tool for testing things
  that change step by step. Hypothesis makes up a random **story** of
  steps and plays it.
- **`self.db` and `self.model`** are the two shops: PyDB in its own
  empty folder, and SQLite in memory. Both start with the same empty
  `cards` table (a `num` that's the PRIMARY KEY, and a `name`).
  `self.pydb(...)` is a one-line helper that parses and runs SQL on PyDB.
- **Each `@rule`** is one kind of step. It does the same thing to both
  shops. PyDB gets real SQL text, with the name in quotes, so the
  tokenizer and parser are tested too; SQLite gets the values handed
  over separately (the `?`s), so it always gets exactly what we meant.
  The real file has rules to insert, rename, delete and look up a card.
- **`numbers = Bundle("numbers")`** is a bag of card numbers used earlier
  in the story. Two random numbers almost never match, so without it the
  story would hardly ever reuse a number. `insert_card` puts its number
  in the bag (`target=numbers`), and every rule picks either a number
  from the bag or a brand-new one, so the story tries cards that exist
  *and* cards that don't.
- **`delete_card`** checks what PyDB *says*, not just what it does: if
  SQLite deleted no rows, PyDB must say `0 rows deleted`.
- **The `@invariant()`** runs after every single step and compares all
  the cards, in number order. (The real one also compares the names in
  name order, where both shops put NULL first.)
- **`TestCardTableModel = ...TestCase`** turns the machine into a test
  that pytest can find and run. One run plays 100 random stories.

The real file has a second machine, `BTreeMachine`, which plays inserts,
deletes, look-ups and range searches on PyDB's [B-Tree](concepts/indexes.md)
and on a plain `dict` of sets, picking a different drawer size for each
story.

SQLite and a `dict` are good models because they're obviously right and
they're *not* PyDB's code. If we built the model by copying PyDB, any
bug would be in both, and they'd agree perfectly about the wrong answer.

## Try It Yourself

Let's break the serializer on purpose and watch the mystery shopper
find it.

Open `src/pydb/serializer.py`, find `_value_from_json`, and change the
TEXT line, as if someone had decided to "tidy up" names as they load:

```python
            return str(raw).strip()  # was: return str(raw)
```

Run the property tests:

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov
```

Hypothesis reports this (trimmed to the interesting part):

```
    @given(table=stored_tables())
    def test_save_then_load_gives_back_the_same_table(table: StoredTable) -> None:
        """Loading a saved table gives back exactly the same name, schema, rows and types."""
        saved = serialize_table_data(
            table.name, table.schema, table.records, table.next_id, indexes=table.indexes
        )
>       assert as_text(deserialize_table(saved)) == as_text(table)
E       assert "('', (Column...''})], 2, [])" == "('', (Column...r'})], 2, [])"
E
E         Skipping 110 identical leading characters in diff, use -v to show
E         - (1, {'': '\r'})], 2, [])
E         ?           --
E         + (1, {'': ''})], 2, [])
E       Failing test case: test_save_then_load_gives_back_the_same_table(
E           table=StoredTable(name='',
E            schema=Schema([:TEXT]),
E            records=[Record(record_id=1, data={'': '\r'})],
E            next_id=2,
E            indexes=[]),
E       )
```

Look at how small that is. Hypothesis tried tables with four columns,
five rows, timestamps, emoji and infinities, then shrank the failure all
the way down to **one TEXT column, one row, and one character**: `'\r'`,
the invisible "carriage return" that ends lines in Windows text files.
It went in, and the empty string `''` came back. (Even the column's name
and the table's name have shrunk to `''`: they don't matter.)

Only the round-trip catches it. The other properties and both model
checks stay green, because none of them saves a table. **The example
tests all still pass. Only the property notices.** They check names
like `"Pikachu"`, and nobody thought to save a name that ends in a
space, let alone one that *is* a carriage return.

Put the line back to `return str(raw)` and run the tests again. They pass.

!!! tip "Older Hypothesis versions"
    Older versions of Hypothesis print `Falsifying example:` instead of
    `Failing test case:`. It means the same thing.

## A Bug We Found

When we first wrote the two junk properties, both failed on PyDB's real
code -- no deliberate bug needed. The bytes one shrank to a single byte:

```
E   UnicodeDecodeError: 'utf-8' codec can't decode byte 0x80 in position 0: invalid start byte
E   Failing test case: test_any_bytes_on_disk_load_or_raise_storage_error(
E       junk=b'\x80',
E   )
```

PyDB's table files are JSON text, stored as UTF-8. The byte `0x80` can
only appear *in the middle* of a UTF-8 character, never at the start, so
that file isn't text at all. The storage engine was ready for a missing
file (`OSError`) and for bad JSON, but a file that isn't text fails in
between, while Python turns the bytes into text, and that error slipped
straight past.

The JSON one found four ways to crash, all with the right labels and the
wrong things under them. The simplest:

```
pydb.errors.SchemaError: A schema must have at least one column
Failing test case: test_any_table_shaped_json_loads_or_raises_storage_error(
    # The test always failed when commented parts were varied together.
    document={
        'name': None,  # or any other generated value
        'schema': {'columns': []},
        'next_id': False,
        'records': None,  # or any other generated value
    },
)
```

The `# or any other generated value` notes mean the name and the rows
don't matter: an empty column list is enough to crash.

The others were a schema of `null`, a column list of `null`, and a column
that was `null` instead of an object: each made Python raise a
`TypeError` deep inside the serializer.

The example tests tried a file of `"not valid json {{{"` and a file with
a label missing. Nobody thought to try a file that isn't text, or a
schema with no columns. The shopper didn't need to think of them.

The fixes:

```python
        except (OSError, UnicodeDecodeError) as exc:  # was: except OSError as exc:
```

in the storage engine, and in the serializer a small helper,
`_refuse_malformed`, that turns any `TypeError`, `ValueError`,
`OverflowError` or `SchemaError` raised while reading a file into a
clear `SerializationError`. Each shrunk input is now an ordinary example
test in `tests/test_storage.py` and `tests/test_serializer.py`, so these
bugs can't sneak back in.

## Run It Yourself

Run just the property tests and the model checks (100 examples each, the
same every time):

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov
```

The `--no-cov` turns off the coverage check, which expects the *whole*
test suite to run and would fail on two files.

Run them the long way (2,000 random examples each, different every run)
to hunt for rarer bugs. It takes a few minutes:

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov --hypothesis-profile=thorough
```

The normal run uses a fixed seed, so it tries the same inputs every time
and a passing build means the same thing on every computer. The thorough
run is random, so each time you run it the mystery shopper brings a
brand-new bag of strange cards. Both settings live in `conftest.py` at
the top of the project.

## Your Turn

Write one new property for the B-Tree: after inserting any list of keys,
a range from the smallest key to the biggest finds every record ID.

What's the strategy? (A list of whole numbers, with at least one in it.)
What's the `assert`? (Compare `find_range` with the record IDs you
handed in.) Run it and watch it pass.

Then break `find_range` on purpose, and see how small a list the
shopper shrinks it to.
