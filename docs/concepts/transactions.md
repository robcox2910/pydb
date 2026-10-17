# Transactions

## The "No Take-Backs" Rule (With a Safety Net)

Imagine you're trading cards with a friend. You agree: "I'll give you my
Pikachu if you give me your Charmander." Both trades have to happen, or
neither does. You wouldn't want to hand over your Pikachu and then have
your friend run away without giving you the Charmander!

A **transaction** is exactly this kind of deal. It groups several
database operations together and promises:

- **Either all of them happen, or none of them do.**

If anything goes wrong halfway through, the database *rolls back* --
it's as if nothing happened at all.

## ACID: The Four Promises

Database people use the acronym **ACID** to describe what transactions
guarantee:

| Letter | Name | Meaning | Analogy |
|--------|------|---------|---------|
| **A** | Atomicity | All-or-nothing | The card trade is complete or cancelled |
| **C** | Consistency | Rules are always followed | You can't trade a card you don't have |
| **I** | Isolation | Others can't see half-done work | Nobody sees you holding both cards mid-swap |
| **D** | Durability | Committed changes stick | Once the trade is done, it's done forever |

## How It Works

```python
from pydb.transaction import Transaction

# Start a transaction -- from now on, every change notes how to undo itself.
txn = Transaction(database)

# Make changes through the transaction.
table = txn.get_table("cards")
table.insert({"name": "Pikachu", "power": 55})
table.delete(record_id=3)

# Happy with the changes? Commit them.
txn.commit()   # Changes are now permanent!

# Something went wrong? Roll back.
txn.rollback()  # Every change is undone, newest first.
```

## Under the Hood

Our transaction uses a simple but effective strategy:

1. **Begin** -- remember which tables exist, and hand each one an empty
   **undo log** (a pad of sticky notes).
2. **Work** -- inserts, updates, and deletes happen on the live tables.
   Just before each change, the table writes a note saying how to undo it.
3. **Commit** -- throw the notes away (changes are already applied).
4. **Rollback** -- follow the notes from newest to oldest, put back any
   table that was dropped, and remove any table that was created.

This is called **undo logging** -- we keep enough information to undo
everything if we need to.

### Write down how to undo each scribble

Imagine you borrow your friend's giant notebook to add a few doodles. You
*could* photocopy all 500 pages first, just in case you mess up -- but that
takes ages, and you only planned to draw three little things! The clever
way is a sticky note for each doodle: "drew a cat on page 12, line 3 --
rub it out", or "crossed out 'dog' on page 40 -- it said 'dog'". If it
all goes wrong, you follow the sticky notes backwards, newest first, and
the notebook is exactly as it was. If it goes right, you just throw the
notes in the bin.

PyDB does exactly this. Each kind of change has a matching undo note:

| Change | The undo note says |
|--------|--------------------|
| insert row 7 | remove row 7 (and put the next ID back) |
| update row 3 | put back the old row 3 (the very same old record) |
| delete row 5 | put row 5 back, with the same ID |
| create an index | drop that index |
| drop an index | put that index back, in the same place |
| create a table | drop it |
| drop a table | put the very same table back |

A note is tiny, and writing one takes the same time whether the table has
ten rows or ten million -- nothing is ever copied. (An update never
changes a record in place; it swaps in a new one. So the note can simply
keep the old record and hand it back.) It doesn't matter *how* the change
arrives: through `txn.get_table(...)`, through a `Table` you fetched
before the transaction began, or through a SQL statement. The indexes and
the register of UNIQUE values (see [Constraints](constraints.md)) are put
back along with the rows.

Real databases use fancier techniques (write-ahead logs, MVCC), but
the idea is the same: always be able to go back to a safe state.

On a journalled database (`Database(path, journal=True)`) a committed
transaction is also written to the [journal](journal.md) as one line, so
it survives a crash -- and `with Transaction(db) as txn:` commits for you,
or rolls back if an error escapes the block. Threads take turns while a
transaction is open (see [Locks and Threads](locks-and-threads.md)).

## What We Test

- A committed transaction's changes are visible.
- A rolled-back transaction's changes are undone.
- Rolling back restores the original row count.
- Rolling back restores the original next ID.
- Inserting one row into a 50 000-row table writes one undo note and
  copies no rows (spies count the notes and the records built).
- Rollback undoes changes from every path: a `Table` fetched earlier, SQL
  `INSERT`/`UPDATE`/`DELETE`, index changes, dropping a table, and
  creating then dropping one -- also on a journalled database after a
  reopen or a crash.
- You can't commit or rollback twice.
- Operations after commit or rollback raise an error.

## Next Up

Try it out in the [REPL](repl.md) -- type SQL and see results in
real time.
