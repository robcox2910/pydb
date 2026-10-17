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

# Start a transaction -- ready to photocopy any table before it changes.
txn = Transaction(database)

# Make changes through the transaction.
table = txn.get_table("cards")
table.insert({"name": "Pikachu", "power": 55})
table.delete(record_id=3)

# Happy with the changes? Commit them.
txn.commit()   # Changes are now permanent!

# Something went wrong? Roll back.
txn.rollback()  # Everything reverts to the snapshots.
```

## Under the Hood

Our transaction uses a simple but effective strategy:

1. **Begin** -- remember which tables exist, and ask each one to shout
   "I'm about to change!" before its first change.
2. **Work** -- inserts, updates, and deletes happen on the live tables. The
   first time a table is about to change, we take a snapshot of its records,
   next ID and index list -- *then* let the change happen.
3. **Commit** -- discard the snapshots (changes are already applied).
4. **Rollback** -- restore every snapshotted table, put back any table that
   was dropped, and remove any table that was created.

This is called **undo logging** -- we keep enough information to undo
everything if we need to.

### Only photocopy the pages you're about to scribble on

Imagine you borrow your friend's giant notebook to add one doodle. You
*could* photocopy all 500 pages first, just in case you mess up -- but
that takes ages, and you only planned to draw on page 12! The clever way
is to photocopy **page 12 only**, right before you draw on it. If the
doodle goes wrong, you stick the copy back; the other 499 pages were never
touched, so they never needed a copy.

PyDB transactions work the same way (this is called **copy-on-first-write**).
A table is copied only the first time the transaction is about to change
it -- an insert, update or delete, creating or dropping an index, or
dropping the table itself. It doesn't matter *how* the change arrives: through
`txn.get_table(...)`, through a `Table` you fetched before the transaction
began, or through a SQL statement. A huge history table that the
transaction never touches is never copied, so a small transaction stays
quick however big the rest of the database grows.

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
- A big table the transaction never touches is never copied (a spy counts
  the snapshots).
- Rollback undoes changes from every path: a `Table` fetched earlier, SQL
  `INSERT`/`UPDATE`/`DELETE`, index changes, dropping a table, and
  creating then dropping one -- also on a journalled database after a
  reopen or a crash.
- You can't commit or rollback twice.
- Operations after commit or rollback raise an error.

## Next Up

Try it out in the [REPL](repl.md) -- type SQL and see results in
real time.
