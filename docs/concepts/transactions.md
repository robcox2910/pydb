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

# Start a transaction -- take a "snapshot" of the current state.
txn = Transaction(database)

# Make changes through the transaction.
table = txn.get_table("cards")
table.insert({"name": "Pikachu", "power": 55})
table.delete(record_id=3)

# Happy with the changes? Commit them.
txn.commit()   # Changes are now permanent!

# Something went wrong? Roll back.
txn.rollback()  # Everything reverts to the snapshot.
```

## Under the Hood

Our transaction uses a simple but effective strategy:

1. **Begin** -- take a snapshot of each table's records and next ID.
2. **Work** -- inserts, updates, and deletes happen on the live tables.
3. **Commit** -- discard the snapshots (changes are already applied).
4. **Rollback** -- restore every table from its snapshot.

This is called **undo logging** -- we keep enough information to undo
everything if we need to.

Real databases use fancier techniques (write-ahead logs, MVCC), but
the idea is the same: always be able to go back to a safe state.

## What We Test

- A committed transaction's changes are visible.
- A rolled-back transaction's changes are undone.
- Rolling back restores the original row count.
- Rolling back restores the original next ID.
- You can't commit or rollback twice.
- Operations after commit or rollback raise an error.

## Next Up

Try it out in the [REPL](repl.md) -- type SQL and see results in
real time.
