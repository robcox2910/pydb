# The Outbox Pattern

## All-or-Nothing Shopping

Imagine you're at a shop buying a toy. Two things need to happen:

1. **Pay** -- the cashier takes your money (database update)
2. **Receipt** -- they print you a receipt (send a message)

What if the cashier takes your money but the receipt printer jams?
You paid but have no proof! Or what if the receipt prints but the
payment didn't go through? The shop thinks you paid but you didn't!

The **outbox pattern** solves this by doing BOTH in one action:

1. Save the payment AND the receipt message **together** in the
   database (one transaction -- either both happen or neither does)
2. Later, a separate process reads the receipt messages from the
   database and actually sends them to the message queue

If anything fails in step 1, everything is rolled back. The receipt
message is never sent because it was never saved. Clean and safe.

## How It Works

```
Step 1: Inside one database transaction
┌─────────────────────────────────────────┐
│  UPDATE accounts SET balance = balance - 10  │
│  INSERT INTO outbox (message) VALUES (...)    │
│  COMMIT                                       │
└─────────────────────────────────────────┘
   ↑ Both succeed or both fail -- guaranteed!

Step 2: The "relay" reads from the outbox
┌─────────────────────────────────────────┐
│  Read unsent messages from outbox table       │
│  Send each one to the message queue           │
│  Mark as sent                                 │
└─────────────────────────────────────────┘
```

## Why Not Just Do Both Separately?

Because computers can crash at any moment:

```
❌ BAD: Do two things separately
   1. Save to database  ← succeeds
   2. Send to queue     ← CRASH! Message lost!

✅ GOOD: Outbox pattern
   1. Save data + message in ONE transaction ← all or nothing
   2. Relay sends message later ← safe to retry
```

## The Outbox in PyDB

```python
from pydb.outbox import Outbox

outbox = Outbox(database)

# Everything in one transaction -- all or nothing
outbox.execute(
    sql="UPDATE accounts SET balance = balance - 10 WHERE name = 'Alice'",
    message={"queue": "receipts", "body": "Alice paid $10"},
)

# Later, relay unsent messages to PyMQ
outbox.relay(message_queue)
```

## What We Test

- Data and message are saved together in one transaction.
- If the SQL fails, the message is NOT saved.
- If the message is invalid, the SQL is NOT committed.
- The relay sends unsent messages to the queue.
- Sent messages are marked so they aren't sent twice.
- The relay is safe to run multiple times (idempotent).

## Next Up

This pattern is used by every serious application that needs to
coordinate between a database and a message queue -- from online
stores to banking systems to social media platforms.
