"""Commit routing -- the post room between the tables and the journal.

Every change a table makes arrives here as a ``Change`` note. Outside a
transaction each note is posted straight away as a commit of one. While a
transaction is open, the notes wait in a bundle (the *batch*) until the
transaction commits -- then the whole bundle is posted as one commit -- or
rolls back, and the bundle goes in the bin.

Posting a commit means handing it to every *commit listener* in turn, and
last of all to the final step (on a journalled database, the journal).
"""

from collections.abc import Callable, Sequence

from pydb.changes import Change, CommitListener

FinalStep = Callable[[Sequence[Change]], None]
"""What runs after every listener accepted a commit (the journal write)."""


class CommitRouter:
    """Hold a transaction's changes, or post each change as its own commit.

    Listeners run before the commit counts as done. If one raises, the
    change is undone in memory and the error propagates: a single change
    made outside a transaction is reversed, and a failing
    ``Transaction.commit()`` rolls the transaction back. A commit with no
    changes calls nobody.

    Listeners must treat the changes as read-only -- never mutate
    ``Change.data``. They run in the order added, so an earlier listener may
    already have seen a batch that a later listener's failure then undid.
    The final step (the journal write) comes after every listener, so a
    listener may also see a batch that a failed journal write then undoes.
    """

    __slots__ = ("_batch", "_listeners")

    def __init__(self) -> None:
        """Start with no listeners and no open batch."""
        self._listeners: list[CommitListener] = []
        # Changes waiting for the active transaction (None = no transaction).
        self._batch: list[Change] | None = None

    def add(self, listener: CommitListener) -> None:
        """Call *listener* once per commit with the list of changes committed."""
        self._listeners.append(listener)

    def remove(self, listener: CommitListener) -> None:
        """Stop calling *listener*."""
        self._listeners.remove(listener)

    def clear(self) -> None:
        """Stop calling every listener."""
        self._listeners.clear()

    @property
    def batching(self) -> bool:
        """Return whether a transaction is holding changes."""
        return self._batch is not None

    def route(self, change: Change, final: FinalStep) -> None:
        """Hold *change* for the open transaction, or commit it now."""
        if self._batch is not None:
            self._batch.append(change)
        else:
            self.commit([change], final)

    def commit(self, changes: Sequence[Change], final: FinalStep) -> None:
        """Post *changes* to every listener, then to *final*.

        The final step goes last: once the journal entry is written the
        commit is done, so nothing afterwards can fail and undo a commit
        that is already on disk.
        """
        if not changes:
            return
        for listener in list(self._listeners):
            listener(list(changes))
        final(changes)

    def begin(self) -> None:
        """Start holding changes (the caller checks ``batching`` first)."""
        self._batch = []

    def finish(self, final: FinalStep) -> None:
        """Post the held changes as one commit, then stop holding.

        If a listener raises, the batch stays open so the transaction can
        roll back (its undo steps are held and thrown away, not committed).
        """
        self.commit(self._batch or [], final)
        self._batch = None

    def discard(self) -> None:
        """Throw the held changes away and stop holding."""
        self._batch = None
