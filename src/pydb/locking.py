"""Locks -- one pen, many writers, taking turns.

Imagine a class diary with a single pen. Anyone may write in it, but only
the person holding the pen. Everyone else waits in line, and when you've
finished a whole entry you hand the pen on. Nobody ever reads half an entry,
because nobody reads without the pen either.

PyDB has two kinds of pen:

- **Inside one program** a ``Database`` owns a re-entrant lock
  (``threading.RLock``) that every table it hands out shares. Every public
  ``Database`` and ``Table`` method takes it (``@locked``), and a
  ``Transaction`` holds it from start to finish -- so threads take turns and
  a reader never sees half a transaction. *Re-entrant* means the thread that
  already holds the pen can pick it up again (a table method calling
  another one) without waiting for itself forever.
- **Between programs** a journalled database writes a ``LOCK`` file
  holding its process ID (PID). A second program that finds it gets
  ``DatabaseLockedError``. If the PID belongs to a program that has died,
  the lock is *stale* and is taken over.

Taking over a stale lock is not perfectly race-free: two programs that both
spot the same dead PID at the same moment could both remove it. That needs
two programs to start within a few microseconds of each other after a crash.
"""

import functools
import gc
import os
import threading
from collections.abc import Callable
from pathlib import Path
from types import FunctionType
from typing import Concatenate, Protocol

from pydb.atomic import sync_folder
from pydb.errors import PyDBError

LOCK_FILE = "LOCK"
"""The lock file (inside the database folder) holding the owner's PID."""

LockType = threading.RLock
"""The type of the re-entrant lock a database and its tables share."""

# Folders with an open journalled Database in *this* process.
_open_here: set[Path] = set()
# Re-entrant: a forgotten Database's finalizer (LockFile.release) can run
# during garbage collection *inside* a guarded block of this same thread.
_open_here_guard = threading.RLock()


class DatabaseLockedError(PyDBError):
    """Raise when another program (or another Database here) has the folder open."""


class Lockable(Protocol):
    """Anything with a ``lock`` that its methods take (a ``Database`` or ``Table``)."""

    @property
    def lock(self) -> LockType:
        """Return the lock to hold while a method runs."""
        ...


def locked[S: Lockable, **P, R](
    method: Callable[Concatenate[S, P], R],
) -> Callable[Concatenate[S, P], R]:
    """Make *method* hold ``self.lock`` while it runs.

    Args:
        method: A method of a ``Lockable`` class.

    Returns:
        The same method, wrapped so it waits its turn for the lock.

    """

    @functools.wraps(method)
    def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
        with self.lock:
            return method(self, *args, **kwargs)

    return wrapper


def lock_public_methods[C: type](cls: C) -> C:
    """Make every public method and property of *cls* hold ``self.lock``.

    A class decorator, so no public method can forget to take its turn.
    Names starting with ``_`` (and ``lock`` itself) are left alone.

    Args:
        cls: A ``Lockable`` class.

    Returns:
        The same class, its public methods wrapped with ``locked``.

    """
    for name, member in list(vars(cls).items()):
        if name.startswith("_") or name == "lock":
            continue
        if isinstance(member, FunctionType):
            setattr(cls, name, locked(member))
        elif isinstance(member, property):
            setattr(
                cls,
                name,
                property(
                    member.fget and locked(member.fget),
                    member.fset and locked(member.fset),
                    member.fdel and locked(member.fdel),
                    member.__doc__,
                ),
            )
    return cls


def _pid_alive(pid: int) -> bool:
    """Return whether a process with this PID is running."""
    try:
        os.kill(pid, 0)  # signal 0 delivers nothing; it only checks
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # it exists, it just isn't ours
    return True


class LockFile:
    """The ``LOCK`` file that keeps one program at a time in a folder.

    Args:
        folder: The database folder.

    """

    __slots__ = ("_folder", "_held", "_key", "_owner", "_path")

    def __init__(self, folder: Path) -> None:
        """Prepare (but don't take) the lock for *folder*."""
        self._folder = folder
        self._path = folder / LOCK_FILE
        self._key = folder.resolve()
        self._held = False
        self._owner = 0  # the PID written into LOCK

    def acquire(self) -> None:
        """Create ``LOCK`` with this process's PID, taking over a stale one.

        Raises:
            DatabaseLockedError: If a living process (or another open
                ``Database`` in this process) holds the folder, or the
                ``LOCK`` file can't be read.

        """
        for attempt in range(2):
            with _open_here_guard:
                if self._key not in _open_here:
                    self._folder.mkdir(parents=True, exist_ok=True)
                    self._create()
                    _open_here.add(self._key)
                    self._held = True
                    self._owner = os.getpid()
                    return
            if attempt == 0:
                # A Database nobody can reach any more lets go when the
                # garbage collector tidies it away -- give it the chance.
                gc.collect()
        msg = f"{self._folder} is already open in this process; close() the other Database first"
        raise DatabaseLockedError(msg)

    def _create(self) -> None:
        """Create the file exclusively, removing at most one stale lock."""
        for _attempt in range(2):
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                self._remove_if_stale()
                continue
            try:
                try:
                    os.write(fd, f"{os.getpid()}\n".encode("ascii"))
                    os.fsync(fd)
                finally:
                    os.close(fd)
                sync_folder(self._folder)
            except BaseException:
                self._path.unlink(missing_ok=True)  # don't leave an empty LOCK behind
                raise
            return
        msg = f"{self._folder} is locked: another process took {LOCK_FILE} first"
        raise DatabaseLockedError(msg)

    def _remove_if_stale(self) -> None:
        """Delete ``LOCK`` if its owner is dead, else raise ``DatabaseLockedError``."""
        try:
            text = self._path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return  # its owner just closed: try again
        except OSError, UnicodeDecodeError:
            text = ""
        if not text.isdigit():
            msg = (
                f"{self._path} doesn't hold a process ID. If no program is using "
                "the database, delete the file and try again"
            )
            raise DatabaseLockedError(msg)
        pid = int(text)
        # Our own PID here means a Database in this process was never closed
        # (it isn't in the open set), so nobody is really holding it.
        if pid != os.getpid() and _pid_alive(pid):
            msg = (
                f"{self._folder} is in use by process {pid}. If that program isn't "
                "using the database (after a crash its process ID can be reused by "
                f"another program), delete {self._path} and try again"
            )
            raise DatabaseLockedError(msg)
        self._path.unlink(missing_ok=True)

    def release(self) -> None:
        """Delete ``LOCK`` (only once; later calls do nothing)."""
        if not self._held:
            return
        self._held = False
        if os.getpid() != self._owner:
            return  # a forked child: the LOCK belongs to its parent
        with _open_here_guard:
            _open_here.discard(self._key)
            self._path.unlink(missing_ok=True)
