"""A B-Tree -- the data structure behind database indexes.

Think of a B-Tree as a magical filing cabinet. Each drawer holds a few
cards in sorted order. When a drawer gets too full, it splits into two
and pushes the middle card up as a label. This keeps the cabinet perfectly
balanced so you can always find any card in just a few steps.

This implementation maps *keys* (column values) to *lists of record IDs*,
because multiple records can share the same value (e.g., many people might
have the name "Alice").
"""

from dataclasses import dataclass, field
from typing import Any

# A key can be any sortable value type the database supports.
# In practice, all keys in one index share the same type (all str or all int).
BTreeKey = str | int | float | bool

# Default order (max children per node). 4 is small enough to see splits
# in tests, big enough to be useful.
DEFAULT_ORDER = 4

# The smallest order that still has a middle key to promote on a split.
MIN_ORDER = 3


@dataclass
class BTreeNode:
    """Represent a single node (drawer) in the B-Tree.

    Each node holds sorted keys, a list of record-ID-lists (one per key),
    and pointers to child nodes.

    Args:
        leaf: Whether this node is a leaf (has no children).

    """

    leaf: bool = True
    keys: list[Any] = field(default_factory=lambda: [])
    values: list[list[int]] = field(default_factory=lambda: [])
    children: list[BTreeNode] = field(default_factory=lambda: [])


class BTree:
    """A balanced search tree for fast key lookups.

    The B-Tree maintains sorted keys across nodes and automatically
    splits nodes that overflow, keeping the tree balanced at all times.

    Args:
        order: Maximum number of children per node (default 4).

    """

    __slots__ = ("_order", "_root")

    def __init__(self, order: int = DEFAULT_ORDER) -> None:
        """Create an empty B-Tree with the given order.

        Raises:
            ValueError: If the order is below 3 (a drawer must fit two cards
                so it has a middle card to promote when it splits).

        """
        if order < MIN_ORDER:
            msg = f"B-Tree order must be at least {MIN_ORDER}, got {order}"
            raise ValueError(msg)
        self._order = order
        self._root = BTreeNode(leaf=True)

    @property
    def order(self) -> int:
        """Return the tree's order (max children per node)."""
        return self._order

    @property
    def root(self) -> BTreeNode:
        """Return the root node (for inspection in tests)."""
        return self._root

    def search(self, key: BTreeKey) -> list[int]:
        """Find all record IDs associated with a key.

        Args:
            key: The value to search for.

        Returns:
            A list of record IDs, or an empty list if not found.

        """
        return self._search_node(self._root, key)

    def _search_node(self, node: BTreeNode, key: BTreeKey) -> list[int]:
        """Recursively search for a key starting from a node."""
        i = self._find_position(node, key)

        # Check if we found an exact match at position i.
        if i < len(node.keys) and node.keys[i] == key:
            return list(node.values[i])

        # If this is a leaf, the key doesn't exist.
        if node.leaf:
            return []

        # Otherwise, recurse into the appropriate child.
        return self._search_node(node.children[i], key)

    def insert(self, key: BTreeKey, record_id: int) -> None:
        """Insert a key-record pair into the tree.

        If the key already exists, the record ID is appended to that
        key's list (supporting duplicate values in the indexed column).

        Args:
            key: The column value to index.
            record_id: The record ID to associate with this key.

        """
        self._insert_into(self._root, key, record_id)

        # If the top drawer overflowed, split it and grow a new top drawer.
        if len(self._root.keys) > self._order - 1:
            new_root = BTreeNode(leaf=False, children=[self._root])
            self._split_child(new_root, 0)
            self._root = new_root

    def _insert_into(self, node: BTreeNode, key: BTreeKey, record_id: int) -> None:
        """Insert into the subtree rooted at ``node``, splitting on the way back up.

        We walk down to the right bottom drawer and drop the card in. If that
        made a drawer one card too full, its parent splits it on the way back
        up -- so overflow only ever travels upwards, one level at a time.
        """
        i = self._find_position(node, key)

        # If the key already exists, just append the record ID.
        if i < len(node.keys) and node.keys[i] == key:
            if record_id not in node.values[i]:
                node.values[i].append(record_id)
            return

        if node.leaf:
            node.keys.insert(i, key)
            node.values.insert(i, [record_id])
            return

        self._insert_into(node.children[i], key, record_id)
        if len(node.children[i].keys) > self._order - 1:
            self._split_child(node, i)

    def _split_child(self, parent: BTreeNode, child_index: int) -> None:
        """Split an over-full child node into two, promoting the middle key.

        This is the heart of the B-Tree -- when a drawer gets too full,
        we split it and push the middle card up as a label. Cards left of
        the middle stay in the old drawer; cards right of it move to a new one.
        """
        child = parent.children[child_index]
        mid = len(child.keys) // 2

        # The right half becomes a new node.
        right = BTreeNode(leaf=child.leaf)
        right.keys = child.keys[mid + 1 :]
        right.values = child.values[mid + 1 :]

        # The middle card goes up to the parent as a label.
        promoted_key = child.keys[mid]
        promoted_value = child.values[mid]

        # The left half keeps the original node.
        child.keys = child.keys[:mid]
        child.values = child.values[:mid]

        if not child.leaf:
            right.children = child.children[mid + 1 :]
            child.children = child.children[: mid + 1]

        # Insert the promoted key into the parent.
        parent.keys.insert(child_index, promoted_key)
        parent.values.insert(child_index, promoted_value)
        parent.children.insert(child_index + 1, right)

    @property
    def _min_keys(self) -> int:
        """Return the fewest cards any drawer except the top one may hold."""
        return (self._order + 1) // 2 - 1

    def delete(self, key: BTreeKey, record_id: int) -> bool:
        """Remove a record ID from a key's entry.

        If the key has no more record IDs after removal, the key itself
        is removed from the tree, and the tree rebalances itself so it
        stays a proper B-Tree.

        Args:
            key: The column value to search for.
            record_id: The record ID to remove.

        Returns:
            True if the record was found and removed, False otherwise.

        """
        removed = self._delete_from_node(self._root, key, record_id)
        # If the top drawer emptied out, its only child becomes the new top.
        if not self._root.keys and not self._root.leaf:
            self._root = self._root.children[0]
        return removed

    def _delete_from_node(self, node: BTreeNode, key: BTreeKey, record_id: int) -> bool:
        """Remove a record ID from the subtree rooted at ``node``.

        Taking a card out of a drawer can leave it too empty. Whenever that
        happens to a child, ``_fix_underflow`` tops it back up by borrowing a
        card from a neighbouring drawer, or by merging two drawers into one.
        """
        i = self._find_position(node, key)

        if i < len(node.keys) and node.keys[i] == key:
            if record_id not in node.values[i]:
                return False
            node.values[i].remove(record_id)
            if node.values[i]:
                return True  # other records still share this key
            if node.leaf:
                node.keys.pop(i)
                node.values.pop(i)
                return True
            # An internal label can't just vanish -- the drawers below rely on
            # it. Replace it with the biggest card from the left subtree (its
            # in-order predecessor), which still sits between the two halves.
            node.keys[i], node.values[i] = self._pop_largest(node.children[i])
            self._fix_underflow(node, i)
            return True

        if node.leaf:
            return False

        removed = self._delete_from_node(node.children[i], key, record_id)
        if removed:
            self._fix_underflow(node, i)
        return removed

    def _pop_largest(self, node: BTreeNode) -> tuple[BTreeKey, list[int]]:
        """Remove and return the largest key (and its records) in a subtree."""
        if node.leaf:
            return node.keys.pop(), node.values.pop()
        last = len(node.children) - 1
        largest = self._pop_largest(node.children[last])
        self._fix_underflow(node, last)
        return largest

    def _fix_underflow(self, parent: BTreeNode, i: int) -> None:
        """Top up ``parent.children[i]`` if it has too few keys.

        First try to borrow a card from the left or right neighbour (passing
        it through the parent's label, like a rotation). If both neighbours
        are only just full enough, merge the drawer with a neighbour instead.
        """
        child = parent.children[i]
        if len(child.keys) >= self._min_keys:
            return
        if i > 0 and len(parent.children[i - 1].keys) > self._min_keys:
            left = parent.children[i - 1]
            child.keys.insert(0, parent.keys[i - 1])
            child.values.insert(0, parent.values[i - 1])
            parent.keys[i - 1] = left.keys.pop()
            parent.values[i - 1] = left.values.pop()
            if not left.leaf:
                child.children.insert(0, left.children.pop())
        elif i < len(parent.children) - 1 and len(parent.children[i + 1].keys) > self._min_keys:
            right = parent.children[i + 1]
            child.keys.append(parent.keys[i])
            child.values.append(parent.values[i])
            parent.keys[i] = right.keys.pop(0)
            parent.values[i] = right.values.pop(0)
            if not right.leaf:
                child.children.append(right.children.pop(0))
        elif i > 0:
            self._merge_children(parent, i - 1)
        else:
            self._merge_children(parent, i)

    @staticmethod
    def _merge_children(parent: BTreeNode, i: int) -> None:
        """Merge ``children[i + 1]`` into ``children[i]`` around label ``i``."""
        left = parent.children[i]
        right = parent.children.pop(i + 1)
        left.keys += [parent.keys.pop(i), *right.keys]
        left.values += [parent.values.pop(i), *right.values]
        left.children += right.children

    def find_range(self, min_key: BTreeKey, max_key: BTreeKey) -> list[int]:
        """Find all record IDs for keys in the range [min_key, max_key].

        Args:
            min_key: The lower bound (inclusive).
            max_key: The upper bound (inclusive).

        Returns:
            A sorted list of unique record IDs within the range.

        """
        result: list[int] = []
        self._range_search(self._root, min_key, max_key, result)
        return sorted(set(result))

    def _range_search(
        self,
        node: BTreeNode,
        min_key: BTreeKey,
        max_key: BTreeKey,
        result: list[int],
    ) -> None:
        """Collect all record IDs in the key range from a subtree."""
        for i, key in enumerate(node.keys):
            # Visit left child if it might contain keys >= min_key.
            if not node.leaf and key >= min_key:
                self._range_search(node.children[i], min_key, max_key, result)
            # Collect this key if it's in range.
            if min_key <= key <= max_key:
                result.extend(node.values[i])

        # Visit the rightmost child if it might contain keys in range.
        if not node.leaf and node.keys and node.keys[-1] <= max_key:
            self._range_search(node.children[-1], min_key, max_key, result)

    def all_keys(self) -> list[BTreeKey]:
        """Return all keys in the tree in sorted order (for debugging)."""
        result: list[BTreeKey] = []
        self._collect_keys(self._root, result)
        return result

    def _collect_keys(self, node: BTreeNode, result: list[BTreeKey]) -> None:
        """Collect all keys from a subtree via in-order traversal."""
        for i, key in enumerate(node.keys):
            if not node.leaf:
                self._collect_keys(node.children[i], result)
            result.append(key)
        if not node.leaf and node.children:
            self._collect_keys(node.children[-1], result)

    @staticmethod
    def _find_position(node: BTreeNode, key: BTreeKey) -> int:
        """Find the index where a key belongs in a node's key list.

        Uses linear search (fine for small orders). Returns the position
        where the key is or should be inserted.
        """
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        return i
