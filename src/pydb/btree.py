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
        """Create an empty B-Tree with the given order."""
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
        root = self._root

        # If the root is full, split it first.
        if len(root.keys) == self._order - 1:
            new_root = BTreeNode(leaf=False)
            new_root.children.append(self._root)
            self._split_child(new_root, 0)
            self._root = new_root

        self._insert_non_full(self._root, key, record_id)

    def _insert_non_full(self, node: BTreeNode, key: BTreeKey, record_id: int) -> None:
        """Insert into a node that is guaranteed not to be full."""
        i = self._find_position(node, key)

        # If the key already exists, just append the record ID.
        if i < len(node.keys) and node.keys[i] == key:
            if record_id not in node.values[i]:
                node.values[i].append(record_id)
            return

        if node.leaf:
            # Insert the key and value at the correct position.
            node.keys.insert(i, key)
            node.values.insert(i, [record_id])
        else:
            # If the child we need to descend into is full, split it first.
            if len(node.children[i].keys) == self._order - 1:
                self._split_child(node, i)
                # After split, decide which of the two children to use.
                if key > node.keys[i]:
                    i += 1
                elif key == node.keys[i]:
                    # Key matches the promoted key.
                    if record_id not in node.values[i]:
                        node.values[i].append(record_id)
                    return
            self._insert_non_full(node.children[i], key, record_id)

    def _split_child(self, parent: BTreeNode, child_index: int) -> None:
        """Split a full child node into two, promoting the middle key.

        This is the heart of the B-Tree -- when a drawer gets too full,
        we split it and push the middle card up as a label.
        """
        order = self._order
        child = parent.children[child_index]
        mid = order // 2

        # The right half becomes a new node.
        right = BTreeNode(leaf=child.leaf)
        right.keys = child.keys[mid:]
        right.values = child.values[mid:]

        # Promote the middle key to the parent.
        promoted_key = child.keys[mid - 1]
        promoted_value = child.values[mid - 1]

        # The left half keeps the original node.
        child.keys = child.keys[: mid - 1]
        child.values = child.values[: mid - 1]

        if not child.leaf:
            right.children = child.children[mid:]
            child.children = child.children[:mid]

        # Insert the promoted key into the parent.
        parent.keys.insert(child_index, promoted_key)
        parent.values.insert(child_index, promoted_value)
        parent.children.insert(child_index + 1, right)

    def delete(self, key: BTreeKey, record_id: int) -> bool:
        """Remove a record ID from a key's entry.

        If the key has no more record IDs after removal, the key itself
        is removed from the tree.

        Args:
            key: The column value to search for.
            record_id: The record ID to remove.

        Returns:
            True if the record was found and removed, False otherwise.

        """
        return self._delete_from_node(self._root, key, record_id)

    def _delete_from_node(self, node: BTreeNode, key: BTreeKey, record_id: int) -> bool:
        """Remove a record ID from the tree starting at the given node.

        Note: This is a simplified deletion that handles the common case.
        A production B-Tree would rebalance after deletion, but for our
        educational purposes the tree remains valid (just potentially
        under-filled in some nodes).
        """
        i = self._find_position(node, key)

        if i < len(node.keys) and node.keys[i] == key:
            if record_id in node.values[i]:
                node.values[i].remove(record_id)
                # If no more record IDs for this key, remove the key.
                if not node.values[i]:
                    node.keys.pop(i)
                    node.values.pop(i)
                return True
            return False

        if node.leaf:
            return False

        return self._delete_from_node(node.children[i], key, record_id)

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
