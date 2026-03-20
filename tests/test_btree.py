"""Tests for the B-Tree data structure.

A B-Tree is a magical filing cabinet -- it keeps everything sorted and
balanced so you can find any card in just a few steps. These tests verify
that the cabinet works correctly even as cards are added and removed.
"""

from pydb.btree import BTree, BTreeNode

# Named constants.
ORDER_4 = 4
ORDER_3 = 3
RECORD_1 = 1
RECORD_2 = 2
RECORD_3 = 3
RECORD_4 = 4
RECORD_5 = 5
RECORD_6 = 6
RECORD_7 = 7
RECORD_8 = 8
RECORD_9 = 9
RECORD_10 = 10
EXPECTED_CHILDREN = 2


class TestBTreeCreation:
    """Verify initial state of a new B-Tree."""

    def test_empty_tree_has_leaf_root(self) -> None:
        """A new tree should have a single leaf node as root."""
        tree = BTree()
        assert tree.root.leaf

    def test_empty_tree_has_no_keys(self) -> None:
        """A new tree should have no keys."""
        tree = BTree()
        assert tree.root.keys == []

    def test_default_order(self) -> None:
        """The default order should be 4."""
        tree = BTree()
        assert tree.order == ORDER_4

    def test_custom_order(self) -> None:
        """A tree can be created with a custom order."""
        tree = BTree(order=ORDER_3)
        assert tree.order == ORDER_3


class TestBTreeInsert:
    """Verify that keys are inserted correctly."""

    def test_insert_single_key(self) -> None:
        """Inserting one key should store it in the root."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        assert tree.root.keys == ["Pikachu"]
        assert tree.root.values == [[RECORD_1]]

    def test_insert_maintains_sorted_order(self) -> None:
        """Keys should always be sorted within a node."""
        tree = BTree(order=ORDER_4)
        tree.insert("Charmander", RECORD_1)
        tree.insert("Pikachu", RECORD_2)
        tree.insert("Abra", RECORD_3)
        assert tree.root.keys == ["Abra", "Charmander", "Pikachu"]

    def test_insert_duplicate_key_appends_record_id(self) -> None:
        """Inserting the same key again should add to the record list."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        tree.insert("Pikachu", RECORD_2)
        assert tree.root.keys == ["Pikachu"]
        assert tree.root.values == [[RECORD_1, RECORD_2]]

    def test_insert_duplicate_record_id_ignored(self) -> None:
        """Inserting the same key+record pair should not duplicate."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        tree.insert("Pikachu", RECORD_1)
        assert tree.root.values == [[RECORD_1]]

    def test_insert_causes_split(self) -> None:
        """Inserting enough keys should cause the root to split."""
        tree = BTree(order=ORDER_3)
        # Order 3 means max 2 keys per node. Third insert triggers split.
        tree.insert("B", RECORD_1)
        tree.insert("A", RECORD_2)
        tree.insert("C", RECORD_3)
        # Root should no longer be a leaf after split.
        assert not tree.root.leaf
        assert len(tree.root.children) == EXPECTED_CHILDREN

    def test_all_keys_sorted_after_many_inserts(self) -> None:
        """After many inserts, all_keys should return sorted order."""
        tree = BTree(order=ORDER_3)
        for i, name in enumerate(["Eve", "Ada", "Cho", "Gio", "Bob"], start=1):
            tree.insert(name, i)
        assert tree.all_keys() == ["Ada", "Bob", "Cho", "Eve", "Gio"]

    def test_integer_keys(self) -> None:
        """The tree should work with integer keys."""
        tree = BTree(order=ORDER_4)
        for i in range(RECORD_1, RECORD_6):
            tree.insert(i * 10, i)
        assert tree.all_keys() == [10, 20, 30, 40, 50]

    def test_many_inserts_stay_balanced(self) -> None:
        """After many inserts the tree should remain balanced."""
        tree = BTree(order=ORDER_4)
        for i in range(RECORD_1, RECORD_10 + 1):
            tree.insert(i, i)

        def _check_depth(node: BTreeNode) -> int:
            if node.leaf:
                return 0
            depths = [_check_depth(child) for child in node.children]
            # All children must be at the same depth.
            assert len(set(depths)) == 1
            return depths[0] + 1

        _check_depth(tree.root)


class TestBTreeSearch:
    """Verify that keys can be found."""

    def test_search_existing_key(self) -> None:
        """Searching for an existing key should return its record IDs."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        assert tree.search("Pikachu") == [RECORD_1]

    def test_search_missing_key(self) -> None:
        """Searching for a missing key should return an empty list."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        assert tree.search("Charmander") == []

    def test_search_empty_tree(self) -> None:
        """Searching an empty tree should return an empty list."""
        tree = BTree()
        assert tree.search("anything") == []

    def test_search_returns_copy(self) -> None:
        """Modifying the returned list should not affect the tree."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        result = tree.search("Pikachu")
        result.append(RECORD_9)
        assert tree.search("Pikachu") == [RECORD_1]

    def test_search_after_split(self) -> None:
        """Keys should still be findable after the tree splits."""
        tree = BTree(order=ORDER_3)
        names = ["Eve", "Ada", "Cho", "Gio", "Bob"]
        for i, name in enumerate(names, start=1):
            tree.insert(name, i)
        for i, name in enumerate(names, start=1):
            assert tree.search(name) == [i]

    def test_search_with_many_records(self) -> None:
        """A key with multiple records should return all of them."""
        tree = BTree(order=ORDER_4)
        tree.insert("Electric", RECORD_1)
        tree.insert("Electric", RECORD_4)
        tree.insert("Electric", RECORD_7)
        assert tree.search("Electric") == [RECORD_1, RECORD_4, RECORD_7]


class TestBTreeDelete:
    """Verify that keys can be removed."""

    def test_delete_existing_record(self) -> None:
        """Deleting an existing record should return True."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        assert tree.delete("Pikachu", RECORD_1)

    def test_delete_removes_record(self) -> None:
        """After deletion, the record should not be found."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        tree.delete("Pikachu", RECORD_1)
        assert tree.search("Pikachu") == []

    def test_delete_missing_key(self) -> None:
        """Deleting a non-existent key should return False."""
        tree = BTree(order=ORDER_4)
        assert not tree.delete("missing", RECORD_1)

    def test_delete_missing_record_id(self) -> None:
        """Deleting a record ID not in the key should return False."""
        tree = BTree(order=ORDER_4)
        tree.insert("Pikachu", RECORD_1)
        assert not tree.delete("Pikachu", RECORD_9)

    def test_delete_one_of_multiple_records(self) -> None:
        """Deleting one record should leave others intact."""
        tree = BTree(order=ORDER_4)
        tree.insert("Electric", RECORD_1)
        tree.insert("Electric", RECORD_4)
        tree.delete("Electric", RECORD_1)
        assert tree.search("Electric") == [RECORD_4]

    def test_delete_from_non_leaf(self) -> None:
        """Deletion should work even when the key is in a non-leaf."""
        tree = BTree(order=ORDER_3)
        for i in range(RECORD_1, RECORD_6):
            tree.insert(i, i)
        # After splits, some keys will be in internal nodes.
        assert tree.delete(RECORD_2, RECORD_2)
        assert tree.search(RECORD_2) == []


class TestBTreeRangeSearch:
    """Verify range queries."""

    def test_range_returns_matching_records(self) -> None:
        """Range search should return records for keys in the range."""
        tree = BTree(order=ORDER_4)
        tree.insert(10, RECORD_1)
        tree.insert(20, RECORD_2)
        tree.insert(30, RECORD_3)
        tree.insert(40, RECORD_4)
        tree.insert(50, RECORD_5)
        result = tree.find_range(20, 40)
        assert sorted(result) == [RECORD_2, RECORD_3, RECORD_4]

    def test_range_no_matches(self) -> None:
        """Range search with no matching keys should return empty."""
        tree = BTree(order=ORDER_4)
        tree.insert(10, RECORD_1)
        tree.insert(50, RECORD_2)
        assert tree.find_range(20, 40) == []

    def test_range_single_match(self) -> None:
        """Range should work when only one key matches."""
        tree = BTree(order=ORDER_4)
        tree.insert(10, RECORD_1)
        tree.insert(30, RECORD_2)
        tree.insert(50, RECORD_3)
        assert tree.find_range(25, 35) == [RECORD_2]

    def test_range_includes_boundaries(self) -> None:
        """Range boundaries should be inclusive."""
        tree = BTree(order=ORDER_4)
        tree.insert(10, RECORD_1)
        tree.insert(20, RECORD_2)
        tree.insert(30, RECORD_3)
        result = tree.find_range(10, 30)
        assert sorted(result) == [RECORD_1, RECORD_2, RECORD_3]

    def test_range_across_splits(self) -> None:
        """Range should work across node boundaries after splits."""
        tree = BTree(order=ORDER_3)
        for i in range(RECORD_1, RECORD_8):
            tree.insert(i * 10, i)
        result = tree.find_range(20, 50)
        assert sorted(result) == [RECORD_2, RECORD_3, RECORD_4, RECORD_5]

    def test_range_deduplicates(self) -> None:
        """Range results should not contain duplicate record IDs."""
        tree = BTree(order=ORDER_4)
        tree.insert(10, RECORD_1)
        tree.insert(20, RECORD_1)
        result = tree.find_range(10, 20)
        assert result == [RECORD_1]


class TestBTreeAllKeys:
    """Verify the all_keys debugging method."""

    def test_empty_tree(self) -> None:
        """An empty tree should have no keys."""
        tree = BTree()
        assert tree.all_keys() == []

    def test_sorted_output(self) -> None:
        """all_keys should return keys in sorted order."""
        tree = BTree(order=ORDER_3)
        for i, name in enumerate(["Zoe", "Ada", "Max"], start=1):
            tree.insert(name, i)
        assert tree.all_keys() == ["Ada", "Max", "Zoe"]
