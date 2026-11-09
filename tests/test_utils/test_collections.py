import unittest
from utils.collections import (
    merge_dicts,
    rename_key,
    _strip_quotes,
    SQLIdentifierSet,
    SQLIdentifierDict,
)


class TestCollections(unittest.TestCase):
    # ==========================
    # merge_dicts tests
    # ==========================
    def test_merge_dicts_empty(self):
        self.assertEqual(merge_dicts(), {})
        self.assertEqual(merge_dicts({}, {}), {})

    def test_merge_dicts_shallow(self):
        d1 = {"a": 1, "b": 2}
        d2 = {"b": 3, "c": 4}
        merged = merge_dicts(d1, d2)
        self.assertEqual(merged, {"a": 1, "b": 3, "c": 4})

    def test_merge_dicts_nested_recursive(self):
        d1 = {"user": {"name": "Alice", "meta": {"role": "admin"}}, "tags": ["a"]}
        d2 = {"user": {"age": 30, "meta": {"active": True}}}
        merged = merge_dicts(d1, d2)
        expected = {
            "user": {
                "name": "Alice",
                "age": 30,
                "meta": {"role": "admin", "active": True},
            },
            "tags": ["a"],
        }
        self.assertEqual(merged, expected)

    def test_merge_dicts_deepcopy_behavior(self):
        sub = {"key": "original"}
        d = {"sub": sub}
        merged = merge_dicts({}, d)
        sub["key"] = "mutated"
        self.assertEqual(merged["sub"]["key"], "original")

    def test_merge_dicts_multiple(self):
        d1 = {"a": 1}
        d2 = {"b": 2}
        d3 = {"c": 3, "a": 10}
        merged = merge_dicts(d1, d2, d3)
        self.assertEqual(merged, {"a": 10, "b": 2, "c": 3})

    # ==========================
    # rename_key tests
    # ==========================
    def test_rename_key_single_and_multiple(self):
        data = {"foo": 1, "bar": 2, "baz": 3}
        mapping = {"foo": "foobar", "bar": "barfoo"}
        res = rename_key(data, mapping)
        self.assertIs(res, data)
        self.assertEqual(res, {"foobar": 1, "barfoo": 2, "baz": 3})

    def test_rename_key_missing_key(self):
        data = {"x": 10}
        mapping = {"not_in_data": "y"}
        res = rename_key(data, mapping)
        self.assertEqual(res, {"x": 10})

    # ==========================
    # _strip_quotes tests
    # ==========================
    def test_strip_quotes(self):
        self.assertEqual(_strip_quotes('"my_col"'), (True, "my_col"))
        self.assertEqual(_strip_quotes("`my_col`"), (True, "my_col"))
        self.assertEqual(_strip_quotes("[my_col]"), (True, "my_col"))
        self.assertEqual(_strip_quotes("my_col"), (False, "my_col"))
        self.assertEqual(_strip_quotes('"mismatched`'), (False, '"mismatched`'))
        self.assertEqual(_strip_quotes('"'), (False, '"'))
        self.assertEqual(_strip_quotes(""), (False, ""))

    # ==========================
    # SQLIdentifierSet tests
    # ==========================
    def test_sql_identifier_set_init_and_len(self):
        s = SQLIdentifierSet(["col_a", "col_b"])
        self.assertEqual(len(s), 2)
        self.assertIn("col_a", s)
        self.assertIn("col_b", s)

    def test_sql_identifier_set_contains_unquoted_stored(self):
        s = SQLIdentifierSet()
        s.add("my_column")

        # Exact match
        self.assertIn("my_column", s)
        # Quoted variations
        self.assertIn('"my_column"', s)
        self.assertIn("`my_column`", s)
        self.assertIn("[my_column]", s)
        # Uppercase match
        self.assertIn("MY_COLUMN", s)
        # Case-insensitive
        self.assertIn("My_Column", s)
        self.assertNotIn("other_col", s)

    def test_sql_identifier_set_contains_quoted_stored(self):
        s = SQLIdentifierSet()
        s.add('"quoted_col"')

        # Exact quoted
        self.assertIn('"quoted_col"', s)
        # Unquoted lookup
        self.assertIn("quoted_col", s)
        # Alternate quotes
        self.assertIn("`quoted_col`", s)
        self.assertIn("[quoted_col]", s)

    def test_sql_identifier_set_get(self):
        s = SQLIdentifierSet(["col_x"])
        self.assertEqual(s.get("col_x"), "col_x")
        self.assertEqual(s.get('"col_x"'), "col_x")
        self.assertEqual(s.get("COL_X"), "col_x")
        self.assertEqual(s.get("non_existent", "default"), "default")
        self.assertIsNone(s.get("non_existent"))

    def test_sql_identifier_set_discard_and_rebuild_map(self):
        s = SQLIdentifierSet(["ABC", "abc"])
        self.assertEqual(len(s), 2)
        s.discard("ABC")
        self.assertEqual(len(s), 1)
        self.assertIn("abc", s)
        # Discard non-existent
        s.discard("non_existent")
        self.assertEqual(len(s), 1)

    def test_sql_identifier_set_iter_and_repr(self):
        empty_s = SQLIdentifierSet()
        self.assertEqual(repr(empty_s), "SQLIdentifierSet()")

        s = SQLIdentifierSet(["col"])
        self.assertIn("col", repr(s))
        items = list(iter(s))
        self.assertEqual(items, ["col"])

    # ==========================
    # SQLIdentifierDict tests
    # ==========================
    def test_sql_identifier_dict_set_get(self):
        d = SQLIdentifierDict()
        d["user_id"] = "BIGINT"
        d['"created_at"'] = "TIMESTAMP"

        # Exact
        self.assertEqual(d["user_id"], "BIGINT")
        # Quoted lookups
        self.assertEqual(d['"user_id"'], "BIGINT")
        self.assertEqual(d["`user_id`"], "BIGINT")
        self.assertEqual(d["[user_id]"], "BIGINT")
        # Case variations
        self.assertEqual(d["USER_ID"], "BIGINT")
        self.assertEqual(d["User_Id"], "BIGINT")

        # Quoted stored lookup unquoted
        self.assertEqual(d['"created_at"'], "TIMESTAMP")
        self.assertEqual(d["created_at"], "TIMESTAMP")

    def test_sql_identifier_dict_fallback_return_key(self):
        d = SQLIdentifierDict()
        # Fallback when key not found is returning queried key string itself
        self.assertEqual(d["unknown_column"], "unknown_column")

    def test_sql_identifier_dict_contains(self):
        d = SQLIdentifierDict()
        d["score"] = 100
        self.assertIn("score", d)
        self.assertIn('"score"', d)
        self.assertIn("[score]", d)
        self.assertIn("SCORE", d)
        self.assertNotIn("missing", d)

    def test_sql_identifier_dict_delitem(self):
        d = SQLIdentifierDict()
        d["col1"] = 1
        d["COL2"] = 2
        del d["col1"]
        self.assertNotIn("col1", d)

        # Deleting non-existent raises KeyError
        with self.assertRaises(KeyError):
            del d["non_existent"]

    def test_sql_identifier_dict_delitem_rebuilds_map(self):
        d = SQLIdentifierDict()
        d["item"] = 1
        d["ITEM"] = 2
        del d["item"]
        self.assertIn("ITEM", d)
        self.assertEqual(d["item"], 2)

    def test_utils_package_exports(self):
        import utils as lu
        self.assertTrue(hasattr(lu, "merge_dicts"))
        self.assertTrue(hasattr(lu, "rename_key"))
        self.assertTrue(hasattr(lu, "SQLIdentifierDict"))
        self.assertTrue(hasattr(lu, "SQLIdentifierSet"))


if __name__ == "__main__":
    unittest.main()
