import os
import sys
import unittest



try:
    import sqlglot
    from sqlglot import exp, parse_one
    from utils.sql import (
        parse_into_select_exp,
        extract_main_table,
        dialect_insensitive_tbl_name,
        identifier_requires_quoting,
        split_and_conditions,
        ExtractedTablePushdown,
        TablePushdownExtractor,
    )
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


@unittest.skipUnless(SQLGLOT_AVAILABLE, "sqlglot is not available")
class TestSqlUtils(unittest.TestCase):
    # ==========================
    # parse_into_select_exp
    # ==========================
    def test_parse_into_select_exp_select_query(self):
        query = "SELECT id, name FROM users WHERE id > 10"
        res = parse_into_select_exp(query)
        self.assertIsInstance(res, exp.Select)
        self.assertEqual(res.sql(), query)

    def test_parse_into_select_exp_ast_objects(self):
        select_node = exp.select("a").from_("tbl")
        self.assertIs(parse_into_select_exp(select_node), select_node)

        table_node = exp.table_("orders")
        res = parse_into_select_exp(table_node)
        self.assertIsInstance(res, exp.Select)
        self.assertEqual(res.sql(), "SELECT * FROM orders")

    def test_parse_into_select_exp_type_errors(self):
        # A standalone column identifier expression or non-table string
        var_node = exp.var("x")
        with self.assertRaises(TypeError):
            parse_into_select_exp(var_node)

        with self.assertRaises(TypeError):
            parse_into_select_exp(123)

        # Standalone identifier string is parsed as Column, which raises TypeError
        with self.assertRaises(TypeError):
            parse_into_select_exp("my_table")

    def test_parse_into_select_exp_parse_error(self):
        with self.assertRaises(ValueError):
            parse_into_select_exp("SELECT FROM WHERE ;;;")

    # ==========================
    # extract_main_table
    # ==========================
    def test_extract_main_table_simple(self):
        self.assertEqual(extract_main_table("SELECT * FROM users"), "users")
        self.assertEqual(extract_main_table("SELECT * FROM schema1.users"), "schema1.users")

    def test_extract_main_table_cte(self):
        query = "WITH my_cte AS (SELECT * FROM raw_data) SELECT * FROM my_cte"
        self.assertEqual(extract_main_table(query), "raw_data")

    def test_extract_main_table_join(self):
        query = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        self.assertEqual(extract_main_table(query), "orders")

    def test_extract_main_table_union(self):
        query = "SELECT * FROM table_a UNION ALL SELECT * FROM table_b"
        self.assertEqual(extract_main_table(query), "table_a")

    def test_extract_main_table_dml_ddl(self):
        self.assertEqual(extract_main_table("CREATE TABLE new_tbl AS SELECT * FROM old_tbl"), "new_tbl")
        self.assertEqual(extract_main_table("INSERT INTO target_tbl SELECT * FROM src"), "target_tbl")
        self.assertEqual(extract_main_table("TRUNCATE TABLE my_tbl"), "my_tbl")

    def test_extract_main_table_quoting_and_dialect(self):
        query = "SELECT * FROM users"
        res_quoted = extract_main_table(query, quoted=True, dialect="postgres")
        self.assertEqual(res_quoted, '"users"')

        res_unquoted = extract_main_table(query, quoted=False, dialect="postgres")
        self.assertEqual(res_unquoted, "users")

    def test_extract_main_table_no_physical_table(self):
        # Table-generating functions in the FROM clause
        query = "SELECT * FROM generate_series(1, 10)"
        with self.assertRaises(ValueError):
            extract_main_table(query)

    # ==========================
    # dialect_insensitive_tbl_name
    # ==========================
    def test_dialect_insensitive_tbl_name(self):
        self.assertEqual(dialect_insensitive_tbl_name("", "postgres"), "")
        self.assertIsNone(dialect_insensitive_tbl_name(None, "postgres"))

        # From MySQL backticks to standard SQL quotes
        res = dialect_insensitive_tbl_name("`mydb`.`mytable`", from_dialect="mysql")
        self.assertEqual(res, '"mydb"."mytable"')

        # Expression
        tbl_exp = exp.table_("my_table", db="my_db")
        self.assertEqual(dialect_insensitive_tbl_name(tbl_exp, "postgres"), "my_db.my_table")

    # ==========================
    # identifier_requires_quoting
    # ==========================
    def test_identifier_requires_quoting(self):
        # With identify_setting=False
        self.assertFalse(identifier_requires_quoting("valid_column", "postgres", identify_setting=False))

        # With identify_setting=True
        self.assertTrue(identifier_requires_quoting("valid_column", "postgres", identify_setting=True))

        # Reserved keywords
        self.assertTrue(identifier_requires_quoting("select", "postgres"))
        self.assertTrue(identifier_requires_quoting("order", "postgres"))

        # Identifier with special characters when forced
        self.assertTrue(identifier_requires_quoting("col-name", "postgres", identify_setting=True))

    # ==========================
    # split_and_conditions
    # ==========================
    def test_split_and_conditions(self):
        self.assertEqual(split_and_conditions(None), [])

        cond = parse_one("a = 1 AND b = 2 AND (c = 3 AND d = 4)")
        parts = split_and_conditions(cond)
        sql_parts = {p.sql() for p in parts}
        self.assertEqual(sql_parts, {"a = 1", "b = 2", "c = 3", "d = 4"})

    # ==========================
    # TablePushdownExtractor
    # ==========================
    def test_table_pushdown_extractor_single_table_where(self):
        ast = parse_one("SELECT * FROM users u WHERE u.age > 21 AND u.status = 'ACTIVE'")
        extractor = TablePushdownExtractor(ast)
        results = extractor.extract()

        self.assertEqual(len(results), 1)
        data = results[0]
        self.assertEqual(data.table_name, "users")
        self.assertEqual(data.alias, "u")
        # Column qualifier 'u.' must be stripped in condition_sql
        self.assertEqual(data.condition_sql, "age > 21 AND status = 'ACTIVE'")

    def test_table_pushdown_extractor_multi_table_join_where(self):
        ast = parse_one(
            "SELECT * FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "WHERE u.age > 21 AND o.amount > 100 AND u.id = o.user_id"
        )
        extractor = TablePushdownExtractor(ast)
        results = extractor.extract()

        table_data = {r.alias: r for r in results}
        self.assertEqual(table_data["u"].condition_sql, "age > 21")
        self.assertEqual(table_data["o"].condition_sql, "amount > 100")

    def test_table_pushdown_extractor_left_join_safety(self):
        ast = parse_one(
            "SELECT * FROM users u "
            "LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'COMPLETED'"
        )
        extractor = TablePushdownExtractor(ast)
        results = extractor.extract()

        table_data = {r.alias: r for r in results}
        # In LEFT JOIN, ON condition for right table ('orders') is pushable
        self.assertEqual(table_data["o"].condition_sql, "status = 'COMPLETED'")
        # But users must not have condition pushed from the ON clause
        self.assertIsNone(table_data["u"].condition_sql)

    def test_table_pushdown_extractor_repeated_calls(self):
        ast = parse_one("SELECT * FROM users WHERE active = true")
        extractor = TablePushdownExtractor(ast)
        first_run = extractor.extract()
        self.assertEqual(first_run[0].condition_sql, "active = TRUE")

        # Second call must clear previous conditions and produce same result without duplicating
        second_run = extractor.extract()
        self.assertEqual(second_run[0].condition_sql, "active = TRUE")


if __name__ == "__main__":
    unittest.main()
