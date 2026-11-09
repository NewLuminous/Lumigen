import unittest
from utils.parsing import extract_params, extract_name_wo_params


class TestParsing(unittest.TestCase):
    def test_extract_params_angle_brackets(self):
        self.assertEqual(extract_params("array<int, string>"), ["int", "string"])

    def test_extract_params_parentheses(self):
        self.assertEqual(extract_params("decimal(10, 2)"), ["10", "2"])

    def test_extract_params_square_brackets(self):
        self.assertEqual(extract_params("list[int, float, str]"), ["int", "float", "str"])

    def test_extract_params_curly_braces(self):
        self.assertEqual(extract_params("map{key, value}"), ["key", "value"])

    def test_extract_params_whitespace_trimming(self):
        self.assertEqual(extract_params("decimal(  18 ,  4  )"), ["18", "4"])

    def test_extract_params_empty_brackets(self):
        self.assertEqual(extract_params("array<>"), [])
        self.assertEqual(extract_params("dummy()"), [])

    def test_extract_params_no_brackets(self):
        self.assertEqual(extract_params("varchar"), [])
        self.assertEqual(extract_params("int"), [])

    def test_extract_params_single_param(self):
        self.assertEqual(extract_params("array<string>"), ["string"])

    def test_extract_name_wo_params_with_brackets(self):
        self.assertEqual(extract_name_wo_params("array<int, string>"), "array")
        self.assertEqual(extract_name_wo_params("decimal(10, 2)"), "decimal")
        self.assertEqual(extract_name_wo_params("map{k, v}"), "map")
        self.assertEqual(extract_name_wo_params("list[float]"), "list")

    def test_extract_name_wo_params_without_brackets(self):
        self.assertEqual(extract_name_wo_params("varchar"), "varchar")
        self.assertEqual(extract_name_wo_params("  bigint  "), "bigint")


if __name__ == "__main__":
    unittest.main()

