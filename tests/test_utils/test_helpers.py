import os
import unittest
from unittest.mock import patch, MagicMock
from utils.helpers import coalesce, get_system_timezone


class TestHelpers(unittest.TestCase):
    def test_coalesce_empty_and_none(self):
        self.assertIsNone(coalesce())
        self.assertIsNone(coalesce(None))
        self.assertIsNone(coalesce(None, None, None))

    def test_coalesce_falsy_values_returned(self):
        self.assertEqual(coalesce(None, 0, 5), 0)
        self.assertIs(coalesce(None, False, True), False)
        self.assertEqual(coalesce(None, "", "hello"), "")
        self.assertEqual(coalesce(None, [], [1]), [])
        self.assertEqual(coalesce(None, {}, {"k": "v"}), {})

    def test_coalesce_multiple_values(self):
        self.assertEqual(coalesce("first", "second"), "first")
        self.assertEqual(coalesce(None, "second", "third"), "second")

    @patch.dict(os.environ, {"TZ": "/Asia/Jakarta "}, clear=True)
    def test_get_system_timezone_from_tz_env(self):
        self.assertEqual(get_system_timezone(), "Asia/Jakarta")

    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.helpers.Path")
    def test_get_system_timezone_from_etc_timezone(self, mock_path):
        tz_file_mock = MagicMock()
        tz_file_mock.is_file.return_value = True
        tz_file_mock.read_text.return_value = " /Europe/Berlin\n"
        mock_path.return_value = tz_file_mock

        self.assertEqual(get_system_timezone(), "Europe/Berlin")
        mock_path.assert_called_with("/etc/timezone")

    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.helpers.os.readlink")
    @patch("utils.helpers.Path")
    def test_get_system_timezone_from_localtime_symlink(self, mock_path, mock_readlink):
        def path_side_effect(path_str):
            mock_inst = MagicMock()
            if path_str == "/etc/timezone":
                mock_inst.is_file.return_value = False
            elif path_str == "/etc/localtime":
                mock_inst.is_symlink.return_value = True
            return mock_inst

        mock_path.side_effect = path_side_effect
        mock_readlink.return_value = "/usr/share/zoneinfo/America/New_York"

        self.assertEqual(get_system_timezone(), "America/New_York")

    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.helpers.Path")
    def test_get_system_timezone_from_sysconfig_clock(self, mock_path):
        def path_side_effect(path_str):
            mock_inst = MagicMock()
            if path_str == "/etc/timezone":
                mock_inst.is_file.return_value = False
            elif path_str == "/etc/localtime":
                mock_inst.is_symlink.return_value = False
            elif path_str == "/etc/sysconfig/clock":
                mock_inst.is_file.return_value = True
                mock_inst.read_text.return_value = 'ZONE="America/Chicago"\n'
            return mock_inst

        mock_path.side_effect = path_side_effect
        self.assertEqual(get_system_timezone(), "America/Chicago")

    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.helpers.Path")
    def test_get_system_timezone_from_tzlocal(self, mock_path):
        def path_side_effect(path_str):
            mock_inst = MagicMock()
            mock_inst.is_file.return_value = False
            mock_inst.is_symlink.return_value = False
            return mock_inst

        mock_path.side_effect = path_side_effect

        mock_tzlocal = MagicMock()
        mock_tzlocal.get_localzone_name.return_value = "/Asia/Tokyo "
        with patch.dict("sys.modules", {"tzlocal": mock_tzlocal}):
            self.assertEqual(get_system_timezone(), "Asia/Tokyo")

    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.helpers.Path")
    def test_get_system_timezone_fallback_utc(self, mock_path):
        def path_side_effect(path_str):
            mock_inst = MagicMock()
            mock_inst.is_file.return_value = False
            mock_inst.is_symlink.return_value = False
            return mock_inst

        mock_path.side_effect = path_side_effect

        with patch.dict("sys.modules", {"tzlocal": None}):
            self.assertEqual(get_system_timezone(), "UTC")


if __name__ == "__main__":
    unittest.main()
