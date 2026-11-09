import sys
import types
import json
import base64
import datetime
from decimal import Decimal
import unittest

# Ensure numpy is mock-available if not installed
REAL_NUMPY = True
try:
    import numpy as np
except ImportError:
    REAL_NUMPY = False
    np = types.ModuleType("numpy")
    class MockInteger: pass
    class MockFloating: pass
    class MockNDArray: pass
    np.integer = MockInteger
    np.floating = MockFloating
    np.ndarray = MockNDArray
    np.array = lambda x: list(x)
    sys.modules["numpy"] = np

from utils.json import (
    PortableJSONEncoder,
    DisplayableJSONEncoder,
    _get_value_type_name,
    encode_typed_value,
    decode_typed_value,
)


class TestJsonUtils(unittest.TestCase):
    # ==========================
    # PortableJSONEncoder
    # ==========================
    def test_portable_encoder_bytes(self):
        raw = b"hello world"
        expected = json.dumps(base64.b64encode(raw).decode("utf-8"))
        res = json.dumps(raw, cls=PortableJSONEncoder)
        self.assertEqual(res, expected)

        res_ba = json.dumps(bytearray(raw), cls=PortableJSONEncoder)
        self.assertEqual(res_ba, expected)

    def test_portable_encoder_datetime_date_time(self):
        dt = datetime.datetime(2025, 6, 5, 12, 30, 45)
        d = datetime.date(2025, 6, 5)
        t = datetime.time(12, 30, 45)

        self.assertEqual(json.dumps(dt, cls=PortableJSONEncoder), f'"{dt.isoformat()}"')
        self.assertEqual(json.dumps(d, cls=PortableJSONEncoder), f'"{d.isoformat()}"')
        self.assertEqual(json.dumps(t, cls=PortableJSONEncoder), f'"{t.isoformat()}"')

    def test_portable_encoder_decimal(self):
        dec = Decimal("123.456")
        self.assertEqual(json.dumps(dec, cls=PortableJSONEncoder), '"123.456"')

    def test_portable_encoder_unsupported_raises_type_error(self):
        class NonSerializable:
            pass

        with self.assertRaises(TypeError):
            json.dumps(NonSerializable(), cls=PortableJSONEncoder)

    # ==========================
    # DisplayableJSONEncoder
    # ==========================
    def test_displayable_encoder_bytes(self):
        raw = b"data"
        res = json.dumps(raw, cls=DisplayableJSONEncoder)
        self.assertEqual(res, '"b\'data\'"')

    # ==========================
    # _get_value_type_name
    # ==========================
    def test_get_value_type_name(self):
        self.assertEqual(_get_value_type_name(None), "NoneType")
        self.assertEqual(_get_value_type_name(b"bytes"), "bytes")
        self.assertEqual(_get_value_type_name(bytearray(b"bytes")), "bytes")
        self.assertEqual(_get_value_type_name(datetime.datetime.now()), "datetime")
        self.assertEqual(_get_value_type_name(datetime.date.today()), "date")
        self.assertEqual(_get_value_type_name(datetime.time(1, 2)), "time")
        self.assertEqual(_get_value_type_name(Decimal("1.0")), "Decimal")
        self.assertEqual(_get_value_type_name("string"), "str")
        self.assertEqual(_get_value_type_name(42), "int")
        self.assertEqual(_get_value_type_name(3.14), "float")
        self.assertEqual(_get_value_type_name([1, 2]), "list")
        self.assertEqual(_get_value_type_name({"k": "v"}), "dict")

    # ==========================
    # encode_typed_value & decode_typed_value Roundtrips
    # ==========================
    def test_roundtrip_primitives(self):
        primitives = [
            "hello",
            42,
            3.1415,
            True,
            False,
            None,
            [1, 2, "three"],
            {"a": 1, "b": "test"},
        ]
        for val in primitives:
            val_str, type_str = encode_typed_value(val)
            decoded = decode_typed_value(val_str, type_str)
            self.assertEqual(decoded, val)

    def test_roundtrip_bytes(self):
        raw = b"secret-binary-\x00\xff-bytes"
        val_str, type_str = encode_typed_value(raw)
        self.assertEqual(type_str, "bytes")
        decoded = decode_typed_value(val_str, type_str)
        self.assertEqual(decoded, raw)

    def test_roundtrip_datetime(self):
        now = datetime.datetime(2025, 1, 15, 10, 20, 30)
        val_str, type_str = encode_typed_value(now)
        self.assertEqual(type_str, "datetime")
        decoded = decode_typed_value(val_str, type_str)
        self.assertEqual(decoded, now)

    def test_roundtrip_date(self):
        d = datetime.date(2025, 1, 15)
        val_str, type_str = encode_typed_value(d)
        self.assertEqual(type_str, "date")
        decoded = decode_typed_value(val_str, type_str)
        self.assertEqual(decoded, d)

    def test_roundtrip_time(self):
        t = datetime.time(10, 20, 30)
        val_str, type_str = encode_typed_value(t)
        self.assertEqual(type_str, "time")
        decoded = decode_typed_value(val_str, type_str)
        self.assertEqual(decoded, t)

    def test_roundtrip_decimal(self):
        dec = Decimal("999999999999999999.99")
        val_str, type_str = encode_typed_value(dec)
        self.assertEqual(type_str, "Decimal")
        decoded = decode_typed_value(val_str, type_str)
        self.assertEqual(decoded, dec)

    def test_decode_unhandled_type_str(self):
        val = decode_typed_value('"custom_data"', "CustomType")
        self.assertEqual(val, "custom_data")


if __name__ == "__main__":
    unittest.main()

