import time
import logging
import unittest
from unittest.mock import patch
from utils.decorators import timer_func, TimeoutProxy


class DummyService:
    def __init__(self):
        self.attr_val = "static_attribute"

    def fast_method(self, x, y=10):
        """Docstring for fast method."""
        return x + y

    def slow_method(self, delay: float):
        time.sleep(delay)
        return "done"

    def failing_method(self):
        raise ValueError("Intentional error inside method")


class TestDecorators(unittest.TestCase):
    def test_timer_func_execution_and_metadata(self):
        @timer_func
        def sample_add(a, b):
            """Sample docstring."""
            return a + b

        self.assertEqual(sample_add.__name__, "sample_add")
        self.assertEqual(sample_add.__doc__, "Sample docstring.")

        with patch("utils.decorators._logger.debug") as mock_debug:
            res = sample_add(3, 4)
            self.assertEqual(res, 7)
            mock_debug.assert_called_once()
            log_msg = mock_debug.call_args[0][0]
            self.assertIn("sample_add() executed in", log_msg)

    def test_timer_func_propagates_exception(self):
        @timer_func
        def faulty():
            raise RuntimeError("Failure in timer")

        with self.assertRaises(RuntimeError) as ctx:
            faulty()
        self.assertEqual(str(ctx.exception), "Failure in timer")

    def test_timeout_proxy_attribute_access(self):
        service = DummyService()
        proxy = TimeoutProxy(service, timeout_seconds=2)
        self.assertEqual(proxy.attr_val, "static_attribute")

    def test_timeout_proxy_fast_call(self):
        service = DummyService()
        proxy = TimeoutProxy(service, timeout_seconds=2)
        self.assertEqual(proxy.fast_method(5, y=15), 20)
        self.assertEqual(proxy.fast_method.__name__, "fast_method")
        self.assertEqual(proxy.fast_method.__doc__, "Docstring for fast method.")

    def test_timeout_proxy_timeout_exceeded(self):
        service = DummyService()
        proxy = TimeoutProxy(service, timeout_seconds=1)
        with self.assertRaises(TimeoutError) as ctx:
            proxy.slow_method(1.5)
        self.assertIn("Operation 'slow_method' timed out after 1s.", str(ctx.exception))

    def test_timeout_proxy_exception_propagation(self):
        service = DummyService()
        proxy = TimeoutProxy(service, timeout_seconds=2)
        with self.assertRaises(ValueError) as ctx:
            proxy.failing_method()
        self.assertEqual(str(ctx.exception), "Intentional error inside method")


if __name__ == "__main__":
    unittest.main()

