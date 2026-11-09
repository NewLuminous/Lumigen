from __future__ import annotations
import typing as t
import functools
import time
import logging
import threading

_logger = logging.getLogger(__name__)


def timer_func(func):
    """A decorator that logs the execution time of a function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        result = func(*args, **kwargs)
        t2 = time.perf_counter()
        _logger.debug(f'{func.__name__}() executed in {(t2-t1):.6f}s')
        return result
    return wrapper

class TimeoutProxy:
    """
    A middleware proxy that intercepts all method calls to a target object
    and executes them in a daemon thread with a strict timeout.
    """
    def __init__(self, target: t.Any, timeout_seconds: int):
        self._target = target
        self._timeout_seconds = timeout_seconds

    def __getattr__(self, name: str) -> t.Any:
        attr = getattr(self._target, name)
        
        if callable(attr):
            @functools.wraps(attr)
            def timeout_wrapper(*args, **kwargs):
                return self._call_with_timeout(attr, *args, **kwargs)
            return timeout_wrapper
            
        return attr

    def _call_with_timeout(self, func: t.Callable, *args, **kwargs) -> t.Any:
        class WorkerThread(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True, name=f"TimeoutWorker-{func.__name__}")
                self.result = None
                self.exception = None

            def run(self):
                try:
                    self.result = func(*args, **kwargs)
                except Exception as e:
                    self.exception = e

        worker = WorkerThread()
        worker.start()
        worker.join(timeout=self._timeout_seconds)

        if worker.is_alive():
            raise TimeoutError(
                f"Operation '{func.__name__}' timed out after {self._timeout_seconds}s."
            )
            
        if worker.exception:
            raise worker.exception
            
        return worker.result