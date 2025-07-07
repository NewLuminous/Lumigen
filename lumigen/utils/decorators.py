import time
import logging

_logger = logging.getLogger(__name__)


def timer_func(func):
    """A decorator that logs the execution time of a function."""
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        _logger.debug(f'{func.__name__}() executed in {(t2-t1):.6f}s')
        return result
    return wrapper