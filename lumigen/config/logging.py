import logging
import sys
import typing as t


class ExitCapableLogger(logging.getLoggerClass()):
    """
    A custom logger class that adds the ability to exit the program on critical errors.
    """
    def critical(self, msg: str, *args, exit_on_call: bool = False, **kwargs):
        """
        Logs a message with level CRITICAL.

        If exit_on_call is True, the program terminates with exit code 1.
        """
        super().critical(msg, *args, **kwargs)
        if exit_on_call:
            sys.exit(1)

    def exception(self, msg: str, *args, exit_on_call: bool = False, **kwargs):
        """
        Logs a message with level ERROR and exception information.

        This method should only be called from an exception handler. It
        behaves like logger.error() but adds exception information to the
        log.

        If exit_on_call is True, the program terminates with exit code 1.
        """
        super().exception(msg, *args, **kwargs)
        if exit_on_call:
            sys.exit(1)


def setup_logger(level: int = logging.INFO, log_file: t.Optional[str] = None):
    """
    Configures and returns the root logger using the ExitCapableLogger class.

    This function should be called ONCE at the start of the application.
    """
    logging.setLoggerClass(ExitCapableLogger)

    root_logger = logging.getLogger()
    
    # Avoid adding duplicate handlers
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')

    # Console handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)