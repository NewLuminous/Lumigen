from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod

if t.TYPE_CHECKING:
    from pandas import DataFrame
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions


class BaseFileHandler(ABC):
    @abstractmethod
    def read(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Iterator[DataFrame]:
        """
        Reads a file and yields chunks of DataFrames to prevent out-of-memory errors on massive files.
        """
        pass

    @abstractmethod
    def describe(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Optional[DataFrame]:
        """
        Returns a DataFrame containing exactly one row (i.e. first data row) when possible for lightweight schema inference.
        """
        pass

    @abstractmethod
    def write(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        """
        pass

    @abstractmethod
    def append(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        """
        Appends data to an existing file, validating schema compatibility first.
        """
        pass


class FileHandlerFactory:
    """
    Registry for providing the correct BaseFileHandler based on the file extension.
    """
    _handlers: t.Dict[str, BaseFileHandler] = {}

    @classmethod
    def register(cls, extensions: t.List[str], handler: BaseFileHandler):
        for ext in extensions:
            cls._handlers[ext.lower()] = handler

    @classmethod
    def get_handler(cls, file_ext: str) -> t.Optional[BaseFileHandler]:
        return cls._handlers.get(file_ext.lower())