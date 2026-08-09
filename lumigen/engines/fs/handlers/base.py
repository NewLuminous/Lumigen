from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod

if t.TYPE_CHECKING:
    from pandas import DataFrame
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions


class BaseFileHandler(ABC):
    @staticmethod
    def _drop_metadata_columns(df: DataFrame, metadata_prefix: str) -> DataFrame:
        """
        Helper for subclasses to drop metadata columns prior to writing.
        """
        metadata_cols = [col for col in df.columns if str(col).startswith(metadata_prefix)]
        return df.drop(columns=metadata_cols, errors="ignore")

    def _validate_written_df(self, written_df: t.Optional[DataFrame], method_name: str, metadata_prefix: str) -> None:
        """
        Audits the DataFrame returned by subclass implementations to guarantee no metadata leaked.
        """
        if written_df is None:
            raise ValueError(
                f"Write operation failed: An unexpected empty data state was returned during the write transaction. "
                f"Ensure the subclass implementation of '{method_name}' returns the active DataFrame used during the write operation."
            )
            
        leaked_cols = [c for c in written_df.columns if str(c).startswith(metadata_prefix)]
        if leaked_cols:
            raise ValueError(
                f"Data integrity check failed: The DataFrame returned from '{method_name}' contains reserved system metadata fields: {leaked_cols}. "
                f"Ensure '_drop_metadata_columns()' is called to sanitize the DataFrame before execution."
            )

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

    def write(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        written_df = self._write_impl(fs, df, file_path, options, metadata_prefix)
        self._validate_written_df(written_df, "_write_impl", metadata_prefix)

    @abstractmethod
    def _write_impl(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> DataFrame:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    def append(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        written_df = self._append_impl(fs, df, file_path, options, metadata_prefix)
        self._validate_written_df(written_df, "_append_impl", metadata_prefix)

    @abstractmethod
    def _append_impl(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> DataFrame:
        """
        Appends data to an existing file, validating schema compatibility first.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass


class FileHandlerFactory:
    """
    Registry for providing the correct BaseFileHandler based on the file extension.
    """
    _handlers: t.Dict[str, t.Type[BaseFileHandler]] = {}

    @classmethod
    def register(cls, extensions: t.List[str], handler_cls: t.Type[BaseFileHandler]):
        for ext in extensions:
            cls._handlers[ext.lower()] = handler_cls

    @classmethod
    def get_handler(cls, file_ext: str) -> t.Optional[BaseFileHandler]:
        handler_cls = cls._handlers.get(file_ext.lower())
        return handler_cls() if handler_cls else None