from __future__ import annotations
import typing as t
import os
import uuid
from abc import ABC, abstractmethod

if t.TYPE_CHECKING:
    from pandas import DataFrame
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions


class BaseFileHandler(ABC):
    @staticmethod
    def _get_temp_path(file_path: str) -> t.Tuple[str, str]:
        """
        Generates a staging path inside the standard '_temporary/' directory.
        """
        parent_dir, base_name = os.path.split(file_path)
        name_part, ext = os.path.splitext(base_name)
        tmp_dir = f"{parent_dir}/_temporary" if parent_dir else "_temporary"
        tmp_path = f"{tmp_dir}/{uuid.uuid4().hex}-{name_part}.tmp{ext}"
        return tmp_dir, tmp_path

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
        tmp_dir, tmp_path = self._get_temp_path(file_path)
        try:
            # Write data to the temporary file inside _temporary/
            written_df = self._write_impl(fs, df, tmp_path, options, metadata_prefix)
            self._validate_written_df(written_df, "_write_impl", metadata_prefix)
            # If successful, clear old destination (file or directory)
            self._force_remove(fs, file_path)
            # Atomically move the temp file to the final destination
            fs.rename(tmp_path, file_path)
        finally:
            self._cleanup_temp(fs, tmp_path, tmp_dir)

    @abstractmethod
    def _write_impl(self, fs: AbstractFileSystem, df: DataFrame, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> DataFrame:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    def append(self, fs: AbstractFileSystem, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        tmp_dir, tmp_path = self._get_temp_path(file_path)
        try:
            written_df = self._append_impl(fs, df, read_path=file_path, write_path=tmp_path, options=options, metadata_prefix=metadata_prefix)
            self._validate_written_df(written_df, "_append_impl", metadata_prefix)
            self._force_remove(fs, file_path)
            fs.rename(tmp_path, file_path)
        finally:
            self._cleanup_temp(fs, tmp_path, tmp_dir)

    @abstractmethod
    def _append_impl(self, fs: AbstractFileSystem, df: DataFrame, read_path: str, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> DataFrame:
        """
        Appends data to an existing file, validating schema compatibility first.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    @staticmethod
    def _force_remove(fs: AbstractFileSystem, path: str) -> None:
        """
        Completely removes a file or directory (including directory markers)
        """
        clean_path = path.rstrip("/")
        try:
            fs.rm(clean_path, recursive=True)
        except (FileNotFoundError, OSError):
            pass
        try:
            fs.rm(clean_path)
        except (FileNotFoundError, OSError):
            pass
        try:
            fs.rm(clean_path + "/")
        except (FileNotFoundError, OSError):
            pass
        if hasattr(fs, "invalidate_cache"):
            try:
                fs.invalidate_cache(clean_path)
                fs.invalidate_cache(os.path.dirname(clean_path))
            except Exception:
                pass

    @staticmethod
    def _cleanup_temp(fs: AbstractFileSystem, tmp_path: str, tmp_dir: str) -> None:
        """
        Safely removes orphaned temp files and empty staging directories.
        """
        try:
            fs.rm(tmp_path)
        except (FileNotFoundError, OSError):
            pass
        try:
            if not fs.ls(tmp_dir):
                BaseFileHandler._force_remove(fs, tmp_dir)
        except (FileNotFoundError, OSError):
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