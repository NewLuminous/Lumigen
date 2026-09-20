from __future__ import annotations
import typing as t
import io
from itertools import islice
import pandas as pd

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions

from engines.fs.handlers.base import BaseFileHandler


class TextFileHandler(BaseFileHandler):
    DEFAULT_CHUNK_SIZE = 100_000  # number of lines 
    COLUMN_NAME = "value"
    SUPPORTED_COMPRESSION_CODECS = {"gzip", "bz2", "xz", "zstd"}

    def read(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Iterator[pd.DataFrame]:
        """
        Reads raw text files line-by-line
        """
        row_delim = options.row_delimiter if (options and options.row_delimiter) else '\n'
        encoding = options.encoding if (options and options.encoding) else "utf-8"
        compression = self._resolve_compression(file_path, options, self.SUPPORTED_COMPRESSION_CODECS)

        with fs.open(file_path, "rt", encoding=encoding, compression=compression, errors="replace", newline="") as f:
            while True:
                lines = list(islice(f, self.DEFAULT_CHUNK_SIZE))
                if not lines:
                    break
                
                cleaned_lines = [
                    line[:-len(row_delim)] if line.endswith(row_delim) else line.rstrip("\r\n") 
                    for line in lines
                ]
                yield pd.DataFrame({self.COLUMN_NAME: cleaned_lines})

    def describe(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Optional[pd.DataFrame]:
        return pd.DataFrame({self.COLUMN_NAME: [""]})

    def _write_impl(self, fs: AbstractFileSystem, df: pd.DataFrame, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> pd.DataFrame:
        clean_df = t.cast("pd.DataFrame", self._drop_metadata_columns(df, metadata_prefix))
        
        row_delim = options.row_delimiter if (options and options.row_delimiter) else '\n'
        col_delim = options.field_delimiter if (options and options.field_delimiter) else ','
        encoding = options.encoding if (options and options.encoding) else "utf-8"
        compression = self._resolve_compression(write_path, options, self.SUPPORTED_COMPRESSION_CODECS)

        if clean_df.empty:
            content = ""
        elif len(clean_df.columns) == 1:
            content = row_delim.join(clean_df.iloc[:, 0].astype(str)) + row_delim
        else:
            # If multiple non-metadata columns exist, join fields using col_delim
            content = clean_df.astype(str).apply(lambda row: col_delim.join(row), axis=1).str.cat(sep=row_delim) + row_delim

        with fs.open(write_path, "wb", compression=compression) as f:
            f.write(content.encode(encoding))

        return clean_df

    def _append_impl(self, fs: AbstractFileSystem, df: pd.DataFrame, read_path: str, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> pd.DataFrame:
        clean_df = t.cast("pd.DataFrame", self._drop_metadata_columns(df, metadata_prefix))
        
        row_delim = options.row_delimiter if (options and options.row_delimiter) else "\n"
        col_delim = options.field_delimiter if (options and options.field_delimiter) else ","
        encoding = options.encoding if (options and options.encoding) else "utf-8"
        read_compression = self._resolve_compression(read_path, options, self.SUPPORTED_COMPRESSION_CODECS)
        write_compression = self._resolve_compression(write_path, options, self.SUPPORTED_COMPRESSION_CODECS)

        if len(clean_df.columns) == 1:
            new_text = row_delim.join(clean_df.iloc[:, 0].astype(str)) + row_delim
        else:
            new_text = clean_df.astype(str).apply(lambda row: col_delim.join(row), axis=1).str.cat(sep=row_delim) + row_delim

        file_buffer = io.BytesIO()
        try:
            with fs.open(read_path, "rb", compression=read_compression) as f:
                file_buffer.write(f.read())
        except (FileNotFoundError, OSError):
            pass
        file_buffer.write(new_text.encode(encoding))

        file_buffer.seek(0)
        with fs.open(write_path, "wb", compression=write_compression) as f:
            f.write(file_buffer.read())

        return clean_df