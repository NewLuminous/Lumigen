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

    def read(self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> t.Iterator[pd.DataFrame]:
        """
        Reads raw text files line-by-line
        """
        row_delim = options.row_delimiter if (options and options.row_delimiter) else '\n'
        with fs.open(file_path, "rt", encoding="utf-8", errors="replace", newline="") as f:
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

    def write(self, fs: AbstractFileSystem, df: pd.DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        metadata_cols = [col for col in df.columns if str(col).startswith(metadata_prefix)]
        clean_df = t.cast("pd.DataFrame", df.drop(columns=metadata_cols, errors="ignore"))
        
        row_delim = options.row_delimiter if (options and options.row_delimiter) else '\n'
        col_delim = options.field_delimiter if (options and options.field_delimiter) else ','

        if clean_df.empty:
            content = ""
        elif len(clean_df.columns) == 1:
            content = row_delim.join(clean_df.iloc[:, 0].astype(str)) + row_delim
        else:
            # If multiple non-metadata columns exist, join fields using col_delim
            content = clean_df.astype(str).apply(lambda row: col_delim.join(row), axis=1).str.cat(sep=row_delim) + row_delim

        with fs.open(file_path, "wb") as f:
            f.write(content.encode("utf-8"))

    def append(self, fs: AbstractFileSystem, df: pd.DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str) -> None:
        metadata_cols = [col for col in df.columns if str(col).startswith(metadata_prefix)]
        clean_df = t.cast("pd.DataFrame", df.drop(columns=metadata_cols, errors="ignore"))
        
        row_delim = options.row_delimiter if (options and options.row_delimiter) else "\n"
        col_delim = options.field_delimiter if (options and options.field_delimiter) else ","

        if len(clean_df.columns) == 1:
            new_text = row_delim.join(clean_df.iloc[:, 0].astype(str)) + row_delim
        else:
            new_text = clean_df.astype(str).apply(lambda row: col_delim.join(row), axis=1).str.cat(sep=row_delim) + row_delim

        file_buffer = io.BytesIO()
        if fs.exists(file_path):
            with fs.open(file_path, "rb") as f:
                file_buffer.write(f.read())
        
        file_buffer.write(new_text.encode("utf-8"))
        
        file_buffer.seek(0)
        with fs.open(file_path, "wb") as f:
            f.write(file_buffer.read())