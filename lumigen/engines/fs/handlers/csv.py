from __future__ import annotations
import typing as t
import io
import pandas as pd
import csv

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions

from engines.fs.handlers.base import BaseFileHandler


class CSVFileHandler(BaseFileHandler):
    DEFAULT_CHUNK_SIZE = 100_000  # Number of rows per streamed chunk
    SUPPORTED_COMPRESSION_CODECS = {"gzip", "bz2", "zip", "xz", "zstd", "tar"}

    def read(
        self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str
    ) -> t.Iterator[pd.DataFrame]:
        read_params = self._get_read_params(file_path, options)
        read_params["chunksize"] = self.DEFAULT_CHUNK_SIZE
        try:
            with fs.open(file_path, "rb") as f:
                reader = pd.read_csv(f, **read_params)
                for chunk in reader:
                    yield chunk
        except pd.errors.EmptyDataError:
            return

    def describe(
        self, fs: AbstractFileSystem, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str
    ) -> t.Optional[pd.DataFrame]:
        """
        Extracts only 1 row of data for lightweight schema inference.
        """
        read_params = self._get_read_params(file_path, options)
        read_params["nrows"] = 1
        try:
            with fs.open(file_path, "rb") as f:
                df = pd.read_csv(f, **read_params)
                return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def _write_impl(
        self, fs: AbstractFileSystem, df: pd.DataFrame, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str
    ) -> pd.DataFrame:
        clean_df = self._drop_metadata_columns(df, metadata_prefix)
        write_params = self._get_write_params(write_path, options)

        with fs.open(write_path, "wb") as f:
            clean_df.to_csv(f, **write_params)

        return clean_df

    def _append_impl(
        self, fs: AbstractFileSystem, df: pd.DataFrame, read_path: str, write_path: str, options: t.Optional[FileOptions], metadata_prefix: str
    ) -> pd.DataFrame:
        clean_df = self._drop_metadata_columns(df, metadata_prefix)
        write_params = self._get_write_params(write_path, options)

        file_buffer = io.BytesIO()
        has_existing = True
        try:
            with fs.open(read_path, "rb") as f:
                file_buffer.write(f.read())
        except (FileNotFoundError, OSError):
            has_existing = False

        file_size = file_buffer.tell()

        if has_existing and file_size > 0:
            file_buffer.seek(0)
            read_params = self._get_read_params(read_path, options)

            # If header=None, read 1 row so Pandas knows the column structure
            is_header_present = options.header if options is not None else True
            read_params["nrows"] = 0 if is_header_present else 1
            try:
                existing_schema_df: pd.DataFrame = pd.read_csv(file_buffer, **read_params)
                existing_cols = list(existing_schema_df.columns)
                new_cols = list(clean_df.columns)
                if existing_cols != new_cols:
                    raise ValueError(
                        f"Schema mismatch for CSV append in '{read_path}'.\n"
                        f"Expected: {existing_cols}\n"
                        f"Got:      {new_cols}"
                    )
            except pd.errors.EmptyDataError:
                has_existing = False

        if has_existing and file_size > 0:
            compression = write_params.get("compression")
            # Compressed CSV (.gz, .bz2): Read existing, concatenate, and re-compress
            if compression:
                file_buffer.seek(0)
                read_params = self._get_read_params(read_path, options)
                existing_df = pd.read_csv(file_buffer, **read_params)
                combined_df = pd.concat([existing_df, clean_df], ignore_index=True)
                
                file_buffer.seek(0)
                file_buffer.truncate(0)
                combined_df.to_csv(file_buffer, **write_params)
            else:
                # Plain CSV: Append rows directly without header to save memory
                row_delim = (options.row_delimiter if (options and options.row_delimiter) else "\n").encode("utf-8")
                delim_len = len(row_delim)
                file_buffer.seek(0, io.SEEK_END)
                curr_size = file_buffer.tell()
                has_trailing_delim = False
                if curr_size >= delim_len:
                    file_buffer.seek(curr_size - delim_len)
                    has_trailing_delim = (file_buffer.read(delim_len) == row_delim)
                
                # Ensure existing buffer ends with a newline before appending
                file_buffer.seek(0, io.SEEK_END)
                if not has_trailing_delim:
                    file_buffer.write(row_delim)

                append_params = write_params.copy()
                append_params["header"] = False
                clean_df.to_csv(file_buffer, **append_params)
        else:
            file_buffer.seek(0)
            file_buffer.truncate(0)
            clean_df.to_csv(file_buffer, **write_params)

        file_buffer.seek(0)
        with fs.open(write_path, "wb") as f:
            f.write(file_buffer.read())

        return clean_df

    def _get_read_params(self, file_path: str, options: t.Optional[FileOptions]) -> t.Dict[str, t.Any]:
        lower_path = file_path.lower()
        is_tsv = lower_path.endswith(".tsv") or ".tsv." in lower_path or lower_path.endswith(".tab")
        if is_tsv and (not options or options.field_delimiter == ","):
            sep_val = "\t"
        else:
            sep_val = options.field_delimiter if (options and options.field_delimiter) else ","

        # Reference: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        params: t.Dict[str, t.Any] = {
            "sep": sep_val,
            "header": 0 if (options.header if options is not None else True) else None,
            "index_col": False,  # Explicitly prevents first column from becoming index
            "encoding": options.encoding if (options and options.encoding) else "utf-8",
            "encoding_errors": "replace",
            "quotechar": options.csv_quote_char if (options and options.csv_quote_char) else '"',
            "escapechar": options.csv_escape_char if (options and options.csv_escape_char) else "\\",
            "doublequote": True,
            # Treat empty strings and explicit null markers as NaN
            "na_values": [""],
            "keep_default_na": True,
        }

        if options and options.row_delimiter and len(options.row_delimiter) == 1 and options.row_delimiter != "\n":
            params["lineterminator"] = options.row_delimiter

        compression = self._resolve_compression(file_path, options, self.SUPPORTED_COMPRESSION_CODECS)
        if compression:
            params["compression"] = compression

        if options and options.csv_comment_char:
            params["comment"] = options.csv_comment_char

        if options and options.csv_on_bad_lines:
            params["on_bad_lines"] = options.csv_on_bad_lines

        return params

    def _get_write_params(self, file_path: str, options: t.Optional[FileOptions]) -> t.Dict[str, t.Any]:
        lower_path = file_path.lower()
        is_tsv = lower_path.endswith(".tsv") or ".tsv." in lower_path or lower_path.endswith(".tab")
        if is_tsv and (not options or options.field_delimiter == ","):
            sep_val = "\t"
        else:
            sep_val = options.field_delimiter if (options and options.field_delimiter) else ","

        # Reference: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
        params: t.Dict[str, t.Any] = {
            "sep": sep_val,
            "header": options.header if options is not None else True,
            "index": False,
            "encoding": options.encoding if (options and options.encoding) else "utf-8",
            "quotechar": options.csv_quote_char if (options and options.csv_quote_char) else '"',
            "escapechar": options.csv_escape_char if (options and options.csv_escape_char) else "\\",
            "na_rep": "",
            # If escapechar is given, use escapechar; otherwise double the quotes
            "doublequote": True,
            "quoting": csv.QUOTE_MINIMAL,
        }

        if options and options.row_delimiter:
            params["lineterminator"] = options.row_delimiter

        compression = self._resolve_compression(file_path, options, self.SUPPORTED_COMPRESSION_CODECS)
        if compression:
            params["compression"] = compression

        return params