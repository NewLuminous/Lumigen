from __future__ import annotations
import typing as t
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from engines.fs.request_parser import FileOptions

from engines.fs.handlers.base import BaseFileHandler


class ORCFileHandler(BaseFileHandler):
    SUPPORTED_COMPRESSION_CODECS = {"uncompressed", "snappy", "zlib", "lz4", "zstd"}

    def read(
        self, 
        fs: AbstractFileSystem, 
        file_path: str, 
        options: t.Optional[FileOptions], 
        metadata_prefix: str
    ) -> t.Iterator[pd.DataFrame]:
        """
        Reads ORC file stripe-by-stripe to keep memory footprint low.
        """
        with fs.open(file_path, "rb") as f:
            orc_file = orc.ORCFile(f)
            num_stripes = orc_file.nstripes
            for i in range(num_stripes):
                stripe_table = orc_file.read_stripe(i)
                df: pd.DataFrame = stripe_table.to_pandas()
                if not df.empty:
                    yield df

    def describe(
        self, 
        fs: AbstractFileSystem, 
        file_path: str, 
        options: t.Optional[FileOptions], 
        metadata_prefix: str
    ) -> t.Optional[pd.DataFrame]:
        with fs.open(file_path, "rb") as f:
            orc_file = orc.ORCFile(f)
            
            # If the file contains data stripes, read only 1 row from the first stripe
            if orc_file.nstripes > 0:
                stripe = orc_file.read_stripe(0)
                table = stripe.slice(0, 1)
                return table.to_pandas()
            else:
                # Return an empty DataFrame with the expected columns
                return pd.DataFrame(columns=orc_file.schema.names)

    def _write_impl(
        self, 
        fs: AbstractFileSystem, 
        df: pd.DataFrame, 
        write_path: str, 
        options: t.Optional[FileOptions], 
        metadata_prefix: str
    ) -> pd.DataFrame:
        clean_df = self._drop_metadata_columns(df, metadata_prefix)

        compression = self._resolve_compression(write_path, options, self.SUPPORTED_COMPRESSION_CODECS)
        compression_arg = compression.upper() if compression else "UNCOMPRESSED"

        table = pa.Table.from_pandas(clean_df, preserve_index=False)
        with fs.open(write_path, "wb") as f:
            orc.write_table(table, f, compression=compression_arg)

        return clean_df

    def _append_impl(
        self, 
        fs: AbstractFileSystem, 
        df: pd.DataFrame, 
        read_path: str, 
        write_path: str, 
        options: t.Optional[FileOptions], 
        metadata_prefix: str
    ) -> pd.DataFrame:
        new_df = self._drop_metadata_columns(df, metadata_prefix)
        new_table = pa.Table.from_pandas(new_df, preserve_index=False)

        compression = self._resolve_compression(write_path, options, self.SUPPORTED_COMPRESSION_CODECS)
        compression_arg = compression.upper() if compression else "UNCOMPRESSED"

        # Read existing ORC dataset safely
        try:
            with fs.open(read_path, "rb") as f:
                existing_table = orc.read_table(f)

            existing_names = existing_table.schema.names
            new_names = new_table.schema.names
            if existing_names != new_names:
                raise ValueError(
                    f"Schema mismatch for ORC append in '{read_path}'.\n"
                    f"Expected columns: {existing_names}\n"
                    f"Received columns: {new_names}"
                )

            combined_table = pa.concat_tables([existing_table, new_table])
        except (FileNotFoundError, OSError):
            # If target file doesn't exist, append acts as write
            combined_table = new_table

        with fs.open(write_path, "wb") as f:
            orc.write_table(combined_table, f, compression=compression_arg)

        return new_df