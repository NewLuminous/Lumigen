from __future__ import annotations
import typing as t

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession, DataFrameReader, DataFrameWriter
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler


class SparkTextFileHandler(BaseSparkFileHandler):
    SUPPORTED_COMPRESSION_CODECS = {"none", "bzip2", "gzip", "lz4", "snappy", "deflate"}

    def _read_hadoop_fs(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        return self._read_native_format(spark, "text", files, consolidated_paths, request, metadata_prefix, fs_conn)

    def describe(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        return self.read(spark, files, request, metadata_prefix, fs_type, fs_conn)

    def _write_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        prepared_df = self._align_text_columns(df, options, metadata_prefix)
        return self._write_native_format(spark, "text", prepared_df, file_path, "overwrite", options, metadata_prefix, fs_conn)

    def _append_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        prepared_df = self._align_text_columns(df, options, metadata_prefix)
        return self._write_native_format(spark, "text", prepared_df, file_path, "append", options, metadata_prefix, fs_conn)

    def _configure_reader(self, reader: DataFrameReader, options: t.Optional[FileOptions], consolidated_paths: t.List[str]) -> DataFrameReader:
        if options and options.row_delimiter:
            reader = reader.option("lineSep", options.row_delimiter)
        return reader

    def _configure_writer(self, writer: DataFrameWriter, options: t.Optional[FileOptions], file_path: str) -> DataFrameWriter:
        if options:
            if options.row_delimiter:
                writer = writer.option("lineSep", options.row_delimiter)

            compression = self._resolve_compression(options, file_path, self.SUPPORTED_COMPRESSION_CODECS)
            if compression:
                writer = writer.option("compression", compression)
        return writer

    def _align_text_columns(self, df: DataFrame, options: t.Optional[FileOptions], metadata_prefix: str) -> DataFrame:
        """
        Formats and aligns the columns of a Spark DataFrame for raw text writing.
        """
        clean_df = self._drop_metadata_columns(df, metadata_prefix)
        col_delim = options.field_delimiter if (options and options.field_delimiter) else ","
        if len(clean_df.columns) > 1:
            escaped_delim = col_delim.replace("'", "''")
            columns_str = ", ".join(f"`{col}`" for col in clean_df.columns)
            expr = f"concat_ws('{escaped_delim}', {columns_str}) AS value"
            clean_df = clean_df.selectExpr(expr)
        elif len(clean_df.columns) == 1:
            col_name = clean_df.columns[0]
            expr = f"CAST(`{col_name}` AS STRING) AS value"
            clean_df = clean_df.selectExpr(expr)
        else:
            return df.sparkSession.createDataFrame([], schema="value STRING")
            
        return clean_df