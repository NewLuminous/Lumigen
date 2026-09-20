from __future__ import annotations
import typing as t

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession, DataFrameReader, DataFrameWriter
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler


class SparkCSVFileHandler(BaseSparkFileHandler):
    """
    Spark File Handler for CSV/TSV format files using Spark's native CSV Data Source.
    """

    SUPPORTED_COMPRESSION_CODECS = {"none", "bzip2", "gzip", "lz4", "snappy", "deflate"}

    def _read_hadoop_fs(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        return self._read_native_format(spark, "csv", files, consolidated_paths, request, metadata_prefix, fs_conn)

    def describe(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        return self.read(spark, files, request, metadata_prefix, fs_type, fs_conn)

    def _write_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        clean_df = self._write_native_format(spark, "csv", df, file_path, "overwrite", options, metadata_prefix, fs_conn)
        self._ensure_tsv_extension(file_path, fs_conn)
        return clean_df

    def _append_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        clean_df = self._write_native_format(spark, "csv", df, file_path, "append", options, metadata_prefix, fs_conn)
        self._ensure_tsv_extension(file_path, fs_conn)
        return clean_df

    def _configure_reader(self, reader: DataFrameReader, options: t.Optional[FileOptions], consolidated_paths: t.List[str]) -> DataFrameReader:
        encoding_val = options.encoding if (options and options.encoding) else "UTF-8"
        header_val = "true" if (options.header if options is not None else True) else "false" # "true" by default
        quote_val = options.csv_quote_char if (options and options.csv_quote_char) else '"'
        escape_val = options.csv_escape_char if (options and options.csv_escape_char) else "\\"

        is_tsv = any(
            p.lower().endswith(".tsv") or ".tsv." in p.lower() or p.lower().endswith(".tab")
            for p in consolidated_paths
        )
        # Default to '\t' for TSV files if delimiter was not explicitly customized
        if is_tsv and (not options or options.field_delimiter == ","):
            sep_val = "\t"
        else:
            sep_val = options.field_delimiter if (options and options.field_delimiter) else ","

        reader = (
            reader
            .option("sep", sep_val)
            .option("encoding", encoding_val)
            .option("quote", quote_val)
            .option("escape", escape_val)
            .option("header", header_val)
            .option("inferSchema", "true")
            .option("multiLine", "true")       # Allows multiline values inside quotes (parity with Pandas)
            .option("emptyValue", "")
            .option("nullValue", "")
            .option("nanValue", "NaN")
            .option("unescapedQuoteHandling", "STOP_AT_DELIMITER")
        )

        # Only set custom lineSep if explicitly non-default to preserve Spark's native (\r, \r\n, \n) autodetection
        if options and options.row_delimiter and options.row_delimiter != "\n":
            reader = reader.option("lineSep", options.row_delimiter)

        if options and options.csv_comment_char:
            reader = reader.option("comment", options.csv_comment_char)

        if options and options.csv_on_bad_lines:
            mode_map = {
                "error": "FAILFAST",
                "skip": "DROPMALFORMED",
                "warn": "DROPMALFORMED",
            }
            spark_mode = mode_map.get(options.csv_on_bad_lines.lower(), "PERMISSIVE")
            reader = reader.option("mode", spark_mode)

        return reader

    def _configure_writer(self, writer: DataFrameWriter, options: t.Optional[FileOptions], file_path: str) -> DataFrameWriter:
        encoding_val = options.encoding if (options and options.encoding) else "UTF-8"
        header_val = "true" if (options.header if options is not None else True) else "false" # "true" by default
        quote_val = options.csv_quote_char if (options and options.csv_quote_char) else '"'
        escape_val = options.csv_escape_char if (options and options.csv_escape_char) else "\\"

        lower_path = file_path.lower()
        is_tsv = lower_path.endswith(".tsv") or ".tsv." in lower_path or lower_path.endswith(".tab")
        if is_tsv and (not options or options.field_delimiter == ","):
            sep_val = "\t"
        else:
            sep_val = options.field_delimiter if (options and options.field_delimiter) else ","

        writer = (
            writer
            .option("sep", sep_val)
            .option("encoding", encoding_val)
            .option("quote", quote_val)
            .option("escape", escape_val)
            .option("escapeQuotes", "true")
            .option("header", header_val)
            .option("emptyValue", "")
            .option("nullValue", "")
            .option("nanValue", "NaN")
        )

        if is_tsv:
            writer = writer.option("extension", "tsv")

        if options and options.row_delimiter:
            writer = writer.option("lineSep", options.row_delimiter)

        compression = self._resolve_compression(options, file_path, self.SUPPORTED_COMPRESSION_CODECS)
        if compression:
            writer = writer.option("compression", compression)

        return writer

    def _ensure_tsv_extension(self, file_path: str, fs_conn: t.Optional[FileSystemConnection]) -> None:
        """
        Post-process for Spark < 4.0 where CSV writer does not support the `extension` option.
        Renames output part files from .csv to .tsv if the target directory implies a TSV format.
        """
        if not fs_conn:
            return

        lower_path = file_path.lower()
        is_tsv = lower_path.endswith(".tsv") or ".tsv." in lower_path or lower_path.endswith(".tab")
        if is_tsv:
            fs = fs_conn.get_fs()
            cleaned_path = self.strip_scheme(file_path)
            try:
                for item in fs.ls(cleaned_path, detail=True):
                    path: str = item["name"]
                    if ".csv" in path:
                        new_path = path.replace(".csv", ".tsv")
                        fs.rename(path, new_path)
            except Exception:
                pass