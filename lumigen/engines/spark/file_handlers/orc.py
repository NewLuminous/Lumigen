from __future__ import annotations
import typing as t

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession, DataFrameWriter
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler


class SparkORCFileHandler(BaseSparkFileHandler):
    SUPPORTED_COMPRESSION_CODECS = {"none", "snappy", "zlib", "lzo", "zstd", "lz4", "brotli"}

    def _read_hadoop_fs(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        return self._read_native_format(spark, "orc", files, consolidated_paths, request, metadata_prefix, fs_conn)

    def describe(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        return self.read(spark, files, request, metadata_prefix, fs_type, fs_conn)

    def _write_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        return self._write_native_format(spark, "orc", df, file_path, "overwrite", options, metadata_prefix, fs_conn)

    def _append_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        return self._write_native_format(spark, "orc", df, file_path, "append", options, metadata_prefix, fs_conn)

    def _configure_writer(self, writer: DataFrameWriter, options: t.Optional[FileOptions], file_path: str) -> DataFrameWriter:
        compression = self._resolve_compression(options, file_path, self.SUPPORTED_COMPRESSION_CODECS)
        if compression:
            writer = writer.option("compression", compression)
        return writer