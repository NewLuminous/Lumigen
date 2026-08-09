from __future__ import annotations
import typing as t
import logging

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from engines.fs.request_parser import FileRequest
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler
from utils.dataset import DataFrameConverter

logger = logging.getLogger(__name__)


class SparkORCFileHandler(BaseSparkFileHandler):

    # Threshold above which Spark's distributed mergeSchema is prefered over driver-side threaded schema inference.
    DRIVER_SCHEMA_INFERENCE_THRESHOLD = 1000

    def _read_native_orc(
        self, 
        spark: SparkSession, 
        files: t.Dict[str, t.Dict[str, t.Any]], 
        consolidated_paths: t.List[str], 
        request: FileRequest, 
        metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        file_count = len(files)
        if file_count == 0:
            return self.create_empty_metadata_df(spark, metadata_prefix)

        reader = (
            spark.read.format("orc")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
        )

        # Driver-Side Threaded Schema Inference
        if fs_conn and file_count <= self.DRIVER_SCHEMA_INFERENCE_THRESHOLD:
            try:
                unified_schema_df = fs_conn.cursor()._infer_unified_schema(files, request)
                if unified_schema_df is not None and len(unified_schema_df.columns) > 0:
                    data_schema_df = unified_schema_df.drop(
                        columns=[col for col in unified_schema_df.columns if str(col).startswith(metadata_prefix)],
                        errors="ignore"
                    )
                    expected_schema = DataFrameConverter.to_spark(data_schema_df, spark).schema
                    reader = reader.schema(expected_schema)
                else:
                    reader = reader.option("mergeSchema", "true")
            except Exception as e:
                logger.warning(
                    "Driver-side schema inference failed; falling back to native Spark mergeSchema.", 
                    exc_info=e
                )
                reader = reader.option("mergeSchema", "true")
        # Native Distributed Schema Merging
        else:
            reader = reader.option("mergeSchema", "true")

        return reader.load(consolidated_paths)

    def _read_hdfs(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        return self._read_native_orc(spark, files, consolidated_paths, request, metadata_prefix, fs_conn)

    def _read_s3(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        return self._read_native_orc(spark, files, consolidated_paths, request, metadata_prefix, fs_conn)

    def describe(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
                 fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        return self.read(spark, files, request, metadata_prefix, fs_type, fs_conn)