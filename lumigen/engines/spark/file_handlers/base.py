from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
from collections import defaultdict
import os
import re
import pandas as pd

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    import pandas as pd
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection, HDFSConnection, S3Connection

from utils.dataset import DataFrameConverter


class BaseSparkFileHandler(ABC):
    SUPPORTED_FILE_SYSTEMS: set = {
        "hdfs", "s3",
    }

    def read(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
             fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        if fs_type == "hdfs":
            hdfs_files = {}
            for path, info in files.items():
                no_scheme_path = self.strip_scheme(path)
                if not no_scheme_path.startswith("/"):
                    no_scheme_path = "/" + no_scheme_path
                hdfs_files["hdfs://" + no_scheme_path] = info
            consolidated_paths = self._consolidate_paths(list(hdfs_files.keys()))

            df = self._read_hdfs(spark, hdfs_files, consolidated_paths, request, metadata_prefix, fs_conn)
        elif fs_type == "s3":
            s3_files = {}
            for path, info in files.items():
                no_scheme_path = self.strip_scheme(path).lstrip("/")
                s3_files["s3a://" + no_scheme_path] = info
            consolidated_paths = self._consolidate_paths(list(s3_files.keys()))

            df = self._read_s3(spark, s3_files, consolidated_paths, request, metadata_prefix, fs_conn)
        else:
            df = self._read_via_pandas(spark, files, request, metadata_prefix, fs_conn)

        # Validate and populate metadata columns
        has_metadata = False
        try:
            df.select("_metadata")
            has_metadata = True
        except Exception:
            pass
        metadata_cols = ["file_path", "file_name", "file_size", "file_modification_time"]
        if has_metadata:
            metadata_struct = df["_metadata"]
            missing_metadata_exprs = [
                metadata_struct[suffix].alias(metadata_prefix + suffix)
                for suffix in metadata_cols
                if (metadata_prefix + suffix) not in df.columns
            ]
            if missing_metadata_exprs:
                df = df.select("*", *missing_metadata_exprs)
        for suffix in metadata_cols:
            col_name = metadata_prefix + suffix
            if col_name not in df.columns:
                class_name = self.__class__.__name__
                expected_list = [metadata_prefix + col for col in metadata_cols]
                raise NotImplementedError(
                    f"The reader subclass '{class_name}' returned a DataFrame missing the required metadata column: '{col_name}', "
                    f"and the native '_metadata' column is not present in the schema to extract it from.\n"
                    f"To resolve this, the subclass's internal read implementations must return a DataFrame populated with the following 4 metadata columns: {expected_list}, "
                    f"or read from a data source that natively exposes the '_metadata' struct."
                )

        # Standardize metadata columns
        path_col = metadata_prefix + "file_path"
        time_col = metadata_prefix + "file_modification_time"
        target_cols = {path_col, time_col}
        passthrough_cols = [c for c in df.columns if c not in target_cols]
        clean_path_expr = f"""CASE 
            WHEN {path_col} LIKE 'hdfs://%' THEN regexp_replace({path_col}, '^hdfs://(?:[^/]+)?', '') 
            ELSE regexp_replace({path_col}, '^[a-zA-Z][a-zA-Z0-9+.-]*://', '') 
        END AS {path_col}"""
        utc_time_expr = f"to_utc_timestamp({time_col}, current_timezone()) AS {time_col}"
        df = df.selectExpr(*passthrough_cols, clean_path_expr, utc_time_expr)
            
        return df

    @abstractmethod
    def _read_hdfs(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[HDFSConnection] = None) -> DataFrame:
        pass

    @abstractmethod
    def _read_s3(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[S3Connection] = None) -> DataFrame:
        pass

    def _read_via_pandas(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection]) -> DataFrame:
        """
        Streams chunks on the Driver.
        """
        if not fs_conn:
            raise ValueError("A valid FileSystemConnection (fs_conn) must be provided to read files via Pandas.")

        # Controls Python-to-JVM Serialization (keeps Python RAM bounded)
        PANDAS_BATCH_SIZE = 10
        
        # Controls JVM Lineage Truncation (keeps Spark Job Execution overhead low)
        SPARK_CHECKPOINT_INTERVAL = 10

        accumulated_df: t.Optional[DataFrame] = None
        pandas_batch: t.List[pd.DataFrame] = []
        converted_df_counter = 0
        for chunk_df in fs_conn.cursor()._generate_read_chunks(files, request):
            if chunk_df is not None and not chunk_df.empty:
                pandas_batch.append(chunk_df)
                if len(pandas_batch) >= PANDAS_BATCH_SIZE:
                    merged_pdf = pd.concat(pandas_batch, ignore_index=True)
                    spark_chunk = DataFrameConverter.to_spark(merged_pdf, spark)
                    pandas_batch.clear()

                    if accumulated_df is None:
                        accumulated_df = spark_chunk
                    else:
                        # PySpark dynamically merges schemas and fills nulls for missing columns
                        accumulated_df = accumulated_df.unionByName(spark_chunk, allowMissingColumns=True)

                    converted_df_counter += 1
                    # Truncate lineage to release JVM Heap memory in batches
                    if converted_df_counter % SPARK_CHECKPOINT_INTERVAL == 0:
                        accumulated_df = accumulated_df.localCheckpoint(eager=True)

        if pandas_batch:
            merged_pdf = pd.concat(pandas_batch, ignore_index=True)
            spark_chunk = DataFrameConverter.to_spark(merged_pdf, spark)
            if accumulated_df is None:
                accumulated_df = spark_chunk
            else:
                accumulated_df = accumulated_df.unionByName(spark_chunk, allowMissingColumns=True)

        if accumulated_df is None:
            return self.create_empty_metadata_df(spark, metadata_prefix)

        return accumulated_df

    def describe(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
                 fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        if not fs_conn:
            lazy_spark_df = self.read(spark, files, request, metadata_prefix, fs_type, fs_conn)
            return lazy_spark_df
        else:
            unified_schema_df = fs_conn.cursor()._infer_unified_schema(files, request)
            if unified_schema_df is not None and len(unified_schema_df.columns) > 0:
                return DataFrameConverter.to_spark(unified_schema_df, spark)
            else:
                return self.create_empty_metadata_df(spark, metadata_prefix)

    @staticmethod
    def drop_metadata_columns(df: DataFrame, metadata_prefix: str) -> DataFrame:
        """
        Helper for subclasses to drop metadata columns prior to writing.
        """
        metadata_cols = [c for c in df.columns if c.startswith(metadata_prefix)]
        return df.drop(*metadata_cols) if metadata_cols else df

    def _validate_written_df(self, written_df: t.Optional[DataFrame], method_name: str, metadata_prefix: str) -> None:
        """
        Audits the PySpark DataFrame returned by subclass implementations to guarantee no metadata leaked.
        """
        if written_df is None:
            raise ValueError(
                f"Write operation failed: An unexpected empty data state was returned during the write transaction. "
                f"Ensure the subclass implementation of '{method_name}' returns the active DataFrame used during the write operation."
            )
            
        leaked_cols = [c for c in written_df.columns if c.startswith(metadata_prefix)]
        if leaked_cols:
            raise ValueError(
                f"Data integrity check failed: The DataFrame returned from '{method_name}' contains reserved system metadata fields: {leaked_cols}. "
                f"Ensure '_drop_metadata_columns()' is called to sanitize the DataFrame before execution."
            )

    def write(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
              fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None) -> None:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        """
        method_name = "_write_impl"
        if fs_type == "hdfs":
            no_scheme_path = self.strip_scheme(file_path)
            if not no_scheme_path.startswith("/"):
                no_scheme_path = "/" + no_scheme_path

            method_name = "_write_hdfs"
            written_df = self._write_hdfs(spark, df, "hdfs://" + no_scheme_path, options, metadata_prefix, fs_conn)
        elif fs_type == "s3":
            no_scheme_path = self.strip_scheme(file_path).lstrip("/")
            method_name = "_write_s3"
            written_df = self._write_s3(spark, df, "s3a://" + no_scheme_path, options, metadata_prefix, fs_conn)
        else:
            method_name = "_write_via_pandas"
            written_df = self._write_via_pandas(spark, df, file_path, options, metadata_prefix, fs_conn)

        self._validate_written_df(written_df, method_name, metadata_prefix)

    @abstractmethod
    def _write_hdfs(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[HDFSConnection] = None) -> DataFrame:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    @abstractmethod
    def _write_s3(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[S3Connection] = None) -> DataFrame:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    def _write_via_pandas(self, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for writing files via Pandas.")

        pdf = DataFrameConverter.to_pandas(df)
        fs_conn.cursor()._write_single_file(pdf, file_path, options)
        return self.drop_metadata_columns(df, metadata_prefix)

    def append(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
               fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None) -> None:
        """
        Appends data to an existing file, validating schema compatibility first.
        """
        raise NotImplementedError()

    @staticmethod
    def strip_scheme(path: str) -> str:
        """
        Removes any RFC 3986 compliant URI scheme (e.g., s3a://, file://, gs://).
        For HDFS, both the scheme and connection authority are removed.
        """
        if not path:
            return path
            
        # HDFS-specific authority stripping
        if path.startswith("hdfs://"):
            cleaned = re.sub(r'^hdfs://(?:[^/]+)?', '', path)
            return cleaned if cleaned else "/"
            
        # Universal scheme stripping (handles any alphanumeric scheme followed by ://)
        return re.sub(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', '', path)

    @staticmethod
    def _consolidate_paths(paths: t.List[str], max_files_per_brace: int = 50) -> t.List[str]:
        """
        Groups and minimizes file paths to optimize Py4J transmission and Spark's directory listings.
        """
        if not paths or len(paths) <= 10:
            return paths

        dir_to_files = defaultdict(list)
        for p in paths:
            parent = os.path.dirname(p)
            filename = os.path.basename(p)
            dir_to_files[parent].append(filename)

        compacted_patterns = []
        for parent, filenames in dir_to_files.items():
            if len(filenames) == 1:
                compacted_patterns.append(os.path.join(parent, filenames[0]))
            else:
                # Chunk filenames to keep pattern string length safe for Hadoop URI parsing
                for i in range(0, len(filenames), max_files_per_brace):
                    chunk = filenames[i : i + max_files_per_brace]
                    if len(chunk) == 1:
                        compacted_patterns.append(os.path.join(parent, chunk[0]))
                    else:
                        brace_expr = "{" + ",".join(chunk) + "}"
                        compacted_patterns.append(os.path.join(parent, brace_expr))

        return compacted_patterns

    @staticmethod
    def create_empty_metadata_df(spark: SparkSession, metadata_prefix: str) -> DataFrame:
        """
        Creates an empty PySpark DataFrame with the standard metadata columns schema.
        """
        schema_str = (
            f"{metadata_prefix}file_path STRING, "
            f"{metadata_prefix}file_name STRING, "
            f"{metadata_prefix}file_size BIGINT, "
            f"{metadata_prefix}file_modification_time TIMESTAMP"
        )
        return spark.createDataFrame([], schema=schema_str)


class SparkFileHandlerFactory:
    """
    Registry for providing the correct BaseSparkFileHandler based on the file extension.
    """
    _handlers: t.Dict[str, t.Type[BaseSparkFileHandler]] = {}

    @classmethod
    def register(cls, extensions: t.List[str], handler_cls: t.Type[BaseSparkFileHandler]):
        for ext in extensions:
            cls._handlers[ext.lower()] = handler_cls

    @classmethod
    def get_handler(cls, file_ext: str) -> t.Optional[BaseSparkFileHandler]:
        handler_cls = cls._handlers.get(file_ext.lower())
        return handler_cls() if handler_cls else None