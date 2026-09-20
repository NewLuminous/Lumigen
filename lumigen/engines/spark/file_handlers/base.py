from __future__ import annotations
import typing as t
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
import os
import re
import pandas as pd
import uuid

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession, DataFrameReader, DataFrameWriter
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from utils.dataset import DataFrameConverter

logger = logging.getLogger(__name__)


class BaseSparkFileHandler(ABC):
    SUPPORTED_FILE_SYSTEMS: set = {
        "hdfs", "s3",
    }

    # Threshold above which Spark's distributed mergeSchema is prefered over driver-side threaded schema inference.
    DRIVER_SCHEMA_INFERENCE_THRESHOLD = 1000

    def read(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        if fs_type == "hdfs":
            hdfs_files = {}
            for path, info in files.items():
                no_scheme_path = self.strip_scheme(path)
                if not no_scheme_path.startswith("/"):
                    no_scheme_path = "/" + no_scheme_path
                hdfs_files["hdfs://" + no_scheme_path] = info
            consolidated_paths = self._consolidate_paths(list(hdfs_files.keys()))

            df = self._read_hadoop_fs(spark, hdfs_files, consolidated_paths, request, metadata_prefix, fs_conn)
        elif fs_type == "s3":
            s3_files = {}
            for path, info in files.items():
                no_scheme_path = self.strip_scheme(path).lstrip("/")
                s3_files["s3a://" + no_scheme_path] = info
            consolidated_paths = self._consolidate_paths(list(s3_files.keys()))

            df = self._read_hadoop_fs(spark, s3_files, consolidated_paths, request, metadata_prefix, fs_conn)
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
                    f"To resolve this, the subclass's internal read implementations must return a DataFrame "
                    f"populated with the following 4 metadata columns: {expected_list}, "
                    f"or read from a data source that natively exposes the '_metadata' struct."
                )

        # Standardize metadata columns
        path_col = metadata_prefix + "file_path"
        time_col = metadata_prefix + "file_modification_time"

        clean_path_expr = f"""CASE 
            WHEN {path_col} LIKE 'hdfs://%' THEN regexp_replace({path_col}, '^hdfs://(?:[^/]+)?', '') 
            ELSE regexp_replace({path_col}, '^[a-zA-Z][a-zA-Z0-9+.-]*://', '') 
        END AS {path_col}"""

        # Formats the timestamp to local ISO 8601 with timezone offset
        formatted_time_expr = f"date_format({time_col}, \"yyyy-MM-dd'T'HH:mm:ssXXX\") AS {time_col}"

        select_exprs = []
        for col_name in df.columns:
            if col_name == path_col:
                select_exprs.append(clean_path_expr)
            elif col_name == time_col:
                select_exprs.append(formatted_time_expr)
            else:
                select_exprs.append(f"`{col_name.replace('`', '``')}`")
        df = df.selectExpr(*select_exprs)
            
        return df

    def _read_native_format(
        self, spark: SparkSession, format_name: str,
        files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        """
        Shared native Spark read execution engine.
        """
        file_count = len(files)
        if file_count == 0:
            return self.create_empty_metadata_df(spark, metadata_prefix)

        reader = (
            spark.read.format(format_name)
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
        )

        # Driver-Side Schema Inference Heuristic
        if fs_conn and file_count <= self.DRIVER_SCHEMA_INFERENCE_THRESHOLD:
            try:
                unified_schema_df = fs_conn.cursor()._infer_unified_schema(files, request)
                if unified_schema_df is not None and len(unified_schema_df.columns) > 0:
                    spark_schema_df = DataFrameConverter.to_spark(unified_schema_df, spark)
                    expected_schema = self._drop_metadata_columns(spark_schema_df, metadata_prefix).schema
                    reader = reader.schema(expected_schema)
                else:
                    reader = reader.option("mergeSchema", "true")
            except Exception as e:
                logger.warning("Driver-side schema inference failed; falling back to mergeSchema.", exc_info=e)
                reader = reader.option("mergeSchema", "true")
        # Native Distributed Schema Merging
        else:
            reader = reader.option("mergeSchema", "true")

        reader = self._configure_reader(reader, request.options, consolidated_paths)
        return reader.load(consolidated_paths)

    def _configure_reader(self, reader: DataFrameReader, options: t.Optional[FileOptions], consolidated_paths: t.List[str]) -> DataFrameReader:
        """
        Hook for subclasses to configure the PySpark DataFrameReader builder by chaining any options, schemas, or custom configurations.
        """
        return reader

    @abstractmethod
    def _read_hadoop_fs(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        """
        Handles reading from storage backends backed by Hadoop's FileSystem Java API.
        """
        pass

    def _read_via_pandas(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection],
    ) -> DataFrame:
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

    def describe(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
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
    def _drop_metadata_columns(df: DataFrame, metadata_prefix: str) -> DataFrame:
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

    def write(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> None:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        """
        method_name = "_write_impl"
        if fs_type == "hdfs":
            no_scheme_path = self.strip_scheme(file_path)
            if not no_scheme_path.startswith("/"):
                no_scheme_path = "/" + no_scheme_path

            method_name = "_write_hadoop_fs"
            written_df = self._write_hadoop_fs(spark, df, "hdfs://" + no_scheme_path, options, metadata_prefix, fs_conn)
        elif fs_type == "s3":
            no_scheme_path = self.strip_scheme(file_path).lstrip("/")
            method_name = "_write_hadoop_fs"
            written_df = self._write_hadoop_fs(spark, df, "s3a://" + no_scheme_path, options, metadata_prefix, fs_conn)
        else:
            method_name = "_write_via_pandas"
            written_df = self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        self._validate_written_df(written_df, method_name, metadata_prefix)

    def _write_native_format(
        self, spark: SparkSession, format_name: str, df: DataFrame,
        file_path: str, mode: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        """
        Shared native Spark write/append execution engine.
        Converts single files to dataset directories automatically when mode='append'.
        """
        if mode == "append":
            self._promote_file_to_dataset_dir(file_path, fs_conn)

        clean_df = self._drop_metadata_columns(df, metadata_prefix)

        writer = clean_df.write.format(format_name).mode(mode)
        writer = self._configure_writer(writer, options, file_path)
        writer.save(file_path)
        return clean_df

    def _configure_writer(self, writer: DataFrameWriter, options: t.Optional[FileOptions], file_path: str) -> DataFrameWriter:
        """
        Hook for subclasses to configure the PySpark DataFrameWriter builder by chaining any options.
        """
        return writer

    @abstractmethod
    def _write_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        """
        Writes a DataFrame to a file, overwriting it if it already exists.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    def _write_via_pandas(
        self, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for writing files via Pandas.")

        pdf = DataFrameConverter.to_pandas(df)
        fs_conn.cursor()._write_single_file(pdf, file_path, options)
        return self._drop_metadata_columns(df, metadata_prefix)

    def append(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_type: str, fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> None:
        """
        Appends data to an existing file/dataset, validating schema compatibility first.
        """
        method_name = "_append_impl"
        if fs_type == "hdfs":
            no_scheme_path = self.strip_scheme(file_path)
            if not no_scheme_path.startswith("/"):
                no_scheme_path = "/" + no_scheme_path

            method_name = "_append_hadoop_fs"
            written_df = self._append_hadoop_fs(spark, df, "hdfs://" + no_scheme_path, options, metadata_prefix, fs_conn)
        elif fs_type == "s3":
            no_scheme_path = self.strip_scheme(file_path).lstrip("/")
            method_name = "_append_hadoop_fs"
            written_df = self._append_hadoop_fs(spark, df, "s3a://" + no_scheme_path, options, metadata_prefix, fs_conn)
        else:
            method_name = "_append_via_pandas"
            written_df = self._append_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        self._validate_written_df(written_df, method_name, metadata_prefix)

    @abstractmethod
    def _append_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        """
        Appends a DataFrame to a distributed storage dataset natively.
        Subclasses must return the DataFrame passed to the writer.
        """
        pass

    def _append_via_pandas(
        self, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for appending files via Pandas.")

        pdf = DataFrameConverter.to_pandas(df)
        fs_conn.cursor()._append_single_file(pdf, file_path, options)
        return self._drop_metadata_columns(df, metadata_prefix)

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

    def _promote_file_to_dataset_dir(self, file_path: str, fs_conn: t.Optional[FileSystemConnection]) -> None:
        """
        If file_path exists as a single binary file,
        converts it into a dataset directory containing the single file 
        formatted as an exact Spark part-file (part-00000-[uuid]-c000.ext).
        This prevents Spark's FileOutputCommitter from wiping out the single file during mode('append').
        """
        if not fs_conn:
            return

        fs = fs_conn.get_fs()
        cleaned_path = fs_conn.cursor()._clean_path(file_path)
        try:
            info = fs.info(cleaned_path)
            if info.get("type") == "file":
                parent_dir, base_name = os.path.split(cleaned_path)
                _, ext = os.path.splitext(base_name)
                
                spark_part_name = f"part-00000-{uuid.uuid4()}-c000{ext}"
                
                # Stage in a temporary directory to avoid ENOTDIR (Not a directory) path collisions
                temp_dir = f"{parent_dir}/_legacy_tmp_{uuid.uuid4().hex[:8]}" if parent_dir else f"_legacy_tmp_{uuid.uuid4().hex[:8]}"
                temp_file_path = f"{temp_dir}/{spark_part_name}"
                
                # Move single file to temporary directory
                fs.rename(cleaned_path, temp_file_path)
                
                # Re-create target path as a directory and move the file inside
                target_part_path = f"{cleaned_path}/{spark_part_name}"
                fs.rename(temp_file_path, target_part_path)
                
                try:
                    fs.rm(temp_dir, recursive=True)
                except Exception:
                    pass
        except (FileNotFoundError, OSError):
            pass

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

    @staticmethod
    def _resolve_compression(
        options: t.Optional[FileOptions] = None, 
        file_path: str = "", 
        supported_codecs: t.Optional[t.Collection[str]] = None
    ) -> t.Optional[str]:
        """
        Universal compression resolver for PySpark file handlers.
        Maps common aliases to canonical Spark compression codec names.
        """
        SPARK_COMPRESSION_MAP: t.Dict[str, str] = {
            "none": "none", "uncompressed": "none",
            "gzip": "gzip", "gz": "gzip",
            "bzip2": "bzip2", "bz2": "bzip2",
            "deflate": "deflate",
            "snappy": "snappy",
            "lz4": "lz4",
            "zstd": "zstd", "zst": "zstd",
            "lzo": "lzo",
            "zlib": "zlib",
            "brotli": "brotli",
        }

        resolved_codec: t.Optional[str] = None

        if options and options.compression_codec and options.compression_codec.strip().lower() != "infer":
            codec_key = options.compression_codec.strip().lower()
            if codec_key not in SPARK_COMPRESSION_MAP:
                raise ValueError(f"Unknown compression codec '{options.compression_codec}'.")
            resolved_codec = SPARK_COMPRESSION_MAP[codec_key]
        elif file_path:
            lower_path = file_path.lower()
            for ext, codec in SPARK_COMPRESSION_MAP.items():
                if ext != "none" and lower_path.endswith(f".{ext}"):
                    resolved_codec = codec
                    break

        if resolved_codec in ("none", "uncompressed"):
            return None

        if resolved_codec and supported_codecs and resolved_codec not in supported_codecs:
            allowed = ", ".join(sorted(supported_codecs))
            raise ValueError(
                f"Compression codec '{resolved_codec}' is not supported by Spark for this format. "
                f"Supported codecs: {allowed}"
            )

        return resolved_codec


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