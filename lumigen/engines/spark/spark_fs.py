from __future__ import annotations
import typing as t
import logging
import os
from collections import defaultdict

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from engines.spark.cursors import SparkStreamToBatchCursor
from engines.spark.spark_session import SparkSessionConnection
from engines.spark.file_handlers import SparkFileHandlerFactory, BaseSparkFileHandler
from engines.fs.clients import FileSystemConnection, HDFSConnection, S3Connection
from engines.fs.request_parser import FileRequest, FileRequestParser, FileOperationType, FileOptions, RequestParseError
from utils.dataset import columns_to_types_from_df, DataFrameConverter

logger = logging.getLogger(__name__)


class SparkFileSystemCursor(SparkStreamToBatchCursor):
    SUPPORTED_FILE_SYSTEMS: set = BaseSparkFileHandler.SUPPORTED_FILE_SYSTEMS

    def __init__(self, spark: SparkSession, fs_type: str, fs_conn: FileSystemConnection):
        super().__init__(spark)
        self.fs_type = fs_type
        self._fs_conn = fs_conn
        self._fs_cursor = fs_conn.cursor()

    def execute(self, request: str, df: t.Optional[DataFrame] = None, batch_id: int = 0, **kwargs) -> None:
        """
        Overrides the base `execute` method to provide FS-specific read/write logic.
        This method is the main entry point for this cursor's functionality.

        Args:
            request: A FileRequest string.
            df: The DataFrame to write. This is None for read operations.
            batch_id: The ID of the micro-batch, provided by `foreachBatch`.
        """
        # If a streaming DataFrame is passed, delegate to the base class's `foreachBatch` setup.
        # This will then call this same `execute` method again for each micro-batch.
        if df and df.isStreaming:
            super().execute(request, df, batch_id=batch_id, **kwargs)

        try:
            parsed_request = FileRequestParser.parse(request)
        except RequestParseError as e:
            logger.error(f"Failed to execute request due to parsing error.", exc_info=e)
            raise

        if parsed_request.operation == FileOperationType.READ:
            target_path = self._fs_cursor._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files = dict(self._fs_cursor._lazy_glob(
                target_path, parsed_request.modified_since,
                require_file_exists=True, require_supported=True, strict_extension_check=True,
            ))

            self._last_df = self._read_files(files, parsed_request)
            self._last_output = None
            self._last_output_cursor = 0
        elif parsed_request.operation == FileOperationType.DESCRIBE:
            target_path = self._fs_cursor._clean_path(parsed_request.file_pattern or parsed_request.file_path)
            files = dict(self._fs_cursor._lazy_glob(
                target_path, parsed_request.modified_since,
                require_file_exists=True, require_supported=True, strict_extension_check=True,
            ))

            self._last_df = self._describe_files(files, parsed_request)
            self._last_output = None
            self._last_output_cursor = 0

        # In a streaming `foreachBatch` write, only the first batch (batch_id=0) should perform an overwrite.
        elif parsed_request.operation == FileOperationType.WRITE and batch_id == 0:
            if df is None:
                raise ValueError("A valid DataFrame payload is required for WRITE operations.")
            cleaned_path = self._fs_cursor._clean_path(parsed_request.file_path)
            self._write_single_file(df, cleaned_path, parsed_request.options)

        # All subsequent batches must be appended.
        elif parsed_request.operation == FileOperationType.APPEND or (batch_id > 0 and parsed_request.operation == FileOperationType.WRITE):
            if df is None:
                raise ValueError("A valid DataFrame payload is required for APPEND operations.")
            cleaned_path = self._fs_cursor._clean_path(parsed_request.file_path)
            self._append_single_file(df, cleaned_path, parsed_request.options)
        elif parsed_request.operation == FileOperationType.REMOVE:
            self._fs_cursor.execute(request, **kwargs)
        elif parsed_request.operation == FileOperationType.LIST:
            self._fs_cursor.execute(request)
            pdf = self._fs_cursor.fetchdf()
            self._last_df = DataFrameConverter.to_spark(pdf, self._spark)
            self._last_output = None
            self._last_output_cursor = 0
        else:
            raise NotImplementedError(f"Operation {parsed_request.operation} for Spark is not implemented.")

    def _dispatch_to_handlers(self, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest, operation_name: str) -> t.Optional[DataFrame]:
        """
        Helper method to group files by extension, execute the target handler operation, and union the outputs.
        """
        files_by_ext = defaultdict(dict)
        for path, info in files.items():
            _, ext = os.path.splitext(path.lower())
            files_by_ext[ext][path] = info

        dfs: t.List[DataFrame] = []
        for ext, group_files in files_by_ext.items():
            handler = SparkFileHandlerFactory.get_handler(ext)
            if not handler:
                raise NotImplementedError(
                    f"No Spark file handler is registered for the extension '{ext}'. "
                    f"Please verify that a compatible handler has been mounted via SparkFileHandlerFactory.register()."
                )
            
            handler_func = getattr(handler, operation_name)
            group_df = handler_func(
                self._spark, group_files, request, self._fs_cursor.METADATA_COLUMN_PREFIX,
                self.fs_type, fs_conn=self._fs_conn
            )
            dfs.append(group_df)

        if not dfs:
            return None

        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        return combined_df

    def _read_files(self, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest) -> DataFrame:
        combined_df = self._dispatch_to_handlers(files, request, "read")
        if combined_df is None:
            return BaseSparkFileHandler.create_empty_metadata_df(self._spark, self._fs_cursor.METADATA_COLUMN_PREFIX)
        return combined_df

    def _describe_files(self, files: t.Dict[str, t.Dict[str, t.Any]], request: FileRequest) -> DataFrame:
        schema_str = "field STRING, type STRING"
        combined_df = self._dispatch_to_handlers(files, request, "describe")
        if combined_df is None:
            return self._spark.createDataFrame([], schema=schema_str)

        describe_rows = [(name, dtype.sql()) for name, dtype in columns_to_types_from_df(combined_df).items()]
        return self._spark.createDataFrame(describe_rows, schema=schema_str)

    def _write_single_file(self, df: DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> None:
        _, file_ext = os.path.splitext(file_path.lower())
        handler = SparkFileHandlerFactory.get_handler(file_ext)
        if not handler:
            raise NotImplementedError(
                f"No Spark file handler is registered for the extension '{file_ext}'. "
                f"Please verify that a compatible handler has been mounted via SparkFileHandlerFactory.register()."
            )

        handler.write(self._spark, df, file_path, file_options, self._fs_cursor.METADATA_COLUMN_PREFIX, self.fs_type, fs_conn=self._fs_conn)

    def _append_single_file(self, df: DataFrame, file_path: str, file_options: t.Optional[FileOptions] = None) -> None:
        cleaned_path = self._fs_cursor._clean_path(file_path)
        # Use _lazy_glob() instead of fsspec.exists()
        existing_files = list(self._fs_cursor._lazy_glob(cleaned_path, require_supported=False))
        if not existing_files:
            return self._write_single_file(df, file_path, file_options)

        _, file_ext = os.path.splitext(file_path.lower())
        handler = SparkFileHandlerFactory.get_handler(file_ext)
        if not handler:
            raise NotImplementedError(
                f"No Spark file handler is registered for the extension '{file_ext}'. "
                f"Please verify that a compatible handler has been mounted via SparkFileHandlerFactory.register()."
            )

        handler.append(self._spark, df, file_path, file_options, self._fs_cursor.METADATA_COLUMN_PREFIX, self.fs_type, fs_conn=self._fs_conn)

class SparkFileSystemConnection(SparkSessionConnection):
    def __init__(self, spark: SparkSession, fs_type: str, fs_connection_factory: t.Callable[[], t.Any]):
        super().__init__(spark)
        self.fs_type = fs_type
        self._fs_conn: FileSystemConnection = fs_connection_factory()

        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        if isinstance(self._fs_conn, HDFSConnection):
            if self._fs_conn.host and self._fs_conn.host != "default":
                port_str = f":{self._fs_conn.port}" if self._fs_conn.port > 0 else ""
                default_fs = f"hdfs://{self._fs_conn.host}{port_str}"
                hadoop_conf.set("fs.defaultFS", default_fs)
                self.spark.conf.set("spark.hadoop.fs.defaultFS", default_fs)
            if self._fs_conn.user:
                self.spark.sparkContext._jvm.java.lang.System.setProperty("HADOOP_USER_NAME", self._fs_conn.user)
            if self._fs_conn.kerb_ticket:
                hadoop_conf.set("hadoop.security.authentication", "kerberos")
                self.spark.conf.set("spark.hadoop.hadoop.security.authentication", "kerberos")

                hadoop_conf.set("hadoop.security.kerberos.ticket.cache.path", self._fs_conn.kerb_ticket)
                self.spark.conf.set("spark.hadoop.hadoop.security.kerberos.ticket.cache.path", self._fs_conn.kerb_ticket)

                # Set JVM system properties for Kerberos/GSSAPI
                jvm = self.spark.sparkContext._jvm
                jvm.java.lang.System.setProperty("java.security.krb5.ticketCache", self._fs_conn.kerb_ticket)
                jvm.java.lang.System.setProperty("KRB5CCNAME", self._fs_conn.kerb_ticket)
            if self._fs_conn.replication:
                hadoop_conf.set("dfs.replication", str(self._fs_conn.replication))
                self.spark.conf.set("spark.hadoop.dfs.replication", str(self._fs_conn.replication))
            if self._fs_conn.extra_conf:
                for k, v in self._fs_conn.extra_conf.items():
                    hadoop_conf.set(k, str(v))
                    self.spark.conf.set(f"spark.hadoop.{k}", str(v))
        elif isinstance(self._fs_conn, S3Connection):
            s3a_impl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
            hadoop_conf.set("fs.s3a.impl", s3a_impl)
            hadoop_conf.set("fs.s3.impl", s3a_impl)
            self.spark.conf.set("spark.hadoop.fs.s3a.impl", s3a_impl)
            self.spark.conf.set("spark.hadoop.fs.s3.impl", s3a_impl)

            endpoint = (self._fs_conn.endpoint_url or "").lower()
            is_aws_s3 = (not endpoint) or "amazonaws.com" in endpoint
            if not is_aws_s3:
                try:
                    jvm = self.spark.sparkContext._jvm
                    jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
                        "org.apache.hadoop.fs.s3a.commit",
                        jvm.org.apache.logging.log4j.Level.ERROR
                    )
                except Exception:
                    pass

            committer_factory = "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
            hadoop_conf.set("fs.s3a.committer.factory.class", committer_factory)
            self.spark.conf.set("spark.hadoop.fs.s3a.committer.factory.class", committer_factory)

            committer_name = "directory" if is_aws_s3 else "file"
            hadoop_conf.set("fs.s3a.committer.name", committer_name)
            self.spark.conf.set("spark.hadoop.fs.s3a.committer.name", committer_name)

            if committer_name == "directory":
                hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")
                self.spark.conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")

            if self._fs_conn.endpoint_url:
                hadoop_conf.set("fs.s3a.endpoint", self._fs_conn.endpoint_url)
                self.spark.conf.set("spark.hadoop.fs.s3a.endpoint", self._fs_conn.endpoint_url)
                
                # Path style access is required for custom/private S3 endpoints
                hadoop_conf.set("fs.s3a.path.style.access", "true")
                self.spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

            key = self._fs_conn.s3_access_key_id or self._fs_conn.client_config.get("aws_access_key_id")
            secret = self._fs_conn.s3_secret_access_key or self._fs_conn.client_config.get("aws_secret_access_key")
            session_token = self._fs_conn.client_config.get("aws_session_token") or self._fs_conn.client_config.get("session_token")
            if session_token:
                provider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
                hadoop_conf.set("fs.s3a.aws.credentials.provider", provider)
                self.spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", provider)

                hadoop_conf.set("fs.s3a.access.key", key or "")
                self.spark.conf.set("spark.hadoop.fs.s3a.access.key", key or "")

                hadoop_conf.set("fs.s3a.secret.key", secret or "")
                self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret or "")

                hadoop_conf.set("fs.s3a.session.token", session_token)
                self.spark.conf.set("spark.hadoop.fs.s3a.session.token", session_token)
            elif key and secret:
                provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                hadoop_conf.set("fs.s3a.aws.credentials.provider", provider)
                self.spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", provider)

                hadoop_conf.set("fs.s3a.access.key", key)
                self.spark.conf.set("spark.hadoop.fs.s3a.access.key", key)

                hadoop_conf.set("fs.s3a.secret.key", secret)
                self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret)
            else:
                # Anonymous access fallback
                provider = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
                hadoop_conf.set("fs.s3a.aws.credentials.provider", provider)
                self.spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", provider)

            # Extra S3 client configs (e.g. AWS Region)
            if self._fs_conn.client_config:
                CLIENT_CONFIG_TO_S3A_MAP = {
                    "region_name": "fs.s3a.endpoint.region",
                    "region": "fs.s3a.endpoint.region",
                    "use_ssl": "fs.s3a.connection.ssl.enabled",
                    "connect_timeout": "fs.s3a.connection.establish.timeout",
                    "read_timeout": "fs.s3a.connection.timeout",
                    "max_pool_connections": "fs.s3a.connection.maximum",
                    "max_concurrency": "fs.s3a.connection.maximum",
                    "path_style_access": "fs.s3a.path.style.access",
                    "server_side_encryption": "fs.s3a.server-side-encryption-algorithm",
                    "sse_kms_key_id": "fs.s3a.server-side-encryption.key",
                }
                for k, v in self._fs_conn.client_config.items():
                    if v is None:
                        continue

                    # Direct pass-through if key is already a Hadoop S3A key
                    if k.startswith("fs.s3a."):
                        val_str = str(v).lower() if isinstance(v, bool) else str(v)
                        hadoop_conf.set(k, val_str)
                        self.spark.conf.set(f"spark.hadoop.{k}", val_str)

                    # Map known boto3/s3fs client_config parameters to S3A properties
                    elif k in CLIENT_CONFIG_TO_S3A_MAP:
                        s3a_key = CLIENT_CONFIG_TO_S3A_MAP[k]
                        val_str = str(v).lower() if isinstance(v, bool) else str(v)
                        hadoop_conf.set(s3a_key, val_str)
                        self.spark.conf.set(f"spark.hadoop.{s3a_key}", val_str)

            self.spark.sparkContext._jvm.java.lang.System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true")
        else:
            pass

    def cursor(self) -> SparkFileSystemCursor:
        super().cursor()
        return SparkFileSystemCursor(self.spark, self.fs_type, self._fs_conn)

    def exists(self, file_path: str) -> bool:
        return self._fs_conn.exists(file_path)


def connection(spark: SparkSession, fs_type: str, fs_connection_factory: t.Callable[[], t.Any]) -> SparkFileSystemConnection:
    return SparkFileSystemConnection(spark, fs_type, fs_connection_factory)