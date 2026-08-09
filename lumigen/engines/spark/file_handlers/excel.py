from __future__ import annotations
import typing as t
import os
import tempfile
import pandas as pd

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler
from utils.dataset import DataFrameConverter


class SparkExcelFileHandler(BaseSparkFileHandler):

    # Row count threshold to switch from fast in-memory batching to safe streaming
    STREAMING_THRESHOLD_ROWS = 50_000
    MAX_EXCEL_ROWS_PER_SHEET = 1_048_576

    def _read_hdfs(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        # As of Spark 4.2, native Excel datasource support is not built into core Spark.
        return self._read_via_pandas(spark, files, request, metadata_prefix, fs_conn)

    def _read_s3(self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        # As of Spark 4.2, native Excel datasource support is not built into core Spark.
        return self._read_via_pandas(spark, files, request, metadata_prefix, fs_conn)

    def _write_hdfs(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        return self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

    def _write_s3(self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        return self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

    def _write_via_pandas(self, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str, fs_conn: t.Optional[FileSystemConnection] = None) -> DataFrame:
        _, file_ext = os.path.splitext(file_path.lower())
        if file_ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            try:
                import xlsxwriter
            except ImportError:
                return super()._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)
        else:
            return super()._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        total_row_count = df.count()
        if total_row_count <= self.STREAMING_THRESHOLD_ROWS:
            return super()._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for writing files via Pandas.")

        sheet_name_col = metadata_prefix + "excel_sheet_name"

        clean_spark_df = self.drop_metadata_columns(df, metadata_prefix)
        clean_spark_df, _ = DataFrameConverter._preprocess_spark_df(clean_spark_df)

        # Helper to convert nested PySpark Row objects (Structs) into clean Python representations
        def _sanitize_row(row):
            return [
                val.asDict(recursive=True) if hasattr(val, "asDict") else val
                for val in row
            ]

        # Use a local temp file on driver disk to eliminate s3fs RAM write buffering
        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            workbook = xlsxwriter.Workbook(tmp_file_path, {"constant_memory": True})
            if sheet_name_col in df.columns:
                distinct_sheets = [
                    row[0] for row in df.select(sheet_name_col).distinct().collect()
                ]
                for sheet_name in distinct_sheets:
                    sheet_str = str(sheet_name) if (sheet_name is not None and pd.notna(sheet_name)) else "Sheet1"
                    
                    if sheet_name is not None:
                        sheet_spark_df = df.filter(df[sheet_name_col] == sheet_name)
                    else:
                        sheet_spark_df = df.filter(df[sheet_name_col].isNull())

                    sheet_count = sheet_spark_df.count()
                    if sheet_count > self.MAX_EXCEL_ROWS_PER_SHEET:
                        raise ValueError(f"Sheet '{sheet_str}' contains {sheet_count:,} rows, exceeding Excel's limit.")

                    clean_sheet_spark_df = self.drop_metadata_columns(sheet_spark_df, metadata_prefix)
                    worksheet = workbook.add_worksheet(sheet_str)
                    # Write Header Row
                    worksheet.write_row(0, 0, clean_sheet_spark_df.columns)

                    # Stream rows from PySpark partitions directly to disk via toLocalIterator
                    for row_idx, row in enumerate(clean_sheet_spark_df.toLocalIterator(), start=1):
                        worksheet.write_row(row_idx, 0, _sanitize_row(row))
            else:
                row_count = clean_spark_df.count()
                if row_count > self.MAX_EXCEL_ROWS_PER_SHEET:
                    raise ValueError(f"Dataset contains {row_count:,} rows, exceeding Excel's limit.")

                worksheet = workbook.add_worksheet("Sheet1")
                # Write Header Row
                worksheet.write_row(0, 0, clean_spark_df.columns)

                # Stream rows from PySpark partitions directly to disk
                for row_idx, row in enumerate(clean_spark_df.toLocalIterator(), start=1):
                    worksheet.write_row(row_idx, 0, _sanitize_row(row))

            workbook.close()
            fs_conn.get_fs().put_file(tmp_file_path, file_path)
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

        return clean_spark_df