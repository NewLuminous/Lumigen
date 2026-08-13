from __future__ import annotations
import typing as t
import os
import tempfile
import pandas as pd

if t.TYPE_CHECKING:
    import openpyxl
    from pyspark.sql import DataFrame, SparkSession
    from engines.fs.request_parser import FileRequest, FileOptions
    from engines.fs.clients import FileSystemConnection

from engines.spark.file_handlers.base import BaseSparkFileHandler
from utils.dataset import DataFrameConverter


class SparkExcelFileHandler(BaseSparkFileHandler):

    # Row count threshold to switch from fast in-memory batching to safe streaming
    STREAMING_THRESHOLD_ROWS = 50_000
    MAX_EXCEL_ROWS_PER_SHEET = 1_048_576

    def _read_hadoop_fs(
        self, spark: SparkSession, files: t.Dict[str, t.Dict[str, t.Any]], consolidated_paths: t.List[str], request: FileRequest, metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None
    ) -> DataFrame:
        # As of Spark 4.2, native Excel datasource support is not built into core Spark.
        return self._read_via_pandas(spark, files, request, metadata_prefix, fs_conn)

    def _write_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        _, file_ext = os.path.splitext(file_path.lower())
        if file_ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            try:
                import xlsxwriter
            except ImportError:
                return self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)
        else:
            return self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        sheet_name_col = metadata_prefix + "excel_sheet_name"
        total_row_count, sheet_counts, sheet_values = self._get_sheet_row_counts(df, sheet_name_col)
        if total_row_count <= self.STREAMING_THRESHOLD_ROWS:
            return self._write_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for writing files.")

        # Use a local temp file on driver disk to eliminate s3fs RAM write buffering
        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            clean_spark_df = self._stream_workbook_to_disk(
                df, tmp_file_path, metadata_prefix,
                total_row_count=total_row_count, sheet_counts=sheet_counts, sheet_values=sheet_values,
            )
            fs_conn.get_fs().put_file(tmp_file_path, file_path)
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

        return clean_spark_df

    def _append_hadoop_fs(
        self, spark: SparkSession, df: DataFrame, file_path: str, options: t.Optional[FileOptions], metadata_prefix: str,
        fs_conn: t.Optional[FileSystemConnection] = None,
    ) -> DataFrame:
        _, file_ext = os.path.splitext(file_path.lower())
        if file_ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            try:
                import xlsxwriter
                import openpyxl
            except ImportError:
                return self._append_via_pandas(df, file_path, options, metadata_prefix, fs_conn)
        else:
            return self._append_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        sheet_name_col = metadata_prefix + "excel_sheet_name"
        total_row_count, sheet_counts, sheet_values = self._get_sheet_row_counts(df, sheet_name_col)
        if total_row_count <= self.STREAMING_THRESHOLD_ROWS:
            return self._append_via_pandas(df, file_path, options, metadata_prefix, fs_conn)

        if not fs_conn:
            raise ValueError("FileSystemConnection (fs_conn) is required for appending files.")

        fs = fs_conn.get_fs()
        cleaned_path = self.strip_scheme(file_path)
        try:
            info = fs.info(cleaned_path)
            has_existing = info.get("type") == "file"
        except (FileNotFoundError, OSError):
            has_existing = False
        if not has_existing:
            with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
            try:
                clean_spark_df = self._stream_workbook_to_disk(
                    df, tmp_file_path, metadata_prefix, 
                    total_row_count=total_row_count, sheet_counts=sheet_counts, sheet_values=sheet_values,
                    old_wb=None
                )
                fs.put_file(tmp_file_path, cleaned_path)
            finally:
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
            return clean_spark_df

        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as in_tmp:
            in_tmp_path = in_tmp.name
        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as out_tmp:
            out_tmp_path = out_tmp.name

        try:
            fs.get_file(cleaned_path, in_tmp_path)
            old_wb = openpyxl.load_workbook(in_tmp_path, read_only=True)
            try:
                clean_spark_df = self._stream_workbook_to_disk(
                    df, out_tmp_path, metadata_prefix, old_wb=old_wb,
                    total_row_count=total_row_count, sheet_counts=sheet_counts, sheet_values=sheet_values,
                )
            finally:
                old_wb.close()
            fs.put_file(out_tmp_path, cleaned_path)
        finally:
            if os.path.exists(in_tmp_path):
                os.remove(in_tmp_path)
            if os.path.exists(out_tmp_path):
                os.remove(out_tmp_path)

        return clean_spark_df

    def _stream_workbook_to_disk(
        self, df: DataFrame, out_tmp_path: str, metadata_prefix: str, old_wb: t.Optional[openpyxl.Workbook] = None,
        total_row_count: t.Optional[int] = None, sheet_counts: t.Optional[t.Dict[str, int]] = None, sheet_values: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> DataFrame:
        """
        Unified logic for streaming PySpark rows to a constant-memory Excel workbook on local disk.
        If `old_wb` (an openpyxl read-only workbook) is provided, it acts as an append operation.
        """
        import xlsxwriter

        sheet_name_col = metadata_prefix + "excel_sheet_name"
        if (sheet_counts is None or sheet_values is None) and sheet_name_col in df.columns:
            _, sheet_counts, sheet_values = self._get_sheet_row_counts(df, sheet_name_col)

        clean_spark_df = self._drop_metadata_columns(df, metadata_prefix)
        clean_spark_df, _ = DataFrameConverter._preprocess_spark_df(clean_spark_df)

        new_wb = xlsxwriter.Workbook(out_tmp_path, {"constant_memory": True})
        try:
            processed_sheets = set()
            if sheet_name_col in df.columns and sheet_values is not None and sheet_counts is not None:
                # Copy old sheets (if appending) and append matching new data
                if old_wb is not None:
                    for sheet_name in old_wb.sheetnames:
                        old_ws = old_wb[sheet_name]
                        new_ws = new_wb.add_worksheet(sheet_name)

                        row_count = 0
                        for row_idx, row_vals in enumerate(old_ws.iter_rows(values_only=True)):
                            new_ws.write_row(row_idx, 0, list(row_vals))
                            row_count += 1

                        if sheet_name in sheet_values:
                            s_val = sheet_values[sheet_name]
                            sheet_spark_df = df.filter(df[sheet_name_col] == s_val) if s_val is not None else df.filter(df[sheet_name_col].isNull())
                            
                            new_rows_count = sheet_counts[sheet_name]
                            if (row_count + new_rows_count) > self.MAX_EXCEL_ROWS_PER_SHEET:
                                raise ValueError(f"Sheet '{sheet_name}' total rows ({row_count + new_rows_count:,}) exceeds Excel limit.")

                            clean_sheet_df = self._drop_metadata_columns(sheet_spark_df, metadata_prefix)
                            for r_offset, row in enumerate(clean_sheet_df.toLocalIterator(), start=row_count):
                                new_ws.write_row(r_offset, 0, self._sanitize_row(row))

                            processed_sheets.add(sheet_name)
                # Add brand new sheets (either new writes, or appends creating non-existent sheets)
                for sheet_name_str, s_val in sheet_values.items():
                    if sheet_name_str not in processed_sheets:
                        new_ws = new_wb.add_worksheet(sheet_name_str)
                        sheet_spark_df = df.filter(df[sheet_name_col] == s_val) if s_val is not None else df.filter(df[sheet_name_col].isNull())
                        
                        sheet_count = sheet_counts[sheet_name_str]
                        if sheet_count > self.MAX_EXCEL_ROWS_PER_SHEET:
                            raise ValueError(f"Sheet '{sheet_name_str}' contains {sheet_count:,} rows, exceeding Excel's limit.")

                        clean_sheet_df = self._drop_metadata_columns(sheet_spark_df, metadata_prefix)
                        # Write Header Row
                        new_ws.write_row(0, 0, clean_sheet_df.columns)

                        # Stream rows from PySpark partitions directly to disk via toLocalIterator
                        for r_offset, row in enumerate(clean_sheet_df.toLocalIterator(), start=1):
                            new_ws.write_row(r_offset, 0, self._sanitize_row(row))
            else: # Single sheet append logic
                if old_wb is not None:
                    target_sheet = old_wb.sheetnames[0] if old_wb.sheetnames else "Sheet1"
                    for s_name in old_wb.sheetnames:
                        old_ws = old_wb[s_name]
                        new_ws = new_wb.add_worksheet(s_name)
                        
                        row_count = 0
                        for row_idx, row_vals in enumerate(old_ws.iter_rows(values_only=True)):
                            new_ws.write_row(row_idx, 0, list(row_vals))
                            row_count += 1
                        
                        if s_name == target_sheet:
                            new_rows_count = total_row_count if total_row_count is not None else df.count()
                            if (row_count + new_rows_count) > self.MAX_EXCEL_ROWS_PER_SHEET:
                                raise ValueError(f"Sheet '{s_name}' total rows exceeds Excel limit.")

                            for r_offset, row in enumerate(clean_spark_df.toLocalIterator(), start=row_count):
                                new_ws.write_row(r_offset, 0, self._sanitize_row(row))
                else:
                    new_rows_count = total_row_count if total_row_count is not None else clean_spark_df.count()
                    if new_rows_count > self.MAX_EXCEL_ROWS_PER_SHEET:
                        raise ValueError(f"Dataset contains {new_rows_count:,} rows, exceeding Excel's limit.")

                    new_ws = new_wb.add_worksheet("Sheet1")
                    # Write Header Row
                    new_ws.write_row(0, 0, clean_spark_df.columns)

                    # Stream rows from PySpark partitions directly to disk
                    for row_idx, row in enumerate(clean_spark_df.toLocalIterator(), start=1):
                        new_ws.write_row(row_idx, 0, self._sanitize_row(row))
        finally:
            new_wb.close()

        return clean_spark_df

    def _get_sheet_row_counts(
        self, df: DataFrame, sheet_name_col: str
    ) -> t.Tuple[int, t.Optional[t.Dict[str, int]], t.Optional[t.Dict[str, t.Any]]]:
        """
        Consolidates row counting and distinct sheet lookup into a single Spark Action.
        Returns: (total_row_count, sheet_counts_map, sheet_values_map)
        """
        if sheet_name_col in df.columns:
            sheet_stats = df.groupBy(sheet_name_col).count().collect()
            sheet_counts = {}
            sheet_values = {}
            total_row_count = 0
            for row in sheet_stats:
                val = row[0]
                cnt = row['count']
                sheet_str = str(val) if (val is not None and pd.notna(val)) else "Sheet1"
                sheet_counts[sheet_str] = cnt
                sheet_values[sheet_str] = val
                total_row_count += cnt
            return total_row_count, sheet_counts, sheet_values
        else:
            return df.count(), None, None

    @staticmethod
    def _sanitize_row(row):
        """Helper to convert nested PySpark Row objects (Structs) into clean Python representations."""
        return [
            val.asDict(recursive=True) if hasattr(val, "asDict") else val
            for val in row
        ]