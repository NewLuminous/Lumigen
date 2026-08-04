"""Contains FileSystemEngineAdapter."""

from __future__ import annotations
import typing as t

import logging
import json
from sqlglot import exp
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)
import pandas as pd
import pyspark.sql
import pyspark.sql.functions as F

if t.TYPE_CHECKING:
    from core._typing import DataFrameType
    from sqlmesh.utils.connection_pool import ConnectionPool

from core.config.job import DataLoadMode
from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin, QueryBuilderMixin, WatermarkStrategyMixin
from utils.errors import EngineAdapterError, MissingColumnError, UnsupportedDataFrameError

logger = logging.getLogger(__name__)


class FileSystemEngineAdapter(
    WatermarkStrategyMixin,
    QueryBuilderMixin,
    FetchBatchMixin,
    EngineAdapterMixin,
):
    DIALECT: str = None
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_TUPLE_IN = False
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_QUERY_EXECUTION_TRACKING = False

    def __init__(
        self,
        connection_factory_or_pool: t.Union[t.Callable[[], t.Any], ConnectionPool],
        file_options: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ):
        super(FileSystemEngineAdapter, self).__init__(connection_factory_or_pool, **kwargs)
        self.file_options = file_options

    def build_query(
        self,
        path: t.Optional[str] = None,
        query: t.Optional[str] = None,
        data_load_mode: t.Optional[DataLoadMode] = None,
        watermark_column: t.Optional[str] = None,
        last_watermark_value: t.Optional[t.Any] = None,
        **kwargs: t.Any,
    ) -> t.Any:
        if query:
            raise ValueError("Query parameter is not supported for FileSystemEngineAdapter. Please provide a file path instead.")

        request = {
            "operation": "read",
            "file_pattern": path,
            "options": self.file_options,
        }
        if data_load_mode == DataLoadMode.INCREMENTAL and last_watermark_value:
            if hasattr(last_watermark_value, "isoformat"):
                request["modified_since"] = last_watermark_value.isoformat()
            else:
                request["modified_since"] = last_watermark_value
        return json.dumps(request, indent=2)
        
    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        self._connection_pool.close_cursor()
        self._connection_pool.close_all(exclude_calling_thread=True)

    def close(self) -> t.Any:
        """Closes all open connections and releases all allocated resources."""
        self._connection_pool.close_cursor()
        self._connection_pool.close_all()
    
    def replace_query(
        self,
        file_path: str,
        df: DataFrameType,
        *args: t.Any, **kwargs: t.Any,
    ) -> None:
        request = json.dumps({
            "operation": "write",
            "file_path": file_path,
            "options": self.file_options,
        })
        self.execute(request, payload=df)

    def insert_append(
        self,
        file_path: str,
        df: DataFrameType,
        *args: t.Any, **kwargs: t.Any,
    ) -> None:
        request = json.dumps({
            "operation": "append",
            "file_path": file_path,
            "options": self.file_options,
        })
        self.execute(request, payload=df)

    def merge(self, *args, **kwargs) -> None:
        raise EngineAdapterError("Unsupported MERGE operation for File sinks.")

    def ping(self) -> None:
        try:
            self.connection.ping()
        except AttributeError as ae:
            raise NotImplementedError("The underlying connection does not support pinging.") from ae
        finally:
            self._connection_pool.close() # Instead of close_cursor()

    def columns(
        self, file_pattern: str, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        request = json.dumps({
            "operation": "describe",
            "file_pattern": file_pattern,
            "options": self.file_options,
        })
        self.execute(request)
        describe_output = self.cursor.fetchall()
        return {
            column_name: exp.DataType.build(column_type, dialect=self.dialect)
            for column_name, column_type, *_ in describe_output
            if column_name and column_name.strip() and column_type and column_type.strip()
        }

    def table_exists(self, file_path: str) -> bool:
        try:
            return self.connection.exists(file_path)
        except AttributeError as ae:
            raise NotImplementedError("The underlying connection does not support checking file existence.") from ae
    
    def _truncate_table(self, file_path: str) -> None:
        raise EngineAdapterError("Unsupported TRUNCATE operation for File sinks.")
    
    def get_all_data_objects(self, keyword: str="", limit: t.Optional[int]=None) -> t.List[DataObject]:
        data_objects = []

        request = json.dumps({
            "operation": "list",
            "file_pattern": keyword,
            "options": self.file_options,
        })
        self.execute(request)
        if limit is None:
            list_output = self.cursor.fetchall()
        else:
            list_output = self.cursor.fetchmany(limit)
        for file_path, *_ in list_output:
            data_objects.append(DataObject(
                schema="",
                name=file_path,
                type=DataObjectType.TABLE,
            ))
        
        return data_objects
    
    def determine_high_watermark(self, df: DataFrameType, watermark_field: str, current_watermark_value: t.Any) -> t.Any:
        # Override watermark_field because file incremental loading relies exclusively on file modification time
        watermark_field = "_metadata_file_modification_time"
        max_df_watermark = None

        if isinstance(df, pyspark.sql.DataFrame):
            if watermark_field not in df.columns:
                raise MissingColumnError(f"Column '{watermark_field}' not found in PySpark DataFrame.")
            
            max_row = df.select(F.max(F.col(watermark_field))).collect()
            if max_row and max_row[0][0] is not None:
                max_df_watermark = max_row[0][0]
        elif isinstance(df, pd.DataFrame):
            if watermark_field not in df.columns:
                raise MissingColumnError(f"Column '{watermark_field}' not found in Pandas DataFrame.")
            
            if not df.empty and not df[watermark_field].isna().all():
                max_val = df[watermark_field].max()
                # Convert Pandas Timestamp/NaT to native Python datetime/None for standard compatibility
                if pd.notna(max_val):
                    max_df_watermark = max_val.to_pydatetime() if hasattr(max_val, 'to_pydatetime') else max_val
        else:
            raise UnsupportedDataFrameError(f"DataFrame type not supported for File watermark calculation: {type(df).__name__}")
        
        if max_df_watermark is None:
            return current_watermark_value
        if current_watermark_value is None:
            return max_df_watermark
        return max(max_df_watermark, current_watermark_value)