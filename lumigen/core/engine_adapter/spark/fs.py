from __future__ import annotations
import typing as t
import logging
import json

if t.TYPE_CHECKING:
    from core._typing import DataFrameType

from core.engine_adapter.spark.base import SparkEngineAdapter
from core.engine_adapter.fs import FileSystemEngineAdapter
from utils.errors import UnsupportedOperationError

logger = logging.getLogger(__name__)


class SparkFileSystemEngineAdapter(SparkEngineAdapter, FileSystemEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

    def replace_query(
        self,
        file_path: str,
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        request = json.dumps({
            "operation": "write",
            "file_path": file_path,
            "options": self.file_options,
        })
        self.execute(request, df=df)
            
    def insert_append(
        self,
        file_path: str,
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        request = json.dumps({
            "operation": "append",
            "file_path": file_path,
            "options": self.file_options,
        })
        self.execute(request, df=df)
            
    def merge(self, *args, **kwargs) -> None:
        raise UnsupportedOperationError("Unsupported MERGE operation for File sinks.")

    def table_exists(self, file_path: str) -> bool:
        return self.connection.exists(file_path)