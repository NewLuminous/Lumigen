from __future__ import annotations
import typing as t
import logging
import json

if t.TYPE_CHECKING:
    from core._typing import DataFrameType
    from engines.spark.spark_kafka import SparkKafkaCursor

from core.engine_adapter.spark.base import SparkEngineAdapter
from core.engine_adapter.fs import FileSystemEngineAdapter
from utils.collections import SQLIdentifierSet
from utils.errors import UnsupportedOperationError, MissingColumnError

logger = logging.getLogger(__name__)


class SparkFileSystemEngineAdapter(SparkEngineAdapter, FileSystemEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

    def replace_query(
        self,
        topics: str, # List of topics separated by comma
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        raise UnsupportedOperationError()
            
    def insert_append(
        self,
        topic: str,
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        raise UnsupportedOperationError()
            
    def merge(self, *args, **kwargs) -> None:
        raise UnsupportedOperationError("Unsupported MERGE operation for Kafka sinks.")

    def table_exists(self, topics: str) -> bool:
        raise UnsupportedOperationError()