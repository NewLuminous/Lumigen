from __future__ import annotations

import typing as t

import logging

import sqlmesh
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
    DataObject,
    DataObjectType,
)

if t.TYPE_CHECKING:
    from core._typing import TableName

from core.engine_adapter.mixins import EngineAdapterMixin

logger = logging.getLogger(__name__)


@set_catalog()
class SparkEngineAdapter(
    EngineAdapterMixin,
    sqlmesh.core.engine_adapter.SparkEngineAdapter,
):
    def wap_supported(self, table_name: TableName) -> bool:
        # Disable WAP to prevent dummy inserting when _create_table
        return False
    
    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            table_pattern = f"{table}*" if table else '*'
        else:
            schema = None
            table_pattern = f"*{keyword}*" if keyword else '*'

        sql = f"SHOW TABLE EXTENDED"
        if schema:
            sql += f" IN {schema}"
        sql += f" LIKE '{table_pattern}'"
        try:
            results = (
                self.fetch_pyspark_df(sql).collect()
                if self._use_spark_session
                else self.fetchdf(sql).to_dict("records")
            )
        # Improvement: Figure out all the different exceptions we could get from executing a query either with or
        # without a Spark Session. In addition Databricks would need to be updated to handle it's own exceptions.
        # Therefore just doing except Exception for now.
        except Exception:
            return []
        data_objects = []
        catalog = self.get_current_catalog()
        for row in results:  # type: ignore
            row_dict = row.asDict() if not isinstance(row, dict) else row
            if row_dict.get("isTemporary"):
                continue
            schema = row_dict.get("namespace") or row_dict.get("database")
            data_objects.append(
                DataObject(
                    catalog=catalog,
                    schema=schema,
                    name=row_dict["tableName"],
                    type=(
                        DataObjectType.VIEW
                        if "Type: VIEW" in row_dict["information"]
                        else DataObjectType.TABLE
                    ),
                )
            )
        return data_objects