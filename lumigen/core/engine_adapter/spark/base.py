from __future__ import annotations

import typing as t

import logging
from sqlglot import exp
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter as SQLMeshSparkEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

if t.TYPE_CHECKING:
    from core._typing import TableName, SchemaName, PySparkDataFrame

from core.engine_adapter.mixins import EngineAdapterMixin

logger = logging.getLogger(__name__)


class SparkEngineAdapter(
    EngineAdapterMixin,
    SQLMeshSparkEngineAdapter,
):
    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> PySparkDataFrame:
        """Fetches a PySpark DataFrame from the cursor"""
        with self.transaction():
            self.execute(query, quote_identifiers=quote_identifiers)
            return self.cursor.fetchdf()

    def wap_supported(self, table_name: TableName) -> bool:
        # Disable WAP to prevent dummy inserting when _create_table
        return False
    
    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        sql = ""
        if schema_name:
            schema_name = to_schema(schema_name).sql(dialect=self.dialect)
            sql += f" IN {schema_name}"
        pattern = "*" if object_names is None else "|".join(object_names)
        sql += f" LIKE '{pattern}'"
        try:
            # TODO: `DESCRIBE TABLE EXTENDED` for `Type:`
            results = (
                self.fetch_pyspark_df("SHOW TABLE EXTENDED" + sql).collect()
                if self._use_spark_session
                else self.fetchdf("SHOW TABLE EXTENDED" + sql).to_dict("records")
            )
        # Improvement: Figure out all the different exceptions we could get from executing a query either with or
        # without a Spark Session. In addition Databricks would need to be updated to handle it's own exceptions.
        # Therefore just doing except Exception for now.
        except Exception as e:
            if "EXTENDED is not supported" in str(e):
                results = (
                    self.fetch_pyspark_df("SHOW TABLES" + sql).collect()
                    if self._use_spark_session
                    else self.fetchdf("SHOW TABLES" + sql).to_dict("records")
                )
            else:
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
                        if "Type: VIEW" in row_dict.get("information", "")
                        else DataObjectType.TABLE
                    ),
                )
            )
        return data_objects
    
    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            table_pattern = f"{table}*" if table else '*'
        else:
            schema = None
            table_pattern = f"*{keyword}*" if keyword else '*'

        return self._get_data_objects(schema, {table_pattern})