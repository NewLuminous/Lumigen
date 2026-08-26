from __future__ import annotations

import typing as t

import logging
from sqlglot import exp
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapter
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
    
    def ping(self) -> None:
        try:
            self._execute(exp.select("1").sql(dialect=self.dialect))
            if hasattr(self, "delegate_adapter") and isinstance(self.delegate_adapter, EngineAdapter):
                t.cast("EngineAdapter", self.delegate_adapter).ping()
        finally:
            self._connection_pool.close_cursor()
    
    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        sql = ""
        if schema_name:
            schema_name = to_schema(schema_name).sql(dialect=self.dialect)
            sql += f" IN {schema_name}"
        pattern = "*" if object_names is None else "|".join(object_names)
        sql += f" LIKE '{pattern}'"
        
        results = None
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
            err_msg = str(e)
            if "EXTENDED is not supported" in err_msg:
                try:
                    results = (
                        self.fetch_pyspark_df("SHOW TABLES" + sql).collect()
                        if self._use_spark_session
                        else self.fetchdf("SHOW TABLES" + sql).to_dict("records")
                    )
                except Exception as inner_e:
                    inner_msg = str(inner_e)
                    # Catch HadoopCatalog S3 Namespace bug
                    if "NoSuchNamespace" in inner_msg or "Namespace does not exist" in inner_msg:
                        results = None 
                    else:
                        return []
            elif "NoSuchNamespace" in err_msg or "Namespace does not exist" in err_msg:
                results = None
            else:
                return []
            
        # Fallback for S3A / HadoopCatalog quirk: 
        # S3 has no directories, so `SHOW TABLES IN namespace` fails if the 0-byte directory marker is missing.
        if results is None:
            results = []
            if object_names:
                for obj_name in object_names:
                    # We can only safely probe concrete table names, not wildcards
                    if "*" in obj_name or "|" in obj_name:
                        continue
                    
                    full_name = f"{schema_name}.{obj_name}" if schema_name else obj_name
                    try:
                        # Direct check using DESCRIBE, which bypasses Iceberg's fs.isDirectory() check
                        self.execute(f"DESCRIBE {full_name}")
                        results.append({"tableName": obj_name, "namespace": schema_name, "isTemporary": False})
                    except Exception:
                        pass

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
    
    def get_all_data_objects(self, keyword: str="", limit: t.Optional[int]=None, exclude_unknown: bool=False, *args) -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            table_pattern = f"{table}*" if table else '*'
        else:
            schema = None
            table_pattern = f"*{keyword}*" if keyword else '*'

        data_objects = self._get_data_objects(schema, {table_pattern})
        if exclude_unknown:
            data_objects = [obj for obj in data_objects if not obj.type.is_unknown]
        if limit is not None:
            data_objects = data_objects[:limit]
        return data_objects