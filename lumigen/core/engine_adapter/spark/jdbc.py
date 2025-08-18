from __future__ import annotations

import typing as t

import logging
import pyspark.sql
from sqlglot import exp, insert

from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
)

if t.TYPE_CHECKING:
    from core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter.base import QueryOrDF
    from sqlmesh.utils.connection_pool import ConnectionPool

from utils.dataset import DataFrameConverter
from core.engine_adapter.spark.base import SparkEngineAdapter

logger = logging.getLogger(__name__)


@set_catalog()
class SparkJDBCEngineAdapter(SparkEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

    def __init__(
        self,
        connection_factory_or_pool: t.Union[t.Callable[[], t.Any], ConnectionPool],
        delegate_adapter: EngineAdapter,
        **kwargs: t.Any,
    ):
        """
        An adapter that uses Spark's native JDBC capabilities to read/write to databases,
        while delegating metadata and complex operations back to a standard EngineAdapter.
        
        Args:
            connection_factory_or_pool: The factory for creating Spark JDBC connections.
            delegate_adapter: The standard EngineAdapter for the target database (e.g., PostgresEngineAdapter).
        """
        super(SparkJDBCEngineAdapter, self).__init__(connection_factory_or_pool, **kwargs)
        self.delegate_adapter = delegate_adapter
        self.dialect = delegate_adapter.dialect
        self.SCHEMA_DIFFER = delegate_adapter.SCHEMA_DIFFER

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Replaces an existing table with a query.

        For partition based engines (hive, spark), insert override is used.

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            columns_to_types: Optional a mapping between the column name and its data type.
            kwargs: Optional create table properties.
        """
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(query_or_df)
        else:
            df = query_or_df

        if isinstance(df, pyspark.sql.DataFrame):
            insert_exp = insert(exp.select("*").from_("DUAL"),
                                table_name,
                                overwrite=True,
                                dialect=self.dialect)
            self.execute(insert_exp, df=df, **kwargs)
        else:
            raise NotImplementedError()
            
    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(query_or_df)
        else:
            df = query_or_df

        if isinstance(df, pyspark.sql.DataFrame):
            insert_exp = insert(exp.select("*").from_("DUAL"),
                                table_name,
                                dialect=self.dialect)
            self.execute(insert_exp, df=df)
        else:
            raise NotImplementedError()
            
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
    ) -> None:
        if isinstance(source_table, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(source_table)
        else:
            df = source_table

        # MERGE INTO TABLE is not supported temporarily by current version of vanilla Spark (3.3.3)
        # Delegate MERGE operation to corresponding engine adapter at the cost of collecting all the rows from PySpark DataFrame.
        df = DataFrameConverter.to_pandas(df)
        self.delegate_adapter.merge(target_table, df, columns_to_types, unique_key, when_matched, merge_filter)

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        return self.delegate_adapter.columns(table_name, include_pseudo_columns)

    def table_exists(self, table_name: TableName) -> bool:
        return self.delegate_adapter.table_exists(table_name)