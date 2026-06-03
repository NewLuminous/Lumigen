from __future__ import annotations

import typing as t

import logging
import time
import pyspark.sql
from sqlglot import exp, insert

from sqlmesh.core.engine_adapter.shared import (
    SourceQuery,
    InsertOverwriteStrategy,
)
from sqlmesh.utils import random_id

if t.TYPE_CHECKING:
    from core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter.base import QueryOrDF, DF
    from sqlmesh.utils.connection_pool import ConnectionPool

from utils.dataset import DataFrameConverter
from core.engine_adapter.spark.base import SparkEngineAdapter

logger = logging.getLogger(__name__)


class SparkJDBCEngineAdapter(SparkEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

    # A threshold in seconds for when to warn about a slow fallback.
    FALLBACK_WARNING_THRESHOLD_SECONDS: int = 5

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
        self.SCHEMA_DIFFER_KWARGS = delegate_adapter.SCHEMA_DIFFER_KWARGS

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        supports_replace_table_override: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> None:
        """Replaces an existing table with a query.

        For partition based engines (hive, spark), insert override is used.

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            target_columns_to_types: Optional a mapping between the column name and its data type.
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
            df = self._flatten_complex_types(df)
            self.execute(insert_exp, df=df, **kwargs)
        else:
            raise NotImplementedError()
            
    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        track_rows_processed: bool = True,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(query_or_df)
        else:
            df = query_or_df

        if isinstance(df, pyspark.sql.DataFrame):
            insert_exp = insert(exp.select("*").from_("DUAL"),
                                table_name,
                                dialect=self.dialect)
            df = self._flatten_complex_types(df)
            self.execute(insert_exp, df=df, track_rows_processed=track_rows_processed)
        else:
            raise NotImplementedError()
        
    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries_or_df: t.Union[t.List[SourceQuery], DF],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        if isinstance(source_queries_or_df, list):
            return super()._insert_overwrite_by_condition(table_name, source_queries_or_df, target_columns_to_types, where, insert_overwrite_strategy_override, **kwargs)
        else:
            df = source_queries_or_df

        if where is None or not self.table_exists(table_name):
            self.replace_query(table_name, df, target_columns_to_types, **kwargs)
        else:
            self.delegate_adapter.delete_from(table_name, where)
            self.insert_append(table_name, df, target_columns_to_types)
            
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        if not self.table_exists(target_table):
            return self.insert_append(target_table, source_table, target_columns_to_types, source_columns=source_columns)

        if isinstance(source_table, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(source_table)
        else:
            df = source_table

        # NOTE: MERGE INTO TABLE is not supported temporarily by current version of vanilla Spark (3.3.3)
        try:
            # --- PRIMARY STRATEGY: Staging Table ---
            target_table_exp = exp.to_table(target_table, dialect=self.dialect)

            # Create a unique temp table name
            temp_table_name = f"__temp_lumigen_{target_table_exp.name}_{random_id(short=True)}"
            temp_table_exp = exp.table_(temp_table_name, db=target_table_exp.args.get("db"), catalog=target_table_exp.args.get("catalog"))
            try:
                # Stage the data frame into a temp table in the target DB
                self.replace_query(temp_table_exp, df, target_columns_to_types=target_columns_to_types, source_columns=source_columns)

                # Delegate MERGE operation to corresponding engine adapter, using the temp table as the source
                self.delegate_adapter.merge(
                    target_table,
                    exp.select("*").from_(temp_table_exp),
                    target_columns_to_types, unique_key, when_matched, merge_filter, source_columns, **kwargs
                )
            finally:
                # Cleanup: Always drop the temp table
                self.drop_table(temp_table_exp)
        except Exception as primary_error:
            # --- FALLBACK STRATEGY: In-Memory Pandas DataFrame ---
            try:
                start_time = time.time()

                # Delegate MERGE operation to corresponding engine adapter at the cost of collecting all the rows from PySpark DataFrame.
                df = DataFrameConverter.to_pandas(df)
                self.delegate_adapter.merge(target_table, df, target_columns_to_types, unique_key, when_matched, merge_filter, source_columns, **kwargs)

                end_time = time.time()
                duration = end_time - start_time
                if duration > self.FALLBACK_WARNING_THRESHOLD_SECONDS:
                    logger.warning(
                        f"High-performance merge failed due to an error: {primary_error}. "
                        f"Fell back to the less efficient in-memory Pandas DataFrame method, which took {duration:.2f} seconds. "
                        "This may cause performance issues or out-of-memory errors for large datasets."
                    )
            except Exception as fallback_error:
                raise fallback_error from primary_error
            
    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        self.delegate_adapter._drop_object(name, exists, kind, cascade, **drop_args)
            
    def _create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool,
        warn_on_error: bool,
        properties: t.List[exp.Expression],
        kind: str,
    ) -> None:
        self.delegate_adapter._create_schema(schema_name, ignore_if_exists, warn_on_error, properties, kind)

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        return self.delegate_adapter.columns(table_name, include_pseudo_columns)

    def table_exists(self, table_name: TableName) -> bool:
        return self.delegate_adapter.table_exists(table_name)
    
    def _flatten_complex_types(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Iterates over a DataFrame's schema and converts complex types
        (arrays, structs, maps) into their JSON string representations.
        This makes the DataFrame compatible with generic JDBC sinks.

        NOTE: This method serves as a workaround for the current limitations of
        the Spark generic JDBC writer, which cannot map complex/nested Spark
        types to corresponding SQL types. If a future version of Spark provides
        native support for writing these types to JDBC targets (e.g., by
        automatically mapping them to JSON/JSONB columns), this entire method
        may become obsolete and can be removed.
        """
        # This list will hold the string expressions for selectExpr.
        select_expressions = []
        for field in df.schema:
            col_name = field.name
            col_type_str = field.dataType.typeName()

            if col_type_str in ('array', 'struct', 'map'):
                # Build a SQL expression string: 'to_json(col_name) AS col_name'
                # Quoting the column name with backticks makes it safe for special characters.
                select_expressions.append(f"to_json(`{col_name}`) AS `{col_name}`")
            else:
                # If it's a simple type, just select the column as is.
                select_expressions.append(f"`{col_name}`")
                
        # If no complex columns were found, there's no need to create a new DataFrame.
        if len(select_expressions) == len(df.columns) and all('to_json' not in expr for expr in select_expressions):
            logger.debug("No complex types found. Returning original DataFrame.")
            return df
        
        return df.selectExpr(*select_expressions)
    
    def get_qualified_table_name(self,
                                 table_name: t.Union[str, exp.Identifier],
                                 schema_name: t.Optional[t.Union[str, exp.Identifier]]=None,
                                 catalog: t.Optional[t.Union[str, exp.Identifier]]=None) -> str:
        try:
            return self.delegate_adapter.get_qualified_table_name(table_name, schema_name, catalog)
        except:
            return super().get_qualified_table_name(table_name, schema_name, catalog)