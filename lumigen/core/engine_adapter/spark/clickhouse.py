from __future__ import annotations
import typing as t

import logging
import time
from sqlglot import exp
from sqlmesh.core.engine_adapter.shared import (
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.utils import random_id, columns_to_types_all_known

if t.TYPE_CHECKING:
    from core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter.base import QueryOrDF
    from sqlmesh.utils.connection_pool import ConnectionPool

from utils.dataset import DataFrameConverter
from core.engine_adapter.spark.base import SparkEngineAdapter

logger = logging.getLogger(__name__)


class SparkClickHouseEngineAdapter(SparkEngineAdapter):
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    # A threshold in seconds for when to warn about a slow fallback.
    FALLBACK_WARNING_THRESHOLD_SECONDS: int = 5

    def __init__(
        self,
        connection_factory_or_pool: t.Union[t.Callable[[], t.Any], ConnectionPool],
        delegate_adapter: EngineAdapter,
        **kwargs: t.Any,
    ):
        """
        An adapter that uses Spark Catalog capabilities to read/write to databases,
        while delegating metadata and complex operations back to a standard EngineAdapter.
        
        Args:
            connection_factory_or_pool: The factory for creating Spark connections.
            delegate_adapter: The standard EngineAdapter for the target database (e.g., PostgresEngineAdapter).
        """
        super(SparkClickHouseEngineAdapter, self).__init__(connection_factory_or_pool, **kwargs)
        self.delegate_adapter = delegate_adapter

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)

        schema = None
        target_columns_to_types_known = target_columns_to_types and columns_to_types_all_known(
            target_columns_to_types
        )
        if (
            column_descriptions
            and target_columns_to_types_known
            and self.COMMENT_CREATION_TABLE.is_in_schema_def_ctas
            and self.comments_enabled
        ):
            schema = self._build_schema_exp(table, target_columns_to_types, column_descriptions)  # type: ignore

        with self.transaction(condition=len(source_queries) > 1):
            for i, source_query in enumerate(source_queries):
                with source_query as query:
                    if target_columns_to_types and target_columns_to_types_known:
                        query = self._order_projections_and_filter(
                            query, target_columns_to_types, coerce_types=True
                        )
                    if i == 0:
                        self.create_table(
                            table,
                            target_columns_to_types,
                            exists=exists,
                            table_description=table_description,
                            column_descriptions=column_descriptions,
                            **kwargs,
                        )
                    self._insert_append_query(
                        table_name,
                        query,
                        target_columns_to_types or self.columns(table),
                        track_rows_processed=track_rows_processed,
                    )

        # Register comments with commands if the engine supports comments and we weren't able to
        # register them with the CTAS call's schema expression.
        if (
            table_description
            and self.COMMENT_CREATION_TABLE.is_comment_command_only
            and self.comments_enabled
        ):
            self._create_table_comment(table_name, table_description)
        if column_descriptions and schema is None and self.comments_enabled:
            self._create_column_comments(table_name, column_descriptions)

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

        try:
            # --- PRIMARY STRATEGY: Staging Table ---
            target_table_exp = exp.to_table(target_table)

            # Create a unique temp table name
            temp_table_name = f"__temp_lumigen_{target_table_exp.name}_{random_id(short=True)}"
            temp_table_exp = exp.to_table(temp_table_name, db=target_table_exp.args.get("db"), catalog=target_table_exp.args.get("catalog"))
            try:
                # Stage the data frame into a temp table in the target DB
                self.replace_query(temp_table_exp, df, target_columns_to_types=target_columns_to_types, source_columns=source_columns)

                # Delegate MERGE operation to corresponding engine adapter, using the temp table as the source
                target_table_wo_catalog_exp = target_table_exp.copy()
                target_table_wo_catalog_exp.set("catalog", None)
                temp_table_wo_catalog_exp = temp_table_exp.copy()
                temp_table_wo_catalog_exp.set("catalog", None)
                self.delegate_adapter.merge(
                    target_table_wo_catalog_exp,
                    exp.select("*").from_(temp_table_wo_catalog_exp),
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
                target_table_wo_catalog_exp = target_table_exp.copy()
                target_table_wo_catalog_exp.set("catalog", None)
                self.delegate_adapter.merge(target_table_wo_catalog_exp, df, target_columns_to_types, unique_key, when_matched, merge_filter, source_columns, **kwargs)

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
            
    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        # FixedString data type family must have exactly one argument - size in bytes.
        def _is_fixed_string_wo_param(dtype: exp.DataType):
            if dtype.is_type(exp.DataType.Type.BINARY):
                return not any(isinstance(e, exp.DataTypeParam) for e in dtype.expressions)
        # Use String instead
        if isinstance(table_name_or_schema, exp.Schema):
            coldef: exp.ColumnDef
            for coldef in table_name_or_schema.expressions:
                if _is_fixed_string_wo_param(coldef.kind):
                    coldef.set("kind", exp.DataType.build("String"))
        if expression:
            for node in expression.find_all(exp.Cast):
                if _is_fixed_string_wo_param(node.to):
                    node.set("to", exp.DataType.build("String"))
        if target_columns_to_types:
            for col in target_columns_to_types:
                if _is_fixed_string_wo_param(target_columns_to_types[col]):
                    target_columns_to_types[col] = exp.DataType.build("String")

        if kwargs.get("partitioned_by"):
            partition_cols = [
                col.name
                for part_expr in kwargs["partitioned_by"]
                for col in part_expr.find_all(exp.Column)
            ]
            if isinstance(table_name_or_schema, exp.Schema):
                for coldef in table_name_or_schema.expressions:
                    if coldef.name in partition_cols:
                        coldef.kind.set("nullable", False)
            if target_columns_to_types:
                for col in partition_cols:
                    target_columns_to_types[col].set("nullable", False)

        super()._create_table(
            table_name_or_schema,
            expression,
            exists,
            replace,
            target_columns_to_types,
            table_description,
            column_descriptions,
            table_kind,
            empty_ctas=(self.engine_run_mode.is_cloud and expression is not None),
            track_rows_processed=track_rows_processed,
            table_properties={
                "engine": exp.Literal(this="MergeTree()", is_string=True),
                "order_by": exp.Literal(this=exp.Tuple(expressions=[]), is_string=True),
            },
            **kwargs,
        )

        # # execute the second INSERT step if on cloud and creating a table
        # # - Additional clause is to avoid clickhouse-connect HTTP client bug where CTAS LIMIT 0
        # #     returns a success code but malformed response
        if (
            self.engine_run_mode.is_cloud
            and table_kind != "VIEW"
            and expression
            and not (
                expression.args.get("limit") is not None
                and expression.args["limit"].expression.this == "0"
            )
        ):
            table_name = (
                table_name_or_schema.this
                if isinstance(table_name_or_schema, exp.Schema)
                else table_name_or_schema
            )
            self._insert_append_query(
                table_name,
                expression,  # type: ignore
                target_columns_to_types or self.columns(table_name),
            )