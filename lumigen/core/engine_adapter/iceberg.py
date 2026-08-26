"""Contains IcebergEngineAdapter."""

from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlglot.dialects.dialect import Dialects
from sqlmesh.core.dialect import add_table, to_schema
from sqlmesh.core.engine_adapter.base import MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh.core.engine_adapter.mixins import (
    HiveMetastoreTablePropertiesMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    InsertOverwriteStrategy,
)

if t.TYPE_CHECKING:
    from core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF, DF
    from engines.iceberg.client import IcebergConnection

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin
from utils.dataset import columns_to_types_from_df


class IcebergEngineAdapter(
    HiveMetastoreTablePropertiesMixin,
    RowDiffMixin,
    FetchBatchMixin,
    EngineAdapterMixin,
):
    DIALECT: str = "iceberg"
    SUPPORTS_TRANSACTIONS = True
    SUPPORTS_INDEXES = False
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    SUPPORTS_CREATE_DROP_CATALOG = False
    SUPPORTED_DROP_CASCADE_OBJECT_KINDS = []
    SUPPORTS_REPLACE_TABLE = True
    SUPPORTS_QUERY_EXECUTION_TRACKING = True
    DEFAULT_CATALOG_TYPE = "rest"
    
    def table_exists(self, table_name: TableName) -> bool:
        table = exp.to_table(table_name)
        conn: IcebergConnection = self.connection
        return conn.table_exists(table.alias_or_name, table.db)
    
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

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            target_columns_to_types: Optional a mapping between the column name and its data type.
            kwargs: Optional create table properties.
        """
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)):
            super().replace_query(table_name, query_or_df, target_columns_to_types, table_description, column_descriptions, source_columns, supports_replace_table_override, **kwargs)
            return
        
        target_table = exp.to_table(table_name)
        target_columns_to_types = target_columns_to_types or columns_to_types_from_df(query_or_df)
        target_columns_to_types = target_columns_to_types or self.columns(target_table)
        
        table_exists = self.table_exists(table_name)
        if not table_exists:
            self._create_table_from_columns(
                table_name,
                target_columns_to_types,
                table_description=table_description,
                column_descriptions=column_descriptions,
            )

        insert_exp = exp.Insert(this=target_table, overwrite=True)
        self.execute(insert_exp, track_rows_processed=bool(kwargs.get("track_rows_processed")), df=query_or_df)

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        track_rows_processed: bool = True,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)):
            super().insert_append(table_name, query_or_df, target_columns_to_types, track_rows_processed, source_columns)
            return
        
        target_table = exp.to_table(table_name)
        target_columns_to_types = target_columns_to_types or columns_to_types_from_df(query_or_df)
        target_columns_to_types = target_columns_to_types or self.columns(table_name)

        table_exists = self.table_exists(table_name)
        if not table_exists:
            self._create_table_from_columns(table_name, target_columns_to_types)

        insert_exp = exp.Insert(this=target_table)
        self.execute(insert_exp, track_rows_processed=track_rows_processed, df=query_or_df)

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries_or_df: t.Union[t.List[SourceQuery], DF],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)
        insert_overwrite_strategy = (
            insert_overwrite_strategy_override or self.INSERT_OVERWRITE_STRATEGY
        )
        if (not insert_overwrite_strategy.is_insert_overwrite) and isinstance(source_queries_or_df, list):
            self._insert_overwrite_by_condition(table_name, source_queries_or_df, target_columns_to_types, where, insert_overwrite_strategy_override, **kwargs)
            return

        with self.transaction(
            condition=len(source_queries_or_df) > 0
        ):
            target_columns_to_types = target_columns_to_types or self.columns(table_name)
            if where:
                table.set("partition", exp.Partition(expressions=[where.sql(self.dialect)]))

            if isinstance(source_queries_or_df, list):
                for i, source_query in enumerate(source_queries_or_df):
                    with source_query as query:
                        query = self._order_projections_and_filter(
                            query, target_columns_to_types, where=where
                        )
                        if i > 0:
                            self._insert_append_query(
                                table_name,
                                query,
                                target_columns_to_types=target_columns_to_types,
                                order_projections=False,
                            )
                        else:
                            insert_exp = exp.insert(
                                query,
                                table,
                                columns=(
                                    list(target_columns_to_types)
                                ),
                                overwrite=True,
                            )
                            self.execute(insert_exp, track_rows_processed=True)
            else:
                insert_exp = exp.Insert(this=table, overwrite=True)
                self.execute(insert_exp, track_rows_processed=True, df=source_queries_or_df)
    
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
        if isinstance(source_table, (exp.Query, exp.DerivedTable)):
            super().merge(target_table, source_table, target_columns_to_types, unique_key, when_matched, merge_filter, source_columns, **kwargs)
            return

        target_columns_to_types = target_columns_to_types or columns_to_types_from_df(source_table)
        target_columns_to_types = target_columns_to_types or self.columns(target_table)
        
        on = exp.and_(
            *(
                add_table(part, MERGE_TARGET_ALIAS).eq(add_table(part, MERGE_SOURCE_ALIAS))
                for part in unique_key
            )
        )
        if merge_filter:
            on = exp.and_(merge_filter, on)

        if not when_matched:
            match_expressions = [
                exp.When(
                    matched=True,
                    source=False,
                    then=exp.Update(
                        expressions=[
                            exp.column(col, MERGE_TARGET_ALIAS).eq(
                                exp.column(col, MERGE_SOURCE_ALIAS)
                            )
                            for col in target_columns_to_types
                        ],
                    ),
                )
            ]
        else:
            match_expressions = when_matched.copy().expressions

        match_expressions.append(
            exp.When(
                matched=False,
                source=False,
                then=exp.Insert(
                    this=exp.Tuple(
                        expressions=[exp.column(col) for col in target_columns_to_types]
                    ),
                    expression=exp.Tuple(
                        expressions=[
                            exp.column(col, MERGE_SOURCE_ALIAS) for col in target_columns_to_types
                        ]
                    ),
                ),
            )
        )

        self._merge(target_table, source_table, on, whens=exp.Whens(expressions=match_expressions))

    def _merge(
        self,
        target_table: TableName,
        query: QueryOrDF,
        on: exp.Expression,
        whens: exp.Whens,
    ) -> None:
        if isinstance(query, (exp.Query, exp.DerivedTable)):
            super()._merge(target_table, query, on, whens)
            return
        
        this = exp.alias_(exp.to_table(target_table), alias=MERGE_TARGET_ALIAS, table=True)
        using = exp.table_(MERGE_SOURCE_ALIAS)
        self.execute(
            exp.Merge(this=this, using=using, on=on, whens=whens), track_rows_processed=True, df=query,
        )
    
    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema.
        """
        show_kwargs = {
            "this": "TABLES",
            "db": to_schema(schema_name).db,
        }
        if object_names:
            show_kwargs["where"] = exp.Where(this=exp.column("table_name").isin(*object_names))
        query = exp.Show(**show_kwargs)
        df = self.fetchdf(query.sql(Dialects.MYSQL))
        return [
            DataObject(
                schema=row.table_schema,
                name=row.table_name,
                type=DataObjectType.from_str(row.table_type),  # type: ignore
            )
            for row in df.itertuples()
        ]
    
    def get_all_data_objects(self, keyword: str="", limit: t.Optional[int]=None, *args) -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            schema_pattern = f"%{schema}" if schema else None
            table_pattern = f"{table}%" if table else None
        else:
            schema_pattern = None
            table_pattern = f"%{keyword}%" if keyword else None

        show_kwargs = {
            "this": "TABLES",
        }
        if table_pattern:
            show_kwargs["like"] = exp.Literal.string(table_pattern)
        if schema_pattern:
            show_kwargs["where"] = exp.Where(this=exp.column("table_schema").ilike(schema_pattern))
        query = exp.Show(**show_kwargs)
        df = self.fetchdf(query.sql(Dialects.MYSQL))
        if limit is not None:
            df = df.head(limit)
        return [
            DataObject(
                schema=row.table_schema,
                name=row.table_name,
                type=DataObjectType.from_str(row.table_type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def ping(self) -> None:
        try:
            self.connection.ping()
        finally:
            self._connection_pool.close_cursor()