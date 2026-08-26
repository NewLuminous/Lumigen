"""Contains ClickHouseEngineAdapter."""
from __future__ import annotations
import typing as t

from sqlglot import exp
from sqlmesh.core.engine_adapter.clickhouse import ClickhouseEngineAdapter as SQLMeshClickhouseEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    SourceQuery,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin
from utils.dataset import columns_to_types_from_df


class ClickHouseEngineAdapter(
    FetchBatchMixin,
    EngineAdapterMixin,
    SQLMeshClickhouseEngineAdapter,
):
    def _df_to_source_queries(
        self,
        df: DF,
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> t.List[SourceQuery]:
        # Fix issue where clickhouse-connect tries to encode string column when writing
        # but it does not work with bytearray.
        df_columns_to_types = columns_to_types_from_df(df)
        for col in df_columns_to_types:
            if df_columns_to_types[col].is_type(exp.DataType.Type.BINARY):
                df[col] = df[col].map(bytes, na_action="ignore")

        return super()._df_to_source_queries(df, target_columns_to_types, batch_size, target_table, source_columns=source_columns, **kwargs)

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

        super()._create_table(
            table_name_or_schema,
            expression,
            exists,
            replace,
            target_columns_to_types,
            table_description,
            column_descriptions,
            table_kind,
            track_rows_processed=track_rows_processed,
            **kwargs,
        )

    def get_all_data_objects(self, keyword: str="", limit: t.Optional[int]=None, exclude_unknown: bool=False, *args) -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            schema_pattern = f"%{schema}" if schema else None
            table_pattern = f"{table}%" if table else None
        else:
            schema_pattern = None
            table_pattern = f"%{keyword}%" if keyword else None

        query = (
            exp.select(
                exp.column("database").as_("schema_name"),
                exp.column("name"),
                exp.case(exp.column("engine"))
                .when(
                    exp.Literal.string("View"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.Literal.string("MaterializedView"),
                    exp.Literal.string("materialized_view"),
                )
                .else_(
                    exp.Literal.string("table"),
                )
                .as_("type"),
            )
            .from_("system.tables")
        )
        if table_pattern:
            query = query.where(exp.column("name").ilike(table_pattern))
        if schema_pattern:
            query = query.where(exp.column("database").ilike(schema_pattern))
        if exclude_unknown:
            query = query.where(
                exp.column("engine").isin("View", "MaterializedView")
                | (~exp.column("engine").isin("Dictionary", "Buffer"))
            )
        if limit is not None:
            query = query.limit(limit)
        df = self.fetchdf(query)
        return [
            DataObject(
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]