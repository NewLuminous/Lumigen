"""Contains MSSQLEngineAdapter."""

from __future__ import annotations
import typing as t

from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter as SQLMeshMSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import QueryOrDF

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin


class MSSQLEngineAdapter(
    FetchBatchMixin,
    EngineAdapterMixin,
    SQLMeshMSSQLEngineAdapter,
):
    def _convert_df_datetime(self, df, columns_to_types: t.Dict[str, exp.DataType]) -> None:
        super()._convert_df_datetime(df, columns_to_types)

        for column, kind in columns_to_types.items():
            # pymssql.Connection.bulk_copy() doesn't work with nvarchar, so load into varbinary column and cast to nvarchar in SELECT statement.
            # Reference: https://github.com/pymssql/pymssql/issues/725#issuecomment-2067618289
            if kind.is_type(exp.DataType.Type.NVARCHAR):
                df[column] = df[column].apply(lambda s: s.encode("UTF-16LE") if isinstance(s, str) else s)
                columns_to_types[column] = exp.DataType.build("VARBINARY(MAX)")

            # Use varbinary intead of binary for temporary table.
            if kind.is_type(exp.DataType.Type.BINARY):
                columns_to_types[column] = exp.DataType.build("VARBINARY(MAX)")

    def _columns_to_types(
        self,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> t.Tuple[t.Optional[t.Dict[str, exp.DataType]], t.Optional[t.List[str]]]:
        """
        Ensures all inferred string/text columns are mapped to NVARCHAR(MAX) for MSSQL.
        This ensures newly created tables support full Unicode.
        """
        target_columns_to_types, source_columns = super()._columns_to_types(
            query_or_df, target_columns_to_types, source_columns
        )

        if target_columns_to_types:
            target_columns_to_types = {
                col: (
                    exp.DataType.build("NVARCHAR(MAX)", dialect=self.dialect)
                    if kind.is_type("text", "varchar") and not kind.is_type("nvarchar")
                    else kind
                )
                for col, kind in target_columns_to_types.items()
            }
        return target_columns_to_types, source_columns

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

        catalog = self.get_current_catalog()
        query = (
            exp.select(
                exp.column("TABLE_NAME").as_("name"),
                exp.column("TABLE_SCHEMA").as_("schema_name"),
                exp.case()
                .when(exp.column("TABLE_TYPE").eq("BASE TABLE"), exp.Literal.string("TABLE"))
                .else_(exp.column("TABLE_TYPE"))
                .as_("type"),
            )
            .from_(exp.table_("TABLES", db="INFORMATION_SCHEMA"))
        )
        if table_pattern:
            query = query.where(exp.column("TABLE_NAME").like(table_pattern))
        if schema_pattern:
            query = query.where(exp.column("TABLE_SCHEMA").like(schema_pattern))
        if exclude_unknown:
            query = query.where(
                exp.column("TABLE_TYPE").isin("BASE TABLE", "VIEW")
            )
        if limit is not None:
            query = query.limit(limit)
        dataframe = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in dataframe.itertuples()
        ]