"""Contains MySQLEngineAdapter."""

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter as SQLMeshMySQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin


class MySQLEngineAdapter(
    FetchBatchMixin,
    EngineAdapterMixin,
    SQLMeshMySQLEngineAdapter,
):
    """
    Temporary fix:
        sqlglot.expressions.convert v25.32.1 cannot convert SET Type.
    """
    def _values_to_sql(
        self,
        values: t.List[t.Tuple[t.Any, ...]],
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
        source_columns: t.Optional[t.List[str]] = None,
    ):
        set_dtype_columns = set()
        nonset_dtype_columns = None
        for i in range(len(values)):
            value = values[i]
            if nonset_dtype_columns is None:
                nonset_dtype_columns = set(range(len(value)))
            for idx in nonset_dtype_columns.copy():
                if value[idx] is None:
                    continue
                if isinstance(value[idx], set):
                    set_dtype_columns.add(idx)
                else:
                    nonset_dtype_columns.remove(idx)
            converted_value = list(value)
            for idx in set_dtype_columns:
                converted_value[idx] = ','.join(value[idx])
            values[i] = tuple(converted_value)
        return super()._values_to_sql(values, target_columns_to_types, batch_start, batch_end, alias, source_columns)
    
    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        track_rows_processed: bool = False,
        **kwargs: t.Any,
    ) -> None:
        for e in ensure_list(expressions):
            if isinstance(e, exp.Expression):
                for node in e.find_all(exp.Cast):
                    # Cast to VARBINARY/BLOB will not work, use BINARY instead
                    need_convert_to_binary = False
                    if node.is_type(exp.DataType.Type.VARBINARY):
                        need_convert_to_binary = True
                    try:
                        if node.is_type(exp.DataType.Type.BLOB):
                            need_convert_to_binary = True
                    except:
                        pass
                    if need_convert_to_binary:
                        node.set("to", exp.DataType.build("binary"))

                    # Cast to BIT will not work
                    if node.is_type(exp.DataType.Type.BIT):
                        node.replace(node.this)

        super().execute(expressions, ignore_unsupported_errors, quote_identifiers, track_rows_processed, **kwargs)

    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
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
                exp.column("table_name").as_("name"),
                exp.column("table_schema").as_("schema_name"),
                exp.case()
                .when(
                    exp.column("table_type").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .when(
                    exp.column("table_type").eq("VIEW"),
                    exp.Literal.string("view"),
                )
                .else_("table_type")
                .as_("type"),
            )
            .from_(exp.table_("tables", db="information_schema"))
        )
        if table_pattern:
            query = query.where(exp.column("table_name").ilike(table_pattern))
        if schema_pattern:
            query = query.where(exp.column("table_schema").ilike(schema_pattern))
        df = self.fetchdf(query)
        return [
            DataObject(
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]