"""Contains PostgresEngineAdapter."""

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list
import sqlmesh
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin


class PostgresEngineAdapter(
    sqlmesh.core.engine_adapter.postgres.PostgresEngineAdapter,
    EngineAdapterMixin,
    FetchBatchMixin,
):
    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        **kwargs: t.Any,
    ) -> None:
        class JsonbObject(exp.Map):
            pass

        for e in ensure_list(expressions):
            if isinstance(e, exp.Expression):
                # The `MAP` function is not part of PostgreSQL's SQL functions, use `JSONB_OBJECT` instead.
                for node in e.find_all(exp.Map):
                    node.replace(JsonbObject(keys=node.args.get("keys"), values=node.args.get("values")))

                # Cast to array will not work
                for node in e.find_all(exp.Cast):
                    if node.is_type(exp.DataType.Type.ARRAY):
                        node.replace(node.this)

        super().execute(expressions, ignore_unsupported_errors, quote_identifiers, **kwargs)

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

        catalog = self.get_current_catalog()
        table_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("tablename").as_("name"),
            exp.Literal.string("TABLE").as_("type"),
        ).from_("pg_tables")
        view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("viewname").as_("name"),
            exp.Literal.string("VIEW").as_("type"),
        ).from_("pg_views")
        materialized_view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("matviewname").as_("name"),
            exp.Literal.string("MATERIALIZED_VIEW").as_("type"),
        ).from_("pg_matviews")
        subquery = exp.union(
            table_query,
            exp.union(view_query, materialized_view_query, distinct=False),
            distinct=False,
        )
        query = (
            exp.select("*")
            .from_(subquery.subquery(alias="objs"))
        )
        if table_pattern:
            query = query.where(exp.column("name").ilike(table_pattern))
        if schema_pattern:
            query = query.where(exp.column("schema_name").ilike(schema_pattern))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]