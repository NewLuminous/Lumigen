"""Contains PostgresEngineAdapter."""

import typing as t
import re

from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.dialects.postgres import Postgres
from sqlmesh.core.engine_adapter.postgres import PostgresEngineAdapter as SQLMeshPostgresEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin


class PostgresEngineAdapter(
    FetchBatchMixin,
    EngineAdapterMixin,
    SQLMeshPostgresEngineAdapter,
):
    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        track_rows_processed: bool = False,
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

        super().execute(expressions, ignore_unsupported_errors, quote_identifiers, track_rows_processed, **kwargs)

    def identifier_needs_quoting(self, identifier: t.Union[str, exp.Identifier]) -> bool:
        if isinstance(identifier, exp.Identifier):
            identifier = identifier.sql(self.dialect)

        if not re.match(r"^[a-z_][a-z0-9_$]*$", identifier):
            return True

        if identifier.upper() in Postgres.Tokenizer.KEYWORDS:
            return True
        
        return False

    def get_all_data_objects(
        self, keyword: str="", limit: t.Optional[int]=None,
        object_types: t.Optional[t.Collection[t.Union[str, DataObjectType]]] = None, *args,
    ) -> t.List[DataObject]:
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

        if object_types is not None:
            if not object_types:
                return []
            has_unknown = False
            included_known_types = set()
            for ot in object_types:
                dot = ot if isinstance(ot, DataObjectType) else DataObjectType.from_str(str(ot))
                if dot.is_unknown:
                    has_unknown = True
                else:
                    included_known_types.add(dot.name)
            if has_unknown:
                all_known_names = {dt.name for dt in DataObjectType if not dt.is_unknown}
                excluded_known_types = all_known_names - included_known_types
                if excluded_known_types:
                    query = query.where(exp.column("type").isin(*sorted(list(excluded_known_types))).not_())
            else:
                if not included_known_types:
                    return []
                if included_known_types:
                    query = query.where(exp.column("type").isin(*sorted(list(included_known_types))))

        if limit is not None:
            query = query.limit(limit)
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