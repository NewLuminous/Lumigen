"""Contains OracleEngineAdapter."""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.errors import ParseError
import numpy as np
import pandas as pd

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh.core.engine_adapter.mixins import (
    InsertOverwriteWithMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    InsertOverwriteStrategy,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin
from utils.dataset import columns_to_types_from_df, DataFrameConverter


@set_catalog()
class OracleEngineAdapter(
    InsertOverwriteWithMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    FetchBatchMixin,
    RowDiffMixin,
    EngineAdapterWithIndexSupport,
    EngineAdapterMixin,
):
    DIALECT: str = "oracle"
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    MAX_TABLE_COMMENT_LENGTH = 4000
    MAX_COLUMN_COMMENT_LENGTH = 4000
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    SUPPORTS_REPLACE_TABLE = False
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            exp.DataType.build("VARCHAR2", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NVARCHAR2", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NUMBER", dialect=DIALECT).this: [(38, 127), (0,)],
            exp.DataType.build("FLOAT", dialect=DIALECT).this: [(126,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(6,)],
            #exp.DataType.build("RAW", dialect=DIALECT).this: [(2000,)],
            #exp.DataType.build("UROWID", dialect=DIALECT).this: [(4000,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NCHAR", dialect=DIALECT).this: [(1,)],
        },
        max_parameter_length={
            exp.DataType.build("VARCHAR2", dialect=DIALECT).this: 4000,
            exp.DataType.build("NVARCHAR2", dialect=DIALECT).this: 4000,
            #exp.DataType.build("RAW", dialect=DIALECT).this: 2000,
            #exp.DataType.build("UROWID", dialect=DIALECT).this: 4000,
            exp.DataType.build("CHAR", dialect=DIALECT).this: 2000,
            exp.DataType.build("NCHAR", dialect=DIALECT).this: 2000,
        },
        coerceable_types={
            exp.DataType.build("TIMESTAMP(6)", dialect=DIALECT): {
                exp.DataType.build("TIMESTAMP", dialect=DIALECT)
            }
        },
    )
    VARIABLE_LENGTH_DATA_TYPES = {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}

    @classmethod
    def _casted_columns(cls, columns_to_types: t.Dict[str, exp.DataType]) -> t.List[exp.Alias]:
        return [
            exp.alias_(
                exp.cast(exp.null(), to=kind if not kind.is_type(exp.DataType.Type.TEXT) else "VARCHAR2(4000)"),
                column, copy=False
            )
            for column, kind in columns_to_types.items()
        ]

    def alter_table(
        self,
        alter_expressions: t.List[exp.Alter],
    ) -> None:
        with self.transaction():
            for alter_expression in alter_expressions:
                for alter_column_exp in alter_expression.find_all(exp.AlterColumn):
                    dtype_exp = alter_column_exp.args.get("dtype", False)
                    if dtype_exp is not False:
                        column_name = self._to_sql(alter_column_exp.this, quote=True)
                        dtype = self._to_sql(dtype_exp, quote=True)
                        alter_column_exp.replace(f"MODIFY {column_name} {dtype}")
                self.execute(alter_expression)

    def columns(
        self,
        table_name: TableName,
        include_pseudo_columns: bool = True,
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""

        table = exp.to_table(table_name)

        sql = (
            exp.select(
                "column_name",
                "data_type",
                "data_length",
                "data_precision",
                "data_scale",
            )
            .from_("all_tab_columns")
            .where(f"table_name = '{table.name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"owner = '{database_name}'")

        columns_raw = self.fetchall(sql, quote_identifiers=False)

        def build_var_length_col(
            column_name: str,
            data_type: str,
            data_length: t.Optional[int] = None,
            data_precision: t.Optional[int] = None,
            data_scale: t.Optional[int] = None,
        ) -> tuple:
            data_type = data_type.upper()
            if (
                data_type in self.VARIABLE_LENGTH_DATA_TYPES
                and data_length is not None
                and data_length > 0
            ):
                return (column_name, f"{data_type}({data_length})")
            if data_type in ["NUMBER", "FLOAT"]:
                dtype_with_params = f"{data_type}"
                if data_precision is not None:
                    dtype_with_params += f"({data_precision}"
                    if data_scale is not None:
                        dtype_with_params += f", {data_scale}"
                    dtype_with_params += ')'
                return (column_name, dtype_with_params)

            return (column_name, data_type)

        columns = [build_var_length_col(*row) for row in columns_raw]
        column_dtype_mapping = {}
        for column_name, data_type in columns:
            try:
                column_dtype = exp.DataType.build(data_type, dialect=self.dialect)
            except ParseError:
                column_dtype = exp.DataType.build(data_type, dialect=self.dialect, udt=True)
            column_dtype_mapping[column_name] = column_dtype

        return column_dtype_mapping
    
    def table_exists(self, table_name: TableName) -> bool:
        table = exp.to_table(table_name)

        sql = (
            exp.select("1")
            .from_("all_tab_columns")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"owner = '{database_name}'")

        result = self.fetchone(sql, quote_identifiers=False)

        return result[0] == 1 if result is not None else False
    
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

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            columns_to_types: Optional a mapping between the column name and its data type.
            kwargs: Optional create table properties.
        """
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)): # TODO
            super().replace_query(table_name, query_or_df, columns_to_types, table_description, column_descriptions, **kwargs)
            return
        
        target_table = exp.to_table(table_name)
        #target_table = f"{target_table.sql(dialect=self.dialect, identify=True)}"
        columns_to_types = columns_to_types or columns_to_types_from_df(query_or_df)
        columns_to_types = columns_to_types or self.columns(target_table)
        table_exists = self.table_exists(target_table)
        if not table_exists:
            casted_columns = self._casted_columns(columns_to_types)
            select_schema_exp = exp.select(*casted_columns).from_(exp.to_table("DUAL"), copy=False).where(exp.condition("0=1"), copy=False)
            self._create_table(target_table, expression=select_schema_exp, exists=False, replace=False)

        with self.transaction():
            if table_exists:
                self._truncate_table(table_name)
            self.insert_append(target_table, query_or_df, columns_to_types=columns_to_types)

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(table_name, (exp.Query, exp.DerivedTable)): # TODO
            super().insert_append(table_name, query_or_df, columns_to_types)
            return
        
        target_table = exp.to_table(table_name)
        columns_to_types = columns_to_types or columns_to_types_from_df(query_or_df)
        columns_to_types = columns_to_types or self.columns(table_name)
        placeholder_map = self._build_placeholder_map(columns_to_types)

        table_exists = self.table_exists(target_table)
        if not table_exists:
            casted_columns = self._casted_columns(columns_to_types)
            select_schema_exp = exp.select(*casted_columns).from_(exp.to_table("DUAL"), copy=False).where(exp.condition("0=1"), copy=False)
            self._create_table(target_table, expression=select_schema_exp, exists=False, replace=False)

        values_exp = exp.values([tuple(exp.Placeholder(this=placeholder_map[col]) for col in columns_to_types)])
        insert_exp = exp.insert(values_exp, table_name, columns=list(columns_to_types))
        t.cast("pd.DataFrame", query_or_df).rename(columns=placeholder_map, inplace=True)
        parameters = DataFrameConverter.to_records(query_or_df)
        self.execute(insert_exp, parameters=parameters)

    def update_table(
        self,
        table_name: TableName,
        properties: t.Dict[str, t.Any],
        where: t.Optional[str | exp.Condition] = None,
    ) -> None:
        placeholder_map = self._build_placeholder_map(properties)
        parameters = {placeholder_map[col]: value for col, value in properties.items()}
        bind_properties = {col: exp.Placeholder(this=placeholder_map[col]) for col in properties}
        self.execute(exp.update(table_name, bind_properties, where=where), parameters=parameters)

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
    ) -> None:
        if isinstance(source_table, (exp.Query, exp.DerivedTable)): # TODO
            return super().merge(
                target_table, source_table,
                columns_to_types=columns_to_types,
                unique_key=unique_key,
                when_matched=when_matched,
                merge_filter=merge_filter
            )
        
        columns_to_types = columns_to_types or columns_to_types_from_df(source_table)
        columns_to_types = columns_to_types or self.columns(target_table)
        placeholder_map = self._build_placeholder_map(columns_to_types)
        on = exp.and_(
            *(
                part.eq(exp.Placeholder(this=placeholder_map[part.sql()]))
                for part in unique_key
            )
        )
        if merge_filter:
            on = exp.and_(merge_filter, on)
        on = exp.Paren(this=on)

        if not when_matched:
            when_matched = exp.Whens()
            when_matched.append(
                "expressions",
                exp.When(
                    matched=True,
                    source=False,
                    then=exp.Update(
                        expressions=[
                            exp.column(col).eq(exp.Placeholder(this=placeholder_map[col]))
                            for col in columns_to_types if exp.column(col) not in unique_key
                        ],
                    ),
                ),
            )
        else:
            for node in when_matched.find_all(exp.Column):
                if node.table == MERGE_TARGET_ALIAS:
                    node.replace(exp.column(node.this))
                elif node.table == MERGE_SOURCE_ALIAS:
                    node.replace(exp.Placeholder(this=placeholder_map[node.this.sql()]))

            for when_exp in when_matched.find_all(exp.When):
                condition = when_exp.args.get("condition", False)
                when_exp.args["condition"] = False
                if condition is not False:
                    if when_exp.args["matched"]:
                        for update_exp in when_exp.find_all(exp.Update):
                            update_exp.replace(self._to_sql(update_exp.where(condition), quote=True))
                    else:
                        for insert_exp in when_exp.find_all(exp.Insert):
                            oracle_when_exp = self._to_sql(exp.When(matched=False, then=insert_exp), quote=True)
                            oracle_when_exp += " " + self._to_sql(exp.Where(this=condition), quote=True)
                            when_exp.replace(oracle_when_exp)

        when_not_matched = exp.When(
            matched=False,
            source=False,
            then=exp.Insert(
                this=exp.Tuple(expressions=[exp.column(col) for col in columns_to_types]),
                expression=exp.Tuple(
                    expressions=[exp.Placeholder(this=placeholder_map[col]) for col in columns_to_types]
                ),
            ),
        )
        when_matched.append("expressions", when_not_matched)

        merge_exp = exp.Merge(this=exp.to_table(target_table), using="DUAL", on=on, whens=when_matched)
        t.cast("pd.DataFrame", source_table).rename(columns=placeholder_map, inplace=True)
        parameters = DataFrameConverter.to_records(source_table)
        self.execute(merge_exp, parameters=parameters)

    def _execute(self, sql: str, parameters: t.Optional[t.List[t.Any]] = None, **kwargs: t.Any) -> None:
        if parameters:
            if isinstance(parameters, list):
                self.cursor.executemany(sql, parameters)
            else:
                self.cursor.execute(sql, parameters)
        else:
            self.cursor.execute(sql)
    
    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema.
        """
        query = (
            exp.select(
                exp.column("object_name").as_("name"),
                exp.column("owner").as_("schema_name"),
                exp.column("object_type").as_("type"),
            )
            .from_(exp.table_("all_objects"))
            .where(exp.column("owner").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("object_name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                schema=row.SCHEMA_NAME,
                name=row.NAME,
                type=DataObjectType.from_str(row.TYPE.replace(' ', '_')),  # type: ignore
            )
            for row in df.itertuples()
        ]
    
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
                exp.column("object_name").as_("name"),
                exp.column("owner").as_("schema_name"),
                exp.column("object_type").as_("type"),
            )
            .from_(exp.table_("all_objects"))
        )
        if table_pattern:
            query = query.where(exp.column("object_name").ilike(table_pattern))
        if schema_pattern:
            query = query.where(exp.column("owner").ilike(schema_pattern))
        df = self.fetchdf(query)
        return [
            DataObject(
                schema=row.SCHEMA_NAME,
                name=row.NAME,
                type=DataObjectType.from_str(row.TYPE.replace(' ', '_')),  # type: ignore
            )
            for row in df.itertuples()
        ]
    
    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        super()._insert_overwrite_by_condition(table_name, source_queries, columns_to_types, where or exp.condition("1=1"), insert_overwrite_strategy_override, **kwargs)

    def ping(self) -> None:
        try:
            self.connection.ping()
        finally:
            self._connection_pool.close_cursor()

    def _build_placeholder_map(self, column_dict: t.Dict[str, t.Any]) -> t.Dict[str, str]:
        unused_placeholder = []
        used_placeholder = set()
        for i in range(len(column_dict)):
            placeholder = f"c{i+1}"
            if placeholder in column_dict:
                used_placeholder.add(placeholder)
            else:
                unused_placeholder.append(placeholder)
        
        placeholder_map = {}
        i = 0
        for col in column_dict:
            if col in used_placeholder:
                placeholder_map[col] = col
            else:
                placeholder_map[col] = unused_placeholder[i]
                i += 1
        return placeholder_map