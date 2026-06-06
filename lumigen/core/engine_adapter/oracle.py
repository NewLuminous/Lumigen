"""Contains OracleEngineAdapter."""

from __future__ import annotations
import typing as t

import re
from sqlglot import exp
from sqlglot.dialects.oracle import Oracle
from sqlglot.errors import ParseError
import pandas as pd

from sqlmesh.core.dialect import add_table, to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh.core.engine_adapter.mixins import (
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
)

if t.TYPE_CHECKING:
    from core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF, DF

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin
from utils.dataset import columns_to_types_from_df, DataFrameConverter


class OracleEngineAdapter(
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
    FetchBatchMixin,
    EngineAdapterMixin,
    EngineAdapterWithIndexSupport,
):
    DIALECT: str = "oracle"
    SUPPORTS_TRANSACTIONS = True
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    MAX_TABLE_COMMENT_LENGTH = 4000
    MAX_COLUMN_COMMENT_LENGTH = 4000
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    SUPPORTS_CREATE_DROP_CATALOG = False
    SUPPORTS_TUPLE_IN = True
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_QUERY_EXECUTION_TRACKING = True
    SCHEMA_DIFFER_KWARGS = {
        "parameterized_type_defaults": {
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
        "max_parameter_length": {
            exp.DataType.build("VARCHAR2", dialect=DIALECT).this: 4000,
            exp.DataType.build("NVARCHAR2", dialect=DIALECT).this: 4000,
            #exp.DataType.build("RAW", dialect=DIALECT).this: 2000,
            #exp.DataType.build("UROWID", dialect=DIALECT).this: 4000,
            exp.DataType.build("CHAR", dialect=DIALECT).this: 2000,
            exp.DataType.build("NCHAR", dialect=DIALECT).this: 2000,
        },
        "coerceable_types": {
            exp.DataType.build("TIMESTAMP(6)", dialect=DIALECT): {
                exp.DataType.build("TIMESTAMP", dialect=DIALECT)
            }
        },
    }
    VARIABLE_LENGTH_DATA_TYPES = {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}

    @classmethod
    def _casted_columns(
        cls,
        target_columns_to_types: t.Dict[str, exp.DataType],
        source_columns: t.Optional[t.List[str]] = None,
    ) -> t.List[exp.Alias]:
        return [
            exp.alias_(
                exp.cast(
                    exp.null(),
                    to=kind if not kind.is_type(exp.DataType.Type.TEXT) else "VARCHAR2(4000)",
                ),
                column,
                copy=False,
            )
            for column, kind in target_columns_to_types.items()
        ]
    
    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        # Oracle versions prior to 23ai did not directly support the IF EXISTS in DDL statements
        if exists:
            exists = False
            if kind.upper() == "TABLE" and not self.table_exists(name):
                return

        self._drop_object(name, exists, kind, cascade, **drop_args)

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

        table = exp.to_table(table_name, dialect=self.dialect)

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
        table = exp.to_table(table_name, dialect=self.dialect)

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
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)): # TODO
            super().replace_query(table_name, query_or_df, target_columns_to_types, table_description, column_descriptions, source_columns, supports_replace_table_override, **kwargs)
            return
        
        target_table = exp.to_table(table_name, dialect=self.dialect)
        #target_table = f"{target_table.sql(dialect=self.dialect, identify=True)}"
        target_columns_to_types = target_columns_to_types or columns_to_types_from_df(query_or_df)
        target_columns_to_types = target_columns_to_types or self.columns(target_table)
        table_exists = self.table_exists(target_table)
        if not table_exists:
            casted_columns = self._casted_columns(target_columns_to_types)
            select_schema_exp = exp.select(*casted_columns).from_(exp.to_table("DUAL"), copy=False).where(exp.condition("0=1"), copy=False)
            self._create_table(target_table, expression=select_schema_exp, exists=False, replace=False)

        with self.transaction():
            if table_exists:
                self._truncate_table(table_name)
            self.insert_append(target_table, query_or_df, target_columns_to_types=target_columns_to_types)

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        track_rows_processed: bool = True,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)): # TODO
            super().insert_append(table_name, query_or_df, target_columns_to_types, track_rows_processed, source_columns)
            return
        
        target_table = exp.to_table(table_name, dialect=self.dialect)
        target_columns_to_types = target_columns_to_types or columns_to_types_from_df(query_or_df)
        target_columns_to_types = target_columns_to_types or self.columns(table_name)
        placeholder_map = self._build_placeholder_map(target_columns_to_types)

        table_exists = self.table_exists(target_table)
        if not table_exists:
            casted_columns = self._casted_columns(target_columns_to_types)
            select_schema_exp = exp.select(*casted_columns).from_(exp.to_table("DUAL"), copy=False).where(exp.condition("0=1"), copy=False)
            self._create_table(target_table, expression=select_schema_exp, exists=False, replace=False)

        values_exp = exp.values([tuple(exp.Placeholder(this=placeholder_map[col]) for col in target_columns_to_types)])
        insert_exp = exp.insert(values_exp, table_name, columns=list(target_columns_to_types))
        t.cast("pd.DataFrame", query_or_df).rename(columns=placeholder_map, inplace=True)
        parameters = DataFrameConverter.to_records(query_or_df)
        self.execute(insert_exp, track_rows_processed=track_rows_processed, parameters=parameters)

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries_or_df: t.Union[t.List[SourceQuery], DF],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        if not isinstance(source_queries_or_df, list):
            target_columns_to_types = target_columns_to_types or columns_to_types_from_df(source_queries_or_df)
        target_columns_to_types = target_columns_to_types or self.columns(table_name)

        if isinstance(source_queries_or_df, list):
            super()._insert_overwrite_by_condition(table_name, source_queries_or_df, target_columns_to_types, where or exp.condition("1=1"), insert_overwrite_strategy_override, **kwargs)
        else:
            with self.transaction():
                self.delete_from(table_name, where or exp.condition("1=1"))
                self.insert_append(table_name, source_queries_or_df, target_columns_to_types)

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
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        if not isinstance(source_table, (exp.Query, exp.DerivedTable)):
            target_columns_to_types = target_columns_to_types or columns_to_types_from_df(source_table)
        target_columns_to_types = target_columns_to_types or self.columns(target_table)

        if isinstance(source_table, (exp.Query, exp.DerivedTable)):
            if not when_matched:
                when_matched = exp.Whens(expressions=[
                    exp.When(
                        matched=True,
                        source=False,
                        then=exp.Update(
                            expressions=[
                                exp.column(col, MERGE_TARGET_ALIAS).eq(
                                    exp.column(col, MERGE_SOURCE_ALIAS)
                                )
                                for col in target_columns_to_types if exp.column(col) not in unique_key
                            ],
                        ),
                    )
                ])

            return super().merge(
                target_table, source_table,
                target_columns_to_types=target_columns_to_types,
                unique_key=unique_key,
                when_matched=when_matched,
                merge_filter=merge_filter,
                source_columns=source_columns,
                **kwargs
            )
        
        placeholder_map = self._build_placeholder_map(target_columns_to_types)
        on = exp.and_(
            *(
                add_table(part, MERGE_TARGET_ALIAS).eq(exp.Placeholder(this=placeholder_map[part.sql()]))
                for part in unique_key
            )
        )
        if merge_filter:
            on = exp.and_(merge_filter, on)

        if not when_matched:
            when_matched = exp.Whens()
            when_matched.append(
                "expressions",
                exp.When(
                    matched=True,
                    source=False,
                    then=exp.Update(
                        expressions=[
                            exp.column(col, MERGE_TARGET_ALIAS).eq(exp.Placeholder(this=placeholder_map[col]))
                            for col in target_columns_to_types if exp.column(col) not in unique_key
                        ],
                    ),
                ),
            )
        else:
            for node in when_matched.find_all(exp.Column):
                if node.table == MERGE_SOURCE_ALIAS:
                    node.replace(exp.Placeholder(this=placeholder_map[node.this.sql()]))

        when_not_matched = exp.When(
            matched=False,
            source=False,
            then=exp.Insert(
                this=exp.Tuple(expressions=[exp.column(col) for col in target_columns_to_types]),
                expression=exp.Tuple(
                    expressions=[exp.Placeholder(this=placeholder_map[col]) for col in target_columns_to_types]
                ),
            ),
        )
        when_matched.append("expressions", when_not_matched)

        t.cast("pd.DataFrame", source_table).rename(columns=placeholder_map, inplace=True)
        self._merge(target_table, source_table, on, whens=when_matched)

    def _merge(
        self,
        target_table: TableName,
        query: QueryOrDF,
        on: exp.Expression,
        whens: exp.Whens,
    ) -> None:
        """
        Executes a MERGE statement against an Oracle database.

        This method is responsible for translating a generic SQLGlot MERGE expression
        into Oracle's specific syntax, especially handling placeholder-based DML
        for DataFrame operations and the unique syntax for conditional WHEN clauses.
        """
        if isinstance(query, (exp.Query, exp.DerivedTable)):
            # For query-based sources, we execute the MERGE statement directly.
            parameters = None
            using = exp.alias_(
                exp.Subquery(this=query), alias=MERGE_SOURCE_ALIAS, copy=False, table=True
            )
        else:
            # For DataFrame-based sources, we use parameter binding for performance and safety.
            # Oracle's MERGE...USING DUAL pattern allows us to treat each row of the DataFrame
            # as a set of parameters for a single-row merge operation, executed efficiently via `executemany`.
            parameters = DataFrameConverter.to_records(query)
            using = "DUAL"
        
        this = exp.alias_(exp.to_table(target_table, dialect=self.dialect), alias=MERGE_TARGET_ALIAS, table=True)
        if not isinstance(on, exp.Paren):
            on = exp.Paren(this=on)

        # Oracle has a non-standard syntax for conditional updates and inserts within a MERGE.
        # This loop finds all WHEN clauses and rewrites them to be Oracle-compliant.
        for when_exp in whens.find_all(exp.When):
            condition = when_exp.args.get("condition", False)
            # Remove the generic 'condition' from the WHEN clause itself,
            # and re-inject it in the Oracle-specific location (the WHERE clause of the UPDATE/INSERT).
            when_exp.args["condition"] = False
            if condition is not False:
                if when_exp.args["matched"]:
                    # WHEN MATCHED AND condition THEN UPDATE SET column_value_pairs
                    # -> WHEN MATCHED THEN UPDATE SET column_value_pairs WHERE condition
                    for update_exp in when_exp.find_all(exp.Update):
                        update_exp.replace(self._to_sql(update_exp.where(condition), quote=True))
                else:
                    # WHEN NOT MATCHED AND condition THEN merge_insert
                    # -> WHEN NOT MATCHED THEN merge_insert WHERE condition
                    for insert_exp in when_exp.find_all(exp.Insert):
                        oracle_when_exp = self._to_sql(exp.When(matched=False, then=insert_exp), quote=True)
                        oracle_when_exp += " " + self._to_sql(exp.Where(this=condition), quote=True)
                        when_exp.replace(oracle_when_exp)
        
        merge_exp = exp.Merge(this=this, using=using, on=on, whens=whens)
        self.execute(merge_exp, track_rows_processed=True, parameters=parameters)

    def _execute(self, sql: str, track_rows_processed: bool = False, parameters: t.Optional[t.List[t.Any]] = None, **kwargs: t.Any) -> None:
        if parameters:
            if isinstance(parameters, list):
                self.cursor.executemany(sql, parameters)
            else:
                self.cursor.execute(sql, parameters)
        else:
            self.cursor.execute(sql)

        if (
            self.SUPPORTS_QUERY_EXECUTION_TRACKING
            and track_rows_processed
            and self._query_execution_tracker
            and self._query_execution_tracker.is_tracking()
        ):
            if (
                rowcount := getattr(self.cursor, "rowcount", None)
            ) is not None and rowcount is not None:
                try:
                    self._record_execution_stats(sql, int(rowcount))
                except (TypeError, ValueError):
                    return
                
    def identifier_needs_quoting(self, identifier: t.Union[str, exp.Identifier]) -> bool:
        if isinstance(identifier, exp.Identifier):
            identifier = identifier.sql(self.dialect)

        if not re.match(r"^[A-Z][A-Z0-9_$#]*$", identifier):
            return True

        if identifier.upper() in Oracle.Tokenizer.KEYWORDS:
            return True
        
        return False
    
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