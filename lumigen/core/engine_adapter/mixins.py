from __future__ import annotations

import logging
import typing as t
import re
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF, Query

from core.dialect import select_from_values_for_batch_range

logger = logging.getLogger(__name__)


class FetchBatchMixin(EngineAdapter):
    def fetchbatch(
        self,
        query: t.Union[exp.Expression, str],
        batch_size: int = None,
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Iterator[t.List[t.Tuple]]:
        with self.transaction():
            self.execute(
                query,
                ignore_unsupported_errors=ignore_unsupported_errors,
                quote_identifiers=quote_identifiers,
            )
            if batch_size:
                while True:
                    batch = self.cursor.fetchmany(batch_size)
                    if not batch:
                        break

                    yield batch
            else:
                yield self.cursor.fetchall()

    def fetch_batch_df(
        self,
        query: t.Union[exp.Expression, str],
        batch_size: int = None,
        quote_identifiers: bool = False,
    ) -> t.Iterator[DF]:
        from warnings import catch_warnings, filterwarnings

        from pandas.io.sql import read_sql_query

        sql = (
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query or ""
        )
        logger.debug(f"Executing SQL:\n{sql}")
        with catch_warnings(), self.transaction():
            filterwarnings(
                "ignore",
                category=UserWarning,
                message=".*pandas only supports SQLAlchemy connectable.*",
            )
            yield from read_sql_query(sql, self._connection_pool.get(), chunksize=batch_size)

class FetchStreamMixin(FetchBatchMixin):
    def fetchstream(
        self,
        query: t.Union[exp.Expression, str],
        batch_size: int = None,
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Iterator[t.List[t.Tuple]]:
        with self.transaction():
            self.execute(
                query,
                ignore_unsupported_errors=ignore_unsupported_errors,
                quote_identifiers=quote_identifiers,
            )
            if batch_size:
                yield from self.cursor.fetchstream(batch_size)
            else:
                yield from self.cursor.fetchstream()
        return

    def fetch_stream_df(
        self,
        query: t.Union[exp.Expression, str],
        batch_size: int = None,
        quote_identifiers: bool = False,
    ) -> t.Iterator[DF]:
        with self.transaction():
            self.execute(
                query,
                quote_identifiers=quote_identifiers,
            )
            if batch_size:
                yield from self.cursor.fetchstreamdf(batch_size)
            else:
                yield from self.cursor.fetchstreamdf()
        return
    
class EngineAdapterMixin(EngineAdapter):
    def get_qualified_table_name(self, table_name: str, schema_name: t.Optional[str]=None) -> str:
        def _general_quoting_check(identifier: str) -> bool:
            if not identifier:
                return False
            if any(char.isspace() for char in identifier):
                return True
            UNQUOTED_IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
            if not UNQUOTED_IDENTIFIER_REGEX.fullmatch(identifier):
                return True
            return False
        
        try:
            table_identifier = exp.Identifier(this=table_name)
            schema_identifier = exp.Identifier(this=schema_name) if schema_name else None
            qualified_table = exp.Table(this=table_identifier, db=schema_identifier)
            if _general_quoting_check(table_name):
                table_name = qualified_table.sql(dialect=self.dialect, identify='safe').split('.')[-1]
            else:
                table_name = qualified_table.sql(dialect=self.dialect, identify=False).split('.')[-1]
        except Exception as e:
            return f"{schema_name}.{table_name}" if schema_name else table_name
        
        try:
            if schema_name:
                if _general_quoting_check(schema_name):
                    schema_name = qualified_table.sql(dialect=self.dialect, identify='safe').split('.')[0]
                else:
                    schema_name = qualified_table.sql(dialect=self.dialect, identify=False).split('.')[0]
        finally:
            return f"{schema_name}.{table_name}" if schema_name else table_name

    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        raise NotImplementedError()
    
    def _values_to_sql(
        self,
        values: t.List[t.Tuple[t.Any, ...]],
        columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
    ) -> Query:
        return select_from_values_for_batch_range(
            values=values,
            columns_to_types=columns_to_types,
            batch_start=batch_start,
            batch_end=batch_end,
            alias=alias,
        )