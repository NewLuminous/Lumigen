from __future__ import annotations

import logging
import typing as t
from abc import ABC, abstractmethod
from sqlglot import exp, Dialect
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from core.config.job import DataLoadMode
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF

from core.dialect import select_from_values_for_batch_range
from utils.dataset import columns_to_types_from_df, empty_pandas_df

logger = logging.getLogger(__name__)


class FetchBatchMixin(EngineAdapter):
    def fetchbatch(
        self,
        query: t.Union[exp.Expression, str],
        batch_size: int = None,
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Iterator[t.List[t.Tuple]]:
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
    def identifier_needs_quoting(self, identifier: t.Union[str, exp.Identifier]) -> bool:
        return False

    def get_qualified_table_name(self,
                                 table_name: t.Union[str, exp.Identifier],
                                 schema_name: t.Optional[t.Union[str, exp.Identifier]]=None,
                                 catalog: t.Optional[t.Union[str, exp.Identifier]]=None) -> str:
        if not table_name:
            raise ValueError("table_name cannot be empty.")
        
        table_exp = exp.Table(
            this=exp.to_identifier(table_name, quoted=self.identifier_needs_quoting(table_name)),
            db=exp.to_identifier(schema_name, quoted=self.identifier_needs_quoting(schema_name)) if schema_name else None,
            catalog=exp.to_identifier(catalog, quoted=self.identifier_needs_quoting(catalog)) if catalog else None,
        )
        try:
            return table_exp.sql(dialect=self.dialect)
        except:
            return table_exp.sql()

    def get_qualified_column_name(self, column_name: t.Union[str, exp.Identifier]):
        if not column_name:
            raise ValueError("column_name cannot be empty.")
        
        column_exp = exp.Column(this=exp.to_identifier(column_name, quoted=self.identifier_needs_quoting(column_name)))
        try:
            return column_exp.sql(dialect=self.dialect)
        except:
            return column_exp.sql()

    def get_all_data_objects(
        self, keyword: str="", limit: t.Optional[int]=None,
        object_types: t.Optional[t.Collection[t.Union[str, DataObjectType]]] = None, *args,
    ) -> t.List[DataObject]:
        raise NotImplementedError()

    def _columns_to_types(
            self,
            query_or_df: QueryOrDF,
            target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
            source_columns: t.Optional[t.List[str]] = None,
        ) -> t.Tuple[t.Optional[t.Dict[str, exp.DataType]], t.Optional[t.List[str]]]:
            import pandas as pd
    
            if not target_columns_to_types and isinstance(query_or_df, pd.DataFrame):
                target_columns_to_types = columns_to_types_from_df(t.cast(pd.DataFrame, query_or_df))
            return super()._columns_to_types(query_or_df, target_columns_to_types=target_columns_to_types, source_columns=source_columns)
    
    def _values_to_sql(
        self,
        values: t.List[t.Tuple[t.Any, ...]],
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
        source_columns: t.Optional[t.List[str]] = None,
    ) -> Query:
        return select_from_values_for_batch_range(
            values=values,
            target_columns_to_types=target_columns_to_types,
            batch_start=batch_start,
            batch_end=batch_end,
            alias=alias,
            source_columns=source_columns,
        )

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        import pandas as pd
        
        df = super()._fetch_native_df(query, quote_identifiers=quote_identifiers)
        if isinstance(df, pd.DataFrame) and len(df.columns) == 0:
            description = getattr(self.cursor, "description", None)
            if not description:
                with self.transaction():
                    self.execute(query, quote_identifiers=quote_identifiers)
                    description = getattr(self.cursor, "description", None)

            if description:
                df_columns_to_types: t.Dict[str, exp.DataType] = {}
                for col in description:
                    col_name = str(col[0]) if col[0] is not None else ""
                    raw_type = col[1]
                    try:
                        if raw_type is not None:
                            df_columns_to_types[col_name] = exp.DataType.build(
                                str(raw_type), dialect=self.dialect, udt=True,
                            )
                        else:
                            df_columns_to_types[col_name] = exp.DataType.build(exp.DataType.Type.TEXT)
                    except Exception as err:
                        logger.debug(f"Could not parse type '{raw_type}' for column '{col_name}': {err}. Defaulting to TEXT.")
                        df_columns_to_types[col_name] = exp.DataType.build(exp.DataType.Type.TEXT)

                try:
                    return empty_pandas_df(df_columns_to_types)
                except:
                    return pd.DataFrame(columns=list(df_columns_to_types.keys()))
        return df
    
class QueryBuilderMixin(ABC):
    """
    An abstract mixin for EngineAdapters that need to construct their own
    engine-specific "query" from a source path.
    """

    @abstractmethod
    def build_query(
        self,
        path: t.Optional[str] = None,
        query: t.Optional[str] = None,
        data_load_mode: t.Optional[DataLoadMode] = None,
        watermark_column: t.Optional[str] = None,
        last_watermark_value: t.Optional[t.Any] = None,
        **kwargs: t.Any,
    ) -> t.Any:
        """
        Constructs a query-like object suitable for the adapter's fetch methods.

        Returns:
            An object that the adapter's `fetchdf`, `fetch_pyspark_df`, etc., methods
            can understand and execute.
        """
        pass

class WatermarkStrategyMixin(ABC):
    """
    An abstract mixin for EngineAdapters that provide a custom strategy
    for determining the high watermark from a processed batch of data.
    """

    @abstractmethod
    def determine_high_watermark(self, df: DF, watermark_field: str, current_watermark_value: t.Any) -> t.Any:
        """
        Determines the highest watermark value from a given DataFrame batch.

        Args:
            df: The DataFrame batch.
            watermark_field: The name of the column that contains the watermark,
                             provided for context even if not used by the strategy.
            current_watermark_value: The current value of watermark.

        Returns:
            The high watermark for this batch.
        """
        pass