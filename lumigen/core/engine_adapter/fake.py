"""Contains FakeEngineAdapter."""

from __future__ import annotations
import typing as t

import logging
import pandas as pd
import pyspark.sql
import time
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

if t.TYPE_CHECKING:
    from core._typing import DataFrameType
    from core.config.connection import ConsoleConfig

from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin
from utils.dataset import DataFrameConverter

logger = logging.getLogger(__name__)


class FakeEngineAdapter(FetchBatchMixin, EngineAdapterMixin):
    DIALECT: str = "fake"

    def fetch_pyspark_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pyspark.sql.DataFrame:
        return DataFrameConverter.to_spark(self.fetchdf(query))
    
    def replace_query(self, table_name, df: DataFrameType, **kwargs: t.Any) -> None:
        self.insert_append(table_name, df)

    def insert_append(self, table_name, df: DataFrameType, **kwargs: t.Any) -> None:
        conn = t.cast("ConsoleConfig", self.connection)
        if not conn.print_data:
            return
        last_print_ts = getattr(self, "last_print_ts", 0)
        delay = max(conn.print_delay - (time.time() - last_print_ts), 0)
        
        if isinstance(df, pyspark.sql.DataFrame):
            if not df.isStreaming:
                if delay > 0:
                    time.sleep(delay)
                df.show(conn.print_limit_per_batch, truncate=False)
            else:
                df.writeStream.format("console") \
                    .option("numRows", conn.print_limit_per_batch) \
                    .option("truncate", False) \
                    .outputMode("append") \
                    .trigger(processingTime=f"{conn.print_delay} seconds") \
                    .start().awaitTermination()
        elif isinstance(df, pd.DataFrame):
            pd.set_option('display.max_columns', None)
            if delay > 0:
                time.sleep(delay)
            print(df.head(conn.print_limit_per_batch))
        else:
            if delay > 0:
                time.sleep(delay)
            print(df)
        
        self.last_print_ts = time.time()

    def merge(self, target_table, df: DataFrameType, *args, **kwargs) -> None:
        self.insert_append(target_table, df)

    def ping(self) -> None: ...

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        sql_gen_kwargs = {
            "pretty": self._pretty_sql,
            "comments": False,
            **self._sql_gen_kwargs,
            **kwargs,
        }

        expression = expression.copy()

        if quote:
            quote_identifiers(expression)

        return expression.sql(**sql_gen_kwargs, copy=False)  # type: ignore
    
    def get_all_data_objects(self, keyword: str="") -> t.List[t.Any]:
        return []