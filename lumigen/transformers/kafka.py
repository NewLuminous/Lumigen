import typing as t

import json
import pyspark
from pyspark.sql.functions import col, coalesce, from_json, row_number, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
import numpy as np
import pandas as pd

from transformers.base import AbstractTransformerDecorator
from utils.errors import MissingColumnError, UnsupportedDataFrameError


class DebeziumParser(AbstractTransformerDecorator):
    def transform(self, df, *args, **kwargs):
        """
        Parameters
        ----------
        df: A data frame of Debezium JSON messages.

        Returns
        -------
        A data frame with three columns `data`, `op`, `ts_ms`
        """
        df = super().transform(df, *args, **kwargs)

        df = self.inline(df)
        if isinstance(df, pyspark.sql.DataFrame):
            df = df.withColumn("data",
                when((col("op")=='c') & (col("after").isNotNull()), col("after"))
                .when((col("op")=='r') & (col("after").isNotNull()), col("after"))
                .when((col("op")=='u') & (col("after").isNotNull()), col("after"))
                .when((col("op")=='d') & (col("before").isNotNull()), col("before"))
                .otherwise(None)
            )
            df = df.withColumn("ts_ms",
                when(col("ts_ns").isNotNull(), col("ts_ns")/1_000_000_000)
                .when(col("ts_us").isNotNull(), col("ts_us")/1_000_000)
                .otherwise(col("ts_ms")/1_000)
                .cast("timestamp")
            )
            df = df.select("data", "op", "ts_ms")
        elif isinstance(df, pd.DataFrame):
            conditions = [
                (df["op"] == 'c') & (df["after"].notnull()),
                (df["op"] == 'r') & (df["after"].notnull()),
                (df["op"] == 'u') & (df["after"].notnull()),
                (df["op"] == 'd') & (df["before"].notnull())
            ]
            values = [
                df["after"],
                df["after"],
                df["after"],
                df["before"]
            ]
            df["data"] = np.select(conditions, values, default=None)

            ts_conditions = [
                df["ts_ns"].notnull(),
                df["ts_us"].notnull()
            ]
            ts_values = [
                df["ts_ns"] / 1_000_000_000,    # ts_ns to seconds
                df["ts_us"] / 1_000_000         # ts_us to seconds
            ]
            df["ts_ms"] = np.select(ts_conditions, ts_values, default=df["ts_ms"] / 1000)
            df["ts_ms"] = pd.to_datetime(df["ts_ms"], unit='s')

            df = df[["data", "op", "ts_ms"]]
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df
    
    def inline(self, df):
        if "value" not in df.columns:
            raise MissingColumnError("Column `value` is required.")

        if isinstance(df, pyspark.sql.DataFrame):
            df = df.selectExpr("CAST(value AS STRING) as value")
            event_schema = StructType([
                StructField("schema", StringType(), True),
                StructField("payload", StringType(), True)
            ])
            df = df.withColumn("event", from_json(col("value"), event_schema))
            df = df.select(coalesce(col("event.payload"), col("value")).alias("value"))

            payload_schema = StructType([
                StructField("before", StringType(), True),
                StructField("after", StringType(), True),
                StructField("source", StringType(), True),
                StructField("op", StringType(), False),
                StructField("ts_ms", LongType(), False),
                StructField("ts_us", LongType(), True),
                StructField("ts_ns", LongType(), True),
            ])
            df = df.select(from_json(col("value"), payload_schema).alias("payload"))
            df = df.select(col("payload.*"))
        elif isinstance(df, pd.DataFrame):
            df["value"] = df["value"].values.astype(str)

            def get_payload(value: str):
                if value is not None:
                    event = json.loads(value)
                    payload = event["payload"] if "payload" in event else event
                else:
                    payload = {}
                return {
                    "before": payload.get("before", None),
                    "after": payload.get("after", None),
                    "source": payload.get("source", None),
                    "op": payload["op"],
                    "ts_ms": payload["ts_ms"],
                    "ts_us": payload.get("ts_us", None),
                    "ts_ns": payload.get("ts_ns", None),
                }
            payload_series = df.pop("value").map(lambda x: get_payload(x))
            df = pd.DataFrame(payload_series.tolist())
        
        return df
    

class CDCDeduplicator(AbstractTransformerDecorator):
    def transform(self, df, key: t.List[str], priority: t.Optional[str]="ts_ms", *args, **kwargs):
        """
        Parameters
        ----------
        df: A data frame of CDC messages.
        key: List of columns to use for duplicate comparison.
        priority: One row having the highest value in this column will be kept and the rest of duplicate rows will be removed.
        """
        df = super().transform(df, *args, **kwargs)

        if "op" not in df.columns:
            raise MissingColumnError("Column `op` is required.")

        if isinstance(df, pyspark.sql.DataFrame):
            orderBy_cols = [
                col(priority).desc(),
                when(col("op")=='t', 0)
                    .when(col("op")=='d', 1).when(col("op")=='u', 2).when(col("op")=='c', 3)
                    .otherwise(4)
            ]
            w = Window.partitionBy(*key).orderBy(*orderBy_cols)
            df = df.withColumn("rownum", row_number().over(w))
            df = df.filter("rownum = 1")
            df = df.drop("rownum")
            df = df.filter(col("op").isin(['d', 'u', 'c'])) # TODO
        elif isinstance(df, pd.DataFrame):
            df["op_priority"] = np.select(
                [df["op"] == 't', df["op"] == 'd', df["op"] == 'u', df["op"] == 'c'],
                [0, 1, 2, 3], default=4
            )
            df["rownum"] = df.sort_values([priority, "op_priority"], ascending=[False, True]) \
                .groupby(key).cumcount() + 1
            
            df = df[df["rownum"] == 1]
            df = df.drop(columns=["op_priority", "rownum"])
            df = df[df["op"].isin(['d', 'u', 'c'])] # TODO
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df