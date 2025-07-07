from __future__ import annotations

import typing as t

import logging
import pyspark
from sqlglot import exp

import sqlmesh
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
    DataObject,
    DataObjectType,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName, DF
    from sqlmesh.core.engine_adapter.base import QueryOrDF

from utils.errors import EngineAdapterError
from utils.dataset import DataFrameConverter
from core.engine_adapter.mixins import EngineAdapterMixin, FetchStreamMixin

logger = logging.getLogger(__name__)


@set_catalog()
class SparkEngineAdapter(sqlmesh.core.engine_adapter.SparkEngineAdapter, EngineAdapterMixin):
    def wap_supported(self, table_name: TableName) -> bool:
        # Disable WAP to prevent dummy inserting when _create_table
        return False
    
    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        keyword = keyword.strip()
        if '.' in keyword:
            schema = keyword.split('.')[0]
            table = '.'.join(keyword.split('.')[1:])
            table_pattern = f"{table}*" if table else '*'
        else:
            schema = None
            table_pattern = f"*{keyword}*" if keyword else '*'

        sql = f"SHOW TABLE EXTENDED"
        if schema:
            sql += f" IN {schema}"
        sql += f" LIKE '{table_pattern}'"
        try:
            results = (
                self.fetch_pyspark_df(sql).collect()
                if self._use_spark_session
                else self.fetchdf(sql).to_dict("records")
            )
        # Improvement: Figure out all the different exceptions we could get from executing a query either with or
        # without a Spark Session. In addition Databricks would need to be updated to handle it's own exceptions.
        # Therefore just doing except Exception for now.
        except Exception:
            return []
        data_objects = []
        catalog = self.get_current_catalog()
        for row in results:  # type: ignore
            row_dict = row.asDict() if not isinstance(row, dict) else row
            if row_dict.get("isTemporary"):
                continue
            schema = row_dict.get("namespace") or row_dict.get("database")
            data_objects.append(
                DataObject(
                    catalog=catalog,
                    schema=schema,
                    name=row_dict["tableName"],
                    type=(
                        DataObjectType.VIEW
                        if "Type: VIEW" in row_dict["information"]
                        else DataObjectType.TABLE
                    ),
                )
            )
        return data_objects

@set_catalog()
class SparkJDBCEngineAdapter(SparkEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

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

        For partition based engines (hive, spark), insert override is used.

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            columns_to_types: Optional a mapping between the column name and its data type.
            kwargs: Optional create table properties.
        """
        target_table = exp.to_table(table_name)

        if isinstance(query_or_df, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(query_or_df)
        else:
            df = query_or_df

        # TODO: When creating table if not exists, create CLOB columns when data length > 255
        if isinstance(df, pyspark.sql.DataFrame):
            df.write.format("jdbc") \
                .options(**self.cursor.jdbc_options) \
                .option("dbtable", target_table.sql(dialect=self.dialect)) \
                .option("truncate", kwargs.get("truncate", True)) \
                .mode("overwrite").save()
        else:
            raise NotImplementedError()
            
    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        target_table = exp.to_table(table_name)

        if isinstance(query_or_df, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(query_or_df)
        else:
            df = query_or_df

        if isinstance(df, pyspark.sql.DataFrame):
            df.write.format("jdbc") \
                .options(**self.cursor.jdbc_options) \
                .option("dbtable", target_table.sql(dialect=self.dialect)) \
                .mode("append").save()
        else:
            raise NotImplementedError()
            
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
    ) -> None:
        if isinstance(source_table, (exp.Query, exp.DerivedTable, str)):
            df = self.fetch_pyspark_df(source_table)
        else:
            df = source_table

        # MERGE INTO TABLE is not supported temporarily by current version of vanilla Spark (3.3.3)
        # Delegate MERGE operation to corresponding engine adapter at the cost of collecting all the rows from PySpark DataFrame.
        df = DataFrameConverter.to_pandas(df)
        self.connection.engine_adapter.merge(target_table, df, columns_to_types, unique_key, when_matched, merge_filter)

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        return self.connection.engine_adapter.columns(table_name, include_pseudo_columns)

    def table_exists(self, table_name: TableName) -> bool:
        return self.connection.engine_adapter.table_exists(table_name)
            
@set_catalog()
class SparkKafkaEngineAdapter(SparkEngineAdapter, FetchStreamMixin):
    SUPPORTS_REPLACE_TABLE = True

    def replace_query(
        self,
        topic: str,
        df: DF,
        **kwargs: t.Any,
    ) -> None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({
            'bootstrap.servers': self.cursor.kafka_options["kafka.bootstrap.servers"]
        })
        delete_response = admin.delete_topics([topic])
        for topic, future in delete_response.items():
            try:
                future.result()  # Block until the operation is complete
                logger.info(f"Topic '{topic}' successfully deleted.")

                self.insert_append(topic, df, **kwargs)
            except Exception as e:
                logger.error(f"Failed to delete topic '{topic}'.")
                raise e
            
    def insert_append(
        self,
        topic: str,
        df: DF,
        **kwargs: t.Any,
    ) -> None:
        if df.isStreaming:
            df.writeStream.format("kafka") \
                .option("kafka.bootstrap.servers", self.cursor.kafka_options["kafka.bootstrap.servers"]) \
                .option("topic", topic) \
                .option("checkpointLocation", f"/spark-ckpts/{self.cursor.kafka_options.get('kafka.group.id', topic)}") \
                .outputMode("append").start().awaitTermination()
        else:
            df.write.format("kafka") \
                .option("kafka.bootstrap.servers", self.cursor.kafka_options["kafka.bootstrap.servers"]) \
                .option("topic", topic) \
                .mode("append").save()
            
    def merge(self, *args, **kwargs) -> None:
        raise EngineAdapterError("Unsupported MERGE operation for Kafka sinks.")
    
    def columns(
        self, topics: str, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        return self.cursor.MESSAGE_SCHEMA

    def table_exists(self, topics: str) -> bool:
        return None # TODO