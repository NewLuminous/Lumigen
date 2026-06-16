from __future__ import annotations
import typing as t
import argparse
import json
from pydantic import BaseModel
import pyspark.sql as pyspark_sql
from functools import partial
from sqlglot import exp, Dialect, Dialects
import logging

if t.TYPE_CHECKING:
    from core._typing import DataFrameType
    from core.config.root import Config
    from core.engine_adapter.mixins import EngineAdapterMixin, FetchBatchMixin

from config.logging import setup_logger
setup_logger(logging.INFO)
logger = logging.getLogger(__name__)

from core.config.loader import load_config
from core.config.connection import parse_connection_config
from core.engine_context import SQLContextManager
from core.engine_adapter import SparkEngineAdapter
from core.engine_adapter.mixins import QueryBuilderMixin
from core.adapters import validate_connections
from core.execution import transform_data
from core.job_context import JobContext, JobContextBuilder
import transformers
from utils import SQLIdentifierSet
from utils.json import DisplayableJSONEncoder
from utils.dataset import DataFrameConverter, columns_to_types_from_df
from utils.sql import dialect_insensitive_tbl_name


DATA_START_DELIMITER = "=== JSON_DATA_START ==="
DATA_END_DELIMITER = "=== JSON_DATA_END ==="

def _print_json(json_string: str):
    print(DATA_START_DELIMITER)
    print(json_string)
    print(DATA_END_DELIMITER)

class SchemaField(BaseModel):
    name: str
    type: str

class PreviewRunner:
    """
    Encapsulates all logic for generating a data preview or schema diff.
    This mimics the JobRunner pattern for a self-contained script.
    """
    def __init__(self, config: Config, resolve_executor: t.Optional[bool] = True):
        self.config = config
        if resolve_executor:
            self.exec_cfg = self._resolve_executor()
        else:
            self.exec_cfg = self.config.executor
        self.context_builder = JobContextBuilder(config, None, executor_override=self.exec_cfg)

    def _resolve_executor(self):
        """Determines if a Spark executor is required, e.g., for SQL UDFs."""
        context_manager = SQLContextManager.get_instance()
        exec_cfg = self.config.get_executor()
        source_cfg = self.config.get_source()
        temp_src_adapter = source_cfg.create_engine_adapter() if source_cfg else None
        for transform_cfg in self.config.transformation_pipeline:
            if transform_cfg.name.upper() == "SQL" and (query := transform_cfg.options.get("query")):
                if exec_cfg and exec_cfg.type_ == "spark":
                    engine_dialect = Dialects.SPARK
                else:
                    engine_dialect = Dialects.DUCKDB
                select_exp = transformers.sql.SQL.parse_and_validate_query(query, temp_src_adapter, fallback_dialect=engine_dialect, **self.config.job_properties)
                # Find all custom function calls
                for func_exp in select_exp.find_all(exp.Anonymous):
                    func_name = func_exp.this
                    # If a single UDF doesn't exist in DuckDB, use Spark.
                    if not context_manager.udf_exists(func_name, "duckdb"):
                        if exec_cfg is None or exec_cfg.type_ != "spark":
                            logger.info(f"UDF '{func_exp.this}' requires Spark. Switching to Spark executor for this preview.")
                            #context_manager.get_spark_session(app_name="Job Preview", hive_support=False)
                            exec_cfg = parse_connection_config({
                                "type": "spark",
                                "app_name": "Job Preview",
                            })
                        return exec_cfg
        return None # Default to Python executor for fast result
    
    def run(self,
            callback: t.Optional[t.Callable[[str], t.Any]] = _print_json,
            do_validate_connections: t.Optional[bool] = True,
        ) -> t.Optional[int]:
        if do_validate_connections:
            validate_connections([self.config.source, self.config.sink])
        with self.context_builder.build() as job_context:
            lazy_dataset = self._read_data(job_context)
            trans_chain = transform_data(self.config.transformation_pipeline, job_context)
            if not self.config.sink:
                logger.info("No sink configured. Will preview transformed source data.")
                trans_chain.add(partial(self._run_data_preview, job_context, callback))
            else:
                logger.info("Sink configured. Will generate a schema diff.")
                trans_chain.add(partial(self._run_schema_diff, job_context, callback))

            for df in lazy_dataset:
                trans_chain.apply(df)

        return 0
    
    def _read_data(self, context: JobContext) -> t.Iterator[DataFrameType]:
        """Reads a single batch of data from the source, respecting the preview limit."""
        reader = context.source_adapter
        job_props = context.job_props_model
        limit = job_props.source_limit
        query = self._get_source_query(context)

        if isinstance(reader, SparkEngineAdapter):
            yield reader.fetch_pyspark_df(query)
        else:
            limit = job_props.source_limit or job_props.source_batch_size
            if limit:
                if isinstance(query, exp.Select):
                    query = query.limit(limit)

                try:
                    reader = t.cast("FetchBatchMixin", reader)
                    for df in reader.fetch_batch_df(query, limit):
                        yield df
                        return
                except AttributeError:
                    logger.error(NotImplementedError(f"Batch fetching is not supported by {reader.__class__.__name__}."))

            yield reader.fetchdf(query)
        return
    
    def _get_source_query(self, context: JobContext) -> t.Any:
        reader = context.source_adapter
        job_props = context.job_props_model

        if isinstance(reader, QueryBuilderMixin):
            return reader.build_query(
                path=job_props.source_path,
                query=job_props.source_query,
            )
        if Dialect.get(reader.dialect):
            return context.build_source_expression()
        raise NotImplementedError(f"Non-standard dialect '{reader.dialect}'.")
    
    def _run_data_preview(self, context: JobContext, callback: t.Callable[[str], t.Any], df: DataFrameType):
        """Final pipeline step: samples data and prints it as JSON."""
        num_rows_to_fetch = context.job_props.get("source_batch_size") or 20
        if isinstance(df, pyspark_sql.DataFrame):
            df = df.limit(num_rows_to_fetch)
        else:
            df = df.head(num_rows_to_fetch)

        schema_fields = [
            SchemaField(name=col_name, type=col_dtype.sql()).model_dump()
            for col_name, col_dtype in columns_to_types_from_df(df).items()
        ]

        """
            Alternative:
            json_rows_list = df.toJSON(use_unicode=True).collect()
            rows_as_dicts = [json.loads(row_str) for row_str in json_rows_list]
        """
        rows_as_dicts = DataFrameConverter.to_records(df)

        payload = {
            "schema": schema_fields,
            "data": rows_as_dicts
        }
        callback(json.dumps(payload, cls=DisplayableJSONEncoder))

    def _run_schema_diff(self, context: JobContext, callback: t.Callable[[str], t.Any], df: DataFrameType):
        """Final pipeline step: compares transformed schema with sink and prints diff as JSON."""
        writer = context.sink_adapter
        source_columns = columns_to_types_from_df(df)
        
        sink_table = context.job_props_model.get_sink_path()
        sink_table = dialect_insensitive_tbl_name(sink_table, writer.dialect)
        destination_columns = {}
        if sink_table and writer.table_exists(sink_table):
            for k, v in writer.columns(sink_table).items():
                try:
                    col_name = t.cast("EngineAdapterMixin", writer).get_qualified_column_name(k)
                except:
                    col_name = k
                destination_columns[col_name] = v
        else:
            logger.info(f"Sink table {sink_table} does not exist. Using empty schema.")

        column_mappings, unmapped_source_columns, unmapped_destination_columns = [], [], []
        dest_col_set = SQLIdentifierSet(destination_columns)
        mapped_destination_columns = set()
        for df_col_name, df_col_dtype in source_columns.items():
            tbl_col_name = dest_col_set.get(df_col_name)
            if tbl_col_name in destination_columns:
                tbl_col_dtype = destination_columns[tbl_col_name]
                is_type_compatible = writer.schema_differ._is_compatible_type(df_col_dtype, tbl_col_dtype)
                if is_type_compatible:
                    mapping_status = "MATCHED_TYPE_COMPATIBLE"
                    notes = f"Source {df_col_dtype} is compatible with destination {tbl_col_dtype}."
                else:
                    mapping_status = "MATCHED_TYPE_MISMATCH"
                    notes = f"Source {df_col_dtype} might need conversion to destination {tbl_col_dtype}."
                column_mappings.append({
                    "source_column": SchemaField(name=df_col_name, type=df_col_dtype.sql()).model_dump(),
                    "destination_column": SchemaField(name=tbl_col_name, type=tbl_col_dtype.sql()).model_dump(),
                    "mapping_status": mapping_status,
                    "notes": notes,
                })
                mapped_destination_columns.add(tbl_col_name)
            else:
                unmapped_source_columns.append(SchemaField(name=df_col_name, type=df_col_dtype.sql()).model_dump())
        for tbl_col_name, tbl_col_dtype in destination_columns.items():
            if tbl_col_name not in mapped_destination_columns:
                unmapped_destination_columns.append(SchemaField(name=tbl_col_name, type=tbl_col_dtype.sql()).model_dump())

        payload = {
            "column_mappings": column_mappings,
            "unmapped_source_columns": unmapped_source_columns,
            "unmapped_destination_columns": unmapped_destination_columns,
        }
        callback(json.dumps(payload))

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", type=str, required=False, default="", help="Config file")
    args = arg_parser.parse_args()
    job_cfg = load_config(path=args.config)
    with SQLContextManager.get_instance() as manager:
        runner = PreviewRunner(job_cfg)
        runner.run()