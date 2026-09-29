from __future__ import annotations
import typing as t
from collections import OrderedDict
import logging
import pandas as pd
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import (
    EngineAdapter, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
)

if t.TYPE_CHECKING:
    from core.job_context import JobContext

from utils import merge_dicts, SQLIdentifierSet
from utils.dataset import columns_to_types_from_df, get_columns_name, is_batch_or_stream_empty
from utils.sql import dialect_insensitive_tbl_name
from core.config.job import TransformationConfig, DataSaveMode, DataLoadMode, JobMode
from core.pipeline.watermark_tracker import WatermarkTracker
from transformers import transformer_factory, TransformationPipeline
from transformers.streaming import ForeachBatch

logger = logging.getLogger(__name__)


def transform_data(transformation_pipeline: t.List[TransformationConfig], context: JobContext) -> TransformationPipeline:
    """
    Constructs the full transformation pipeline for a job.
    This involves:
    1. Injecting system-level transformers (like WatermarkTracker if auditing is available).
    2. Appending all user-defined transformers from the configuration.
    """
    trans_chain = TransformationPipeline()
    foreachbatch_options = _find_first_foreachbatch_options(transformation_pipeline)
    audit_manager = context.audit_manager
    # If auditing is available, the WatermarkTracker must be before all user-defined operations.
    if audit_manager and context.job_props_model.source_watermark:
        # Inherit checkpointing options of the first user-defined ForeachBatch
        foreachbatch_options = foreachbatch_options or {}
        trans_chain.add_transformer(WatermarkTracker(context), **merge_dicts(context.job_props, foreachbatch_options))

    # Build transformation pipeline
    for transform_cfg in transformation_pipeline:
        transformer = transformer_factory(transform_cfg.name)
        try:
            trans_chain.add_transformer(transformer, **merge_dicts(context.job_props, transform_cfg.options))
        except TypeError as e:
            transformer_logger = logging.getLogger(transformer.__class__.__name__)
            transformer_logger.exception(f"TypeError during transformer setup: {e}")
            raise e
        
    data_save_mode = context.job_props_model.data_save_mode
    # NOTE: Many sink adapters expect a static, batch DataFrame for a MERGE operation,
    # which is incompatible with an unbounded streaming source.
    # If a streaming pipeline is configured with a MERGE sink but lacks a ForeachBatch transformer,
    # this injects one automatically and converts the stream into compatible micro-batches.
    if data_save_mode == DataSaveMode.MERGE and foreachbatch_options is None:
        foreachbatch_options = {}
        trans_chain.add_transformer(ForeachBatch(), **context.job_props)

    return trans_chain


def write_data(dataset, t_chain: TransformationPipeline, context: JobContext):
    # ===================================================================
    # 1. Setup Phase: Configure the final write action in the pipeline
    # ===================================================================
    # This section prepares the `t_chain` by adding the appropriate sink
    # operation to the end of the pipeline. This operation will be called
    # for each DataFrame batch during the execution phase.
    writer = MicroBatchWriter(context.sink_adapter)
    audit_manager = context.audit_manager
    sink_table = context.job_props_model.get_sink_path()
    sink_table = dialect_insensitive_tbl_name(sink_table, writer.dialect)

    data_save_mode = context.job_props_model.data_save_mode
    if data_save_mode == DataSaveMode.OVERWRITE:
        t_chain.add(writer.replace_query, [sink_table],
                    [], context.job_props_model.replace_query_kwargs or {})
    elif data_save_mode == DataSaveMode.APPEND:
        t_chain.add(writer.insert_append, [sink_table])
    elif data_save_mode == DataSaveMode.MERGE:
        t_chain.add(writer.merge, [sink_table],
                    [context.job_props_model.sink_key],
                    {"when_matched_filter": context.job_props_model.when_matched_filter})
    elif data_save_mode == DataSaveMode.ERROR_IF_EXISTS:
        raise NotImplementedError(f"{DataSaveMode.ERROR_IF_EXISTS} not implemented.") # TODO
    else:
        raise NotImplementedError(f"{DataSaveMode.IGNORE} not implemented.") # TODO

    # ===================================================================
    # 2. Execution Phase: Determine the transactional strategy and run
    # ===================================================================
    # This block determines HOW the above pipeline is executed.
    data_load_mode = context.job_props_model.data_load_mode
    job_mode = context.job_props_model.job_mode
    if data_load_mode == DataLoadMode.FULL and job_mode == JobMode.BATCH:
        # For BATCH FULL Loads, wrap the entire loop in a single transaction for atomicity and performance.
        with context.sink_adapter.transaction():
            if audit_manager:
                with audit_manager.adapter.transaction():
                    _process_dataset_loop(dataset, t_chain, writer, context)
            else:
                _process_dataset_loop(dataset, t_chain, writer, context)
    else:
        # For all other modes, process data batch-by-batch:
        # - INCREMENTAL loads require per-batch commits to safely update state.
        # - STREAMING loads (both FULL and INCREMENTAL) are unbounded and must
        #   be processed as discrete micro-batches.
        _process_dataset_loop(dataset, t_chain, writer, context)

def _process_dataset_loop(
    dataset,
    t_chain: TransformationPipeline,
    writer: MicroBatchWriter,
    context: JobContext,
):
    job_mode = context.job_props_model.job_mode
    batch_size = context.job_props_model.source_batch_size
    is_streaming = (job_mode == JobMode.STREAMING)
    should_log_info = (not is_streaming) or (batch_size is not None and batch_size > 1)
    writer.should_log_info = should_log_info
    if is_streaming:
        if not should_log_info:
            logger.info(
                "Streaming job running with unbuffered micro-batches (`source_batch_size` <= 1 or not set). "
                "Demoting per-batch read/write logs to DEBUG level to prevent log flooding."
            )
        else:
            logger.info(
                f"Streaming job configured with `source_batch_size` = {batch_size:,}. "
                "Per-batch read/write logs will be logged at INFO level."
            )
    
    total_input_rows = 0
    batch_count = 0
    for batch_idx, df in enumerate(dataset, start=1):
        batch_count = batch_idx
        if not get_columns_name(df): # no column
            logger.warning(f"Batch {batch_idx}: Encountered DataFrame with no columns; skipping.")
            continue

        if isinstance(df, pd.DataFrame):
            count = len(df)
            total_input_rows += count
            msg = f"Batch {batch_idx}: Read {count:,} rows from source."
            if should_log_info:
                logger.info(msg)
            else:
                logger.debug(msg)

        t_chain.apply(df)

    if batch_count > 1 and total_input_rows > 0:
        logger.info(
            f"Job completed: {total_input_rows:,} total rows read, "
            f"{writer.total_written_rows:,} total rows written across {batch_count} batches."
        )

def _find_first_foreachbatch_options(
    transformation_pipeline: t.List[TransformationConfig]
) -> t.Dict[str, t.Any]:
    """
    Scans the user-defined transformation pipeline to find the first instance of a ForeachBatch transformer.
    """
    for transform_cfg in transformation_pipeline:
        transformer = transformer_factory(transform_cfg.name)
        if isinstance(transformer, ForeachBatch):
            return transform_cfg.options.copy()
    return None

class MicroBatchWriter:
    def __init__(
        self,
        base_writer: EngineAdapter,
        should_log_info: bool = True,
    ):
        self.base = base_writer
        self.dialect = self.base.dialect
        self.should_log_info = should_log_info
        self.batch_id = 0
        self.total_written_rows = 0
        self._columns_to_types = None

    def _log_output_rows(self, table_name: str, df: t.Any):
        if isinstance(df, pd.DataFrame):
            count = len(df)
            self.total_written_rows += count
            batch_num = self.batch_id + 1
            msg = f"Batch {batch_num}: Wrote {count:,} rows to '{table_name}'."
            if self.should_log_info:
                logger.info(msg)
            else:
                logger.debug(msg)

    def replace_query(self, table_name, df, *args, **kwargs):
        if self.batch_id == 0:
            if not is_batch_or_stream_empty(df):
                self.base.replace_query(table_name, df, self.get_columns_to_types(df, table_name), *args, **kwargs)
                self._log_output_rows(table_name, df)
            elif self.base.table_exists(table_name):
                self.base._truncate_table(table_name)
                logger.info(f"Batch {self.batch_id + 1}: Source was empty. Truncated target table '{table_name}'.")
        else:
            if not is_batch_or_stream_empty(df):
                self.base.insert_append(table_name, df, self.get_columns_to_types(df, table_name), *args)
                self._log_output_rows(table_name, df)

        self.batch_id += 1

    def insert_append(self, table_name, df, *args, **kwargs):
        if is_batch_or_stream_empty(df):
            logger.debug(f"Batch {self.batch_id + 1}: No records to append; skipping write.")
            return

        self.base.insert_append(table_name, df, self.get_columns_to_types(df, table_name), *args, **kwargs)
        self._log_output_rows(table_name, df)
        self.batch_id += 1

    def merge(self, target_table, df, key, when_matched_filter=None, *args, **kwargs):
        if is_batch_or_stream_empty(df):
            logger.debug(f"Batch {self.batch_id + 1}: No records to merge; skipping write.")
            return

        unique_key = [exp.column(k) for k in key]
        if when_matched_filter:
            columns_to_types = self.base._columns_to_types(df)
            when_matched = exp.Whens()
            when_matched.append(
                "expressions",
                exp.When(
                    matched=True,
                    source=False,
                    condition=exp.condition(when_matched_filter),
                    then=exp.Update(
                        expressions=[
                            exp.column(col, MERGE_TARGET_ALIAS).eq(
                                exp.column(col, MERGE_SOURCE_ALIAS)
                            )
                            for col in columns_to_types if col not in key
                        ],
                    ),
                ),
            )
        else:
            when_matched = None

        self.base.merge(target_table, df, self.get_columns_to_types(df, target_table), unique_key, when_matched=when_matched, *args, **kwargs)
        self._log_output_rows(target_table, df)
        self.batch_id += 1

    def get_columns_to_types(self, df, table_name: str = None) -> t.Dict[str, exp.DataType]:
        if self._columns_to_types is None:
            if table_name and self.base.table_exists(table_name):
                table_columns = self.base.columns(table_name)
                tbl_col_set = SQLIdentifierSet(table_columns)

                # Must match the order of columns in the data frame to map correctly
                self._columns_to_types = OrderedDict()
                df_columns = get_columns_name(df)
                for df_col in df_columns:
                    tbl_col = tbl_col_set.get(df_col)
                    if tbl_col in table_columns:
                        self._columns_to_types[tbl_col] = table_columns[tbl_col]
            else:
                self._columns_to_types = columns_to_types_from_df(df)
        return self._columns_to_types