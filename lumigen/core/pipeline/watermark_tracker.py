from __future__ import annotations
import typing as t
import logging
import json
import time
from collections import defaultdict
import pandas as pd
import pyspark.sql
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

if t.TYPE_CHECKING:
    from core.job_context import JobContext

from core.config.job import JobMode, DataSaveMode
from core.engine_adapter.mixins import WatermarkStrategyMixin
from transformers.streaming import ForeachBatch
from utils import SQLIdentifierSet
from utils.dataset import DataFrameConverter, get_columns_name, is_batch_or_stream_empty
from utils.json import DisplayableJSONEncoder
from utils.errors import UnsupportedDataFrameError, ConfigError

logger = logging.getLogger(__name__)


class WatermarkTracker(ForeachBatch):
    """
    A system-level transformer that enables stateful, incremental data processing.

    It intercepts each data batch to:

    1. Calculate the high watermark value from the batch.
    2. Atomically update the persistent state in the audit store AND execute the
       downstream user pipeline. This ensures that state is only advanced if the
       batch is successfully processed and written.

    As a subclass of `ForeachBatch`, it seamlessly handles both batch and
    Spark streaming DataFrames.
    """
    def __init__(self, context: JobContext, **kwargs):
        super(WatermarkTracker, self).__init__(**kwargs)
        self.context = context
        self.audit_manager = context.audit_manager
        if not self.audit_manager:
            raise ConfigError("WatermarkTracker cannot be initialized without an AuditManager.")

        self.reader = context.source_adapter
        self.job_props = context.job_props_model
        if not self.job_props.source_watermark:
            raise ConfigError("`source_watermark` is required when using WatermarkTracker.")

        # Fetch the last known high watermark once at the beginning of the job.
        self.watermark_exists = self.audit_manager.get_high_watermark_by_id(context.job_props_model.job_id) is not None
        self.high_watermark_max = self.audit_manager.get_validated_high_watermark(context, silent=False)
        
    def transform(self, df,
                  foreach_batch_func: t.Optional[t.Callable[[t.Any, int], None]] = None, 
                  *args, **kwargs):
        """
        Overrides the parent transform to ensure our custom, transaction-aware
        `foreach_batch_func_and_callback` is used by the execution engine.
        """
        return super().transform(df, foreach_batch_func=self.foreach_batch_func_and_callback, *args, **kwargs)
    
    def foreach_batch_func_and_callback(self, df: pyspark.sql.DataFrame, batch_id: int) -> None:
        """
        Defines the atomic unit of work for a batch.
        For Spark Streaming, this method is passed to Spark's `.foreachBatch()`.
        """
        t0 = time.perf_counter()
        is_spark = isinstance(df, pyspark.sql.DataFrame)
        if is_spark:
            if self.job_props.job_mode == JobMode.STREAMING:
                df.persist(StorageLevel.MEMORY_ONLY)
            else:
                df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            # Step 1: Calculate the potential new high watermark from the current batch.
            if isinstance(self.reader, WatermarkStrategyMixin):
                row_count = _get_row_count(df)
                batch_min_watermark, batch_max_watermark = None, None
                high_watermark_max_in_batch = self.reader.determine_high_watermark(
                    df, self.job_props.source_watermark, self.high_watermark_max
                )
            else:
                batch_metrics = _compute_batch_metrics(df, self.job_props.source_watermark)
                row_count = batch_metrics["count"]
                batch_min_watermark = batch_metrics["min"]
                batch_max_watermark = batch_metrics["max"]
                
                high_watermark_max_in_batch = batch_max_watermark
                if high_watermark_max_in_batch is not None and self.high_watermark_max is not None \
                    and high_watermark_max_in_batch <= self.high_watermark_max:
                    high_watermark_max_in_batch = None

            t1 = time.perf_counter()
            prev_high_watermark = self.high_watermark_max

            # Step 2: Perform state update and data processing.
            # We guarantee atomicity (using a transaction) for APPEND mode to ensure the watermark is not advanced if the downstream pipeline fails.
            # For other modes (like MERGE), we avoid a long-running transaction to prevent holding too many connection locks during expensive operations.
            if self.job_props.data_save_mode == DataSaveMode.APPEND:
                with self.audit_manager.adapter.transaction():
                    if high_watermark_max_in_batch is not None:
                        self.audit_manager.update_high_watermark(self.context, high_watermark_max_in_batch, self.watermark_exists)
                        # Update in-memory state for the next batch in this run.
                        self.watermark_exists = True
                        self.high_watermark_max = high_watermark_max_in_batch

                    # Apply all downstream transformations (including the sink write).
                    ret = self._apply_callbacks(df)
            else:
                # Apply transformations first
                ret = self._apply_callbacks(df)
                
                # Then update the watermark without a transaction block
                if high_watermark_max_in_batch is not None:
                    self.audit_manager.update_high_watermark(self.context, high_watermark_max_in_batch, self.watermark_exists)
                    self.watermark_exists = True
                    self.high_watermark_max = high_watermark_max_in_batch

            t2 = time.perf_counter()
            total_duration_sec = t2 - t0
            if high_watermark_max_in_batch is not None and self.job_props.job_id:
                progress_report = defaultdict(dict)
                if batch_id >= 0:
                    progress_report["batchId"] = batch_id
                progress_report["numInputRows"] = row_count
                progress_report["processedRowsPerSecond"] = round(row_count / total_duration_sec, 2) if total_duration_sec > 0 else 0.0

                if not isinstance(self.reader, WatermarkStrategyMixin):
                    progress_report["watermark"]["field"] = self.job_props.source_watermark
                    progress_report["watermark"]["batchRange"] = {
                        "min": batch_min_watermark,
                        "max": batch_max_watermark,
                    }

                progress_report["watermark"]["previousHigh"] = prev_high_watermark
                progress_report["watermark"]["currentHigh"] = self.high_watermark_max

                msg = f"Batch progress: {json.dumps(progress_report, cls=DisplayableJSONEncoder)}"
                is_streaming = (self.job_props.job_mode == JobMode.STREAMING)
                batch_size = self.job_props.source_batch_size
                should_log_info = (not is_streaming) or is_spark or (batch_size is not None and batch_size > 1)
                if should_log_info:
                    logger.info(msg)
                else:
                    logger.debug(msg)

            return ret
        finally:
            if is_spark:
                df.unpersist()

            # Release the audit connection so that no connections are held between batches.
            if self.audit_manager and self.audit_manager.adapter:
                self.audit_manager.adapter.close()

def _get_row_count(df: t.Any) -> int:
    """Helper to safely get count for DataFrames without computing column stats."""
    if is_batch_or_stream_empty(df):
        return 0
    if isinstance(df, pyspark.sql.DataFrame):
        return df.count()
    elif isinstance(df, pd.DataFrame):
        return len(df)
    else:
        raise UnsupportedDataFrameError(f"Unsupported DataFrame type: {type(df).__name__}")

def _compute_batch_metrics(df: t.Any, watermark_field: str) -> t.Dict[str, t.Any]:
    if is_batch_or_stream_empty(df):
        return {"count": 0, "min": None, "max": None}

    df_case_insens_cols = SQLIdentifierSet(get_columns_name(df))
    if watermark_field not in df_case_insens_cols:
        raise ValueError(f"Watermark field '{watermark_field}' not found in DataFrame.")
    canonical_watermark = df_case_insens_cols.get(watermark_field, watermark_field)

    if isinstance(df, pyspark.sql.DataFrame):
        stats = df.agg(
            F.count("*").alias("count"),
            F.min(F.col(canonical_watermark)).alias("min"),
            F.max(F.col(canonical_watermark)).alias("max"),
        ).collect()[0]
        return {
            "count": stats["count"],
            "min": stats["min"],
            "max": stats["max"],
        }
    elif isinstance(df, pd.DataFrame):
        count = len(df)
        if count == 0:
            return {"count": 0, "min": None, "max": None}

        min_val = df[canonical_watermark].min()
        max_val = df[canonical_watermark].max()
        # Convert to a standard Python type to ensure JSON serializability and consistency
        records = DataFrameConverter.to_records(
            pd.DataFrame({canonical_watermark: [min_val, max_val]})
        )
        return {
            "count": count,
            "min": records[0][canonical_watermark],
            "max": records[1][canonical_watermark],
        }
    else:
        raise UnsupportedDataFrameError(f"Unsupported DataFrame type for watermark calculation: {type(df).__name__}")