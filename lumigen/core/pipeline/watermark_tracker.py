from __future__ import annotations
import typing as t
import logging
import pandas as pd
import pyspark.sql
from pyspark.storagelevel import StorageLevel

if t.TYPE_CHECKING:
    from core.job_context import JobContext

from core.config.job import JobMode
from core.engine_adapter.mixins import WatermarkStrategyMixin
from transformers.streaming import ForeachBatch
from utils import FallbackSet
from utils.dataset import DataFrameConverter, get_columns_name, is_batch_or_stream_empty
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
        self.watermark_exists = self.audit_manager.get_high_watermark_by_id(context.job_props_model.job_id) != None
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
        is_spark = isinstance(df, pyspark.sql.DataFrame)
        if is_spark:
            if self.job_props.job_mode == JobMode.STREAMING:
                df.persist(StorageLevel.MEMORY_ONLY)
            else:
                df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            # Step 1: Calculate the potential new high watermark from the current batch.
            if isinstance(self.reader, WatermarkStrategyMixin):
                high_watermark_max_in_batch = self.reader.determine_high_watermark(df, self.job_props.source_watermark, self.high_watermark_max)
            else:
                high_watermark_max_in_batch = _calculate_max_watermark(df, self.job_props.source_watermark)
                if high_watermark_max_in_batch and self.high_watermark_max and high_watermark_max_in_batch <= self.high_watermark_max:
                    high_watermark_max_in_batch = None

            # Step 2: Perform state update and data processing within a single transaction.
            # This guarantees that if the downstream pipeline fails, the watermark is NOT advanced.
            with self.audit_manager.adapter.transaction():
                if high_watermark_max_in_batch is not None:
                    self.audit_manager.update_high_watermark(self.context, high_watermark_max_in_batch, self.watermark_exists)
                    # Update in-memory state for the next batch in this run.
                    self.high_watermark_max = high_watermark_max_in_batch

                # Step 3: Apply all downstream transformations (including the sink write).
                return self._apply_callbacks(df)
        finally:
            if is_spark:
                df.unpersist()

def _calculate_max_watermark(df, watermark_field: str) -> t.Any:
    if is_batch_or_stream_empty(df):
        return None

    df_case_insens_cols = FallbackSet(get_columns_name(df))
    if watermark_field not in df_case_insens_cols:
        raise ValueError(
            f"Watermark field '{watermark_field}' not found. "
        )
    watermark_field = df_case_insens_cols.get(watermark_field, watermark_field)
    
    if isinstance(df, pyspark.sql.DataFrame):
        return df.agg({watermark_field: "max"}).collect()[0][0]
    elif isinstance(df, pd.DataFrame):
        high_watermark = df[watermark_field].max()
        # Convert to a standard Python type to ensure JSON serializability and consistency.
        high_watermark = DataFrameConverter.to_records(
            pd.DataFrame({watermark_field: [high_watermark]})
        )[0][watermark_field]
        return high_watermark
    else:
        raise UnsupportedDataFrameError(f"Unsupported DataFrame type for watermark calculation: {type(df).__name__}")