from __future__ import annotations
import typing as t
import logging
import tempfile
import shutil
from functools import partial

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery

from engines.spark.spark_session import SparkSessionCursor

logger = logging.getLogger(__name__)


class SparkStreamToBatchCursor(SparkSessionCursor):
    """
    A specialized Spark cursor that can adapt a batch DataFrame operation
    to a streaming DataFrame source.
    
    This acts as a bridge, allowing batch-only sinks (like JDBC) to be used
    with streaming DataFrames by leveraging Spark's `foreachBatch` functionality.
    """

    def execute(self, query: str,
                df: t.Optional[DataFrame] = None,
                batch_id: int = 0,          # An internal ID provided by `foreachBatch` for each micro-batch.
                self_call: bool = False,    # A flag to prevent infinite recursion.
                trigger_interval: t.Optional[int] = 1,
                **kwargs) -> None:
        # --- Base Case 1: No DataFrame provided or it's a batch DataFrame ---
        # If df is None, it means this is a read operation, not a write.
        # If self_call is True, it means we are processing a micro-batch
        if df is None or self_call:
            # We only want to execute the read query once at the start.
            if batch_id == 0:
                return super().execute(query)
            else:
                return None

        # --- Base Case 2: The DataFrame is already a batch DataFrame ---
        # If the input is not a streaming DataFrame, we can execute the batch
        # logic directly. The `self_call=True` flag ensures we hit the base case above
        # and avoid an infinite loop.
        if not df.isStreaming:
            # --- DELEGATION TO SUBCLASS IMPLEMENTATION ---
            # This is the crucial delegation point. The `self.execute` call here will invoke the
            # `execute` method of the concrete subclass (e.g., `SparkJDBCCursor`).
            #
            # Spark's `foreachBatch` function requires the callable it receives to accept
            # two positional arguments: a DataFrame (the micro-batch) and a batch_id (an integer).
            # Our use of `partial` in the streaming case effectively creates a function like:
            #   `lambda batch_df, batch_id: self.execute(query, batch_df, batch_id, **kwargs)`
            #
            # To ensure that both the direct batch call (here) and the `foreachBatch` call
            # work seamlessly, any subclass that overrides this method MUST have an `execute`
            # signature that can handle these arguments.
            #
            # The simplest compatible signature is:
            #   `execute(self, query: str, df: DataFrame, batch_id: int = 0, **kwargs)`
            return self.execute(query, df, batch_id=batch_id, self_call=True, **kwargs)

        # --- Recursive/Streaming Case: The DataFrame is a stream ---
        # This is the core logic of the adapter. We are given a streaming DataFrame
        # but need to apply a batch operation (defined by the subclass's implementation).
        checkpoint_location = self._spark.conf.get("spark.sql.streaming.checkpointLocation", None)
        temp_dir_to_clean = None
        if not checkpoint_location:
            try:
                temp_dir_to_clean = tempfile.mkdtemp(prefix="spark-tmp-ckpt_")
                checkpoint_location = f"file://{temp_dir_to_clean}"
                logger.info(
                    f"No spark.sql.streaming.checkpointLocation configured. "
                    f"Using temporary checkpoint for this write: {checkpoint_location}"
                )
            except Exception as e:
                logger.error("Failed to create temporary checkpoint directory.", exc_info=True)
                raise RuntimeError("Could not create a temporary directory for Spark checkpointing.") from e
            
        try:
            writer = df.writeStream
            writer = writer.option("checkpointLocation", checkpoint_location)

            # `foreachBatch` is the key function. It takes a function that will be
            # executed for each micro-batch from the stream. The micro-batch itself
            # is passed as a static DataFrame to that function.
            #
            # We use `partial(self.execute, query, **kwargs)` to create a callable.
            # This is equivalent to `lambda batch_df, batch_id: self.execute(query, batch_df, batch_id, **kwargs)`
            query: StreamingQuery = (
                writer.outputMode("append")
                    .trigger(processingTime=f"{trigger_interval} seconds")
                    .foreachBatch(partial(self.execute, query, **kwargs))
                    .start()
            )
            logger.info(f"Spark streaming query '{query.name}' started. Awaiting termination...")
            query.awaitTermination()
            logger.info(f"Spark streaming query '{query.name}' terminated normally.")
        finally:
            if temp_dir_to_clean:
                try:
                    logger.info(f"Cleaning up temporary checkpoint directory: {temp_dir_to_clean}")
                    shutil.rmtree(temp_dir_to_clean)
                    logger.debug("Temporary directory successfully removed.")
                except Exception as e:
                    logger.warning(
                        f"Failed to clean up temporary checkpoint directory: {temp_dir_to_clean}. "
                        f"Manual cleanup may be required. Error: {e}"
                    )