import typing as t
import tempfile
import shutil
import pyspark.sql
import pandas as pd
import logging

from transformers.base import AbstractTransformer, AbstractTransformerDecorator
from utils.errors import UnsupportedDataFrameError, TransformerError

logger = logging.getLogger(__name__)


class ForeachBatch(AbstractTransformerDecorator):
    kwargs: t.Optional[t.Dict[str, t.Any]] = None

    def transform(self,
                  df,
                  foreach_batch_func: t.Optional[t.Callable[[t.Any, int], None]] = None,
                  checkpoint_location: t.Optional[str] = None,
                  trigger_interval: t.Optional[int] = 5,
                  *args, **kwargs):
        """
        Terminal operation for streaming data frames.
        Call a custom function in every micro-batch and start the streaming computation.
        This method DOES NOT apply downstream callbacks itself. Instead, it
        injects the callback-aware function into `foreachBatch` function.

        Parameters
        ----------
        df: A data frame.
        foreach_batch_func: function to process every micro-batch. Must have 2 parameters: the data itself and the batch identifier.
        checkpoint_location: the location to store checkpoint information in the file system (e.g. HDFS)
        trigger_interval: a processing time interval in second(s).

        Returns
        -------
        None.
        """
        if foreach_batch_func is None:
            foreach_batch_func = self.foreach_batch_func
        df = super().transform(df, *args, **kwargs)

        self.kwargs = kwargs
        if not isinstance(df, pyspark.sql.DataFrame):
            # For non-Spark DataFrames, just run the function once as a batch.
            return foreach_batch_func(df, batch_id=0)
        
        if not df.isStreaming:
            # For non-streaming Spark DataFrames, run once as a batch.
            return foreach_batch_func(df, batch_id=0)

        temp_dir_to_clean = None
        if not checkpoint_location:
            try:
                temp_dir_to_clean = tempfile.mkdtemp(prefix="spark-tmp-ckpt_")
                checkpoint_location = f"file://{temp_dir_to_clean}"
                logger.info(f"No checkpointLocation provided. Created temporary checkpoint directory: {checkpoint_location}")
            except Exception as e:
                logger.error("Failed to create temporary checkpoint directory.", exc_info=True)
                raise TransformerError("Could not create a temporary directory for Spark checkpointing.") from e
            
        try:
            writer = df.writeStream
            writer = writer.option("checkpointLocation", checkpoint_location)
            query = (
                writer.outputMode("append")
                    .trigger(processingTime=f"{trigger_interval} seconds")
                    .foreachBatch(foreach_batch_func)
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
        return None
    
    def foreach_batch_func(self, df, batch_id: int) -> t.Any:
        return df

    def foreach_batch_func_and_callback(self, df, batch_id: int) -> None:
        df = self.foreach_batch_func(df, batch_id)
        
        return self._apply_callbacks(df)

    def transform_and_callback(self, df, *args, **kwargs):
        return self.transform(df, foreach_batch_func = self.foreach_batch_func_and_callback, *args, **kwargs)

    def chain(self, other: AbstractTransformer, *transform_args, **transform_kwargs) -> AbstractTransformer:
        super().chain(other, *transform_args, **transform_kwargs)
        return self