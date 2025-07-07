import typing as t
import pyspark
import pandas as pd
from abc import abstractmethod

from transformers.base import AbstractTransformer, AbstractTransformerDecorator
from utils.errors import UnsupportedDataFrameError, TransformerError


class ForeachBatch(AbstractTransformerDecorator):
    kwargs: t.Optional[t.Dict[str, t.Any]] = None

    def transform(self,
                  df,
                  foreach_batch_func: t.Optional[t.Callable[[t.Any, int], None]] = None,
                  checkpoint_location: t.Optional[str] = None,
                  trigger_interval: t.Optional[int] = 0,
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
        if isinstance(df, pyspark.sql.DataFrame):
            if df.isStreaming:
                writer = df.writeStream
                if checkpoint_location:
                    writer = writer.option("checkpointLocation", checkpoint_location)
                    
                writer.outputMode("append") \
                    .trigger(processingTime=f"{trigger_interval} seconds") \
                    .foreachBatch(foreach_batch_func) \
                    .start() \
                    .awaitTermination()
            else:
                # For non-streaming, just run it once.
                foreach_batch_func(df, batch_id=0)
        else:
            foreach_batch_func(df, batch_id=0)

        # This transformer is terminal, it returns None, stopping the chain.
        return None
    
    #@abstractmethod
    def foreach_batch_func(self, df, batch_id: int) -> t.Any:
        return df

    def foreach_batch_func_and_callback(self, df, batch_id: int) -> None:
        df = self.foreach_batch_func(df, batch_id)
        
        return self._apply_callbacks(df)

    def transform_and_callback(self, df, *args, **kwargs):
        self.transform(df, foreach_batch_func = self.foreach_batch_func_and_callback, *args, **kwargs)

    def chain(self, other: AbstractTransformer, *transform_args, **transform_kwargs) -> AbstractTransformer:
        super().chain(other, *transform_args, **transform_kwargs)
        return self