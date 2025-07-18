from __future__ import annotations
import typing as t

import six
from abc import ABCMeta, abstractmethod
import pyspark.sql as pyspark_sql
import pandas as pd
from utils.errors import TransformerError


# Must be tuple
SUPPORTED_DATA_FRAME_CLASSES = (pyspark_sql.DataFrame, pd.DataFrame)

@six.add_metaclass(ABCMeta)
class AbstractTransformer(object):
    """
    Represents a single, atomic transformation step.
    
    This class supports a callback mechanism, allowing subsequent transformations
    to be chained, which is essential for terminal operations like PySpark's
    `foreachBatch` that do not return a DataFrame.
    """
    def __init__(self):
        self.callbacks: t.List[t.Callable[[t.Any], t.Any]] = []

    @abstractmethod
    def transform(self, df, *args, **kwargs):
        """The core transformation logic. Must be implemented by subclasses."""
        pass

    def _apply_callbacks(self, df: t.Any) -> t.Any:
        """Applies all registered downstream callbacks in sequence."""
        for cb in self.callbacks:
            if not isinstance(df, SUPPORTED_DATA_FRAME_CLASSES):
                raise TransformerError(
                    f"{self.__class__.__name__}: Cannot apply callbacks on unsupported data frame type {type(df)}."
                )

            df = cb(df)
        return df

    def transform_and_callback(self, df, *args, **kwargs):
        """Executes the transform and then any chained callbacks."""
        df = self.transform(df, *args, **kwargs)
        
        return self._apply_callbacks(df)
    
    def add_callback(self, callback_fn: t.Callable[[t.Any], t.Any]) -> None:
        """Registers a simple callable to be executed after this transform."""
        self.callbacks.append(callback_fn)

    def chain(self, other: AbstractTransformer, *transform_args, **transform_kwargs) -> AbstractTransformer:
        """
        Chains another transformer to run after this one.
        
        This is the primary mechanism for building a pipeline.
        
        Returns:
            The `other` instance, which becomes the new end of the chain.
        """
        def transform_wrapper(df):
            return other.transform_and_callback(df, *transform_args, **transform_kwargs)
        
        self.add_callback(transform_wrapper)
        return other

@six.add_metaclass(ABCMeta)
class AbstractTransformerDecorator(AbstractTransformer):
    """A transformer that decorates/wraps a preceding transformer."""
    _pre_transformer: AbstractTransformer = None

    def __init__(self, pre_transformer: t.Optional[AbstractTransformer] = None):
        super(AbstractTransformerDecorator, self).__init__()
        self._pre_transformer = pre_transformer

    @property
    def pre_transformer(self) -> t.Optional[AbstractTransformer]:
        return self._pre_transformer
    
    @abstractmethod
    def transform(self, df, *args, **kwargs):
        if self._pre_transformer is not None:
            return self._pre_transformer.transform(df, *args, **kwargs)
        return df
    
class ExampleTransformer(AbstractTransformerDecorator):
    def transform(self, df, *args, **kwargs):
        df = super().transform(df, *args, **kwargs)

        pass # Do some transformations here

        return df
    
class IdentityTransformer(AbstractTransformerDecorator):
    """
    A transformer that performs no operation on the DataFrame.
    It simply passes the input DataFrame through to the output.
    """
    def transform(self, df, *args, **kwargs):
        df = super().transform(df, *args, **kwargs)
        return df