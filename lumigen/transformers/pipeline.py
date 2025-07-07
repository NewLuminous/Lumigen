import typing as t
from transformers.base import AbstractTransformer, IdentityTransformer
from utils.errors import TransformerError, InvalidOperationError


class TransformationPipeline:
    def __init__(self):
        self._head: t.Optional[t.Callable[[t.Any], t.Any]] = None
        self._tail: t.Optional[AbstractTransformer] = None

    def add_transformer(self, transformer: AbstractTransformer, *transform_args, **transform_kwargs) -> None:
        def transform_wrapper(df):
            return transformer.transform_and_callback(df, *transform_args, **transform_kwargs)

        if self._head is None:
            self._head = transform_wrapper
            self._tail = transformer
        else:
            # Chain the new transformer to the current tail.
            self._tail = self._tail.chain(transformer, *transform_args, **transform_kwargs)
            if self._tail is None:
                raise TransformerError(f"`chain` of {type(transformer)} must return a transformer object.")
            
        return self
            
    def add(self, transform_fn: t.Callable, pre_df_args: t.Optional[list] = [], post_df_args: t.Optional[list] = [], kwargs: t.Optional[dict] = {}):
        def transform_wrapper(df):
            return transform_fn(*pre_df_args, df, *post_df_args, **kwargs)
        
        if self._tail is None:
            # If the pipeline is empty, we must start it with a transformer
            # so that we have an object to attach the callback to.
            self.add_transformer(IdentityTransformer())
        self._tail.add_callback(transform_wrapper)

        return self

    def apply(self, df) -> None:
        """
        Applies the entire transformation pipeline to a DataFrame.
        """
        if self._head is None:
            raise InvalidOperationError("Cannot apply empty transformation chain.")
        
        self._head(df)