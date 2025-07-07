import typing as t
from functools import lru_cache
import transformers
from transformers.base import AbstractTransformerDecorator
from utils.errors import ConfigError
from utils.modules import find_subclasses_in_package


@lru_cache(maxsize=1)
def get_transformer_registry() -> t.Dict[str, t.Type[AbstractTransformerDecorator]]:
    """
    Discovers and returns a dictionary of available transformers by
    walking the 'transformers' package.
    """
    # This is much cleaner and more robust!
    classes = find_subclasses_in_package(transformers, AbstractTransformerDecorator)
    return {c.__name__: c for c in classes}

# REGISTER TRANSFORMER NAME HERE
TRANSFORMER_ALIASES = {
}


def transformer_factory(
    transformer_name: str, pre_transformer: t.Optional[AbstractTransformerDecorator]=None, **kwargs: t.Any
) -> AbstractTransformerDecorator:
    """
    Instantiates a transformer by name.
    """
    registry = get_transformer_registry()
    canonical_name = TRANSFORMER_ALIASES.get(transformer_name, transformer_name)
    transformer_class = registry.get(canonical_name)
    if transformer_class is None:
        raise ConfigError(f"Cannot find tranformer with name '{transformer_name}'. Check if transformer with name '{transformer_name}' was registered properly.")
    
    return transformer_class(pre_transformer, **kwargs)