from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
import logging

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig

from utils.errors import ConfigError

logger = logging.getLogger(__name__)


class AdapterResolver(ABC):
    """
    Interface for a class that resolves an executor-specific engine adapter.
    """
    def __init__(self, executor_config: ConnectionConfig):
        self.executor_config = executor_config

    @abstractmethod
    def resolve(self, external_db_config: ConnectionConfig) -> t.Optional[EngineAdapter]:
        """
        Attempts to create a specialized adapter. Returns None if it cannot.
        """
        pass

ADAPTER_RESOLVER_REGISTRY: t.Dict[str, t.Type[AdapterResolver]] = {}

def register_adapter_resolver(executor_type: str, resolver_class: t.Type[AdapterResolver]):
    """
    A decorator or function to register a concrete AdapterResolver class
    with the central registry.
    """
    if executor_type in ADAPTER_RESOLVER_REGISTRY:
        logger.warning(
            f"AdapterResolver for executor '{executor_type}' is being overwritten."
        )
    logger.debug(f"Registering AdapterResolver '{resolver_class.__name__}' for executor '{executor_type}'.")
    ADAPTER_RESOLVER_REGISTRY[executor_type] = resolver_class


def engine_adapter_factory(
    executor_config: ConnectionConfig,
    external_db_config: t.Optional[ConnectionConfig] = None
) -> EngineAdapter:
    """
    Creates the most appropriate engine adapter for the given configuration.
    """
    # Case 1: No external DB, just return the executor's adapter.
    if external_db_config is None:
        if executor_config:
            return executor_config.create_engine_adapter()
        raise ConfigError("Cannot create engine adapter: No executor or external connection was provided.")
    
    # Case 2: Try to find a specialized resolver for the given executor.
    if executor_config:
        resolver_class = ADAPTER_RESOLVER_REGISTRY.get(executor_config.type_)
        if resolver_class:
            resolver = resolver_class(executor_config)
            specialized_adapter = resolver.resolve(external_db_config)
            
            if specialized_adapter:
                logger.debug(f"Resolved specialized adapter '{specialized_adapter.__class__.__name__}'.")
                return specialized_adapter
            
            logger.debug(f"Executor '{executor_config.type_}' could not resolve a special adapter for '{external_db_config.type_}'. Falling back.")

    # Case 3 (Fallback): Use the default adapter for the external database.
    logger.debug(f"Using default adapter for connection type '{external_db_config.type_}'.")
    return external_db_config.create_engine_adapter()
    

def validate_connections(connections: t.List[t.Optional[ConnectionConfig]]):
    """
    Validates that a connection can be established for each configuration.
    This is a core pre-flight check for any job.
    """
    for conn in connections:
        if conn is None:
            continue
        
        conn_logger = logging.getLogger(conn.__class__.__name__)
        conn_logger.info(f"Validating {conn.type_} connection at {conn.get_address()}")
        conn.connection_validator()()
        conn_logger.info(f"Connected to {conn.type_} at {conn.get_address()} successfully")