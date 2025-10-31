from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, SparkConnectionConfig


class SparkAdapterConfigurator(ABC):
    """
    Abstract Base Class for a "plug-in" that configures a Spark Session and
    creates a specialized Engine Adapter for a specific external data source.
    """

    @abstractmethod
    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: ConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        """
        If this configurator can handle the `external_db_config` type, it will create and return a specialized Spark Engine Adapter.
        If it cannot handle the config type, it must return None.

        Args:
            spark_config: The Spark executor's configuration object.
            external_db_config: The configuration for the external data source.

        Returns:
            A specialized EngineAdapter instance (e.g., SparkKafkaEngineAdapter)
            or None if this configurator is not applicable.
        """
        pass