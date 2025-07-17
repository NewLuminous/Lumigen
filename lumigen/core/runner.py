from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
import logging
from core.config.root import Config
from core.config.connection import SparkConnectionConfig, SeaTunnelConnectorConfig
from core.adapters import validate_connections
from core.execution import transform_data, write_data
from core.job_context import JobContext, JobContextBuilder

logger = logging.getLogger(__name__)


class JobRunner(ABC):
    """
    Abstract base class for a job execution strategy, using the Template Method pattern.
    The `run` method defines the skeleton of the ETL job, and subclasses
    implement the specific steps (hooks).
    """
    def __init__(self, config: Config):
        self.config = config
        self.context_builder = JobContextBuilder(config)

    def run(self) -> t.Optional[int]:
        """
        The main execution method for the runner. This is the template.
        It orchestrates the entire ETL flow.
        """
        validate_connections([self.config.source, self.config.sink])
        with self.context_builder.build() as job_context:
            lazy_dataset = self._read_data(job_context)
            trans_chain = transform_data(self.config.transformation_pipeline, job_context.job_props)
            write_data(lazy_dataset, trans_chain, job_context.sink_adapter, job_context.job_props)
        return 0

    @abstractmethod
    def _read_data(self, context: JobContext) -> t.Iterator[t.Any]:
        """Hook for reading data in the engine-specific format."""
        pass
    

# Factory to create the appropriate runner
def get_runner(config: Config) -> JobRunner:
    """Factory function to instantiate the correct JobRunner based on config."""
    if (isinstance(config.source, SeaTunnelConnectorConfig) or
        isinstance(config.sink, SeaTunnelConnectorConfig) or
        config.job_properties.get("seatunnel_transform") or
        config.job_properties.get("seatunnel_env")):
        logger.info("SeaTunnel configuration found. Setting SeaTunnel as the main engine...")
        from entry_points.runners.seatunnel_runner import SeaTunnelJobRunner
        return SeaTunnelJobRunner(config)
    
    if isinstance(config.executor, SparkConnectionConfig):
        from entry_points.runners.spark_runner import SparkJobRunner
        logger.info("Spark executor configured. Using SparkJobRunner.")
        return SparkJobRunner(config)
    
    from entry_points.runners.python_runner import PythonJobRunner
    logger.info("Defaulting to standard Python runner.")
    return PythonJobRunner(config)