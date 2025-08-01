from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
from sqlglot import Dialect, exp
import logging

if t.TYPE_CHECKING:
    from core.job_context import RuntimeEnvironment

from core.config.root import Config
from core.config.connection import SparkConnectionConfig, SeaTunnelConnectorConfig
from core.config.job import DataLoadMode
from core.engine_adapter.mixins import QueryBuilderMixin
from core.adapters import validate_connections
from core.execution import transform_data, write_data
from core.job_context import JobContext, JobContextBuilder
from core.dialect import transform_value
from utils.sql import parse_into_select_exp
from utils.errors import ConfigError

logger = logging.getLogger(__name__)


class JobRunner(ABC):
    """
    Abstract base class for a job execution strategy, using the Template Method pattern.
    The `run` method defines the skeleton of the ETL job, and subclasses
    implement the specific steps (hooks).
    """
    @property
    @abstractmethod
    def runtime_environment(self) -> RuntimeEnvironment:
        pass

    def __init__(self, config: Config):
        self.config = config
        self.context_builder = JobContextBuilder(config, self.runtime_environment)

    def run(self) -> t.Optional[int]:
        """
        The main execution method for the runner. This is the template.
        It orchestrates the entire ETL flow.
        """
        validate_connections([self.config.source, self.config.sink])
        with self.context_builder.build() as job_context:
            lazy_dataset = self._read_data(job_context)
            trans_chain = transform_data(self.config.transformation_pipeline, job_context)
            write_data(lazy_dataset, trans_chain, job_context)
        return 0
    
    def _get_source_query(self, context: JobContext) -> t.Any:
        reader = context.source_adapter
        job_props = context.job_props_model

        last_watermark = None
        if job_props.data_load_mode == DataLoadMode.INCREMENTAL:
            last_watermark = context.audit_manager.get_validated_high_watermark(context, silent=True)

        if isinstance(reader, QueryBuilderMixin):
            # The adapter knows best how to build its query.
            logger.debug(f"Adapter {reader.__class__.__name__} implements {QueryBuilderMixin.__name__}. Delegating query construction.")
            return reader.build_query(
                path=job_props.source_path,
                query=job_props.source_query,
                data_load_mode=job_props.data_load_mode,
                watermark_column=job_props.source_watermark,
                last_watermark_value=last_watermark,
            )
        
        if Dialect.get(reader.dialect):
            logger.debug(f"Adapter dialect '{reader.dialect}' is a known SQL dialect. Using generic expression builder.")
            base_query = context.build_source_expression()
            if job_props.data_load_mode == DataLoadMode.INCREMENTAL:
                if isinstance(base_query, str):
                    base_query: exp.Select = parse_into_select_exp(base_query, read=reader.dialect)

                if not job_props.source_watermark:
                    raise ValueError(f"`source_watermark` must be provided when `data_load_mode` is '{DataLoadMode.INCREMENTAL}'.")
                watermark_column = exp.to_column(job_props.source_watermark)

                query = exp.select("*").from_(base_query.subquery(alias="foo"))
                if last_watermark:
                    watermark_literal = transform_value(last_watermark)
                    query = query.where(exp.GT(this=watermark_column, expression=watermark_literal))

                # Order by the watermark column for reliable batching.
                if job_props.source_batch_size and self.requires_sequential_watermarks:
                        query = query.order_by(watermark_column.asc())
                return query

            return base_query

        error_message = (
            f"Engine Adapter '{reader.__class__.__name__}' has a non-standard dialect '{reader.dialect}' "
            f"but does not implement the 'QueryBuilderMixin' to handle it."
        )
        raise ConfigError(error_message)
    
    @property
    def requires_sequential_watermarks(self) -> bool:
        """
        Indicates if this runner, when performing an incremental load,
        processes data in sequential chunks. If True, source data must be
        ordered by the watermark column to ensure state correctness.
        """
        return True # Default for cursor-based runners like Python's.

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
        from runners.seatunnel_runner import SeaTunnelJobRunner
        return SeaTunnelJobRunner(config)
    
    if isinstance(config.executor, SparkConnectionConfig):
        from runners.spark_runner import SparkJobRunner
        logger.info("Spark executor configured. Using SparkJobRunner.")
        return SparkJobRunner(config)
    
    from runners.python_runner import PythonJobRunner
    logger.info("Defaulting to standard Python runner.")
    return PythonJobRunner(config)