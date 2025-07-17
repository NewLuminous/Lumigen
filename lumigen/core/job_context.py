from __future__ import annotations
import typing as t
from dataclasses import dataclass
import logging
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import EngineAdapter

from core.config.root import Config
from core.config.job import JobPropertiesModel
from core.adapters import engine_adapter_factory

logger = logging.getLogger(__name__)


@dataclass
class JobContext:
    """
    An immutable container for the runtime context of a job.
    Holds all dynamic, stateful objects needed for execution.
    """
    job_props_model: JobPropertiesModel
    job_props: dict
    source_adapter: EngineAdapter
    sink_adapter: t.Optional[EngineAdapter] = None

    def build_source_expression(self) -> t.Union[exp.Expression, str]:
        return self.job_props_model.build_source_expression(dialect=self.source_adapter.dialect)
    
    def __enter__(self) -> "JobContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Closing all resources in JobContext...")
        
        try:
            if self.source_adapter:
                self.source_adapter.close()
                logger.debug("Source engine adapter closed successfully.")
        except Exception as e:
            logger.error("Error closing source engine adapter", exc_info=e)

        try:
            if self.sink_adapter:
                self.sink_adapter.close()
                logger.debug("Sink engine adapter closed successfully.")
        except Exception as e:
            logger.error("Error closing sink engine adapter", exc_info=e)
    
class JobContextBuilder:
    """
    Constructs a complete and consistent JobContext for a job run or preview.
    This is the single source of truth for context creation.
    """
    def __init__(self, config: Config, executor_override: t.Optional[EngineAdapter] = None):
        self.config = config
        self.executor = executor_override if executor_override is not None else self.config.executor

    def build(self) -> JobContext:
        """
        Returns a fully populated JobContext.
        """
        source_adapter = engine_adapter_factory(self.executor, self.config.source)
        
        sink_adapter = None
        if self.config.sink:
            sink_adapter = engine_adapter_factory(self.executor, self.config.sink)

        job_props_model = JobPropertiesModel(**self.config.job_properties)
        job_props = job_props_model.model_dump(exclude_none=True, by_alias=False)

        # --- Dynamic Context Injection ---
        # Inject live runtime objects into the job properties
        # This allows downstream transformers to access not just static config, but also stateful objects.
        job_props["source"] = source_adapter
        job_props["sink"] = sink_adapter

        return JobContext(
            job_props_model=job_props_model,
            job_props=job_props,
            source_adapter=source_adapter,
            sink_adapter=sink_adapter,
        )