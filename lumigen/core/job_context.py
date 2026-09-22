from __future__ import annotations
import typing as t
from dataclasses import dataclass, field
import logging
from enum import Enum
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import EngineAdapter

if t.TYPE_CHECKING:
    from core.config.connection import ConnectionConfig

from core.config.root import Config
from core.config.job import JobPropertiesModel, DataLoadMode
from core.adapters import engine_adapter_factory
from core.audit import AuditManager

logger = logging.getLogger(__name__)


class RuntimeEnvironment(str, Enum):
    """Identifies the execution engine running the job."""
    PYTHON = "PYTHON"
    SPARK = "SPARK"
    SEATUNNEL = "SEATUNNEL"

    def __str__(self) -> str:
        return self.value

@dataclass
class JobContext:
    """
    An immutable container for the runtime context of a job.
    Holds all dynamic, stateful objects needed for execution.
    """
    runtime_environment: t.Optional[RuntimeEnvironment]
    job_props_model: JobPropertiesModel
    job_props: dict
    source_adapter: EngineAdapter
    sink_adapter: t.Optional[EngineAdapter] = None
    audit_manager: t.Optional[AuditManager] = None

    # Use a mutable object for transient state during the run
    _transient_state: t.Dict[str, t.Any] = field(default_factory=dict, repr=False)

    def build_source_expression(self) -> t.Union[exp.Select, str]:
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

        audit_adapter: EngineAdapter = self._transient_state.get("audit_adapter")
        try:
            if audit_adapter:
                audit_adapter.close()
                logger.debug("Audit store adapter closed successfully.")
        except Exception as e:
            logger.error("Error closing audit store adapter", exc_info=e)
    
class JobContextBuilder:
    """
    Constructs a complete and consistent JobContext for a job run or preview.
    This is the single source of truth for context creation.
    """
    def __init__(self,
                 config: Config,
                 runtime_environment: RuntimeEnvironment,
                 executor_override: t.Optional[EngineAdapter] = None,
                 ):
        self.config = config
        self.runtime_environment = runtime_environment
        self.executor = executor_override if executor_override is not None else self.config.executor

    def build(self, log_properties: bool = True) -> JobContext:
        """
        Returns a fully populated JobContext.
        """
        source_adapter = engine_adapter_factory(self.executor, self.config.source)
        
        sink_adapter = None
        if self.config.sink:
            sink_adapter = engine_adapter_factory(self.executor, self.config.sink)

        job_props_model = JobPropertiesModel(**self.config.job_properties)
        if log_properties:
            logger.info(f"Job properties: {job_props_model.model_dump_json(indent=4, exclude_none=True, exclude='seatunnel_env')}")

        audit_manager = None
        if self.config.audit_store:
            # Force 1 single connection for audit tasks
            audit_adapter = t.cast("ConnectionConfig", self.config.audit_store).create_engine_adapter(concurrent_tasks=1)
            try:
                audit_manager = AuditManager(audit_adapter=audit_adapter)
            except Exception as e:
                logger.warning(e)
                if job_props_model.data_load_mode == DataLoadMode.INCREMENTAL:
                    raise ValueError(
                        f"An 'audit_store' connection must be successful when `data_load_mode` is '{DataLoadMode.INCREMENTAL}': {e}."
                        f" For stateless jobs that do not require an 'audit_store', consider changing `data_load_mode` to '{DataLoadMode.FULL}'."
                    )

        job_props = job_props_model.model_dump(exclude_none=True, by_alias=False)

        # --- Transient State Setup ---
        # Prepare a dictionary for internal, temporary state needed for the duration
        # of the run, such as objects that require cleanup in JobContext.__exit__.
        transient_state = {"audit_adapter": audit_adapter if audit_manager else None}

        # --- Dynamic Context Injection ---
        # Inject live runtime objects into the job properties
        # This allows downstream transformers to access not just static config, but also stateful objects.
        job_props["source"] = source_adapter
        job_props["sink"] = sink_adapter

        return JobContext(
            runtime_environment=self.runtime_environment,
            job_props_model=job_props_model,
            job_props=job_props,
            source_adapter=source_adapter,
            sink_adapter=sink_adapter,
            audit_manager=audit_manager,
            _transient_state=transient_state,
        )