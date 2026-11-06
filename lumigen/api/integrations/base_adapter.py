from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
import logging
import threading
from sqlglot import exp, Dialects

if t.TYPE_CHECKING:
    from core.config.root import Config
    from core.config.connection import SparkConnectionConfig

from core.config.job import JobMode
from core.engine_context import SQLContextManager, SparkStatus
from utils.helpers import get_system_timezone
from api.config import settings
from api.enums import JobStatus
from api.schemas import JobStatusResponse, JobInfoResponse, JobMetricsResponse
from api.services.job_previewer import JobPreviewerBase
import transformers

logger = logging.getLogger(__name__)


class JobManagerAdapter(JobPreviewerBase, ABC):
    """
    Abstract base class for job management adapters.
    Defines the interface that all adapters (Docker, Kubernetes, etc.) must implement.
    """
    _submission_lock = threading.Lock()
    _active_submissions = set()

    def submit_job(self, config: Config, id: str, name: t.Optional[str] = None) -> JobStatusResponse:
        with self._submission_lock:
            if id in self._active_submissions:
                logger.info(f"Race condition prevented: Job '{id}' is already being submitted by another thread.")
                return JobStatusResponse(
                    job_id=id,
                    job_name=name,
                    status=JobStatus.INITIALIZING,
                    message="Job submission is already in progress."
                )
            self._active_submissions.add(id)

        try:
            prepared_config = self._prepare_config_for_submission(config)
            return self._perform_submit(config=prepared_config, id=id, name=name)
        finally:
            with self._submission_lock:
                self._active_submissions.discard(id)

    @abstractmethod
    def _perform_submit(self, config: Config, id: str, name: t.Optional[str] = None) -> JobStatusResponse:
        pass

    @abstractmethod
    def stop_job(self, id: str) -> JobStatusResponse:
        pass

    @abstractmethod
    def delete_job(self, id: str) -> JobStatusResponse:
        pass

    @abstractmethod
    def get_job_status(self, id: str) -> JobStatus:
        pass

    @abstractmethod
    def get_job_info(self, id: str) -> JobInfoResponse:
        pass

    def get_batch_job_info(self, ids: t.List[str]) -> t.Dict[str, JobInfoResponse]:
        """
        Default implementation: Loop over get_job_info for each ID.
        Subclasses (like Kubernetes) should override this to perform optimized bulk queries.
        """
        return {job_id: self.get_job_info(job_id) for job_id in ids}

    @abstractmethod
    def get_job_metrics(self, id: str) -> JobMetricsResponse:
        pass

    @abstractmethod
    def list_jobs(self, sparse: t.Optional[bool] = True, name: t.Optional[str] = None) -> t.List[t.Dict[str, t.Any]]:
        pass

    @abstractmethod
    def get_job_logs(self, id: str,
                     tail: t.Optional[int] = None, since: t.Optional[int] = None,
                     stdout: t.Optional[bool] = True, stderr: t.Optional[bool] = True,
                     timestamps: t.Optional[bool] = False) -> t.Optional[str]:
        pass

    def _prepare_config_for_submission(self, config: Config) -> Config:
        if config.get_executor() and config.get_executor().type_ == "spark":
            executor: SparkConnectionConfig = config.executor
            executor.log_level = settings.SPARK_LOG_LEVEL

            options: dict = config.job_properties.setdefault("spark_submit_options", {})
            options.setdefault("master", settings.SPARK_MASTER)
            conf: t.List[str] = options.setdefault("conf", [])
            existing_keys = {c.split('=', 1)[0] for c in conf if '=' in c}

            if settings.SPARK_EVENT_LOG_ENABLED:
                defaults = {
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.dir": settings.SPARK_EVENT_LOG_DIR,
                    "spark.eventLog.compress": "true",
                }
                for key, val in defaults.items():
                    if key not in existing_keys:
                        conf.append(f"{key}={val}")

            if t.cast("str", options["master"]).startswith("k8s"):
                # Get the timezone from the host/API container
                tz_val = get_system_timezone()

                k8s_defaults = {
                    "spark.driver.port": 0,
                    "spark.driver.bindAddress": "0.0.0.0",
                    "spark.kubernetes.namespace": settings.KUBE_EXECUTOR_NAMESPACE,
                    "spark.kubernetes.container.image": settings.EXECUTOR_IMAGE_NAME,
                    "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": settings.KUBE_SERVICE_ACCOUNT,
                    "spark.kubernetes.authenticate.executor.serviceAccountName": settings.KUBE_EXECUTOR_SERVICE_ACCOUNT,

                    "spark.kubernetes.driverEnv.TZ": tz_val,
                    "spark.executorEnv.TZ": tz_val,
                }

                if options.get("driver-cores") is None:
                    k8s_defaults["spark.driver.cores"] = settings.SPARK_DRIVER_CORES
                if options.get("driver-memory") is None:
                    k8s_defaults["spark.driver.memory"] = settings.SPARK_DRIVER_MEMORY
                if options.get("executor-cores") is None:
                    k8s_defaults["spark.executor.cores"] = settings.SPARK_EXECUTOR_CORES
                if options.get("executor-memory") is None:
                    k8s_defaults["spark.executor.memory"] = settings.SPARK_EXECUTOR_MEMORY

                context_manager = SQLContextManager.get_instance()
                has_python_udf = False
                for transform_cfg in config.transformation_pipeline:
                    if transform_cfg.name.upper() == "SQL":
                        query = transform_cfg.options.get("query")
                        if not query:
                            continue

                        source_cfg = config.get_source()
                        engine_adapter_src = source_cfg.create_engine_adapter() if source_cfg else None
                        select_exp = transformers.sql.SQL.parse_and_validate_query(query, engine_adapter_src, engine_dialect=Dialects.SPARK, **config.job_properties)
                        for func_exp in select_exp.find_all(exp.Anonymous):
                            spark_status = context_manager.get_spark_status()
                            if spark_status != SparkStatus.READY: # Could not use Spark Session to check if a function is truly a UDF
                                has_python_udf = True
                                break

                            func_name = func_exp.name.lower()
                            if func_name and context_manager.udf_exists(func_name, "spark"):
                                has_python_udf = True
                                break
                    if has_python_udf:
                        break

                if has_python_udf:
                    # Scales off-heap memory to prevent OOMKilled errors from Python/Arrow UDF execution.
                    k8s_defaults["spark.kubernetes.memoryOverheadFactor"] = 0.4

                    # Prevent PyArrow from holding massive micro-batches in RAM
                    k8s_defaults["spark.sql.execution.arrow.maxRecordsPerBatch"] = 10000

                job_mode = config.job_properties.get("job_mode", JobMode.BATCH)
                if job_mode == JobMode.STREAMING:
                    # Dedicated static executor pool & warm Kafka cache
                    streaming_defaults = {
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.sql.streaming.kafka.consumer.cache.timeout": "300000",
                        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
                    }
                    k8s_defaults.update(streaming_defaults)
                else:
                    # Dynamic Resource Allocation for Auto-scaling
                    batch_defaults = {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true",
                        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "67108864", 

                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": settings.SPARK_MIN_EXECUTORS,
                        "spark.dynamicAllocation.maxExecutors": settings.SPARK_MAX_EXECUTORS,
                        "spark.dynamicAllocation.initialExecutors": settings.SPARK_MIN_EXECUTORS,
                        "spark.dynamicAllocation.executorIdleTimeout": "60s",
                        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
                    }
                    k8s_defaults.update(batch_defaults)

                for key, val in k8s_defaults.items():
                    if key not in existing_keys:
                        conf.append(f"{key}={val}")

        return config

    def _build_command(
        self,
        config: Config,
        config_path_in_container: str,
        is_preview: bool = False,
    ) -> list[str]:
        """
        Builds the command list for running a job or a preview.

        Args:
            config: The job's configuration object.
            config_path_in_container: The path to the config file *inside* the container/pod.
            is_preview: Flag to indicate if the command is for a preview job.

        Returns:
            A list of strings representing the command and its arguments.
        """
        if not is_preview:
            return ["python", "main.py", "-c", config_path_in_container]

        exec_cfg = config.get_executor()
        if exec_cfg and exec_cfg.type_ == "spark":
            command = ["spark-submit", "api/tools/preview_worker.py"]
        else:
            command = ["python", "api/tools/preview_worker.py"]
            
        command.extend(["-c", config_path_in_container])
        return command

    def auto_heal_routine(self) -> None:
        """Global reconciliation routine to heal orphaned/failed jobs."""
        pass

    def auto_heal_job(self, id: str) -> bool:
        """Attempts to autonomously heal a specific FAILED job."""
        return False