from __future__ import annotations
import typing as t
import logging
import json
import threading
import traceback
from abc import ABC, abstractmethod
from sqlglot import exp

if t.TYPE_CHECKING:
    from core.config.root import Config
    from core.config.connection import SparkConnectionConfig

from core.config.connection import parse_connection_config
from core.engine_context import SQLContextManager, SparkStatus
import transformers
from api.schemas import JobPreviewResponse, JobColumnMapping, DataFetchResponse
from api.tools.preview_worker import PreviewRunner


logger = logging.getLogger(__name__)

class JobPreviewerBase(ABC):
    """
    An abstract base class that provides the generic logic for previewing
    a job's data output or schema mapping without running a full, persistent job.
    Subclasses must implement the platform-specific fallback execution strategy.
    """

    async def get_data_preview(self, config: Config, sample_size: int = 20) -> JobPreviewResponse:
        """
        Generates a preview of the data a job would produce.
        """
        executor: SparkConnectionConfig = config.executor
        if executor:
            executor.log_level = "ERROR" # Reduce log noise for previews

        config.job_properties["source_limit"] = 8192
        config.job_properties["source_batch_size"] = sample_size
        config.audit_store = None
        config.sink = None

        try:
            if self._can_run_preview_directly(config):
                result = self._run_direct_preview(config)
                return JobPreviewResponse(
                    success=True,
                    schema=result.get("schema"),
                    data=result.get("data"),
                    detail=f"{'Job preview'} ran successfully.",
                    logs="Direct in-process execution."
                )
        except Exception as e:
            logger.warning(f"Direct data preview failed, using fallback. Reason: {e}\n{traceback.format_exc()}")

        data_res = await self.run_disposable_job(config, temp_job_name="Job preview")
        return JobPreviewResponse(
            success=data_res.success,
            schema=data_res.data["schema"] if data_res.data else None,
            data=data_res.data["data"] if data_res.data else None,
            detail=data_res.detail,
            logs=data_res.logs if data_res.logs else None,
        )

    async def get_column_mapping_preview(self, config: Config) -> JobColumnMapping:
        """
        Generates a preview of the column mapping between the job's
        transformed output and the target sink.
        """
        executor: SparkConnectionConfig = config.executor
        if executor:
            executor.log_level = "ERROR"

        config.job_properties["source_limit"] = 1
        config.job_properties["source_batch_size"] = 1
        config.audit_store = None

        try:
            if self._can_run_preview_directly(config, schema_only=True):
                result = self._run_direct_preview(config)
                return JobColumnMapping(
                    success=True,
                    column_mappings=result.get("column_mappings"),
                    unmapped_source_columns=result.get("unmapped_source_columns"),
                    unmapped_destination_columns=result.get("unmapped_destination_columns"),
                    detail=f"{'Job output column mapping'} ran successfully.",
                    logs="Direct in-process execution."
                )
        except Exception as e:
            logger.warning(f"Direct column mapping preview failed, using fallback. Reason: {e}\n{traceback.format_exc()}")

        data_res = await self.run_disposable_job(config, temp_job_name="Column mapping preview")
        return JobColumnMapping(
            success=data_res.success,
            column_mappings=data_res.data["column_mappings"] if data_res.data else None,
            unmapped_source_columns=data_res.data["unmapped_source_columns"] if data_res.data else None,
            unmapped_destination_columns=data_res.data["unmapped_destination_columns"] if data_res.data else None,
            detail=data_res.detail,
            logs=data_res.logs if data_res.logs else None,
        )

    def _run_direct_preview(self, config: Config) -> dict:
        """Helper to run the PreviewRunner in-process and capture its result."""
        result = None
        def set_output(json_string: str):
            nonlocal result
            result = json.loads(json_string)

        config_direct = config.model_copy()
        runner = PreviewRunner(config_direct, resolve_executor=False)
        runner.run(callback=set_output, do_validate_connections=False)

        if not result:
            raise Exception("Direct preview ran but the callback was not executed or produced no result.")
        
        return result

    def _can_run_preview_directly(self, config: Config, schema_only: bool = False) -> bool:
        if config.executor and config.executor.type_ == "spark":
            context_manager = SQLContextManager.get_instance()
            status = context_manager.get_spark_status()
            if status != SparkStatus.READY:
                if status != SparkStatus.INITIALIZING:
                    logger.info(f"Spark session status is {status}. Triggering background initialization.")
                    threading.Thread(
                        target=context_manager.get_spark_session,
                        daemon=True,
                        name="PreviewSparkInitThread",
                    ).start()
                return False

        source_cfg = config.get_source()
        engine_adapter_src = source_cfg.create_engine_adapter() if source_cfg else None
        if engine_adapter_src:
            engine_adapter_src.ping()

        sink_cfg = config.get_sink()
        engine_adapter_dest = sink_cfg.create_engine_adapter() if sink_cfg else None
        if engine_adapter_dest:
            engine_adapter_dest.ping()

        if not schema_only:
            for transform_cfg in config.transformation_pipeline:
                if transform_cfg.name.upper() == "SQL":
                    query = transform_cfg.options.get("query")
                    if not query:
                        continue

                    select_exp = transformers.sql.SQL.parse_and_validate_query(query, engine_adapter_src, **config.job_properties)
                    for _ in select_exp.find_all(exp.Join):
                        return False
        return True

    @abstractmethod
    async def run_disposable_job(self, config: Config, temp_job_name: str) -> DataFetchResponse:
        """
        The fallback strategy for running a preview in an isolated environment.
        This must be implemented by the concrete JobManagerAdapter subclasses.
        """
        pass