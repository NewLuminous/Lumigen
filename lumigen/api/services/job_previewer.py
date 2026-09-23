from __future__ import annotations
import typing as t
import logging
import json
import uuid
import threading
import asyncio
import traceback
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from sqlglot import exp, Dialects

if t.TYPE_CHECKING:
    from core.config.root import Config
    from core.config.connection import SparkConnectionConfig

from core.engine_context import SQLContextManager, SparkStatus
import transformers
from api.config import settings
from api.schemas import JobPreviewResponse, JobColumnMapping, DataFetchResponse
from api.tools.preview_worker import PreviewRunner


logger = logging.getLogger(__name__)

# Dedicated thread pool quarantined strictly for in-process preview execution.
_preview_executor = ThreadPoolExecutor(
    max_workers=max(settings.PREVIEW_JOB_CONCURRENCY_LIMIT or 10, 2),
    thread_name_prefix="preview_worker",
)

class JobPreviewerBase(ABC):
    """
    An abstract base class that provides the generic logic for previewing
    a job's data output or schema mapping without running a full, persistent job.
    Subclasses must implement the platform-specific fallback execution strategy.
    """

    async def get_data_preview(self, config: Config, sample_size: int = 20, source_limit: int = 8192) -> JobPreviewResponse:
        """
        Generates a preview of the data a job would produce.
        """
        cfg = config.model_copy(deep=True) if hasattr(config, "model_copy") else config.copy(deep=True)
        if cfg.executor and cfg.executor.type_ == "spark":
            t.cast("SparkConnectionConfig", cfg.executor).log_level = "ERROR" # Reduce log noise for previews

        if source_limit:
            cfg.job_properties["source_limit"] = source_limit
        cfg.job_properties["source_batch_size"] = sample_size
        cfg.audit_store = None
        cfg.sink = None

        try:
            await self._validate_pre_flight(cfg)
        except Exception as e:
            logger.info(f"Fast-check validation failed. Aborting preview: {e}")
            return JobPreviewResponse(
                success=False, schema=None, data=None,
                detail=str(e), logs=traceback.format_exc(),
            )

        try:
            if self._can_run_preview_directly(cfg):
                result = await self._run_direct_preview(cfg)
                return JobPreviewResponse(
                    success=True,
                    schema=result.get("schema"),
                    data=result.get("data"),
                    detail="Job preview ran successfully.",
                    logs="Direct in-process execution."
                )
        except asyncio.TimeoutError as e:
            logger.warning(f"Direct data preview failed, using fallback. Reason: {e}")
        except Exception as e:
            logger.warning(f"Direct data preview failed, using fallback. Reason: {e}\n{traceback.format_exc()}")

        data_res = await self.run_disposable_job(cfg, temp_job_name="Job preview")
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
        cfg = config.model_copy(deep=True) if hasattr(config, "model_copy") else config.copy(deep=True)
        if cfg.executor and cfg.executor.type_ == "spark":
            t.cast("SparkConnectionConfig", cfg.executor).log_level = "ERROR"

        cfg.job_properties["source_limit"] = 1
        cfg.job_properties["source_batch_size"] = 1
        cfg.audit_store = None

        try:
            await self._validate_pre_flight(cfg)
        except Exception as e:
            logger.info(f"Fast-check validation failed. Aborting column mapping: {e}")
            return JobColumnMapping(
                success=False, column_mappings=None, unmapped_source_columns=None, unmapped_destination_columns=None,
                detail=str(e), logs=traceback.format_exc(),
            )

        try:
            if self._can_run_preview_directly(cfg, schema_only=True):
                result = await self._run_direct_preview(cfg)
                return JobColumnMapping(
                    success=True,
                    column_mappings=result.get("column_mappings"),
                    unmapped_source_columns=result.get("unmapped_source_columns"),
                    unmapped_destination_columns=result.get("unmapped_destination_columns"),
                    detail="Job output column mapping ran successfully.",
                    logs="Direct in-process execution."
                )
        except asyncio.TimeoutError as e:
            logger.warning(f"Direct column mapping preview failed, using fallback. Reason: {e}")
        except Exception as e:
            logger.warning(f"Direct column mapping preview failed, using fallback. Reason: {e}\n{traceback.format_exc()}")

        data_res = await self.run_disposable_job(cfg, temp_job_name="Column mapping preview")
        return JobColumnMapping(
            success=data_res.success,
            column_mappings=data_res.data["column_mappings"] if data_res.data else None,
            unmapped_source_columns=data_res.data["unmapped_source_columns"] if data_res.data else None,
            unmapped_destination_columns=data_res.data["unmapped_destination_columns"] if data_res.data else None,
            detail=data_res.detail,
            logs=data_res.logs if data_res.logs else None,
        )

    async def _run_with_timeout(
        self,
        runner: PreviewRunner,
        callback: t.Callable[[str], None],
        is_spark: bool,
        timeout: float,
        description: str,
    ):
        """
        Executes a PreviewRunner inside the dedicated thread pool with an enforced timeout.
        """
        loop = asyncio.get_running_loop()
        context_manager = SQLContextManager.get_instance()

        if is_spark:
            job_group_id = f"{description}_{uuid.uuid4().hex}"
            spark = context_manager.get_spark_session()

            def _worker_spark():
                spark.sparkContext.setJobGroup(job_group_id, description, interruptOnCancel=True)
                try:
                    runner.run(callback=callback, do_validate_connections=False)
                finally:
                    try:
                        spark.sparkContext.setJobGroup("", "", interruptOnCancel=False)
                    except Exception:
                        pass

            try:
                await asyncio.wait_for(
                    loop.run_in_executor(_preview_executor, _worker_spark),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Spark {description} exceeded {timeout}s. Cancelling Spark job group {job_group_id}...")
                try:
                    spark.sparkContext.cancelJobGroup(job_group_id)
                    logger.debug(f"Successfully sent cancellation to Spark job group {job_group_id}.")
                except Exception as cancel_err:
                    logger.error(f"Failed to cancel Spark job group {job_group_id}: {cancel_err}")
                raise asyncio.TimeoutError(f"Spark {description} timed out after {timeout}s.")
        else:
            duckdb_conn = context_manager.get_duckdb_conn()

            def _worker_duckdb():
                runner.run(callback=callback, do_validate_connections=False)

            try:
                await asyncio.wait_for(
                    loop.run_in_executor(_preview_executor, _worker_duckdb),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"DuckDB {description} exceeded {timeout}s. Interrupting DuckDB query...")
                try:
                    duckdb_conn.interrupt()
                    logger.debug(f"Successfully interrupted DuckDB connection for {description}.")
                except Exception as cancel_err:
                    logger.error(f"Failed to interrupt DuckDB query: {cancel_err}")
                raise asyncio.TimeoutError(f"DuckDB {description} timed out after {timeout}s.")

    async def _validate_pre_flight(self, config: Config):
        """
        Orchestrates fail-fast pre-flight validation.
        """
        loop = asyncio.get_running_loop()

        def _ping_adapters():
            source_cfg = config.get_source()
            if source_cfg:
                source_cfg.create_engine_adapter().ping()

            sink_cfg = config.get_sink()
            if sink_cfg:
                sink_cfg.create_engine_adapter().ping()
        await loop.run_in_executor(_preview_executor, _ping_adapters)

        source_cfg = config.get_source()
        engine_adapter_src = source_cfg.create_engine_adapter() if source_cfg else None
        for transform_cfg in config.transformation_pipeline:
            if transform_cfg.name.upper() == "SQL":
                query = transform_cfg.options.get("query")
                if query:
                    if config.executor and config.executor.type_ == "spark":
                        engine_dialect = Dialects.SPARK
                    else:
                        engine_dialect = Dialects.DUCKDB
                    transformers.sql.SQL.parse_and_validate_query(query, engine_adapter_src, engine_dialect=engine_dialect, **config.job_properties)

        # Run 1-row check
        config_check = config.model_copy(deep=True) if hasattr(config, "model_copy") else config.copy(deep=True)
        config_check.job_properties["source_limit"] = 1
        config_check.job_properties["source_batch_size"] = 1
        config_check.audit_store = None

        runner = PreviewRunner(config_check, resolve_executor=True)
        is_spark = bool(runner.exec_cfg and runner.exec_cfg.type_ == "spark")
        context_manager = SQLContextManager.get_instance()

        if is_spark:
            status = context_manager.get_spark_status()
            if status != SparkStatus.READY:
                if status != SparkStatus.INITIALIZING:
                    logger.info(f"Spark status is {status}. Triggering background initialization.")
                    threading.Thread(
                        target=context_manager.get_spark_session,
                        daemon=True,
                        name="PreviewSparkInitThread",
                    ).start()
                # Skip 1-row check if Spark is not yet initialized to avoid false failures
                return

        timeout = settings.PREVIEW_PREFLIGHT_TIMEOUT_SECONDS or 15.0
        try:
            await self._run_with_timeout(
                runner=runner,
                callback=lambda s: None,
                is_spark=is_spark,
                timeout=timeout,
                description="pre-flight check",
            )
        except asyncio.TimeoutError:
            # Timeout is treated as 'no fatal error found', proceed to direct/fallback run
            return
        except Exception as e:
            # If the thread exited due to cancellation/interrupt, treat as timeout/success
            err_str = str(e).lower()
            if "cancelled" in err_str or "interrupted" in err_str:
                return
            raise e

    async def _run_direct_preview(self, config: Config) -> dict:
        """Helper to run the PreviewRunner in-process and capture its result."""
        result = None
        def set_output(json_string: str):
            nonlocal result
            result = json.loads(json_string)

        config_direct = config.model_copy(deep=True) if hasattr(config, "model_copy") else config.copy(deep=True)
        runner = PreviewRunner(config_direct, resolve_executor=False)
        is_spark = bool(runner.exec_cfg and runner.exec_cfg.type_ == "spark")
        timeout = settings.PREVIEW_DIRECT_TIMEOUT_SECONDS or 60.0
        await self._run_with_timeout(
            runner=runner,
            callback=set_output,
            is_spark=is_spark,
            timeout=timeout,
            description="direct preview",
        )

        if not result:
            raise RuntimeError("Direct preview ran but the callback was not executed or produced no result.")
        
        return result

    def _can_run_preview_directly(self, config: Config, schema_only: bool = False) -> bool:
        """Strictly evaluates if the submitted configuration can safely execute in-process."""
        if config.executor and config.executor.type_ == "spark":
            context_manager = SQLContextManager.get_instance()
            status = context_manager.get_spark_status()
            if status != SparkStatus.READY:
                return False

        source_cfg = config.get_source()
        engine_adapter_src = source_cfg.create_engine_adapter() if source_cfg else None

        if not schema_only:
            for transform_cfg in config.transformation_pipeline:
                if transform_cfg.name.upper() == "SQL":
                    query = transform_cfg.options.get("query")
                    if not query:
                        continue

                    if config.executor and config.executor.type_ == "spark":
                        engine_dialect = Dialects.SPARK
                    else:
                        engine_dialect = Dialects.DUCKDB
                    select_exp = transformers.sql.SQL.parse_and_validate_query(query, engine_adapter_src, engine_dialect=engine_dialect, **config.job_properties)
                    if select_exp.find(exp.Join):
                        return False
                    if engine_dialect == Dialects.DUCKDB:
                        if select_exp.find(exp.Unnest, exp.Explode):
                            return False
                        if select_exp.find(exp.GenerateSeries):
                            return False
                        for func_exp in select_exp.find_all(exp.Anonymous):
                            func_name = func_exp.name.lower()
                            if func_name in ("range",):
                                return False
                        for with_exp in select_exp.find_all(exp.With):
                            if with_exp.args.get("recursive"):
                                return False
        return True

    @abstractmethod
    async def run_disposable_job(self, config: Config, temp_job_name: str) -> DataFetchResponse:
        """
        The fallback strategy for running a preview in an isolated environment.
        This must be implemented by the concrete JobManagerAdapter subclasses.
        """
        pass