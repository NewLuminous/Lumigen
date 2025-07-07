import typing as t
import os
import re
import time
import logging
import traceback
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from pathlib import Path
from dateutil import parser
import tempfile
import json
from sqlglot import exp
import docker
from docker.errors import ContainerError
from core.config.root import Config
from core.config.connection import parse_connection_config
from core.engine_context import SQLContextManager, SparkStatus
import transformers
from utils import yaml
from utils.formatting import format_seconds_timestamp
from api.config import settings
from api.enums import JobStatus
from api.schemas import JobStatusResponse, JobInfoResponse, JobMetricsResponse, DataFetchResponse, JobPreviewResponse, JobColumnMapping
from api.integrations.base_adapter import JobManagerAdapter
from api.integrations.docker.enums import ContainerStatus
from api.integrations.docker.schemas import ContainerInfo
from api.integrations.docker.parser import PySparkLogParser
from api.entry_points.app_preview import PreviewRunner


logger = logging.getLogger(__name__)
_docker_adapter_instance: "DockerAdapter" = None

class DockerAdapter(JobManagerAdapter):
    SEPARATOR = '.'
    DATA_START_DELIMITER = "=== JSON_DATA_START ==="
    DATA_END_DELIMITER = "=== JSON_DATA_END ==="

    client: docker.DockerClient = None
    job_dict: t.Dict[str, ContainerInfo] = {}
    jobs_by_name: t.Dict[str, t.Set[str]] = {}

    def __init__(self):
        try:
            self.client = docker.from_env()
            # Test connection
            self.client.ping()
            logger.debug("Successfully connected to Docker daemon.")
        except docker.errors.DockerException as e:
            logger.error(f"Could not connect to Docker daemon: {e}")

        self._refresh_job_dict()

    def submit_job(self, config: Config, id: str, name: t.Optional[str]=None, checkpoint_dir: t.Optional[str]=settings.CHECKPOINT_DIR):
        id = DockerAdapter._normalize_job_id(id)
        if name is None:
            name = settings.JOB_IMAGE_NAME
        name = DockerAdapter._normalize_container_name(name)
        container_name = f"{name}{self.SEPARATOR}{id}"
        self._refresh_job_dict()

        container_running = False
        # Job already ran before
        if id in self.job_dict:
            logger.warning(f"Container for job '{id}' already exists.")
            container = self.job_dict[id].object
            status = container.status
            try:
                if status in {ContainerStatus.RUNNING, ContainerStatus.CREATED}:
                    logger.warning(f"Container for job '{id}' is already running. Skipping this run.")
                    container_running = True
                elif status == ContainerStatus.RESTARTING:
                    logger.warning(f"Existing container for job '{id}' failed to run. Attempting to remove it.")
                    self._remove_container(id)
                elif status == ContainerStatus.REMOVING:
                    container.wait(condition="removed")
                elif status == ContainerStatus.PAUSED:
                    logger.info(f"Container for job '{id}' is being paused. Attempting to unpause it.")
                    container.unpause()
                    container_running = True
                elif status == ContainerStatus.EXITED:
                    # TODO: Uncomment below code when changes in the config file can be detected in the future
                    # if self.job_dict[id].exit_code in {0, 143, 137}:
                    #     Logger.info(f"Attempting to restart the existing container {container_name} using its config.", __name__)
                    #     self._start_container(id)
                    #     container_running = True
                    # else:
                    #     Logger.warning(f"Existing container for job '{id}' failed to run. Attempting to remove it.", __name__)
                    #     self._remove_container(id)
                    
                    logger.warning(f"Existing container for job '{id}' exited. Attempting to remove it.")
                    self._remove_container(id)
                elif status == ContainerStatus.DEAD:
                    logger.warning(f"Existing container for job '{id}' failed to clean up. Attempting to remove it.")
                    self._remove_container(id)
                else:
                    raise Exception(f"Unknown container status: {status}")
                
            except docker.errors.NotFound:
                pass
            except docker.errors.APIError as e:
                logger.error(f"Error handling existing container '{container_name}': {e}")
                raise HTTPException(status_code=500, detail=f"Could not handle pre-existing container '{container_name}': {e}")
            
        if not container_running:
            old_name = self.get_job_name(id)
            if old_name is not None:
                self.jobs_by_name[old_name].discard(id)
            self.job_dict.pop(id, None)

            logger.info(f"Attempting to run job '{id}' in container '{container_name}'")
            temp_config_file_host_path = None
            try:
                current_file_path = Path(__file__).resolve()
                host_project_path = current_file_path.parent.parent.parent.parent
                volumes = {
                    str(host_project_path): {'bind': '/home/spark', 'mode': 'ro'},
                }
                # Sync container time with host time for accurate logging
                if Path('/etc/localtime').exists():
                    volumes['/etc/localtime'] = {'bind': '/etc/localtime', 'mode': 'ro'}

                with tempfile.NamedTemporaryFile(mode='w', suffix=".yaml", prefix=f"{name}_{id}_", delete=False) as tmp_file:
                    config_as_dict = json.loads(config.json())
                    yaml.dump(config_as_dict, tmp_file)
                    temp_config_file_host_path = tmp_file.name
                    temp_config_file_container_path = f'/tmp/{Path(temp_config_file_host_path).name}'
                    os.chmod(temp_config_file_host_path, 0o644) # Sets rw-r--r--
                    volumes[temp_config_file_host_path] = {'bind': temp_config_file_container_path, 'mode': 'ro'}

                if checkpoint_dir:
                    volumes[checkpoint_dir] = {'bind': '/spark-ckpts', 'mode': 'rw'}
                    os.makedirs(checkpoint_dir, exist_ok=True)

                command = ["python", "entry_points/app.py"]
                exec_cfg = config.get_executor()
                if exec_cfg and exec_cfg.type_ == "spark":
                    command = ["spark-submit", "entry_points/app_pyspark.py"]
                command.extend(["-c", temp_config_file_container_path])

                container = self.client.containers.run(
                    detach=True,        # Run in the background
                    auto_remove=False,
                    network_mode="host",
                    volumes=volumes,
                    name=container_name,
                    image=settings.JOB_IMAGE_NAME,
                    command=command,
                )
                self.job_dict[id].object = container
                self.job_dict[id].name = container_name
                self.jobs_by_name[name] = id
                logger.debug(f"Successfully submitted job '{id}'. Container ID: {container.id}, Name: {container_name}")
                response = JobStatusResponse(
                    job_id=id,
                    job_name=name,
                    status=JobStatus.INITIALIZING,
                    message=f"Job submitted. Container: {container_name}"
                )
            except docker.errors.ImageNotFound:
                logger.error(f"Docker image '{settings.JOB_IMAGE_NAME}' not found.")
                raise HTTPException(status_code=500, detail=f"Docker image '{settings.JOB_IMAGE_NAME}' not found.")
            except docker.errors.APIError as e:
                logger.error(f"Docker API error when starting job '{id}': {e}")
                raise HTTPException(status_code=500, detail=f"Could not start job '{id}'")
            except Exception as e:
                logger.error(f"Unexpected error submitting job '{id}': {e}")
                raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        else:
            container = self.job_dict[id].object
            response = JobStatusResponse(
                job_id=id,
                job_name=name,
                status=self.get_job_status(id),
                message=f"Job submitted. Container: {self.job_dict[id].name}"
            )

        self._prune_containers()
        return response
    
    def stop_job(self, id: str):
        id = DockerAdapter._normalize_job_id(id)
        if id in self.job_dict:
            container_info = self.job_dict[id]
            container = container_info.object
            try:
                logger.info(f"Attempting to stop job '{id}' in container '{container_info.name}'")
                container.stop(timeout=10)
                logger.debug(f"Successfully stopped job '{id}'. Container ID: {container.id}, Name: {container_info.name}")

                # Update container info
                status = self.get_job_status(id)

                return JobStatusResponse(
                    job_id=id,
                    job_name=self.get_job_name(id),
                    status=status,
                    message=f"Job '{id}' stopped successfully."
                )
            except docker.errors.NotFound as e:
                logger.debug(f"Failed to stop container '{container_info.name}' (ID: {container.short_id}): {e}")
            except docker.errors.APIError as e:
                logger.debug(f"Failed to stop container '{container_info.name}' (ID: {container.short_id}): {e}")
                # Attempt to force remove if stop failed badly
                try:
                    logger.warning(f"Attempting to force remove container '{container_info.name}' (ID: {container.short_id}) after failed stop.")
                    self._remove_container(id, force=True)
                except Exception as force_remove_e:
                    raise HTTPException(status_code=500, detail=f"Could not stop job '{id}'")
            except Exception as e:
                logger.error(f"Unexpected error stopping job '{id}': {e}")
                raise HTTPException(status_code=500, detail=f"Could not stop job '{id}'")
        else:
            logger.debug(f"Container for job '{id}' does not exist. Cannot stop it.")

        return JobStatusResponse(
            job_id=id,
            job_name=None,
            status=JobStatus.UNKNOWABLE,
            message=f"Job with ID '{id}' does not exist or was removed."
        )
    
    def delete_job(self, id: str):
        id = DockerAdapter._normalize_job_id(id)
        if id in self.job_dict:
            job_name=self.get_job_name(id)
            try:
                self._remove_container(id)
                return JobStatusResponse(
                    job_id=id,
                    job_name=job_name,
                    status=JobStatus.UNKNOWABLE,
                    message=f"Successfully deleted job '{id}'."
                )
            except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Could not delete job '{id}': {e}")
        return JobStatusResponse(
            job_id=id,
            job_name=None,
            status=JobStatus.UNKNOWABLE,
            message=f"Job with ID '{id}' does not exist or was already deleted."
        )
    
    def get_job_name(self, job_id: str) -> t.Optional[str]:
        if job_id in self.job_dict:
            container_info = self.job_dict[job_id]
            return self.SEPARATOR.join(container_info.name.split(self.SEPARATOR)[:-1])
        return None
    
    def get_job_status(self, id: str) -> JobStatus:
        id = DockerAdapter._normalize_job_id(id)
        if id not in self.job_dict:
            return JobStatus.UNKNOWABLE
        
        try:
            self._reload_container(id, force=True)
            container_info = self.job_dict[id]
            container = container_info.object
            status = container.status
            if status == ContainerStatus.CREATED:
                return JobStatus.CREATED
            elif status == ContainerStatus.RESTARTING:
                return JobStatus.PENDING
            elif status == ContainerStatus.RUNNING:
                return JobStatus.RUNNING
            elif status == ContainerStatus.REMOVING:
                return JobStatus.UNKNOWABLE
            elif status == ContainerStatus.PAUSED:
                return JobStatus.CANCELED
            elif status == ContainerStatus.EXITED:
                if self.job_dict[id].exit_code == 0:
                    return JobStatus.FINISHED
                elif self.job_dict[id].exit_code in {143, 137}:
                    return JobStatus.CANCELED
                else:
                    return JobStatus.FAILED   
            else:
                return JobStatus.UNKNOWABLE
        except:
            return JobStatus.UNKNOWABLE
        
    def get_job_info(self, id: str) -> JobInfoResponse:
        id = DockerAdapter._normalize_job_id(id)
        if id not in self.job_dict:
            return JobInfoResponse(job_id=id, status=JobStatus.UNKNOWABLE)
        
        # self._reload_container(id) is needed but already called in #get_job_status()
        status = self.get_job_status(id)
        container_info = self.job_dict[id]
        container = container_info.object
        container_attrs = container.attrs

        job_info = JobInfoResponse(
            job_id=id,
            job_name=self.get_job_name(id),
            status=status,
            message=f"Job is currently {str(status).lower()}.",
            start_time=container_info.started_at or container_attrs["Created"],
            finish_time=container_info.finished_at,
        )
        return job_info
    
    def get_job_metrics(self, id: str) -> JobMetricsResponse:
        id = DockerAdapter._normalize_job_id(id)
        if id not in self.job_dict:
            return JobMetricsResponse()
        
        container_info = self.job_dict[id]
        if container_info.latest_metrics is not None:
            previous_stats = container_info.latest_metrics
        else:
            previous_stats = self._get_container_stats_snapshot(id)
            time.sleep(1.0)
        if not previous_stats:
            return JobMetricsResponse()

        cpu_percent = None
        mem_usage_bytes = None
        read_iops_rate = None
        write_iops_rate = None
        current_raw_stats = self._get_container_stats_snapshot(id)
        if not current_raw_stats:
            mem_usage_bytes = previous_stats['memory_stats'].get('usage', 0)
            return JobMetricsResponse(memory_usage_bytes=mem_usage_bytes)

        # CPU Usage
        cpu_percent = 0.0
        cpu_delta = current_raw_stats['cpu_stats']['cpu_usage']['total_usage'] - \
                    previous_stats['cpu_stats']['cpu_usage']['total_usage']
        
        if cpu_delta != 0:
            system_cpu_delta = current_raw_stats['cpu_stats'].get('system_cpu_usage', 0) - \
                               previous_stats['cpu_stats'].get('system_cpu_usage', 0)
        
        online_cpus = current_raw_stats['cpu_stats'].get('online_cpus', len(current_raw_stats['cpu_stats']['cpu_usage'].get('percpu_usage') or []))
        if online_cpus > 0 and cpu_delta > 0 and system_cpu_delta > 0:
            cpu_percent = (cpu_delta / system_cpu_delta) * 100.0
            cpu_percent *= online_cpus

        # Memory Usage
        mem_usage_bytes = current_raw_stats['memory_stats'].get('usage', 0)
        
        # Disk I/O
        read_iops_cumulative = 0
        write_iops_cumulative = 0
        read_iops_rate = 0
        write_iops_rate = 0
        for stat in current_raw_stats['blkio_stats']['io_serviced_recursive'] or []:
            if stat['op'] == 'Read':
                read_iops_cumulative += stat['value']
            elif stat['op'] == 'Write':
                write_iops_cumulative += stat['value']

        current_time_obj = parser.isoparse(current_raw_stats['read'])
        previous_time_obj = parser.isoparse(previous_stats['read'])
        time_delta_s = (current_time_obj - previous_time_obj).total_seconds()
        if time_delta_s > 0:
            prev_read_iops = 0
            prev_write_iops = 0
            for stat in previous_stats['blkio_stats']['io_serviced_recursive'] or []:
                if stat['op'] == 'Read': prev_read_iops += stat['value']
                elif stat['op'] == 'Write': prev_write_iops += stat['value']

            read_iops_rate = (read_iops_cumulative - prev_read_iops) / time_delta_s
            write_iops_rate = (write_iops_cumulative - prev_write_iops) / time_delta_s

        return JobMetricsResponse(
            cpu_usage_percent=cpu_percent,
            memory_usage_bytes=mem_usage_bytes,
            volume_bytes=None,
            read_iops=f"{read_iops_rate}",
            write_iops=f"{write_iops_rate}",
            read_bytes_per_second=None,
            write_bytes_per_second=None,
        )

    def get_job_logs(self, id: str,
                     tail: t.Optional[int] = None, since: t.Optional[int] = None,
                     stdout: t.Optional[bool] = True, stderr: t.Optional[bool] = True,
                     timestamps: t.Optional[bool] = False) -> t.Optional[str]:
        id = DockerAdapter._normalize_job_id(id)
        if id not in self.job_dict:
            raise HTTPException(status_code=404, detail=f"Job with ID '{id}' does not exist or was removed.")
        
        container_info = self.job_dict[id]
        container = container_info.object
        try:
            logs = container.logs(
                stdout=stdout,
                stderr=stderr,
                stream=False,
                timestamps=timestamps,
                tail=tail or "all",
                since=since,
            ).decode('utf-8')
            return logs
        except docker.errors.NotFound as e:
            raise HTTPException(status_code=404, detail=f"Job with ID '{id}' does not exist or was removed.")
        except docker.errors.APIError as e:
            logger.error(f"Docker API error when fetching logs for container '{container.name}': {e}")
            raise HTTPException(status_code=500, detail=f"Could not retrieve logs for job '{id}'.")
        except Exception as e:
            logger.error(f"Unexpected error fetching logs for job '{id}': {e}")
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching logs for job '{id}'.")
    
    def list_jobs(self, sparse: t.Optional[bool] = True, name: t.Optional[str] = None) -> t.List[t.Dict[str, t.Any]]:
        result = []
        if name:
            job_ids = self.jobs_by_name.get(name, set())
        else:
            job_ids = self.job_dict.keys()
        for id in job_ids:
            if id not in self.job_dict:
                continue

            container_info = self.job_dict[id]
            job_name = self.get_job_name(id)
            if not sparse:
                job_info = self.get_job_info(id)
                if job_info.status == JobStatus.UNKNOWABLE:
                    continue
                result.append(job_info.model_dump())
            else:
                container = container_info.object
                container_attrs = container.attrs
                started_at = container_info.started_at or container_attrs["Created"]
                if isinstance(started_at, int):
                    started_at = format_seconds_timestamp(started_at)
                result.append({
                    "job_id": id,
                    "job_name": job_name,
                    "start_time": started_at,
                })
        return result
    
    async def submit_temp_job(self, config: Config, temp_job_name: t.Optional[str] = "Temp job"):
        temp_config_file_host_path = None
        try:
            current_file_path = Path(__file__).resolve()
            host_project_path = current_file_path.parent.parent.parent.parent
            volumes = {
                str(host_project_path): {'bind': '/home/spark', 'mode': 'ro'},
            }
            # Sync container time with host time for accurate logging
            if Path('/etc/localtime').exists():
                volumes['/etc/localtime'] = {'bind': '/etc/localtime', 'mode': 'ro'}

            with tempfile.NamedTemporaryFile(mode='w', suffix=".yaml", prefix=f"temp_", delete=False) as tmp_file:
                config_as_dict = json.loads(config.json())
                yaml.dump(config_as_dict, tmp_file)
                temp_config_file_host_path = tmp_file.name
                temp_config_file_container_path = f'/tmp/{Path(temp_config_file_host_path).name}'
                os.chmod(temp_config_file_host_path, 0o644) # Sets rw-r--r--
                volumes[temp_config_file_host_path] = {'bind': temp_config_file_container_path, 'mode': 'ro'}

            # Run in local mode for now
            # If spark-submit later, may need to reimplement how to parse log
            # since spark-submit prints errors to STDOUT.
            command = ["python", "api/entry_points/app_preview.py"]
            command.extend(["-c", temp_config_file_container_path])

            # --- Offload blocking Docker call to a thread ---
            docker_result = await asyncio.to_thread(
                self._run_disposable_container_sync,
                settings.JOB_IMAGE_NAME,
                command,
                volumes,
            )
            log_parser = PySparkLogParser()
            docker_run_successful = (docker_result["exit_code"] == 0 and docker_result["error"] is None)
            if not docker_run_successful:
                #error_msg = docker_result["error"] or f"{temp_job_name} failed."
                error_logs = docker_result["stderr"]
                short_err_logs = error_logs
                last_exception = log_parser.parse_last_exception(error_logs)
                if last_exception and last_exception.get("message"):
                    short_err_logs = last_exception["message"]
                return DataFetchResponse(
                    success=False,
                    detail=short_err_logs,
                    logs=error_logs,
                )
            
            stdout_data = docker_result["stdout"]
            parsed_data = log_parser.parse_json_data_with_delimiters(stdout_data, self.DATA_START_DELIMITER, self.DATA_END_DELIMITER)
            if parsed_data is not None:
                logger.debug(f"{temp_job_name} ran successfully.")
                return DataFetchResponse(
                    success=True,
                    data=parsed_data,
                    detail=f"{temp_job_name} ran successfully.",
                    logs=log_parser.redact_delimited_content(stdout_data, self.DATA_START_DELIMITER, self.DATA_END_DELIMITER, replacement_text=""),
                )
            else:
                error_msg = "Data fetch failed or data not found."
                return DataFetchResponse(
                    success=False,
                    detail=error_msg,
                    logs=stdout_data,
                )
        except docker.errors.ImageNotFound:
            raise HTTPException(status_code=500, detail=f"Docker image '{settings.JOB_IMAGE_NAME}' not found.")
        except docker.errors.APIError as e:
            raise HTTPException(status_code=500, detail=f"Could not start container: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        finally:
            # --- Clean up the temporary config file from the host ---
            if temp_config_file_host_path:
                temp_file = Path(temp_config_file_host_path)
                if temp_file.is_file():
                    try:
                        temp_file.unlink()
                    except OSError as e:
                        logger.error(f"Failed to delete temp config file '{temp_file.resolve()}': {e}")

    async def preview_job(self, config: Config, sample_size: int = 20):
        config.job_properties["source_limit"] = 10000
        config.job_properties["source_batch_size"] = sample_size
        config.executor = None # Python by default for faster response
        config.sink = None
        try:
            context_manager = SQLContextManager.get_instance()
            if self._can_run_job_directly(config) and context_manager.get_spark_status() == SparkStatus.READY:
                result = None

                def set_output(json_string):
                    nonlocal result
                    result = json.loads(json_string)

                config.executor = parse_connection_config({"type": "spark"})
                runner = PreviewRunner(config, resolve_executor=False)
                runner.run(callback=set_output, do_validate_connections=False)

                if result:
                    return JobPreviewResponse(
                        success=True,
                        schema=result.get("schema"),
                        data=result.get("data"),
                        detail=f"{'Job preview'} ran successfully.",
                        logs=f"{'Job preview'} ran successfully.",
                    )
                else:
                    raise Exception("Data preview callback was not executed.")
        except Exception as e:
            return JobPreviewResponse(
                success=False,
                schema=None,
                data=None,
                detail=str(e),
                logs=traceback.format_exc(),
            )

        data_res = await self.submit_temp_job(config, temp_job_name="Job preview")
        return JobPreviewResponse(
            success=data_res.success,
            schema=data_res.data["schema"] if data_res.data else None,
            data=data_res.data["data"] if data_res.data else None,
            detail=data_res.detail,
            logs=data_res.logs if data_res.logs else None,
        )
    
    async def preview_job_output_column_mapping(self, config: Config):
        config.job_properties["source_limit"] = 1
        config.job_properties["source_batch_size"] = 1
        config.executor = None # Python by default for faster response
        try:
            context_manager = SQLContextManager.get_instance()
            if self._can_run_job_directly(config, schema_only=True) and context_manager.get_spark_status() == SparkStatus.READY:
                result = None

                def set_output(json_string):
                    nonlocal result
                    result = json.loads(json_string)

                config.executor = parse_connection_config({"type": "spark"})
                runner = PreviewRunner(config, resolve_executor=False)
                runner.run(callback=set_output, do_validate_connections=False)

                if result:
                    return JobColumnMapping(
                        success=True,
                        column_mappings=result.get("column_mappings"),
                        unmapped_source_columns=result.get("unmapped_source_columns"),
                        unmapped_destination_columns=result.get("unmapped_destination_columns"),
                        detail=f"{'Job output column mapping'} ran successfully.",
                        logs=f"{'Job output column mapping'} ran successfully.",
                    )
                else:
                    raise Exception("Data preview callback was not executed.")
        except Exception as e:
            return JobColumnMapping(
                success=False,
                column_mappings=None,
                unmapped_source_columns=None,
                unmapped_destination_columns=None,
                detail=str(e),
                logs=traceback.format_exc(),
            )

        data_res = await self.submit_temp_job(config, temp_job_name="Job output column mapping")
        return JobColumnMapping(
            success=data_res.success,
            column_mappings=data_res.data["column_mappings"] if data_res.data else None,
            unmapped_source_columns=data_res.data["unmapped_source_columns"] if data_res.data else None,
            unmapped_destination_columns=data_res.data["unmapped_destination_columns"] if data_res.data else None,
            detail=data_res.detail,
            logs=data_res.logs if data_res.logs else None,
        )

    def _refresh_job_dict(self):
        new_job_dict = defaultdict(ContainerInfo)
        containers = self.client.containers.list(all=True, sparse=True)
        for container in containers:
            container_attrs = container.attrs
            container_name = container_attrs["Names"][0].lstrip('/')
            if self.SEPARATOR not in container_name:
                continue

            job_id = container_name.split(self.SEPARATOR)[-1]
            if job_id in self.job_dict:
                old_container = self.job_dict[job_id].object
                # HACK: Might need to detect restart in the future
                if old_container.id == container.id and old_container.status == container.status:
                    new_job_dict[job_id].started_at = self.job_dict[job_id].started_at
                    new_job_dict[job_id].finished_at = self.job_dict[job_id].finished_at
            new_job_dict[job_id].object = container
            new_job_dict[job_id].name = container_name
            if container.status == ContainerStatus.EXITED:
                if isinstance(container_attrs['State'], dict):
                    new_job_dict[job_id].exit_code = container_attrs['State']["ExitCode"]
                else:
                    exit_code = int(container_attrs["Status"].split('(')[1].split(')')[0])
                    new_job_dict[job_id].exit_code = exit_code
            else:
                new_job_dict[job_id].exit_code = 0
        self.job_dict = new_job_dict

        new_jobs_by_name = defaultdict(set)
        for id in self.job_dict:
            job_name = self.get_job_name(id)
            new_jobs_by_name[job_name].add(id)
        self.jobs_by_name = new_jobs_by_name

    def _reload_container(self, job_id: str, force: bool = False):
        if job_id not in self.job_dict:
            if not force:
                return
            else:
                raise ValueError(f"Job ID '{job_id}' not found.")
        
        container_info = self.job_dict[job_id]
        container = container_info.object
        try:
            container.reload()
            container_info.exit_code = container.attrs['State']["ExitCode"]
            container_info.started_at = container.attrs['State']['StartedAt']
            container_info.finished_at = container.attrs['State']['FinishedAt']
        except (docker.errors.NotFound, docker.errors.APIError) as e:
            if not force:
                logger.warning(f"Failed to reload container '{container_info.name}' (ID: {container.short_id}): {e}")
            else:
                raise e
            
    def _get_container_stats_snapshot(self, job_id: str) -> t.Optional[t.Dict[str, t.Any]]:
        if job_id not in self.job_dict:
            return None

        container_info = self.job_dict[job_id]
        container = container_info.object
        for _ in range(3): # Retry a few times for complete stats
            try:
                stats = container.stats(stream=False)
                if ('cpu_stats' in stats and 'precpu_stats' in stats and
                    'memory_stats' in stats and 'blkio_stats' in stats and
                    'read' in stats):
                    container_info.latest_metrics = stats
                    return stats
                time.sleep(0.1)
            except docker.errors.NotFound: # Container might have stopped
                logger.warning(f"Container {container.short_id} not found during stats snapshot.")
                return None
            except Exception as e:
                logger.warning(f"Error getting single stats snapshot for {container.short_id}: {e}")
                return None
        logger.warning(f"Could not get complete stats snapshot for {container.short_id} after retries.")
        return None
    
    def _start_container(self, job_id: str):
        if job_id not in self.job_dict:
            return
        
        container_info = self.job_dict[job_id]
        container = container_info.object
        try:
            container.start()
            # Reset container info
            self.job_dict[job_id].restart()
        except (docker.errors.NotFound, docker.errors.APIError) as e:
            logger.error(f"Failed to start container '{container_info.name}' (ID: {container.short_id}): {e}")

    def _remove_container(self, job_id: str):
        container_info = self.job_dict[job_id]
        container = container_info.object
        job_name = self.get_job_name(job_id)
        try:
            container_attrs = container.attrs
            container.remove(v=True, force=True)
            self.jobs_by_name[job_name].discard(job_id)
            self.job_dict.pop(job_id, None)
        except docker.errors.NotFound as e:
            logger.error(f"Failed to remove container '{container_info.name}' (ID: {container.short_id}): {e}")
            self.jobs_by_name[job_name].discard(job_id)
            self.job_dict.pop(job_id, None)
            raise e
        except docker.errors.APIError as e:
            logger.error(f"Failed to remove container '{container_info.name}' (ID: {container.short_id}): {e}")
            raise e
        finally:
            # Delete associated temp files
            mounts = container_attrs["Mounts"]
            for mount in mounts:
                if mount["Type"] != "bind":
                    continue
                if not mount["Destination"].startswith("/tmp/"):
                    continue
                temp_file = Path(mount["Source"])
                if not temp_file.is_file():
                    continue
                try:
                    temp_file.unlink()
                except OSError as e:
                    logger.error(f"Failed to delete temp config file '{temp_file.resolve()}': {e}")
        
    def _prune_containers(self,
                          prune_stopped_for_minutes: timedelta = timedelta(minutes=settings.PRUNE_JOB_STOPPED_FOR_MINUTES),
                          refresh_job_dict: bool=True):
        """
        Prunes containers in a two-pass process:
        1. Time-Based Pruning: Hard-deletes any EXITED or DEAD container that finished
           longer ago than the specified time limit.
        2. Count-Based Pruning: Enforces a soft limit on the number of containers per job name.
           It prioritizes keeping running containers and will remove the oldest non-running
           containers to meet the limit.
        """
        if refresh_job_dict:
            self._refresh_job_dict()

        total_pruned_cnt = 0

        # --- Phase 1: Time-Based Pruning (for Exited/Dead containers) ---
        now_utc = datetime.now(timezone.utc)
        job_ids_to_prune_by_time = set()
        for job_id in list(self.job_dict.keys()):
            if job_id not in self.job_dict:
                continue
            container_info = self.job_dict[job_id]
            container = container_info.object
            status = container.status
            if status not in {ContainerStatus.EXITED, ContainerStatus.DEAD}:
                continue

            try:
                if container_info.finished_at is None:
                    self._reload_container(job_id)
                if container_info.finished_at is None: # Cannot reload container
                    continue
                # Docker API's FinishedAt time can have nanoseconds and 'Z'
                # Python's datetime.fromisoformat doesn't handle 'Z' directly before 3.11
                # and nanoseconds can cause issues if not truncated.
                finished_at_str = container_info.finished_at
                if finished_at_str.endswith('Z'):
                    finished_at_str = finished_at_str[:-1]
                if '.' in finished_at_str:
                    parts = finished_at_str.split('.')
                    if len(parts[1]) > 6: # more than microseconds
                        parts[1] = parts[1][:6]
                    finished_at_str = '.'.join(parts)

                if finished_at_str.startswith("0001-01-01"): # Container never ran or exited abnormally
                    continue

                finished_at_utc = datetime.fromisoformat(finished_at_str).replace(tzinfo=timezone.utc)
                if now_utc - finished_at_utc > prune_stopped_for_minutes:
                    job_ids_to_prune_by_time.add(job_id)
            except (KeyError, ValueError) as e:
                logger.warning(f"Could not parse FinishedAt for container '{container_info.name}' (ID: {container.short_id}): {e}. Status: {container.status}")
                continue

        phase1_pruned_cnt = 0
        for job_id in job_ids_to_prune_by_time:
            try:
                logger.debug(f"Pruning container for job '{job_id}' (reason: exceeded time limit).")
                self._remove_container(job_id)
                phase1_pruned_cnt += 1
            except (docker.errors.NotFound, docker.errors.APIError) as e:
                continue
        logger.debug(f"Phase 1 pruning finished. Containers pruned: {phase1_pruned_cnt}.")
        total_pruned_cnt += phase1_pruned_cnt

        # --- Phase 2: Count-Based Pruning (Soft Limit for All Containers) ---
        containers_by_job_name = defaultdict(list)
        for job_id in list(self.job_dict.keys()):
            job_name = self.get_job_name(job_id)
            if job_name is None:
                continue

            try:
                container_info = self.job_dict[job_id]
                created_at = container_info.object.attrs['Created']
                if isinstance(created_at, int):
                    created_at = format_seconds_timestamp(created_at)
                created_at = parser.isoparse(created_at)
                containers_by_job_name[job_name].append({
                    "job_id": job_id,
                    "status": container_info.object.status,
                    "exit_code": container_info.exit_code,
                    "created_at": created_at
                })
            except (KeyError, ValueError, docker.errors.NotFound) as e:
                logger.warning(f"Could not process container '{container_info.name}' for count-based pruning: {e}")
                continue
        job_ids_to_prune_by_count = set()
        for job_name, containers_list in containers_by_job_name.items():
            keep_cnt = 0
            containers = list(containers_list)
            containers.sort(key=lambda c: c["created_at"], reverse=True)
            finished_containers = []
            prune_candidates = []
            for c in containers:
                if c["status"] == ContainerStatus.RUNNING:
                    # Rule: Keep all running containers
                    keep_cnt += 1
                elif c["status"] == ContainerStatus.EXITED and c["exit_code"] == 0:
                    finished_containers.append(c["job_id"])
                else:
                    # Consider all unsuccessful non-running containers for deletion
                    prune_candidates.append(c["job_id"])

            # Rule: If can keep more than 1 container, keep only the last created finished container
            # and remove other finished containers.
            if len(finished_containers) > 0 and keep_cnt + 2 <= settings.MAX_KEPT_JOBS_PER_NAME:
                keep_cnt += 1
                prune_candidates.extend(finished_containers[1:])
            else:
                prune_candidates.extend(finished_containers)

            # Rule: Remove the oldest non-running containers until the limit
            add_keep_cnt = max(settings.MAX_KEPT_JOBS_PER_NAME - keep_cnt, 0)
            for job_id in prune_candidates[add_keep_cnt:]:
                job_ids_to_prune_by_count.add(job_id)

        phase2_pruned_cnt = 0
        for job_id in job_ids_to_prune_by_count:
            try:
                logger.info(f"Pruning container for job '{job_id}' (reason: exceeded count limit).")
                self._remove_container(job_id)
                phase2_pruned_cnt += 1
            except (docker.errors.NotFound, docker.errors.APIError) as e:
                continue
        logger.debug(f"Phase 2 pruning finished. Containers pruned: {phase2_pruned_cnt}.")
        total_pruned_cnt += phase2_pruned_cnt

        logger.debug(f"Pruning process finished. Total containers pruned: {total_pruned_cnt}.")
        return total_pruned_cnt
    
    def _run_disposable_container_sync(self, image_name: str, command: t.List[str], volumes: t.Dict[str, t.Dict[str, str]], container_name: t.Optional[str] = None) -> t.Dict[str, t.Any]:
        """
        Synchronously runs a Docker container and captures its output.
        """
        result = {"stdout": None, "stderr": None, "exit_code": -1, "error": None}
        try:
            container_output = self.client.containers.run(
                detach=False,        # Run in foreground
                stdout=True,
                stderr=True,
                remove=True,
                network_mode="host",
                volumes=volumes,
                name=container_name,
                image=image_name,
                command=command,
            )
            result["stdout"] = container_output.decode('utf-8').strip()
            result["exit_code"] = 0
            logger.debug(f"Temp container completed successfully.")
        except ContainerError as e:
            logger.error(f"Temp container exited with error (code {e.exit_status}): {e.stderr.decode('utf-8', 'replace')}")
            result["stderr"] = e.stderr.decode('utf-8', 'replace').strip() if e.stderr else ""
            result["exit_code"] = e.exit_status
            result["error"] = f"Container exited with code {e.exit_status}."
        except docker.errors.ImageNotFound:
            logger.error(f"Docker image '{image_name}' not found.")
            raise
        except docker.errors.APIError as e:
            logger.error(f"Docker API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
        
        return result
    
    def _can_run_job_directly(self, config: Config, schema_only: t.Optional[bool] = False):
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

    @staticmethod
    def _normalize_container_name(container_name: str) -> str:
        name = container_name.strip()
        if not name:
            raise ValueError(f"Name is empty or contains only whitespace.")
        
        # Replace any character not in the allowed set [a-zA-Z0-9_.-] with an underscore.
        name = re.sub(r"[^a-zA-Z0-9_.-]", "_", name)

        # Remove invalid leading characters.
        i = 0
        while i < len(name):
            if name[i] not in ['_', '.', '-']:
                break
            i += 1
        name = name[i:]

        if not name:
            raise ValueError(
                f"Original name '{container_name}' resulted in an empty string after normalization. "
                "Docker container names must start with an alphanumeric character and can then contain "
                "alphanumerics, underscores, periods, or hyphens."
            )
        return name
    
    @staticmethod
    def _normalize_job_id(id: str) -> str:
        id = DockerAdapter._normalize_container_name(id)
        id = id.replace(DockerAdapter.SEPARATOR, "__")
        return id
        
def get_docker_adapter():
    global _docker_adapter_instance
    if _docker_adapter_instance is None or _docker_adapter_instance.client is None:
        _docker_adapter_instance = DockerAdapter()
    if _docker_adapter_instance.client is None:
        raise HTTPException(status_code=503, detail="Docker daemon not available.")

    return _docker_adapter_instance