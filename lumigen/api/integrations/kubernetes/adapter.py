import typing as t
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from contextlib import suppress
import json
import asyncio
import traceback
import requests
import logging
from fastapi import HTTPException
from kubernetes import client, config, watch
from kubernetes.client import CustomObjectsApi, V1ContainerStatus, V1Job, V1JobCondition, V1JobStatus, V1ObjectMeta, V1Pod, V1PodSpec, V1PodStatus
from kubernetes.client.rest import ApiException
from core.config.root import Config
from core.config.job import JobMode
from utils import yaml
from api.config import settings
from api.enums import JobStatus
from api.schemas import JobStatusResponse, JobInfoResponse, JobMetricsResponse, DataFetchResponse
from api.integrations.base_adapter import JobManagerAdapter
from api.integrations.parser import PySparkLogParser
from api.entry_points.app_preview import DATA_START_DELIMITER, DATA_END_DELIMITER


logger = logging.getLogger(__name__)
_kubernetes_adapter_instance: "KubernetesAdapter" = None

class KubernetesAdapter(JobManagerAdapter):
    """
    Manages ETL jobs as Kubernetes Jobs.
    
    This adapter uses a hybrid strategy:
    - BATCH jobs are configured with no retries (fail-fast).
    - STREAMING jobs are configured with a limited number of retries to handle transient errors.
    """
    _SEPARATOR = '-'

    def __init__(self, kubeconfig_path: t.Optional[str] = None, namespace: t.Optional[str] = None):
        try:
            config.load_incluster_config()
            logger.info("Successfully loaded in-cluster Kubernetes configuration.")
        except config.ConfigException:
            logger.info("Not in a cluster. Attempting to load configuration from kubeconfig file(s).")
            try:
                final_kubeconfig_path = kubeconfig_path or settings.KUBECONFIG
                config.load_kube_config(config_file=final_kubeconfig_path)
                logger.info(f"Loaded Kubernetes configuration from kubeconfig path: {final_kubeconfig_path}")
            except (config.ConfigException, FileNotFoundError) as e:
                logger.error(f"Could not configure Kubernetes client from any source. "
                             f"Please ensure you are either running inside a Kubernetes cluster or your "
                             f"kubeconfig file is correctly configured at the path specified by KUBECONFIG. Error: {e}")
                raise e

        self.namespace = namespace or settings.KUBE_NAMESPACE
        self.core_v1_api = client.CoreV1Api()
        self.batch_v1_api = client.BatchV1Api()
        self.custom_objects_api = CustomObjectsApi()

        try:
            self.core_v1_api.read_namespace(name=self.namespace)
            logger.info(f"Namespace '{self.namespace}' already exists.")
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Namespace '{self.namespace}' not found. Attempting to create it.")
                namespace_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=self.namespace))
                try:
                    self.core_v1_api.create_namespace(body=namespace_body)
                    logger.info(f"Successfully created namespace '{self.namespace}'.")
                except ApiException as create_e:
                    logger.error(f"Failed to create namespace '{self.namespace}'. Error: {create_e.reason}")
                    raise create_e # Re-raise if creation fails
            else:
                logger.error(f"Failed to read namespace '{self.namespace}'. Error: {e.reason}")
                raise e # Re-raise for other errors (e.g., permissions)

        logger.info(f"Kubernetes adapter initialized for namespace '{self.namespace}'.")

        self.host_timezone = "Etc/UTC"
        try:
            localtime_path = Path("/etc/localtime")
            if localtime_path.exists() and localtime_path.is_symlink():
                resolved_path = localtime_path.resolve()
                tz_name = str(resolved_path.relative_to("/usr/share/zoneinfo"))
                self.host_timezone = tz_name
                logger.info(f"Automatically detected host timezone as '{self.host_timezone}'.")
            else:
                logger.warning("Could not resolve /etc/localtime symlink. Defaulting to UTC timezone for pods.")
        except Exception as e:
            logger.error(f"Error detecting host timezone, defaulting to UTC. Reason: {e}")

    def _perform_submit(self, config: Config, id: str, name: t.Optional[str] = None, checkpoint_dir: t.Optional[str]=None) -> JobStatusResponse:
        id_in_k8s_name = self._normalize_k8s_name(id)
        name_in_k8s_name = self._normalize_k8s_name(name)
        resource_name = f"{name_in_k8s_name}{self._SEPARATOR}{id_in_k8s_name}"
        resource_name_norm = self._normalize_k8s_name(resource_name)
        if len(resource_name_norm) < len(resource_name):
            name_in_k8s_name = name_in_k8s_name[:len(resource_name_norm)-len(resource_name)]
            resource_name = self._normalize_k8s_name(f"{name_in_k8s_name}{self._SEPARATOR}{id_in_k8s_name}")

        id_in_label = self._normalize_label_value(id)
        name_in_label = self._normalize_label_value(name)
        app_name_label = self._normalize_k8s_name(settings.PROJECT_NAME)
        job_mode = config.job_properties.get("job_mode", JobMode.BATCH)
        labels = {
            "app.kubernetes.io/managed-by": app_name_label,
            f"{app_name_label}/job-id": id_in_label,
            f"{app_name_label}/job-name": name_in_label,
            f"{app_name_label}/job-mode": str(job_mode).lower()
        }

        if job_mode == JobMode.STREAMING:
            checkpoint_dir = checkpoint_dir or settings.CHECKPOINT_DIR
        if checkpoint_dir:
            pvc_name = settings.KUBE_CHECKPOINT_PVC_NAME
            if not self._check_pvc_exists(pvc_name):
                # Fail fast with a clear error message
                error_detail = (
                    f"Required PersistentVolumeClaim '{pvc_name}' not found in namespace '{self.namespace}'."
                )
                logger.error(f"Job '{id}' submission failed: {error_detail}")
                raise HTTPException(status_code=404, detail=error_detail)

        existing_job = self._get_job_by_id(id)
        if existing_job:
            status = self._get_status_from_k8s_job(existing_job)
            if status in {JobStatus.RUNNING, JobStatus.PENDING, JobStatus.CANCELING}:
                logger.warning(f"A job with ID '{id}' is already active. Skipping submission.")
                return JobStatusResponse(job_id=id, job_name=name, status=status, message=f"Job is already active with status: {status.value}.")
            
            logger.info(f"Found a non-active job for ID '{id}'. Deleting old resources before resubmission.")
            self._delete_job_resources(id)

        # Check for orphaned ConfigMaps, just in case.
        if self._get_config_map_by_name(resource_name):
            logger.warning(f"Found an orphaned ConfigMap '{resource_name}'. Deleting it.")
            self._cleanup_config_map(resource_name)

        try:
            job_object = self._create_k8s_job_object(resource_name, labels, config, checkpoint_dir)
            created_job = self.batch_v1_api.create_namespaced_job(namespace=self.namespace, body=job_object)
            self._create_owned_config_map(resource_name, labels, config, owner=created_job)
            logger.info(f"Successfully submitted Kubernetes Job '{resource_name}' for ETL job '{id}'.")

            # After successful submission, briefly monitor the pod for critical startup errors.
            try:
                self._verify_pod_startup(id, timeout_seconds=30)
                logger.info(f"Pod for job '{id}' started successfully without critical issues.")
            except (ImagePullError, StartupTimeoutError) as verification_error:
                logger.error(f"Post-submission verification failed for job '{id}': {verification_error}")
                self._delete_job_resources(id, wait_for_deletion=False)
                raise HTTPException(status_code=400, detail=str(verification_error)) from None

            self._prune_jobs()
            return JobStatusResponse(job_id=id, job_name=name, status=JobStatus.PENDING, message=f"Job submitted to Kubernetes as '{resource_name}'.")
        except ApiException as e:
            logger.error(f"Kubernetes API error while submitting job '{id}': {e.reason}")
            with suppress(ApiException):
                self.batch_v1_api.delete_namespaced_job(name=resource_name, namespace=self.namespace)
            raise HTTPException(status_code=e.status, detail=f"Failed to submit job: {e.reason}")
    
    def stop_job(self, id: str) -> JobStatusResponse:
        job = self._get_job_by_id(id)
        if not job:
            return JobStatusResponse(job_id=id, status=JobStatus.UNKNOWABLE, message="Job not found.")

        current_status = self._get_status_from_k8s_job(job)
        if current_status.is_end_state():
             return JobStatusResponse(job_id=id, status=current_status, message="Job is already in a terminal state.")

        resource_name = job.metadata.name
        job_name = job.metadata.labels.get(f"{self._normalize_k8s_name(settings.PROJECT_NAME)}/job-name")

        try:
            patch_body = {"spec": {"suspend": True}}
            self.batch_v1_api.patch_namespaced_job(name=resource_name, namespace=self.namespace, body=patch_body)
            logger.info(f"Successfully suspended Kubernetes Job '{resource_name}' for job ID '{id}'.")
            return JobStatusResponse(job_id=id, job_name=job_name, status=JobStatus.CANCELING, message="Job stop request accepted and is being processed.")
        except ApiException as e:
            logger.error(f"Failed to suspend job '{id}': {e.reason}")
            raise HTTPException(status_code=e.status, detail=f"Could not suspend job '{id}': {e.reason}")
        
    def delete_job(self, id: str) -> JobStatusResponse:
        self._delete_job_resources(id, wait_for_deletion=False)
        return JobStatusResponse(job_id=id, status=JobStatus.UNKNOWABLE, message="Job deletion request accepted and is being processed.")
    
    def get_job_name(self, job_id: str) -> t.Optional[str]:
        job = self._get_job_by_id(job_id)
        if job:
            return job.metadata.labels.get(f"{self._normalize_k8s_name(settings.PROJECT_NAME)}/job-name")
        return None

    def get_job_status(self, id: str) -> JobStatus:
        job = self._get_job_by_id(id)
        return self._get_status_from_k8s_job(job) if job else JobStatus.UNKNOWABLE

    def _get_status_from_k8s_job(self, job: client.V1Job) -> JobStatus:
        if not job or not job.status:
            return JobStatus.UNKNOWABLE
        
        status: V1JobStatus = job.status
        if status.conditions:
            def get_transition_time(condition: client.V1JobCondition) -> datetime:
                return condition.last_transition_time or datetime.min.replace(tzinfo=timezone.utc)
            sorted_conditions = sorted(
                status.conditions,
                key=get_transition_time,
                reverse=True
            )
            condition: V1JobCondition
            for condition in sorted_conditions:
                if condition.type == "Complete" and condition.status == "True":
                    return JobStatus.FINISHED
                if (condition.type == "Failed" or condition.type == "FailureTarget") and condition.status == "True":
                    return JobStatus.FAILED
                if condition.type == "Suspended" and condition.status == "True":
                    return JobStatus.CANCELED

        # If no terminal condition is found, fall back to the pod counters.
        if status.terminating and status.terminating > 0: return JobStatus.CANCELING
        if status.failed and status.failed > 0: return JobStatus.FAILING
        if status.succeeded and status.succeeded > 0: return JobStatus.FINISHED
        if status.active and status.active > 0:
            if status.ready and status.ready > 0:
                return JobStatus.RUNNING
            return JobStatus.PENDING
        return JobStatus.CREATED
    
    def get_job_info(self, id: str) -> JobInfoResponse:
        job = self._get_job_by_id(id)
        if not job:
            return JobInfoResponse(job_id=id, status=JobStatus.UNKNOWABLE)

        status = self._get_status_from_k8s_job(job)
        start_time_str = None
        if job.status.start_time and isinstance(job.status.start_time, datetime):
            start_time_str = job.status.start_time.isoformat()

        finish_time_str = None
        if job.status.completion_time and isinstance(job.status.completion_time, datetime):
            finish_time_str = job.status.completion_time.isoformat()
        return JobInfoResponse(
            job_id=id,
            job_name=self.get_job_name(id),
            status=status,
            message=f"Job is currently {str(status).lower()}.",
            start_time=start_time_str,
            finish_time=finish_time_str,
        )
    
    def get_job_metrics(self, id: str) -> JobMetricsResponse:
        pod = self._get_pod_for_job(id)
        if not pod:
            logger.debug(f"No active or recent pod found for job '{id}' to fetch metrics.")
            return JobMetricsResponse()

        if settings.METRICS_PROVIDER.lower() == "prometheus":
            logger.debug(f"Fetching metrics for pod '{pod.metadata.name}' from Prometheus.")
            return self._get_metrics_from_prometheus(pod)
        else: # Default to 'metrics-api'
            logger.debug(f"Fetching metrics for pod '{pod.metadata.name}' from Metrics API.")
            return self._get_metrics_from_api(pod)
        
    def _get_metrics_from_api(self, pod: V1Pod) -> JobMetricsResponse:
        """
        Fetches basic resource metrics (CPU, Memory) from the Kubernetes Metrics API.
        Requires the 'metrics-server' to be installed.
        """
        pod_name = pod.metadata.name
        try:
            metrics = self.custom_objects_api.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="pods",
                name=pod_name,
            )
        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    f"Metrics API endpoint not found for pod '{pod_name}'. "
                    "Ensure the Kubernetes Metrics Server is installed and running."
                )
            else:
                logger.error(
                    f"API error fetching metrics for pod '{pod_name}': {e.reason}"
                )
            return JobMetricsResponse()
        except Exception as e:
            logger.error(f"Unexpected error fetching metrics for pod '{pod_name}': {e}")
            return JobMetricsResponse()

        cpu_usage_cores = 0.0
        memory_usage_bytes = 0
        if "containers" in metrics and metrics["containers"]:
            for container_metrics in metrics["containers"]:
                usage = container_metrics.get("usage", {})
                
                # Parse CPU: '10253456n' (nanocores) -> 0.01025... cores
                cpu_str = usage.get("cpu", "0n")
                if cpu_str.endswith('n'):
                    cpu_usage_cores += int(cpu_str[:-1]) / 1e9 # Convert nanocores to cores
                elif cpu_str.endswith('u'):
                    cpu_usage_cores += int(cpu_str[:-1]) / 1e6 # Convert microcores to cores
                elif cpu_str.endswith('m'):
                    cpu_usage_cores += int(cpu_str[:-1]) / 1e3 # Convert millicores to cores
                else:
                    cpu_usage_cores += float(cpu_str)

                # Parse Memory: '134567Ki' (kibibytes) -> bytes
                mem_str = usage.get("memory", "0Ki")
                if mem_str.endswith('Ki'):
                    memory_usage_bytes += int(mem_str[:-2]) * 1024
                elif mem_str.endswith('Mi'):
                    memory_usage_bytes += int(mem_str[:-2]) * 1024**2
                elif mem_str.endswith('Gi'):
                    memory_usage_bytes += int(mem_str[:-2]) * 1024**3
                else: # Assume bytes if no suffix
                    with suppress(ValueError):
                        memory_usage_bytes += int(mem_str)

        # This represents the number of cores being used.
        cpu_usage_percent = cpu_usage_cores * 100

        return JobMetricsResponse(
            cpu_usage_percent=cpu_usage_percent,
            memory_usage_bytes=memory_usage_bytes,
        )
    
    def _get_metrics_from_prometheus(self, pod: V1Pod) -> JobMetricsResponse:
        """
        Fetches rich metrics from a Prometheus instance. Requires a Prometheus
        stack (like prometheus-operator) and node-exporter to be running.
        """
        pod_name = pod.metadata.name
        pod_ip = pod.status.pod_ip
        
        if not pod_ip:
            logger.warning(f"Pod '{pod_name}' has no IP address. Cannot query Prometheus for metrics.")
            return JobMetricsResponse()

        try:
            # Note: The 'instance' label in Prometheus often corresponds to the node's IP and a port.
            # We will query by pod name. The exact PromQL queries might need tuning
            # based on your specific Prometheus label configuration (e.g., from prometheus-operator).
            
            # These queries are adapted for a specific pod.
            queries = {
                "cpu_usage": f'sum(rate(container_cpu_usage_seconds_total{{pod="{pod_name}",namespace="{self.namespace}"}}[5m]))',
                "memory_used": f'sum(container_memory_usage_bytes{{pod="{pod_name}",namespace="{self.namespace}"}})',
                "disk_read_throughput": f'sum(rate(container_fs_reads_bytes_total{{pod="{pod_name}",namespace="{self.namespace}"}}[5m]))',
                "disk_write_throughput": f'sum(rate(container_fs_writes_bytes_total{{pod="{pod_name}",namespace="{self.namespace}"}}[5m]))',
                "network_rx": f'sum(rate(container_network_receive_bytes_total{{pod="{pod_name}",namespace="{self.namespace}"}}[5m]))',
                "network_tx": f'sum(rate(container_network_transmit_bytes_total{{pod="{pod_name}",namespace="{self.namespace}"}}[5m]))',
            }
            results = {k: self._query_prometheus(q) for k, q in queries.items()}

            def get_value(result: list) -> float:
                return float(result[0]['value'][1]) if result and result[0] else 0.0

            cpu_cores = get_value(results.get("cpu_usage"))
            mem_bytes = get_value(results.get("memory_used"))
            read_bytes_sec = get_value(results.get("disk_read_throughput"))
            write_bytes_sec = get_value(results.get("disk_write_throughput"))
            
            # Note: Prometheus doesn't provide IOPS directly in the same way. Rate of completion is a good proxy.   
            return JobMetricsResponse(
                cpu_usage_percent=cpu_cores * 100,
                memory_usage_bytes=mem_bytes,
                volume_bytes=None, 
                read_bytes_per_second=read_bytes_sec,
                write_bytes_per_second=write_bytes_sec,
                # TODO: IOPS requires different queries, often from node_exporter
                read_iops=None,
                write_iops=None
            )

        except Exception as e:
            logger.error(f"Failed to query or parse Prometheus metrics for pod '{pod_name}': {e}", exc_info=True)
            return JobMetricsResponse()

    def _query_prometheus(self, promql: str) -> t.Optional[dict]:
        if not settings.PROMETHEUS_URL:
            logger.warning("PROMETHEUS_URL is not set. Cannot query Prometheus.")
            return None
        
        try:
            url = f"{settings.PROMETHEUS_URL}/api/v1/query"
            response = requests.get(url, params={'query': promql}, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "success":
                return data['data']['result']
            else:
                logger.error(f"Prometheus query failed with status '{data.get('status')}': {data.get('error')}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request to Prometheus failed: {e}")
            return None
    
    def get_job_logs(self, id: str,
                     tail: t.Optional[int] = None, since: t.Optional[int] = None,
                     stdout: t.Optional[bool] = True, stderr: t.Optional[bool] = True,
                     timestamps: t.Optional[bool] = False) -> t.Optional[str]:
        pod = self._get_pod_for_job(id)
        if not pod:
            raise HTTPException(status_code=404, detail=f"Job with ID '{id}' does not exist or was removed.")
        
        if not stdout and not stderr:
            return ""
        
        since_seconds_param = None
        if since is not None:
            try:
                now_utc = datetime.now(timezone.utc)
                since_dt_utc = datetime.fromtimestamp(since, tz=timezone.utc)
                duration_seconds = (now_utc - since_dt_utc).total_seconds()

                if duration_seconds < 0:
                    logger.warning(f"The 'since' timestamp for job '{id}' is in the future. Returning all logs.")
                else:
                    since_seconds_param = int(duration_seconds)
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid 'since' timestamp provided for job '{id}': {since}. Error: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid 'since' timestamp: {since}. Must be a valid UNIX timestamp.")
        
        try:
            return self.core_v1_api.read_namespaced_pod_log(
                name=pod.metadata.name, namespace=self.namespace,
                tail_lines=tail, since_seconds=since_seconds_param, timestamps=timestamps
            )
        except ApiException as e:
            error_detail = f"Could not retrieve logs for job '{id}': {e.reason}"
            if e.status == 400: # Bad Request, e.g., pod is still creating
                error_detail = f"Logs for job '{id}' are not yet available. The pod may still be initializing."
            raise HTTPException(status_code=e.status, detail=error_detail)
    
    def list_jobs(self, sparse: t.Optional[bool] = True, name: t.Optional[str] = None) -> t.List[t.Dict[str, t.Any]]:
        app_name_label = self._normalize_k8s_name(settings.PROJECT_NAME)
        label_selector = f"app.kubernetes.io/managed-by={app_name_label}"
        if name:
            label_selector += f",{app_name_label}/job-name={self._normalize_k8s_name(name)}"
        
        try:
            jobs = self.batch_v1_api.list_namespaced_job(self.namespace, label_selector=label_selector).items
            result = []
            job: V1Job
            for job in jobs:
                job_metadata: V1ObjectMeta = job.metadata
                # Should be de-normalized in a real app
                id_in_label = job_metadata.labels.get(f"{app_name_label}/job-id")
                if not id_in_label: continue
                
                if sparse:
                    start_time = job_metadata.creation_timestamp
                    start_time_str = start_time.isoformat() if start_time else None
                    result.append({
                        "job_id": id_in_label,
                        "job_name": job_metadata.labels.get(f"{app_name_label}/job-name"),
                        "start_time": start_time_str,
                        "job_mode": job_metadata.labels.get(f"{app_name_label}/job-mode", str(JobMode.BATCH).lower()).upper(),
                    })
                else:
                    result.append(self.get_job_info(id_in_label).model_dump())
            return result
        except ApiException as e:
            logger.error(f"Could not list Kubernetes jobs: {e.reason}")
            return []

    async def run_disposable_job(self, config: Config, temp_job_name: t.Optional[str] = "Temp job") -> DataFetchResponse:
        temp_id = f"temp-{os.urandom(4).hex()}"
        resource_name = self._normalize_k8s_name(f"{temp_job_name}{self._SEPARATOR}{temp_id}")
        app_name_label = self._normalize_k8s_name(settings.PROJECT_NAME)
        labels = {
            "app.kubernetes.io/managed-by": f"{app_name_label}-temp",
            f"{app_name_label}/job-id": self._normalize_label_value(temp_id),
        }
        
        # We need to run the blocking k8s calls in a separate thread
        def _run_disposable_job_sync():
            try:
                # Create resources
                job_object = self._create_k8s_job_object(resource_name, labels, config, ttl_override=60) # Short TTL for temp jobs
                created_job = self.batch_v1_api.create_namespaced_job(namespace=self.namespace, body=job_object)
                self._create_owned_config_map(resource_name, labels, config, owner=created_job)

                # Watch for completion
                w = watch.Watch()
                final_status = None
                for event in w.stream(
                    self.batch_v1_api.list_namespaced_job,
                    namespace=self.namespace,
                    field_selector=f"metadata.name={resource_name}",
                    timeout_seconds=settings.TEMP_JOB_TIMEOUT_SECONDS
                ):
                    job_status: V1JobStatus = event['object'].status
                    if (job_status.succeeded and job_status.succeeded > 0) or \
                       (job_status.failed and job_status.failed > 0):
                        final_status = self._get_status_from_k8s_job(event['object'])
                        w.stop()
                        break
                
                logs = self.get_job_logs(temp_id)
                if final_status is None:
                    error_msg = "Temp job did not complete within the timeout."
                    return DataFetchResponse(
                        success=False,
                        detail=error_msg,
                        logs=logs,
                    )

                log_parser = PySparkLogParser()
                if final_status != JobStatus.FINISHED:
                    short_err_logs = logs
                    last_exception = log_parser.parse_last_exception(logs)
                    if last_exception and last_exception.get("message"):
                        short_err_logs = last_exception["message"]
                    return DataFetchResponse(
                        success=False,
                        detail=short_err_logs,
                        logs=logs,
                    )

                parsed_data = log_parser.parse_json_data_with_delimiters(logs, DATA_START_DELIMITER, DATA_END_DELIMITER)
                if parsed_data is not None:
                    logger.debug(f"{temp_job_name} ran successfully.")
                    return DataFetchResponse(
                        success=True,
                        data=parsed_data,
                        detail=f"{temp_job_name} ran successfully.",
                        logs=log_parser.redact_delimited_content(logs, DATA_START_DELIMITER, DATA_END_DELIMITER, replacement_text=""),
                    )
                else:
                    error_msg = "Data fetch failed or data not found."
                    return DataFetchResponse(
                        success=False,
                        detail=error_msg,
                        logs=logs,
                    )
            finally:
                self._delete_job_resources(temp_id, wait_for_deletion=False)

        try:
            return await asyncio.to_thread(_run_disposable_job_sync)
        except Exception as e:
            logger.error(f"Error running disposable job '{resource_name}': {traceback.format_exc()}")
            return DataFetchResponse(success=False, detail=str(e), logs=traceback.format_exc())

    def _create_owned_config_map(self, resource_name: str, labels: dict, config: Config, owner: client.V1Job):
        config_as_dict = json.loads(config.json())
        config_yaml_str = yaml.dump(config_as_dict)
        owner_metadata: V1ObjectMeta = owner.metadata
        owner_reference = client.V1OwnerReference(
            api_version=owner.api_version, kind=owner.kind,
            name=owner_metadata.name, uid=owner_metadata.uid,
            block_owner_deletion=True
        )
        
        metadata = client.V1ObjectMeta(
            name=resource_name, labels=labels, owner_references=[owner_reference]
        )
        
        config_map = client.V1ConfigMap(
            api_version="v1", kind="ConfigMap", metadata=metadata, data={"config.yaml": config_yaml_str}
        )
        self.core_v1_api.create_namespaced_config_map(namespace=self.namespace, body=config_map)

    def _get_config_map_by_name(self, resource_name: str) -> t.Optional[client.V1ConfigMap]:
        try:
            return self.core_v1_api.read_namespaced_config_map(name=resource_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise # Re-raise other API errors

    def _cleanup_config_map(self, resource_name: str):
        with suppress(ApiException):
            self.core_v1_api.delete_namespaced_config_map(name=resource_name, namespace=self.namespace)

    def _check_pvc_exists(self, pvc_name: str) -> bool:
        try:
            self.core_v1_api.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace)
            logger.debug(f"Confirmed that PVC '{pvc_name}' exists in namespace '{self.namespace}'.")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"PVC '{pvc_name}' not found in namespace '{self.namespace}'.")
                return False
            else:
                # For other errors (e.g., permissions), we should fail loudly.
                logger.error(f"API error while checking for PVC '{pvc_name}': {e.reason}")
                raise
    
    def _create_k8s_pod_template_spec(self, resource_name: str, labels: dict, config: Config, command: t.List[str],
                                      checkpoint_dir: t.Optional[str]=None) -> client.V1PodTemplateSpec:
        volume_mounts = [
            client.V1VolumeMount(name="config-volume", mount_path="/config")
        ]
        volumes = [
            client.V1Volume(name="config-volume", config_map=client.V1ConfigMapVolumeSource(name=resource_name))
        ]
        if checkpoint_dir:
            pvc_name = settings.KUBE_CHECKPOINT_PVC_NAME
            logger.info(f"Adding checkpoint volume mount from PVC '{pvc_name}' to path '{checkpoint_dir}'")
            
            volume_mounts.append(
                client.V1VolumeMount(name="checkpoint-volume", mount_path=checkpoint_dir)
            )
            volumes.append(
                client.V1Volume(
                    name="checkpoint-volume",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
                )
            )

        env_vars = [
            client.V1EnvVar(name="TZ", value=self.host_timezone)
        ]

        container = client.V1Container(
            name="etl-container", image=settings.JOB_IMAGE_NAME, command=command,
            volume_mounts=volume_mounts,
            env=env_vars,
            image_pull_policy="IfNotPresent",
        )
        
        pod_labels = {"job-name": resource_name}
        
        return client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=pod_labels),
            spec=client.V1PodSpec(containers=[container], volumes=volumes)
        )
    
    def _create_k8s_job_object(self, resource_name: str, labels: dict, config: Config,
                               checkpoint_dir: t.Optional[str]=None,
                               ttl_override: t.Optional[int]=None) -> client.V1Job:
        is_preview = "preview" in resource_name.lower()
        config_path_in_pod = "/config/config.yaml" 
        command = self._build_command(config, config_path_in_pod, is_preview=is_preview)

        pod_template = self._create_k8s_pod_template_spec(resource_name, labels, config, command, checkpoint_dir)
        job_mode = config.job_properties.get("job_mode", JobMode.BATCH)
        
        pod_spec: V1PodSpec = pod_template.spec
        retry_limit = 0
        if job_mode == JobMode.STREAMING and not is_preview:
            retry_limit = settings.STREAMING_JOB_RETRY_LIMIT
            pod_spec.restart_policy = "OnFailure"
        else: # BATCH
            pod_spec.restart_policy = "Never"
            
        ttl_seconds = ttl_override if ttl_override is not None else (settings.PRUNE_JOB_STOPPED_FOR_MINUTES * 60)
        job_spec = client.V1JobSpec(
            template=pod_template,
            backoff_limit=retry_limit,
            ttl_seconds_after_finished=ttl_seconds,
        )
        return client.V1Job(
            api_version="batch/v1", kind="Job",
            metadata=client.V1ObjectMeta(name=resource_name, labels=labels),
            spec=job_spec
        )
    
    def _get_job_by_id(self, raw_id: str) -> t.Optional[client.V1Job]:
        id_in_label = self._normalize_label_value(raw_id)
        app_name_label = self._normalize_k8s_name(settings.PROJECT_NAME)
        label_selector = f"{app_name_label}/job-id={id_in_label}"
        try:
            jobs = self.batch_v1_api.list_namespaced_job(self.namespace, label_selector=label_selector).items
            return jobs[0] if jobs else None
        except ApiException as e:
            logger.warning(f"API exception while trying to get job for ID '{raw_id}': {e.reason}")
            return None
        
    def _get_pod_for_job(self, raw_id: str) -> t.Optional[client.V1Pod]:
        job = self._get_job_by_id(raw_id)
        if not job: return None
        
        pod_label_selector = f"job-name={job.metadata.name}"
        try:
            pods = self.core_v1_api.list_namespaced_pod(self.namespace, label_selector=pod_label_selector).items
            if not pods: return None
            pods.sort(key=lambda p: p.metadata.creation_timestamp, reverse=True)
            return pods[0]
        except ApiException:
            return None
        
    def _verify_pod_startup(self, raw_id: str, timeout_seconds: int):
        start_time = time.time()
        pod = None
        
        # 1. Find the pod, it might take a moment to be created.
        while time.time() - start_time < timeout_seconds:
            pod = self._get_pod_for_job(raw_id)
            if pod:
                break
            time.sleep(1)
        
        if not pod:
            raise StartupTimeoutError(f"Timed out waiting for pod to be created for job '{raw_id}'.")

        # 2. Monitor the pod's status for critical errors.
        while time.time() - start_time < timeout_seconds:
            try:
                # Get the latest pod status
                pod: V1Pod = self.core_v1_api.read_namespaced_pod(name=pod.metadata.name, namespace=self.namespace)
                
                # Check container statuses for waiting state with image pull errors
                pod_status: V1PodStatus = pod.status
                if pod_status and pod_status.container_statuses:
                    cs: V1ContainerStatus
                    for cs in pod_status.container_statuses:
                        if cs.state and cs.state.waiting:
                            reason = cs.state.waiting.reason
                            if reason in ("ImagePullBackOff", "ErrImagePull"):
                                raise ImagePullError(f"Failed to pull image '{cs.image}': {cs.state.waiting.message}")
                
                # If pod is running or completed, verification is successful
                if pod_status.phase in ("Running", "Succeeded"):
                    return
                # If pod has definitively failed for other reasons, stop monitoring
                if pod_status.phase == "Failed":
                    return # Let the regular status check handle this failure type

            except ApiException as e:
                if e.status == 404: # Pod might have been deleted quickly, consider it ok
                    return
                raise # Re-raise other API errors
            
            time.sleep(2)

        raise StartupTimeoutError(f"Timed out waiting for pod of job '{raw_id}' to reach a stable state.")
    
    def _delete_job_resources(self, raw_id: str, wait_for_deletion: bool = True):
        job = self._get_job_by_id(raw_id)
        if not job: return

        resource_name = job.metadata.name
        try:
            # Issue the delete command
            self.batch_v1_api.delete_namespaced_job(
                name=resource_name,
                namespace=self.namespace,
                propagation_policy='Foreground'
            )
            logger.info(f"Initiated deletion of Kubernetes Job '{resource_name}'.")

            if not wait_for_deletion: return
            # Poll the API until the job is no longer found (or we time out).
            timeout_seconds = 60
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                if self._get_job_by_id(raw_id) is None:
                    logger.info(f"Successfully confirmed deletion of Job '{resource_name}'.")
                    # The owned ConfigMap will be garbage collected automatically.
                    return
                time.sleep(1) # Wait 1 second between checks
            
            logger.error(f"Timed out waiting for job '{resource_name}' to be deleted.")
            raise Exception(f"Deletion timeout for job {resource_name}")

        except ApiException as e:
            if e.status == 404: # Already gone, which is our desired state
                logger.info(f"Job '{resource_name}' was already deleted.")
                return
            logger.error(f"API Error during deletion or waiting for job '{raw_id}': {e.reason}")
            raise # Re-raise other API errors

    def _prune_jobs(self):
        """
        Prunes old jobs based on a configured count per job name.
        Time-based pruning is handled automatically by Kubernetes via `ttlSecondsAfterFinished` set on the Job spec.
        """
        app_name_label = self._normalize_k8s_name(settings.PROJECT_NAME)
        label_selector = f"app.kubernetes.io/managed-by={app_name_label}"

        try:
            all_jobs = self.batch_v1_api.list_namespaced_job(self.namespace, label_selector=label_selector).items
        except ApiException as e:
            logger.error(f"Failed to list Kubernetes jobs for pruning: {e.reason}")
            return 0

        jobs_by_name = defaultdict(list)
        job: V1Job
        for job in all_jobs:
            job_name = job.metadata.labels.get(f"{app_name_label}/job-name")
            if not job_name:
                continue
            jobs_by_name[job_name].append(job)

        jobs_to_prune_by_count = []
        for name, job_list in jobs_by_name.items():
            keep_cnt = 0
            # Sort jobs by creation time, newest first, for consistent processing
            job_list.sort(key=lambda j: j.metadata.creation_timestamp, reverse=True)
            finished_jobs = []
            prune_candidates = []
            for job in job_list:
                status = self._get_status_from_k8s_job(job)
                if status in {JobStatus.RUNNING, JobStatus.PENDING, JobStatus.FAILING, JobStatus.CANCELING, JobStatus.CREATED}:
                    # Rule: Keep all running containers
                    keep_cnt += 1
                elif status == JobStatus.FINISHED:
                    finished_jobs.append(job)
                else:
                    # Consider all unsuccessful non-running (FAILED, CANCELED, UNKNOWABLE) jobs for deletion
                    prune_candidates.append(job)

            # Rule: If can keep more than 1 container, keep only the last created finished container
            # and remove other finished containers.
            if len(finished_jobs) > 0 and keep_cnt + 2 <= settings.MAX_KEPT_JOBS_PER_NAME:
                keep_cnt += 1
                prune_candidates.extend(finished_jobs[1:])
            else:
                prune_candidates.extend(finished_jobs)

            # Rule: Remove the oldest non-running containers until the limit
            add_keep_cnt = max(settings.MAX_KEPT_JOBS_PER_NAME - keep_cnt, 0)
            for job in prune_candidates[add_keep_cnt:]:
                jobs_to_prune_by_count.append(job)

        total_pruned_count = 0
        job_to_prune: V1Job
        for job_to_prune in jobs_to_prune_by_count:
            try:
                id_in_label = job_to_prune.metadata.labels.get(f"{app_name_label}/job-id")
                logger.info(
                    f"Pruning job '{job_to_prune.metadata.name}' for ID '{id_in_label}' "
                    f"(reason: exceeded count limit of {settings.MAX_KEPT_JOBS_PER_NAME} for job name '{name}')."
                )
                # Note: label id is currently used instead of raw id
                self._delete_job_resources(id_in_label, wait_for_deletion=False)
                total_pruned_count += 1
            except Exception as e:
                logger.warning(
                    f"Failed to prune job '{job_to_prune.metadata.name}': {e}",
                    exc_info=settings.LOG_LEVEL == "DEBUG"
                )
        if total_pruned_count > 0:
            logger.debug(f"Count-based pruning finished. Total jobs pruned: {total_pruned_count}.")
        else:
            logger.debug("Count-based pruning finished. No jobs met the criteria for pruning.")
        return total_pruned_count
        
    @staticmethod
    def _normalize_k8s_name(name: str) -> str:
        if not name:
            # Handle empty input gracefully
            return f"unnamed-{os.urandom(4).hex()}"
        name = name.lower()
        # Allows only lowercase letters, numbers, and hyphens.
        name = re.sub(r'[^a-z0-9-]', '-', name)
        # Replaces multiple hyphens with a single one.
        name = re.sub(r'-+', '-', name).strip('-')
        # Kubernetes names must be <= 63 characters.
        return name[:63]

    @staticmethod
    def _normalize_label_value(value: str) -> str:
        if not value:
            return "unknown"
        # Replaces invalid characters with an underscore.
        value = re.sub(r'[^a-zA-Z0-9_.-]', '_', value)
        # Cannot start or end with invalid characters.
        value = value.strip('_.-')
        # Kubernetes label values must be <= 63 characters.
        if not value:
            return "empty"
        return value[:63]

class ImagePullError(Exception):
    pass

class StartupTimeoutError(Exception):
    pass

def get_kubernetes_adapter():
    global _kubernetes_adapter_instance
    if _kubernetes_adapter_instance is None:
        try:
            _kubernetes_adapter_instance = KubernetesAdapter()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Kubernetes adapter not available: {e}")
        
    return _kubernetes_adapter_instance