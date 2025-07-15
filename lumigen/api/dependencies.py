from fastapi import HTTPException, Request
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from api.config import settings
from api.integrations.base_adapter import JobManagerAdapter
from api.integrations.docker.adapter import get_docker_adapter
from api.integrations.kubernetes.adapter import get_kubernetes_adapter


_logger = logging.getLogger(__name__)
_job_manager_instance: JobManagerAdapter = None
_adapter_lock = asyncio.Lock() # For thread-safe initialization

async def get_job_manager() -> JobManagerAdapter:
    global _job_manager_instance
    if _job_manager_instance is None:
        async with _adapter_lock:
            if _job_manager_instance is None:
                if settings.JOB_MANAGER_TYPE.lower() == "kubernetes":
                    _logger.info("Using Kubernetes job manager.")
                    _job_manager_instance = get_kubernetes_adapter()
                elif settings.JOB_MANAGER_TYPE.lower() == "docker":
                    _logger.info("Using Docker job manager.")
                    _job_manager_instance = get_docker_adapter()
                else:
                     raise ValueError(f"Unsupported JOB_MANAGER_TYPE: {settings.JOB_MANAGER_TYPE}")

    if _job_manager_instance is None:
        raise HTTPException(status_code=500, detail="Job manager could not be initialized.")

    return _job_manager_instance

def get_custom_executor(request: Request) -> ThreadPoolExecutor:
    if hasattr(request.app.state, "custom_blocking_task_executor"):
        return request.app.state.custom_blocking_task_executor
    else:
        _logger.warning("WARNING: Custom executor not found in app.state. Falling back to default asyncio behavior (less ideal).")
        return None