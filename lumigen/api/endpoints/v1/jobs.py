import typing as t
from fastapi import APIRouter, Request, Response, HTTPException, status, Depends, BackgroundTasks, Query
from fastapi.responses import PlainTextResponse, StreamingResponse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from core.config.root import Config
from api.config import settings
from api.enums import JobStatus
from api.schemas import JobRequest, JobStatusResponse, JobInfoResponse, JobMetricsResponse, JobPreviewResponse, JobColumnMapping
from api.integrations.base_adapter import JobManagerAdapter
from api.dependencies import get_job_manager, get_custom_executor
from api.services.watcher import JobStatusWatcher

logger = logging.getLogger(__name__)


router = APIRouter()
preview_job_semaphore = asyncio.Semaphore(settings.PREVIEW_JOB_CONCURRENCY_LIMIT)

@router.post("/preview", response_model=JobPreviewResponse)
async def preview_job(response: Response,
                      config: Config, sample_size: int = 20, source_limit: int = 8192,
                      job_manager: JobManagerAdapter = Depends(get_job_manager)):
    async with preview_job_semaphore:
        response_data = await job_manager.get_data_preview(config, sample_size=sample_size, source_limit=source_limit)
    if not response_data.success:
        response.status_code = status.HTTP_400_BAD_REQUEST
    return response_data

@router.post("/column-mapping", response_model=JobColumnMapping)
async def column_mapping(response: Response, config: Config,
                         job_manager: JobManagerAdapter = Depends(get_job_manager)):
    async with preview_job_semaphore:
        response_data = await job_manager.get_column_mapping_preview(config)
    if not response_data.success:
        response.status_code = status.HTTP_400_BAD_REQUEST
    return response_data

def run_job_submission_background(job_manager: JobManagerAdapter, config: Config, id: str, name: t.Optional[str]):
    """Wrapper function for the background task to handle exceptions gracefully."""
    try:
        job_manager.submit_job(config, id, name)
    except Exception:
        logger.exception(f"Background submission for job ID '{id}' failed.")

@router.post("/submit", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(background_tasks: BackgroundTasks,
                     config: Config, id: str, name: t.Optional[str]=None,
                     job_manager: JobManagerAdapter = Depends(get_job_manager)):
    background_tasks.add_task(run_job_submission_background, job_manager, config, id, name)
    response = JobStatusResponse(
        job_id=id,
        job_name=name,
        status=JobStatus.INITIALIZING,
        message=f"Job '{id}' submission accepted and is being processed in the background.",
    )
    return response

@router.get("/status", response_model=JobStatusResponse)
async def get_job_status(id: str,
                         job_manager: JobManagerAdapter = Depends(get_job_manager),
                         executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, job_manager.get_job_status, id)
    return JobStatusResponse(job_id=id, status=response, message=f"Job is currently {str(response).lower()}.")

@router.get("/info", response_model=JobInfoResponse)
async def get_job_info(id: str,
                       job_manager: JobManagerAdapter = Depends(get_job_manager),
                       executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, job_manager.get_job_info, id)
    return response

@router.get("/watch")
async def watch_job_status(request: Request,
                           id: str):
    """
    Streams job information to the client via Server-Sent Events (SSE).
    Data is only yielded when the job's status changes.
    The connection closes automatically when the job reaches a terminal state.
    """
    watcher: JobStatusWatcher = request.app.state.job_status_watcher

    def _attempt_recovery():
        logger.warning("Attempting recovery of JobStatusWatcher...")
        try:
            watcher.start(watcher._job_manager, watcher._executor)
        except Exception as e:
            logger.error(f"Mid-stream recovery failed: {e}", exc_info=True)
            raise RuntimeError("Background monitoring service is currently unavailable.") from e

    if not watcher.is_alive:
        try:
            _attempt_recovery()
        except RuntimeError:
            raise HTTPException(status_code=503, detail="Background monitoring service is unavailable.")

    client_queue = await watcher.subscribe(id)
    async def event_generator():
        watch_timeout = 7.0 
        try:
            while True:
                try:
                    job_info: JobInfoResponse = await asyncio.wait_for(
                        client_queue.get(), 
                        timeout=watch_timeout,
                    )
                except asyncio.TimeoutError:
                    if not watcher.is_alive:
                        _attempt_recovery()
                    continue

                # Yield data in standard SSE format
                json_data = job_info.model_dump_json() if hasattr(job_info, "model_dump_json") else job_info.json()
                yield f"data: {json_data}\n\n"

                # Disconnect the client stream if the job has hit a definitive terminal state
                if job_info.status.is_end_state():
                    break
        except asyncio.CancelledError: # Client disconnected
            pass
        finally:
            watcher.unsubscribe(id, client_queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@router.get("/metrics", response_model=JobMetricsResponse)
async def get_job_metrics(id: str,
                          job_manager: JobManagerAdapter = Depends(get_job_manager),
                          executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, job_manager.get_job_metrics, id)
    return response

@router.post("/stop", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def stop_job(background_tasks: BackgroundTasks,
                   id: str,
                   job_manager: JobManagerAdapter = Depends(get_job_manager)):
    background_tasks.add_task(job_manager.stop_job, id)
    response = JobStatusResponse(
        job_id=id,
        status=JobStatus.CANCELING,
        message=f"Job '{id}' termination accepted and is being processed in the background.",
    )
    return response

def _batch_stop_sync(job_ids: t.List[JobRequest], job_manager: JobManagerAdapter):
    response = []
    for request in job_ids:
        try:
            job_info = job_manager.stop_job(request.id) # This is the sync call
            response.append(job_info)
        except:
            continue
    return response

@router.post("/batch-stop", response_model=t.List[JobStatusResponse])
async def batch_stop_job(job_ids: t.List[JobRequest],
                         job_manager: JobManagerAdapter = Depends(get_job_manager),
                         executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, _batch_stop_sync, job_ids, job_manager)
    return response

@router.post("/delete", response_model=JobStatusResponse)
async def delete_job(id: str,
                     job_manager: JobManagerAdapter = Depends(get_job_manager),
                     executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, job_manager.delete_job, id)
    return response

@router.get("/", response_model=t.List[t.Dict[str, t.Any]])
async def list_jobs(sparse: t.Optional[bool] = True, name: t.Optional[str] = None,
                    job_manager: JobManagerAdapter = Depends(get_job_manager),
                    executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    if sparse:
        jobs = job_manager.list_jobs(sparse=sparse, name=name)
    else:
        loop = asyncio.get_running_loop()
        jobs = await loop.run_in_executor(executor, partial(job_manager.list_jobs, sparse=sparse, name=name))
    return jobs

@router.get("/logs", response_class=PlainTextResponse)
async def get_job_logs(id: str,
                       tail: t.Optional[int] = Query(None, description="Number of lines to show from the end of the logs."),
                       since: t.Optional[int] = Query(None, description="Show logs since a given UNIX timestamp."),
                       stdout: t.Optional[bool] = Query(True, description="Include stdout logs."),
                       stderr: t.Optional[bool] = Query(True, description="Include stderr logs."),
                       timestamps: t.Optional[bool] = Query(False, description="Show timestamps for log entries."),
                       job_manager: JobManagerAdapter = Depends(get_job_manager),
                       executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, partial(
        job_manager.get_job_logs, id, tail=tail, since=since, stdout=stdout, stderr=stderr, timestamps=timestamps
    ))
    return response