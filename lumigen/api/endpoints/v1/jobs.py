import typing as t
from fastapi import APIRouter, Response, status, Depends, BackgroundTasks, Query
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

logger = logging.getLogger(__name__)


router = APIRouter()
preview_job_semaphore = asyncio.Semaphore(settings.PREVIEW_JOB_CONCURRENCY_LIMIT)

@router.post("/preview", response_model=JobPreviewResponse)
async def preview_job(response: Response,
                      config: Config, sample_size: int = 20,
                      job_manager: JobManagerAdapter = Depends(get_job_manager)):
    async with preview_job_semaphore:
        response_data = await job_manager.get_data_preview(config, sample_size=sample_size)
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
async def watch_job_status(id: str,
                           max_retries: int = Query(3, description="Number of consecutive UNKNOWABLE states before closing."),
                           job_manager: JobManagerAdapter = Depends(get_job_manager),
                           executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    """
    Streams job information to the client via Server-Sent Events (SSE).
    Data is only yielded when the job's status changes.
    The connection closes automatically when the job reaches a terminal state.
    """
    async def event_generator():
        poll_interval = 1.0
        unknowable_count = 0
        loop = asyncio.get_running_loop()
        last_status = None
        try:
            while True:
                job_info: JobInfoResponse = await loop.run_in_executor(
                    executor, job_manager.get_job_info, id
                )
                current_status = job_info.status
                if current_status == JobStatus.UNKNOWABLE:
                    unknowable_count += 1
                    if unknowable_count < max_retries:
                        await asyncio.sleep(poll_interval)
                        continue
                else:
                    unknowable_count = 0

                is_valid = True if last_status is None else last_status.is_valid_transition_to(current_status)
                if is_valid:
                    last_status = current_status
                    
                    # Yield data in standard SSE format
                    json_data = job_info.model_dump_json() if hasattr(job_info, "model_dump_json") else job_info.json()
                    yield f"data: {json_data}\n\n"
                    
                    # Stop if it reached a definitive terminal state (excluding UNKNOWABLE)
                    if last_status.is_end_state() and last_status != JobStatus.UNKNOWABLE:
                        break

                # Stop if we've hit the UNKNOWABLE limit
                if current_status == JobStatus.UNKNOWABLE and unknowable_count >= max_retries:
                    break
                
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError: # Client disconnected
            pass
        except Exception as e:
            yield f"data: {{\"error\": \"Internal server error: {str(e)}\"}}\n\n"

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