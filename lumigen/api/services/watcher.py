from __future__ import annotations
import typing as t
import asyncio
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from api.enums import JobStatus
from api.schemas import JobInfoResponse
from api.integrations.base_adapter import JobManagerAdapter

logger = logging.getLogger(__name__)

class JobStatusWatcher:
    """
    Background service that watches active job statuses using batch polling
    and streams updates to connected SSE clients.
    """
    def __init__(self, max_retries: int = 3):
        self.subscribers: t.Dict[str, t.Set[asyncio.Queue]] = defaultdict(set)
        self._last_job_info: t.Dict[str, JobInfoResponse] = {}
        self._unknowable_counts: t.Dict[str, int] = defaultdict(int)
        
        self.max_retries = max_retries
        self._task: t.Optional[asyncio.Task] = None
        self._job_manager: t.Optional[JobManagerAdapter] = None
        self._executor: t.Optional[ThreadPoolExecutor] = None

    @property
    def is_alive(self) -> bool:
        """Checks if the background polling task is actively running."""
        return self._task is not None and not self._task.done()

    def start(self, job_manager: JobManagerAdapter, executor: ThreadPoolExecutor):
        if self._task and not self._task.done():
            logger.debug("JobStatusWatcher is already running. Skipping start request.")
            return

        self._job_manager = job_manager
        self._executor = executor
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("JobStatusWatcher started.")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("JobStatusWatcher stopped.")

    async def subscribe(self, job_id: str) -> asyncio.Queue:
        q = asyncio.Queue()
        self.subscribers[job_id].add(q)

        # Immediately feed the current known state to the new client
        if job_id in self._last_job_info:
            q.put_nowait(self._last_job_info[job_id])

        return q

    def unsubscribe(self, job_id: str, q: asyncio.Queue):
        if job_id in self.subscribers:
            self.subscribers[job_id].discard(q)
            if not self.subscribers[job_id]:
                del self.subscribers[job_id]
                self._last_job_info.pop(job_id, None)
                self._unknowable_counts.pop(job_id, None)

    async def _poll_loop(self):
        loop = asyncio.get_running_loop()
        poll_interval = 1.0
        while True:
            try:
                if not self.subscribers:
                    await asyncio.sleep(poll_interval)
                    continue

                # Snapshot the unique jobs currently being watched
                job_ids_to_poll = list(self.subscribers.keys())
                batch_results: t.Dict[str, JobInfoResponse] = await loop.run_in_executor(
                    self._executor, self._job_manager.get_batch_job_info, job_ids_to_poll
                )
                
                for job_id, job_info in batch_results.items():
                    current_status = job_info.status
                    last_info = self._last_job_info.get(job_id)
                    last_status = last_info.status if last_info else None

                    # Debounce transient UNKNOWABLE states
                    if current_status == JobStatus.UNKNOWABLE:
                        self._unknowable_counts[job_id] += 1
                        if self._unknowable_counts[job_id] < self.max_retries:
                            continue
                    else:
                        self._unknowable_counts[job_id] = 0

                    is_valid = True if last_status is None else last_status.is_valid_transition_to(current_status)
                    if is_valid:
                        self._last_job_info[job_id] = job_info
                        
                        # Route the update to all clients watching this job
                        for q in self.subscribers.get(job_id, []):
                            await q.put(job_info) 
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"JobStatusWatcher loop error: {e}", exc_info=True)
            
            await asyncio.sleep(poll_interval)