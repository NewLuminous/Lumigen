import typing as t
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from api.integrations.base_adapter import JobManagerAdapter

logger = logging.getLogger(__name__)

class JobReconciler:
    """
    Global background service that acts as a Kubernetes Operator loop.
    It periodically scans the entire environment for orphaned or failed resources
    and autonomously heals them, regardless of active UI clients.
    """
    def __init__(self, interval_seconds: int = 60):
        self.interval_seconds = interval_seconds
        self._task: t.Optional[asyncio.Task] = None
        self._job_manager: t.Optional[JobManagerAdapter] = None
        self._executor: t.Optional[ThreadPoolExecutor] = None

    def start(self, job_manager: JobManagerAdapter, executor: ThreadPoolExecutor):
        if self._task and not self._task.done():
            return

        self._job_manager = job_manager
        self._executor = executor
        self._task = asyncio.create_task(self._reconcile_loop())
        logger.info(f"JobReconciler started. Polling every {self.interval_seconds}s.")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("JobReconciler stopped.")

    async def _reconcile_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Run the global auto-healing routine
                await loop.run_in_executor(
                    self._executor, 
                    self._job_manager.auto_heal_routine
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"JobReconciler loop encountered an error: {e}", exc_info=True)
            
            # Sleep for a longer interval so we don't spam K8s
            await asyncio.sleep(self.interval_seconds)