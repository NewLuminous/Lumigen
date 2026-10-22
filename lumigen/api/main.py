import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ThreadPoolExecutor

from api.config import settings
from config.logging_config import setup_logger
setup_logger(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)
middleware_logger = logging.getLogger("api.middleware")

from api.endpoints import v1


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the custom blocking thread pool executor
    app.state.custom_blocking_task_executor = ThreadPoolExecutor(
        max_workers=settings.MAX_WORKER_THREADS,
        thread_name_prefix="custom_blocking_pool"
    )
    logger.info(f"Custom ThreadPoolExecutor initialized with max_workers={settings.MAX_WORKER_THREADS}")

    # Background Spark Session initialization
    from core.engine_context import SQLContextManager
    from api.services.embedded_spark import start_embedded_spark_session

    start_embedded_spark_session()

    from api.dependencies import get_job_manager
    from api.services.watcher import JobStatusWatcher
    from api.services.reconciler import JobReconciler

    job_manager = await get_job_manager()

    # Start the UI Watcher (Fast polling, only for active SSE clients)
    watcher = JobStatusWatcher(max_retries=3)
    watcher.start(job_manager, app.state.custom_blocking_task_executor)
    app.state.job_status_watcher = watcher

    # Start the Global Reconciler (Slow polling, heals unattended jobs)
    reconciler = JobReconciler(interval_seconds=60)
    reconciler.start(job_manager, app.state.custom_blocking_task_executor)
    app.state.job_reconciler = reconciler

    yield

    if app.state.custom_blocking_task_executor:
        logger.info("Shutting down Custom ThreadPoolExecutor...")
        # Allow pending tasks to complete, but don't wait indefinitely
        app.state.custom_blocking_task_executor.shutdown(wait=True)
        logger.info("Custom ThreadPoolExecutor shut down.")

    if hasattr(app.state, "job_reconciler"):
        logger.info("Shutting down JobReconciler...")
        await app.state.job_reconciler.stop()
        logger.info("JobReconciler shut down.")

    if hasattr(app.state, "job_status_watcher"):
        logger.info("Shutting down JobStatusWatcher...")
        await app.state.job_status_watcher.stop()
        logger.info("JobStatusWatcher shut down.")

    SQLContextManager.get_instance().close()

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)    

# Set up CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this appropriately in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(v1.connection.router, prefix=f"{settings.API_PREFIX_V1}/connection", tags=["connection"])
app.include_router(v1.jobs.router, prefix=f"{settings.API_PREFIX_V1}/jobs", tags=["jobs"])

@app.middleware("http")
async def audit_log(request: Request, call_next):
    middleware_logger.info(f"Request: {request.method} {request.url.path}")
    try:
        response = await call_next(request)
        middleware_logger.info(f"Response: {response.status_code}")
        return response
    except Exception as e:
        middleware_logger.error(f"Request failed: {str(e)}")
        raise

@app.get("/")
async def root():
    """Root endpoint for health checks and basic information"""
    return {
        "application": settings.PROJECT_NAME,
        "status": "operational",
        "docs_url": "/docs"
    }