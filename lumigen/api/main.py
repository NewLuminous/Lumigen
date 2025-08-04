import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import threading
from concurrent.futures import ThreadPoolExecutor

from api.config import settings
from config.logging import setup_logger
setup_logger(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)
middleware_logger = logging.getLogger("api.middleware")

from api.endpoints import v1


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.custom_blocking_task_executor = ThreadPoolExecutor(
        max_workers=settings.MAX_WORKER_THREADS,
        thread_name_prefix="custom_blocking_pool"
    )
    logger.info(f"Custom ThreadPoolExecutor initialized with max_workers={settings.MAX_WORKER_THREADS}")

    from core.engine_context import SQLContextManager
    from pyspark.conf import SparkConf
    context_manager = SQLContextManager.get_instance()

    def initialize_spark_in_background():
        spark_conf = SparkConf(loadDefaults=False)
        spark_conf.setMaster("local[*]")
        spark_conf.set("spark.eventLog.enabled", False)
        context_manager.get_spark_session(app_name="Job runner API", conf=spark_conf, hive_support=False, log_level="WARN")

    spark_init_thread = threading.Thread(target=initialize_spark_in_background, daemon=True)
    spark_init_thread.start()

    yield

    if app.state.custom_blocking_task_executor:
        logger.info("Shutting down Custom ThreadPoolExecutor...")
        # Allow pending tasks to complete, but don't wait indefinitely
        app.state.custom_blocking_task_executor.shutdown(wait=True)
        logger.info("Custom ThreadPoolExecutor shut down.")

    context_manager.close()

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