from typing import Final
import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator, Field


DOTENV = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=DOTENV,
        case_sensitive=False,
        extra='ignore',
    )

    PROJECT_NAME: Final[str] = "Lumigen"
    VERSION: Final[str] = "1.0.0"
    API_PREFIX_V1: Final[str] = "/v1"
    APP_PORT: int = 7777

    # Logging
    LOG_LEVEL: str = "INFO"

    # Kubernetes settings
    KUBECONFIG: str = os.pathsep.join([
        str(Path.home() / ".kube" / "config"),
        str(Path(__file__).parent / "kubeconfig")
    ])
    KUBE_NAMESPACE: str = "lumigen"
    KUBE_EXECUTOR_NAMESPACE: str = "spark"
    KUBE_SERVICE_ACCOUNT: str = "spark"
    KUBE_EXECUTOR_SERVICE_ACCOUNT: str = "default"
    KUBE_CHECKPOINT_PVC_NAME: str = "spark-checkpoints-pvc"
    KUBE_LOG_PVC_NAME: str = "spark-log-pvc"

    # Metrics settings
    METRICS_PROVIDER: str = Field(default="metrics-api",
                                  description="The source for Kubernetes metrics ('metrics-api' or 'prometheus').")
    
    PROMETHEUS_URL: str = Field(default="http://localhost:9090",
                                description="The URL for the Prometheus query API endpoint.")

    # Logs Fallback
    LOGS_PROVIDER: str = Field(default="kubernetes-api",
                               description="The fallback source for historical logs ('kubernetes-api', 'victorialogs').")

    VICTORIA_LOGS_URL: str = Field(default="http://localhost:9428",
                                   description="The base URL for the VictoriaLogs query API endpoint.")

    # Threading
    MAX_WORKER_THREADS: int = Field(default=32,
                                    description="Maximum number of worker threads for the application.")

    JOB_IMAGE_NAME: str = Field(default="newluminous/lumigen",
                                description="The Docker image to use for running jobs.")
    EXECUTOR_IMAGE_NAME: str = Field(default="newluminous/lumigen",
                                     description="The Docker image to use for executors (e.g., Spark workers).")
    CHECKPOINT_DIR: str = Field(default="/tmp/spark-ckpts",
                                description="The directory for Spark checkpoint data.")
    PREVIEW_JOB_CONCURRENCY_LIMIT: int = 10
    PREVIEW_PREFLIGHT_TIMEOUT_SECONDS: float = 15.0
    PREVIEW_DIRECT_TIMEOUT_SECONDS: float = 60.0

    # Job settings
    JOB_MANAGER_TYPE: str = "docker"  # Can be "docker" or "kubernetes"
    STREAMING_JOB_RETRY_LIMIT: int = 7
    STREAMING_HEALTHY_UPTIME_SECONDS: int = 3600
    TEMP_JOB_TIMEOUT_SECONDS: int = 600

    # Job Pruning
    # Pass 1
    PRUNE_JOB_STOPPED_FOR_MINUTES: int = Field(default=2000,
                                               description="Jobs stopped for more than these minutes will be pruned.")
    # Pass 2
    MAX_KEPT_JOBS_PER_NAME: int = Field(default=3,
                                        description="Maximum number of job history to keep for each job name.")
    
    # Spark settings
    SPARK_LOG_LEVEL: str = "WARN"
    SPARK_MASTER: str = Field(
        default="local[*]",
        description="The --master argument for spark-submit. Examples: 'local[*]', 'yarn', or 'k8s://<api_server>'."
    )
    SPARK_EVENT_LOG_ENABLED: bool = False
    SPARK_EVENT_LOG_DIR: str = "file:///tmp/spark-events"

    # Spark Auto-scaling Limits
    SPARK_MIN_EXECUTORS: int = 1
    SPARK_MAX_EXECUTORS: int = 10

    # Spark Resources (Driver & Executor)
    SPARK_DRIVER_CORES: int = Field(
        default=1,
        description="Number of CPU cores to allocate for the Spark driver."
    )
    SPARK_DRIVER_MEMORY: str = Field(
        default="2g",
        description="Amount of memory to allocate for the Spark driver (e.g., '1g', '2048m')."
    )
    SPARK_EXECUTOR_CORES: int = Field(
        default=1,
        description="Number of CPU cores to allocate per Spark executor."
    )
    SPARK_EXECUTOR_MEMORY: str = Field(
        default="2g",
        description="Amount of memory to allocate per Spark executor (e.g., '2g', '4096m')."
    )
    
    @model_validator(mode='after')
    def calculate_derived_settings(self) -> 'Settings':
        self.PREVIEW_JOB_CONCURRENCY_LIMIT = max(self.MAX_WORKER_THREADS // 2, 10)
        return self

settings = Settings()