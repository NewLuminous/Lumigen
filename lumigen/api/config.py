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
    KUBE_CHECKPOINT_PVC_NAME: str = "spark-checkpoints-pvc"

    # Metrics settings
    METRICS_PROVIDER: str = Field(default="metrics-api",
                                  description="The source for Kubernetes metrics ('metrics-api' or 'prometheus').")
    
    PROMETHEUS_URL: str = Field(default="http://localhost:9090",
                                description="The URL for the Prometheus query API endpoint.")

    # Threading
    MAX_WORKER_THREADS: int = Field(default=32,
                                    description="Maximum number of worker threads for the application.")

    JOB_IMAGE_NAME: str = Field(default="newluminous/lumigen",
                                description="The Docker image to use for running jobs.")
    CHECKPOINT_DIR: str = Field(default="/tmp/spark-ckpts",
                                description="The directory for Spark checkpoint data.")
    PREVIEW_JOB_CONCURRENCY_LIMIT: int = 10

    # Job settings
    JOB_MANAGER_TYPE: str = "docker"  # Can be "docker" or "kubernetes"
    STREAMING_JOB_RETRY_LIMIT: int = 3
    TEMP_JOB_TIMEOUT_SECONDS: int = 600

    # Job Pruning
    # Pass 1
    PRUNE_JOB_STOPPED_FOR_MINUTES: int = Field(default=2000,
                                               description="Jobs stopped for more than these minutes will be pruned.")
    # Pass 2
    MAX_KEPT_JOBS_PER_NAME: int = Field(default=3,
                                        description="Maximum number of job history to keep for each job name.")
    
    @model_validator(mode='after')
    def calculate_derived_settings(self) -> 'Settings':
        self.PREVIEW_JOB_CONCURRENCY_LIMIT = max(self.MAX_WORKER_THREADS // 2, 10)
        return self

settings = Settings()