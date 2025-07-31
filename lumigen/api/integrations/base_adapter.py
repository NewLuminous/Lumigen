from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod
import logging
from core.config.root import Config
from api.config import settings
from api.enums import JobStatus
from api.schemas import JobStatusResponse, JobInfoResponse, JobMetricsResponse
from api.services.job_previewer import JobPreviewerBase

logger = logging.getLogger(__name__)


class JobManagerAdapter(JobPreviewerBase, ABC):
    """
    Abstract base class for job management adapters.
    Defines the interface that all adapters (Docker, Kubernetes, etc.) must implement.
    """

    def submit_job(self, config: Config, id: str, name: t.Optional[str] = None) -> JobStatusResponse:
        prepared_config = self._prepare_config_for_submission(config)
        return self._perform_submit(config=prepared_config, id=id, name=name)

    @abstractmethod
    def _perform_submit(self, config: Config, id: str, name: t.Optional[str] = None) -> JobStatusResponse:
        pass

    @abstractmethod
    def stop_job(self, id: str) -> JobStatusResponse:
        pass

    @abstractmethod
    def delete_job(self, id: str) -> JobStatusResponse:
        pass

    @abstractmethod
    def get_job_status(self, id: str) -> JobStatus:
        pass

    @abstractmethod
    def get_job_info(self, id: str) -> JobInfoResponse:
        pass

    @abstractmethod
    def get_job_metrics(self, id: str) -> JobMetricsResponse:
        pass

    @abstractmethod
    def list_jobs(self, sparse: t.Optional[bool] = True, name: t.Optional[str] = None) -> t.List[t.Dict[str, t.Any]]:
        pass

    @abstractmethod
    def get_job_logs(self, id: str,
                     tail: t.Optional[int] = None, since: t.Optional[int] = None,
                     stdout: t.Optional[bool] = True, stderr: t.Optional[bool] = True,
                     timestamps: t.Optional[bool] = False) -> t.Optional[str]:
        pass

    def _prepare_config_for_submission(self, config: Config) -> Config:
        if config.get_executor().type_ == "spark":
            options: dict = config.job_properties.setdefault("spark_submit_options", {})
            options.setdefault("master", settings.SPARK_MASTER)

        return config

    def _build_command(
        self,
        config: Config,
        config_path_in_container: str,
        is_preview: bool = False,
    ) -> list[str]:
        """
        Builds the command list for running a job or a preview.

        Args:
            config: The job's configuration object.
            config_path_in_container: The path to the config file *inside* the container/pod.
            is_preview: Flag to indicate if the command is for a preview job.

        Returns:
            A list of strings representing the command and its arguments.
        """
        if not is_preview:
            return ["python", "entry_points/app.py", "-c", config_path_in_container]

        exec_cfg = config.get_executor()
        if exec_cfg and exec_cfg.type_ == "spark":
            command = ["spark-submit", "api/entry_points/app_preview.py"]
        else:
            command = ["python", "api/entry_points/app_preview.py"]
            
        command.extend(["-c", config_path_in_container])
        return command