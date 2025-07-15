import typing as t
from abc import ABC, abstractmethod
from core.config.root import Config
from api.enums import JobStatus
from api.schemas import JobStatusResponse, JobInfoResponse, JobMetricsResponse
from api.services.job_previewer import JobPreviewerBase


class JobManagerAdapter(JobPreviewerBase, ABC):
    """
    Abstract base class for job management adapters.
    Defines the interface that all adapters (Docker, Kubernetes, etc.) must implement.
    """

    @abstractmethod
    def submit_job(self, config: Config, id: str, name: t.Optional[str] = None) -> JobStatusResponse:
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