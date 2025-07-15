from enum import Enum


class JobStatus(str, Enum):
    INITIALIZING = "INITIALIZING"       # The job has been received, and is waiting to be created.
    CREATED = "CREATED"                 # Job is newly created, but has not run yet.
    PENDING = "PENDING"                 # The job is waiting for resources.
    RUNNING = "RUNNING"                 # The job is already running.
    FAILING = "FAILING"                 # The job has failed and is currently waiting for the cleanup to complete.
    FAILED = "FAILED"                   # The job has failed with a non-recoverable task failure.
    CANCELING = "CANCELING"             # Job is being cancelled.
    CANCELED = "CANCELED"               # Job has been cancelled.
    FINISHED = "FINISHED"               # All of the job's tasks have successfully finished.
    UNKNOWABLE = "UNKNOWABLE"           # Cannot find the JobID or the job status has already been cleared.

    def __str__(self) -> str:
        return self.value

    def is_end_state(self) -> bool:
        return self in (
            JobStatus.FAILED,
            JobStatus.CANCELED,
            JobStatus.FINISHED,
            JobStatus.UNKNOWABLE,
        )

    @classmethod
    def from_string(cls, status: str):
        try:
            return cls[status.upper()]
        except KeyError:
            raise ValueError(f"Invalid JobStatus string: {status}") from None
    
class DataObjectType(str, Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"

    def __str__(self) -> str:
        return self.value