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
    
    def is_valid_transition_to(self, new_status: "JobStatus") -> bool:
        """
        Validates the transition. Allows forward progression and cross-branch jumping,
        but strictly prevents moving backwards along the job lifecycle.
        """
        if self == new_status:
            return False

        # The standard, forward-moving lifecycle DAG
        dag_edges = {
            JobStatus.INITIALIZING: {JobStatus.CREATED},
            JobStatus.CREATED:      {JobStatus.PENDING},
            JobStatus.PENDING:      {JobStatus.RUNNING},
            JobStatus.RUNNING:      {JobStatus.FINISHED, JobStatus.FAILING, JobStatus.CANCELING},
            JobStatus.FAILING:      {JobStatus.FAILED},
            JobStatus.CANCELING:    {JobStatus.CANCELED},
            JobStatus.FINISHED:     {JobStatus.UNKNOWABLE},
            JobStatus.FAILED:       {JobStatus.UNKNOWABLE},
            JobStatus.CANCELED:     {JobStatus.UNKNOWABLE},
            JobStatus.UNKNOWABLE:   set()
        }

        # To prevent moving backwards, we check if 'self' (the old state) is reachable from 'new_status'. 
        # If it is -> we are moving backwards (Invalid).
        # If it isn't -> we are moving forwards or jumping branches (Valid).
        visited = set()
        queue = [new_status]
        while queue:
            current = queue.pop(0)
            
            if current == self:
                return False # The old state is downstream of the new state. Invalid!
            
            if current not in visited:
                visited.add(current)
                queue.extend(dag_edges.get(current, set()) - visited)
                
        return True

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