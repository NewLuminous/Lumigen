from enum import Enum


class ContainerStatus(str, Enum):
    CREATED = "created"         # Container is newly created from image, no command/process inside has started to run.
    RESTARTING = "restarting"   # Container has failed to start and is currently trying to start again.
    RUNNING = "running"         # Container is already running.
    REMOVING = "removing"       # Container is being deleted and currently waiting for the cleanup to complete.
    PAUSED = "paused"           # Container has been suspended, still consumes memory, but is not using any CPU cycles.
    EXITED = "exited"           # Container has finished or terminated.
    DEAD = "dead"               # Container is effectively gone, but the cleanup has failed to release resources and remove the container.

    def is_end_state(self) -> bool:
        return self in (
            ContainerStatus.PAUSED,
            ContainerStatus.EXITED,
            ContainerStatus.DEAD,
        )

    @classmethod
    def from_string(cls, status: str):
        try:
            return cls[status.upper()]
        except KeyError:
            raise ValueError(f"Invalid ContainerStatus string: {status}") from None

    def __str__(self) -> str:
        return self.value