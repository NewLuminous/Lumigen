import typing as t
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone
from docker.models.containers import Container


class ContainerInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    object: Container = None
    name: t.Optional[str] = None
    exit_code: t.Optional[int] = 0
    started_at: t.Optional[str] = None
    finished_at: t.Optional[str] = None
    latest_metrics: t.Optional[t.Dict[str, t.Any]] = None

    def restart(self):
        self.exit_code = 0
        self.started_at = datetime.now(timezone.utc)
        self.finished_at = None
        self.latest_metrics = None