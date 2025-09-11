import logging
import typing as t

from pandas import DataFrame
from faker import Faker

logger = logging.getLogger(__name__)


class FakeCursor:
    def __init__(self, schema_type: t.List[t.Dict[str, t.Any]], num_row: t.Optional[int] = 5):
        self._schema_type = schema_type
        self.size = num_row
        self._last_output_cursor = 0
        self.faker = Faker()

    def execute(self, *args, **kwargs) -> None:
        self._last_output_cursor = 0

    def fetchmany(self, size: int = 1) -> t.List[t.Tuple]:
        batch = []
        cursor_stop = min(self._last_output_cursor + size, self.size)
        while self._last_output_cursor < cursor_stop:
            data = []
            for field in self._schema_type:
                faker_method = field["fake_type"]
                faker_params = {k: v for k, v in field.items() if k not in {"name", "fake_type"}}
                data.append(getattr(self.faker, faker_method)(**faker_params))
            batch.append(tuple(data))
            self._last_output_cursor += 1
        return batch
    
    def fetchone(self) -> t.Optional[t.Tuple]:
        return self.fetchmany(size=1)

    def fetchall(self) -> t.List[t.Tuple]:
        return self.fetchmany(size=self.size)

    def fetchdf(self) -> t.Optional[DataFrame]:
        batch = self.fetchall()
        return DataFrame(batch, columns=[field[0] for field in self.description])
    
    def close(self) -> None: ...
    
    @property
    def description(self):
        return [(field["name"],) for field in self._schema_type]


class FakeConnection:
    def __init__(self, *args, **kwargs):
        self.args = args
        for k, v in kwargs.items():
            setattr(self, k, v)

    def cursor(self) -> FakeCursor:
        return FakeCursor(getattr(self, "schema_type", []), getattr(self, "num_row", 5))
    
    def commit(self) -> None: ...
    
    def rollback(self) -> None: ...

    def close(self) -> None: ...

def connection(*args, **kwargs) -> FakeConnection:
    return FakeConnection(*args, **kwargs)
