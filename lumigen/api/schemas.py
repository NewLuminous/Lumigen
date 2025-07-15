import typing as t
from pydantic import BaseModel, Field, ValidationError
from fastapi import HTTPException
from core.config.connection import (
    ConnectionConfig,
    connection_config_validator,
)
from api.enums import JobStatus


class JobRequest(BaseModel):
    id: str

class JobStatusResponse(BaseModel):
    job_id: str
    job_name: t.Optional[str] = None
    status: JobStatus
    message: t.Optional[str] = None

class JobInfoResponse(JobStatusResponse):
    start_time: t.Optional[str] = None
    finish_time: t.Optional[str] = None

class JobMetricsResponse(BaseModel):
    cpu_usage_percent: t.Optional[float] = None
    memory_usage_bytes: t.Optional[int] = None
    volume_bytes: t.Optional[int] = None
    read_iops: t.Optional[str] = None
    write_iops: t.Optional[str] = None
    read_bytes_per_second: t.Optional[float] = None
    write_bytes_per_second: t.Optional[float] = None

class DataFetchResponse(BaseModel):
    success: bool
    data: t.Optional[t.Dict[str, t.Any]] = None
    detail: str
    logs: t.Optional[str] = None

class SchemaField(BaseModel):
    name: str
    type: str

class SchemaMappingRequest(BaseModel):
    source_schema: t.List[SchemaField]
    sink_schema: t.List[SchemaField]

class JobPreviewResponse(BaseModel):
    success: bool
    data_schema: t.Optional[t.List[SchemaField]] = Field(default=None, alias="schema")
    data: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    detail: str
    logs: t.Optional[str] = None

class ColumnMapping(BaseModel):
    source_column: SchemaField
    destination_column: SchemaField
    mapping_status: str
    notes: t.Optional[str] = ""

class ConnectionColumnMapping(BaseModel):
    column_mappings: t.List[ColumnMapping]
    unmapped_source_columns: t.List[SchemaField]
    unmapped_destination_columns: t.List[SchemaField]

class JobColumnMapping(BaseModel):
    success: bool
    column_mappings: t.Optional[t.List[ColumnMapping]] = None
    unmapped_source_columns: t.Optional[t.List[SchemaField]] = None
    unmapped_destination_columns: t.Optional[t.List[SchemaField]] = None
    detail: str
    logs: t.Optional[str] = None

def parse_connection(conn_config: t.Dict[str, t.Any]):
    try:
        conn = connection_config_validator(ConnectionConfig, conn_config)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"{e}")
    
    try:
        conn.connection_validator()()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"{e}")
    
    return conn