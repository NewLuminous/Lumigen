import typing as t
from fastapi import APIRouter, Depends, HTTPException
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from sqlglot.errors import ParseError
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from core.engine_adapter.mixins import EngineAdapterMixin

from utils import FallbackKeyDict
from utils.parsing import dialect_insensitive_tbl_name
from api.enums import DataObjectType
from api.schemas import parse_connection, SchemaField, SchemaMappingRequest, ConnectionColumnMapping
from api.dependencies import get_custom_executor


router = APIRouter()

@router.post("/test", response_model=t.Dict[str, t.Any])
async def test_connection(conn_config: t.Dict[str, t.Any],
                          executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    conn = await loop.run_in_executor(executor, parse_connection, conn_config)
    return {
        "detail": f"Connected to {conn.type_} at {conn.get_address()} successfully.",
    }
    
@router.post("/data-objects", response_model=t.List[t.Dict[str, t.Any]])
async def list_data_objects(conn_config: t.Dict[str, t.Any],
                            type: t.Optional[DataObjectType]=None,
                            keyword: t.Optional[str]='',
                            executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    loop = asyncio.get_running_loop()
    conn = await loop.run_in_executor(executor, parse_connection, conn_config)
    engine_adapter = t.cast("EngineAdapterMixin", conn.create_engine_adapter())
    try:
        data_objects = await loop.run_in_executor(executor, engine_adapter.get_all_data_objects, keyword)
    except:
        return []

    tbl_or_views = []
    for obj in data_objects:
        if obj.type == "unknown":
            continue
        if type == DataObjectType.TABLE and obj.type not in {"table", "managed_table"}:
            continue
        if type == DataObjectType.VIEW and obj.type not in {"view", "materialized_view"}:
            continue
        tbl_or_views.append({
            "catalog": obj.catalog,
            "schema": obj.schema_name,
            "name": obj.name,
            "qualified_name": engine_adapter.get_qualified_table_name(obj.name, obj.schema_name),
            "type": obj.type,
        })
    tbl_or_views.sort(key=lambda item: (item["schema"], item["name"]))

    return tbl_or_views

@router.post("/exists", response_model=t.Dict[str, t.Any])
async def table_exists(conn_config: t.Dict[str, t.Any], table: str,
                       executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    async with database_operation_handler(conn_config, table, executor) as (engine_adapter, table_name):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, engine_adapter.table_exists, table_name)
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
            
        return {"detail": f"Table '{table}' exists."}

@router.post("/describe", response_model=t.Dict[str, str])
async def describe_table(conn_config: t.Dict[str, t.Any], table: str,
                         executor: ThreadPoolExecutor = Depends(get_custom_executor)):
    async with database_operation_handler(conn_config, table, executor) as (engine_adapter, table_name):
        loop = asyncio.get_running_loop()
        try:
            table_columns = await loop.run_in_executor(executor, engine_adapter.columns, table_name)
        except ParseError as e:
            err_msg = str(e)
            if err_msg.startswith("No ") and "'" in err_msg:
                data_type = err_msg.split("'")[1]
                raise HTTPException(status_code=500, detail=f"Unknown data type: {data_type}")
            else:
                raise HTTPException(status_code=500, detail=f"{e}")
            
        try:
            table_schema = {k: v.sql(dialect=engine_adapter.dialect) for k, v in table_columns.items()}
        except ValueError:
            table_schema = {k: v.sql() for k, v in table_columns.items()}
        if not table_schema:
            raise HTTPException(status_code=404, detail=f"Table {table} not found.")
            
        return table_schema

@router.post("/schema-mapping", response_model=ConnectionColumnMapping)
async def schema_mapping(schemas: SchemaMappingRequest):
    import re

    def get_type_parameters(type: str) -> t.List[t.Union[int, float, str]]:
        def _str_to_number(string: str) -> t.Union[int, float, str]:
            try:
                return int(string)
            except ValueError:
                try:
                    return float(string)
                except ValueError:
                    if string.upper() == "MAX":
                        return float('inf')
                    return string #raise ValueError(f"Could not convert '{string}' to a number")

        match = re.search(r'\((.*?)\)', type)
        if not match:
            return []
        params_str = match.group(1)
        if not params_str.strip():
            return []
        params = [_str_to_number(p.strip()) for p in params_str.split(',')]
        return params

    def _is_precision_increase(current_type: str, new_type: str) -> bool:
        current_params = get_type_parameters(current_type)
        new_params = get_type_parameters(new_type)

        if len(current_params) == 0 or len(current_params) != len(new_params):
            return False

        return all(new >= current for current, new in zip(current_params, new_params))

    def _is_compatible_type(current_type: str, new_type: str) -> bool:
        # types are identical or both types are parameterized and new has higher precision
        if current_type == new_type or _is_precision_increase(current_type, new_type):
            return True
        # types are un-parameterized and compatible
        if current_type == "INT":
            return new_type in {"BIGINT", "DOUBLE"}
        return False

    source_columns = {c.name: c.type for c in schemas.source_schema}
    destination_columns = {c.name: c.type for c in schemas.sink_schema}

    column_mappings = []
    unmapped_source_columns = []
    unmapped_destination_columns = []
    col_map = FallbackKeyDict()
    for c in destination_columns:
        col_map[c] = c
    mapped_destination_columns = set()
    for s_col_name, s_col_dtype in source_columns.items():
        d_col_name = col_map[s_col_name]
        if d_col_name in destination_columns:
            d_col_dtype = destination_columns[d_col_name]
            is_type_compatible = _is_compatible_type(s_col_dtype, d_col_dtype)
            if is_type_compatible:
                mapping_status = "MATCHED_TYPE_COMPATIBLE"
                notes = f"Source {s_col_dtype} is compatible with destination {d_col_dtype}."
            else:
                mapping_status = "MATCHED_TYPE_MISMATCH"
                notes = f"Source {s_col_dtype} might need conversion to destination {d_col_dtype}."
            column_mappings.append({
                "source_column": SchemaField(name=s_col_name, type=s_col_dtype).model_dump(),
                "destination_column": SchemaField(name=d_col_name, type=d_col_dtype).model_dump(),
                "mapping_status": mapping_status,
                "notes": notes,
            })
            mapped_destination_columns.add(d_col_name)
        else:
            unmapped_source_columns.append(SchemaField(name=s_col_name, type=s_col_dtype).model_dump())
    for d_col_name, d_col_dtype in destination_columns.items():
        if d_col_name not in mapped_destination_columns:
            unmapped_destination_columns.append(SchemaField(name=d_col_name, type=d_col_dtype).model_dump())

    result = {
        "column_mappings": column_mappings,
        "unmapped_source_columns": unmapped_source_columns,
        "unmapped_destination_columns": unmapped_destination_columns,
    }
    return result

@asynccontextmanager
async def database_operation_handler(
    conn_config: t.Dict[str, t.Any],
    table: str,
    executor: ThreadPoolExecutor
):
    """
    Handles the boilerplate of creating an engine adapter and manages
    common exceptions for database operations.
    """
    if not table:
        raise HTTPException(status_code=400, detail="Table name is required.")

    loop = asyncio.get_running_loop()
    try:
        conn = await loop.run_in_executor(executor, parse_connection, conn_config)
        engine_adapter = conn.create_engine_adapter()
        table_name = dialect_insensitive_tbl_name(table, engine_adapter.dialect)
        
        # Yield the ready-to-use objects to the endpoint
        yield engine_adapter, table_name
    except ParseError as e:
        err_msg = str(e)
        if err_msg.startswith("Failed ") and "'" in err_msg:
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
        else:
            raise HTTPException(status_code=500, detail=f"{e}")
    except SQLMeshError as e:
        if "Table not found" in str(e):
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
        else:
            raise HTTPException(status_code=500, detail=f"{e}")
    except Exception as e:
        if isinstance(e, HTTPException):
             raise e
        raise HTTPException(status_code=500, detail=str(e))