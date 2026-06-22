from __future__ import annotations
import typing as t

from collections import defaultdict, OrderedDict
from functools import singledispatch
import datetime
import numpy as np
import pandas as pd
import pyspark.sql
import pyspark.sql.types as pyspark_types
from pyspark.sql.functions import col, date_format
from sqlglot import exp

if t.TYPE_CHECKING:
    from core._typing import DataFrameType

from utils import SQLIdentifierDict
from utils.parsing import extract_name_wo_params


@singledispatch
def is_batch_or_stream_empty(df: DataFrameType) -> bool:
    raise NotImplementedError(
        f"is_batch_or_stream_empty is not implemented for DataFrame type: {type(df).__name__}"
    )

@is_batch_or_stream_empty.register(pd.DataFrame)
def _(df: pd.DataFrame) -> bool:
    return df.empty

@is_batch_or_stream_empty.register(pyspark.sql.DataFrame)
def _(df: pyspark.sql.DataFrame) -> bool:
    if df.isStreaming:
        return False
    else:
        return df.isEmpty()

def get_columns_name(df: DataFrameType) -> t.List[str]:
    columns = None
    if isinstance(df, pd.DataFrame):
        columns = [c[0] for c in df.dtypes.items()]
    elif isinstance(df, pyspark.sql.DataFrame):
        columns = [c.name for c in df.schema]
    return columns

class DataFrameConverter:
    @staticmethod
    def to_spark(df: DataFrameType, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:            
        if isinstance(df, pd.DataFrame):
            # pandas.DataFrame.iteritems has been deprecated: https://pandas.pydata.org/pandas-docs/version/1.5/reference/api/pandas.DataFrame.iteritems.html
            if not hasattr(df, "iteritems"):
                df.iteritems = df.items
            df = spark.createDataFrame(df)
            
        return df

    @staticmethod
    def to_pandas(df: DataFrameType) -> pd.DataFrame:
        if isinstance(df, pyspark.sql.DataFrame):
            col_group_by_dtype = defaultdict(list)
            for column in df.dtypes:
                col_dtype = extract_name_wo_params(column[1])
                col_group_by_dtype[col_dtype].append(column[0])
            if col_group_by_dtype.get("decimal"):
                # There is no decimal data type in Pandas so convert all decimal data type to double type
                df = df.withColumns({c: col(c).cast(pyspark_types.DoubleType()) for c in col_group_by_dtype.get("decimal")})
            if col_group_by_dtype.get("timestamp"):
                # Set timestamp columns to string using date_format function
                df = df.withColumns({c: date_format(c, "yyyy-MM-dd HH:mm:ss.SSS") for c in col_group_by_dtype.get("timestamp")})

            df = df.toPandas()
            for column_header in col_group_by_dtype.get("binary", []):
                df[column_header] = df[column_header].map(bytes, na_action="ignore")
            for column_header in col_group_by_dtype.get("timestamp", []):
                df[column_header] = df[column_header].astype("datetime64[ns]")
        
        return df
        
    @staticmethod
    def to_records(df: DataFrameType) -> t.List[t.Dict[str, t.Any]]:
        if isinstance(df, pyspark.sql.DataFrame):
            try:
                df = DataFrameConverter.to_pandas(df)
            except Exception as e:
                df = [row.asDict(recursive=True) for row in df.collect()]
            
        if isinstance(df, pd.DataFrame):
            num_cols = list(df.select_dtypes(include='number'))
            df[num_cols] = df[num_cols].replace(np.nan, None)
            df = df.to_dict(orient='records')
            
        return df


_TYPE_MAPPINGS = {
    str: exp.DataType.build(exp.DataType.Type.TEXT),
    bytes: exp.DataType.build(exp.DataType.Type.BINARY),
    bytearray: exp.DataType.build(exp.DataType.Type.BINARY),
    datetime.date: exp.DataType.build(exp.DataType.Type.DATE),
    datetime.datetime: exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    list: exp.DataType.build(exp.DataType.Type.ARRAY),
    dict: exp.DataType.build(exp.DataType.Type.MAP),
    np.dtypes.Int8DType: exp.DataType.build(exp.DataType.Type.TINYINT),
    np.dtypes.Int16DType: exp.DataType.build(exp.DataType.Type.SMALLINT),
    np.dtypes.Int32DType: exp.DataType.build(exp.DataType.Type.INT),
    np.dtypes.Int64DType: exp.DataType.build(exp.DataType.Type.BIGINT),
    np.dtypes.UInt8DType: exp.DataType.build(exp.DataType.Type.UTINYINT),
    np.dtypes.UInt16DType: exp.DataType.build(exp.DataType.Type.USMALLINT),
    np.dtypes.UInt32DType: exp.DataType.build(exp.DataType.Type.UINT),
    np.dtypes.UInt64DType: exp.DataType.build(exp.DataType.Type.UBIGINT),
    np.dtypes.Float16DType: exp.DataType.build(exp.DataType.Type.FLOAT),
    np.dtypes.Float32DType: exp.DataType.build(exp.DataType.Type.FLOAT),
    np.dtypes.Float64DType: exp.DataType.build(exp.DataType.Type.DOUBLE),
    np.dtypes.Complex64DType: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    np.dtypes.Complex128DType: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    np.dtypes.ObjectDType: exp.DataType.build(exp.DataType.Type.TEXT),
    np.dtypes.BytesDType: exp.DataType.build(exp.DataType.Type.BINARY),
    np.dtypes.StrDType: exp.DataType.build(exp.DataType.Type.TEXT),
    np.dtypes.StringDType: exp.DataType.build(exp.DataType.Type.TEXT),
    np.dtypes.BoolDType: exp.DataType.build(exp.DataType.Type.BOOLEAN),
    np.dtypes.DateTime64DType: exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtypes.TimeDelta64DType: exp.DataType.build(exp.DataType.Type.INTERVAL),
    np.dtypes.IntDType: exp.DataType.build(exp.DataType.Type.INT),
    np.dtypes.UIntDType: exp.DataType.build(exp.DataType.Type.UINT),
    np.dtypes.LongDType: exp.DataType.build(exp.DataType.Type.BIGINT),
    np.dtypes.ULongDType: exp.DataType.build(exp.DataType.Type.UBIGINT),
    np.dtypes.LongLongDType: exp.DataType.build(exp.DataType.Type.BIGINT),
    np.dtypes.ULongLongDType: exp.DataType.build(exp.DataType.Type.UBIGINT),
    np.dtypes.LongDoubleDType: exp.DataType.build(exp.DataType.Type.DOUBLE),
    np.dtypes.CLongDoubleDType: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    np.dtypes.VoidDType: exp.DataType.build(exp.DataType.Type.NULL),
    np.ndarray: exp.DataType.build(exp.DataType.Type.ARRAY),
    pd.Int8Dtype: exp.DataType.build(exp.DataType.Type.TINYINT),
    pd.Int16Dtype: exp.DataType.build(exp.DataType.Type.SMALLINT),
    pd.Int32Dtype: exp.DataType.build(exp.DataType.Type.INT),
    pd.Int64Dtype: exp.DataType.build(exp.DataType.Type.BIGINT),
    pd.UInt8Dtype: exp.DataType.build(exp.DataType.Type.UTINYINT),
    pd.UInt16Dtype: exp.DataType.build(exp.DataType.Type.USMALLINT),
    pd.UInt32Dtype: exp.DataType.build(exp.DataType.Type.UINT),
    pd.UInt64Dtype: exp.DataType.build(exp.DataType.Type.UBIGINT),
    pd.Float32Dtype: exp.DataType.build(exp.DataType.Type.FLOAT),
    pd.Float64Dtype: exp.DataType.build(exp.DataType.Type.DOUBLE),
    pd.StringDtype: exp.DataType.build(exp.DataType.Type.TEXT),  # type: ignore
    pd.BooleanDtype: exp.DataType.build(exp.DataType.Type.BOOLEAN),
    pd.DatetimeTZDtype: exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ),
    pd.ArrowDtype: exp.DataType.build(exp.DataType.Type.ARRAY),
    pd.CategoricalDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.SparseDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.PeriodDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.IntervalDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pyspark_types.ByteType: exp.DataType.build(exp.DataType.Type.TINYINT),
    pyspark_types.ShortType: exp.DataType.build(exp.DataType.Type.SMALLINT),
    pyspark_types.IntegerType: exp.DataType.build(exp.DataType.Type.INT),
    pyspark_types.LongType: exp.DataType.build(exp.DataType.Type.BIGINT),
    pyspark_types.FloatType: exp.DataType.build(exp.DataType.Type.FLOAT),
    pyspark_types.DoubleType: exp.DataType.build(exp.DataType.Type.DOUBLE),
    pyspark_types.DecimalType: exp.DataType.build(exp.DataType.Type.DECIMAL),
    pyspark_types.StringType: exp.DataType.build(exp.DataType.Type.TEXT),
    pyspark_types.BinaryType: exp.DataType.build(exp.DataType.Type.BINARY),
    pyspark_types.BooleanType: exp.DataType.build(exp.DataType.Type.BOOLEAN),
    pyspark_types.DateType: exp.DataType.build(exp.DataType.Type.DATE),
    pyspark_types.TimestampType: exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    pyspark_types.TimestampNTZType: exp.DataType.build(exp.DataType.Type.TIMESTAMPNTZ),
    pyspark_types.DayTimeIntervalType: exp.DataType.build(exp.DataType.Type.INTERVAL),
    pyspark_types.ArrayType: exp.DataType.build(exp.DataType.Type.ARRAY),
    pyspark_types.MapType: exp.DataType.build(exp.DataType.Type.MAP),
    pyspark_types.StructType: exp.DataType.build(exp.DataType.Type.STRUCT),
    pyspark_types.NullType: exp.DataType.build(exp.DataType.Type.NULL),
    pyspark_types.UserDefinedType: exp.DataType.build(exp.DataType.Type.USERDEFINED),
}

def columns_to_types_from_df(df: DataFrameType) -> t.Dict[str, exp.DataType]:
    if isinstance(df, pd.DataFrame):
        result = OrderedDict()
        for column_name, column_type in df.dtypes.items():
            exp_type: t.Optional[exp.DataType] = None
            if isinstance(column_type, np.dtypes.ObjectDType):
                series = df[column_name]
                series_not_null = series[~series.isnull()]
                if series_not_null.shape[0] > 0:
                    exp_type = _TYPE_MAPPINGS.get(type(series_not_null.iat[0]), exp.DataType.build("text"))
                else:
                    exp_type = _TYPE_MAPPINGS.get(type(column_type))
            else:
                exp_type = _TYPE_MAPPINGS.get(type(column_type))
            if not exp_type:
                raise ValueError(f"Unsupported pandas type '{column_type}'")
            result[str(column_name)] = exp_type
    elif isinstance(df, pyspark.sql.DataFrame):
        result = OrderedDict()
        for column in df.schema:
            column_name = column.name
            column_type = type(column.dataType)
            exp_type: t.Optional[exp.DataType] = _TYPE_MAPPINGS.get(column_type)
            if not exp_type:
                raise ValueError(f"Unsupported pyspark type '{column_type}'")
            result[str(column_name)] = exp_type
    else:
        result = None

    return result

def needs_conversion(old_dtype: exp.DATA_TYPE, new_dtype: exp.DATA_TYPE):
    old_dtype = exp.DataType.build(old_dtype)
    new_dtype = exp.DataType.build(new_dtype)
    if old_dtype.this in exp.DataType.NUMERIC_TYPES and new_dtype.this in exp.DataType.NUMERIC_TYPES:
        return False
    if old_dtype.this in exp.DataType.TEXT_TYPES and new_dtype.this in exp.DataType.TEXT_TYPES:
        return False
    if old_dtype.this in {exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY} and new_dtype.this in {exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY}:
        return False
    if old_dtype.this in exp.DataType.ARRAY_TYPES and new_dtype.this in exp.DataType.ARRAY_TYPES:
        return False
    if old_dtype.this in {exp.DataType.Type.USERDEFINED, exp.DataType.Type.UNKNOWN} or new_dtype.this in {exp.DataType.Type.USERDEFINED, exp.DataType.Type.UNKNOWN}:
        return True
    return not old_dtype.is_type(new_dtype)

def cast_pandas(df: pd.DataFrame, column_name: str, dtype: str) -> pd.Series:
    mapping = {
        "integer": "int",
        "decimal": "double",
        "binary": "bytes",
        "date": "datetime64[D]",
        "timestamp": "datetime64[ns]",
        "timestamp_ntz": "datetime64[ns]",
        "interval": "timedelta64[ns]",
    }
    dtype = mapping.get(dtype, dtype)
    if dtype == "datetime64[D]":
        return df[column_name].values.astype(dtype)
    elif dtype == "array":
        return df[column_name].map(list, na_action="ignore")
    elif dtype == "map":
        return df[column_name].map(dict, na_action="ignore")
    return df[column_name].astype(dtype)

def get_spark_data_type(data_type: str, data_type_params: t.Optional[t.List[t.Any]] = []):
    from pyspark.sql.types import _parse_datatype_string

    ddl = data_type
    if data_type_params:
        if data_type.startswith("array") or data_type.startswith("map"):
            ddl = ddl + f"""<{','.join(str(p) for p in data_type_params)}>"""
        else:
            ddl = ddl + f"""({','.join(str(p) for p in data_type_params)})"""

    return _parse_datatype_string(ddl)

def case_insensitive_column_name_mapping(df: DataFrameType) -> t.Dict[str, str]:
    col_map = SQLIdentifierDict()
    columns = get_columns_name(df)

    for c in columns:
        col_map[c] = c

    return col_map