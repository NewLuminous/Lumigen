from __future__ import annotations
import typing as t

from collections import defaultdict, OrderedDict
from functools import singledispatch, lru_cache
import datetime
import decimal
import numpy as np
import pandas as pd
import pyarrow as pa
import pyspark.sql
import pyspark.sql.types as pyspark_types
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.functions import col, date_format
from sqlglot import exp

# pandas.DataFrame.iteritems has been deprecated: https://pandas.pydata.org/pandas-docs/version/1.5/reference/api/pandas.DataFrame.iteritems.html
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

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
    def _preprocess_spark_df(df: pyspark.sql.DataFrame) -> t.Tuple[pyspark.sql.DataFrame, t.Dict[str, t.List[str]]]:
        """
        Harmonizes Spark data types (Decimal, Timestamp) before converting to Pandas.
        """
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
            
        return df, col_group_by_dtype

    @staticmethod
    def _postprocess_pandas_df(pdf: pd.DataFrame, col_group_by_dtype: t.Dict[str, t.List[str]]) -> pd.DataFrame:
        """
        Applies final type conversions (Binary, Timestamp, Structs) natively on the Pandas DataFrame.
        """
        for column_header in col_group_by_dtype.get("binary", []):
            pdf[column_header] = pdf[column_header].map(bytes, na_action="ignore")
        for column_header in col_group_by_dtype.get("timestamp", []):
            pdf[column_header] = pdf[column_header].astype("datetime64[ns]")
        for column_header in col_group_by_dtype.get("struct", []):
            pdf[column_header] = pdf[column_header].map(
                lambda x: t.cast("pyspark_types.Row", x).asDict(recursive=True) if hasattr(x, "asDict") else x,
                na_action="ignore"
            )
        return pdf

    @staticmethod
    def _get_spark_schema_from_pandas(df: pd.DataFrame) -> pyspark_types.StructType:
        """
        Infers a strict PySpark StructType schema from a Pandas DataFrame.
        """
        try:
            arrow_schema = pa.Schema.from_pandas(df)
        except Exception:
            # If PyArrow fails on mixed object columns (e.g. bytes + int), coerce those columns to str
            object_cols = t.cast("pd.DataFrame", df.select_dtypes(include=["object", "string"])).columns
            for col_name in object_cols:
                try:
                    pa.Array.from_pandas(df[col_name])
                except Exception:
                    # Coerce problematic mixed-type object column to string
                    df[col_name] = df[col_name].astype(str)
            arrow_schema = pa.Schema.from_pandas(df)

        schema = from_arrow_schema(arrow_schema)
        
        # Spark does not accept NullType for columns containing actual data (or empty rows).
        # Convert any completely null column to StringType as a safe default.
        fields = [
            pyspark_types.StructField(field.name, pyspark_types.StringType(), field.nullable)
            if isinstance(field.dataType, pyspark_types.NullType)
            else field
            for field in schema.fields
        ]
        return pyspark_types.StructType(fields)

    @staticmethod
    def to_spark(df: DataFrameType, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:            
        if isinstance(df, pd.DataFrame):
            df_clean: pd.DataFrame = df.copy()
            if not df_clean.empty:
                object_cols = t.cast("pd.DataFrame", df_clean.select_dtypes(include=["object", "string"])).columns
                if not object_cols.empty:
                    df_clean[object_cols] = t.cast("pd.DataFrame", df_clean[object_cols]).replace({np.nan: None})

            spark_schema = DataFrameConverter._get_spark_schema_from_pandas(df_clean)
            if df_clean.empty:
                return spark.createDataFrame([], schema=spark_schema)
            else:
                return spark.createDataFrame(df_clean, schema=spark_schema)
            
        return df

    @staticmethod
    def to_pandas(df: DataFrameType) -> pd.DataFrame:
        if isinstance(df, pyspark.sql.DataFrame):
            df, col_group_by_dtype = DataFrameConverter._preprocess_spark_df(df)

            spark = df.sparkSession
            orig_arrow_setting = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
            try:
                # Enable PyArrow to eliminate Py4J JVM-to-Python serialization overhead
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
                df = df.toPandas()
            except Exception:
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                df = df.toPandas()
            finally:
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", orig_arrow_setting)

            df = DataFrameConverter._postprocess_pandas_df(df, col_group_by_dtype)
        
        return df

    @staticmethod
    def to_pandas_chunks(df: DataFrameType, chunk_size: int = 50_000) -> t.Iterator[pd.DataFrame]:
        """
        Streams a DataFrame in memory-safe Pandas chunks.
        """
        if isinstance(df, pd.DataFrame):
            for i in range(0, len(df), chunk_size):
                yield df.iloc[i : i + chunk_size]

        elif isinstance(df, pyspark.sql.DataFrame):
            df, col_group_by_dtype = DataFrameConverter._preprocess_spark_df(df)

            columns = df.columns
            batch = []
            for row in df.toLocalIterator():
                batch.append(row)
                if len(batch) >= chunk_size:
                    chunk_pdf = pd.DataFrame(batch, columns=columns)
                    yield DataFrameConverter._postprocess_pandas_df(chunk_pdf, col_group_by_dtype)
                    batch = []
            if batch:
                chunk_pdf = pd.DataFrame(batch, columns=columns)
                yield DataFrameConverter._postprocess_pandas_df(chunk_pdf, col_group_by_dtype)
        
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


_TYPE_MAPPINGS: t.Dict[t.Any, exp.DataType] = {
    # Python Native Types
    str: exp.DataType.build(exp.DataType.Type.TEXT),
    bytes: exp.DataType.build(exp.DataType.Type.BINARY),
    bytearray: exp.DataType.build(exp.DataType.Type.BINARY),
    datetime.date: exp.DataType.build(exp.DataType.Type.DATE),
    datetime.datetime: exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    datetime.time: exp.DataType.build(exp.DataType.Type.TIME),
    datetime.timedelta: exp.DataType.build(exp.DataType.Type.INTERVAL),
    decimal.Decimal: exp.DataType.build(exp.DataType.Type.DECIMAL),
    int: exp.DataType.build(exp.DataType.Type.BIGINT),
    float: exp.DataType.build(exp.DataType.Type.DOUBLE),
    bool: exp.DataType.build(exp.DataType.Type.BOOLEAN),
    list: exp.DataType.build(exp.DataType.Type.ARRAY),
    dict: exp.DataType.build(exp.DataType.Type.MAP),
    type(None): exp.DataType.build(exp.DataType.Type.NULL),

    # NumPy DType Classes
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

    # NumPy DType Instances
    np.dtype("int8"): exp.DataType.build(exp.DataType.Type.TINYINT),
    np.dtype("int16"): exp.DataType.build(exp.DataType.Type.SMALLINT),
    np.dtype("int32"): exp.DataType.build(exp.DataType.Type.INT),
    np.dtype("int64"): exp.DataType.build(exp.DataType.Type.BIGINT),
    np.dtype("uint8"): exp.DataType.build(exp.DataType.Type.UTINYINT),
    np.dtype("uint16"): exp.DataType.build(exp.DataType.Type.USMALLINT),
    np.dtype("uint32"): exp.DataType.build(exp.DataType.Type.UINT),
    np.dtype("uint64"): exp.DataType.build(exp.DataType.Type.UBIGINT),
    np.dtype("float16"): exp.DataType.build(exp.DataType.Type.FLOAT),
    np.dtype("float32"): exp.DataType.build(exp.DataType.Type.FLOAT),
    np.dtype("float64"): exp.DataType.build(exp.DataType.Type.DOUBLE),
    np.dtype("O"): exp.DataType.build(exp.DataType.Type.TEXT),
    np.dtype("bool"): exp.DataType.build(exp.DataType.Type.BOOLEAN),
    np.dtype("datetime64"): exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtype("datetime64[D]"): exp.DataType.build(exp.DataType.Type.DATE),
    np.dtype("datetime64[s]"): exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtype("datetime64[ms]"): exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtype("datetime64[us]"): exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtype("datetime64[ns]"): exp.DataType.build(exp.DataType.Type.TIMESTAMP),
    np.dtype("timedelta64"): exp.DataType.build(exp.DataType.Type.INTERVAL),
    np.dtype("timedelta64[s]"): exp.DataType.build(exp.DataType.Type.INTERVAL),
    np.dtype("timedelta64[ms]"): exp.DataType.build(exp.DataType.Type.INTERVAL),
    np.dtype("timedelta64[us]"): exp.DataType.build(exp.DataType.Type.INTERVAL),
    np.dtype("timedelta64[ns]"): exp.DataType.build(exp.DataType.Type.INTERVAL),

    # Pandas Extension Types (Classes & Instances)
    pd.Int8Dtype: exp.DataType.build(exp.DataType.Type.TINYINT),
    pd.Int8Dtype(): exp.DataType.build(exp.DataType.Type.TINYINT),
    pd.Int16Dtype: exp.DataType.build(exp.DataType.Type.SMALLINT),
    pd.Int16Dtype(): exp.DataType.build(exp.DataType.Type.SMALLINT),
    pd.Int32Dtype: exp.DataType.build(exp.DataType.Type.INT),
    pd.Int32Dtype(): exp.DataType.build(exp.DataType.Type.INT),
    pd.Int64Dtype: exp.DataType.build(exp.DataType.Type.BIGINT),
    pd.Int64Dtype(): exp.DataType.build(exp.DataType.Type.BIGINT),
    pd.UInt8Dtype: exp.DataType.build(exp.DataType.Type.UTINYINT),
    pd.UInt8Dtype(): exp.DataType.build(exp.DataType.Type.UTINYINT),
    pd.UInt16Dtype: exp.DataType.build(exp.DataType.Type.USMALLINT),
    pd.UInt16Dtype(): exp.DataType.build(exp.DataType.Type.USMALLINT),
    pd.UInt32Dtype: exp.DataType.build(exp.DataType.Type.UINT),
    pd.UInt32Dtype(): exp.DataType.build(exp.DataType.Type.UINT),
    pd.UInt64Dtype: exp.DataType.build(exp.DataType.Type.UBIGINT),
    pd.UInt64Dtype(): exp.DataType.build(exp.DataType.Type.UBIGINT),
    pd.Float32Dtype: exp.DataType.build(exp.DataType.Type.FLOAT),
    pd.Float32Dtype(): exp.DataType.build(exp.DataType.Type.FLOAT),
    pd.Float64Dtype: exp.DataType.build(exp.DataType.Type.DOUBLE),
    pd.Float64Dtype(): exp.DataType.build(exp.DataType.Type.DOUBLE),
    pd.StringDtype: exp.DataType.build(exp.DataType.Type.TEXT),  # type: ignore
    pd.StringDtype(): exp.DataType.build(exp.DataType.Type.TEXT),
    pd.BooleanDtype: exp.DataType.build(exp.DataType.Type.BOOLEAN),
    pd.BooleanDtype(): exp.DataType.build(exp.DataType.Type.BOOLEAN),
    pd.DatetimeTZDtype: exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ),
    pd.CategoricalDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.SparseDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.PeriodDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),
    pd.IntervalDtype: exp.DataType.build(exp.DataType.Type.USERDEFINED),

    # PySpark Types
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
    pyspark_types.Row: exp.DataType.build(exp.DataType.Type.STRUCT),
    pyspark_types.NullType: exp.DataType.build(exp.DataType.Type.NULL),
    pyspark_types.UserDefinedType: exp.DataType.build(exp.DataType.Type.USERDEFINED),
}
try:
    _TYPE_MAPPINGS[pd.StringDtype("pyarrow")] = exp.DataType.build(exp.DataType.Type.TEXT)
except Exception:
    pass

@lru_cache()
def get_pandas_type_mappings() -> t.Dict[t.Any, exp.DataType]:
    return _TYPE_MAPPINGS

def _arrow_type_to_sqlglot(pa_type: pa.DataType) -> exp.DataType:
    """
    Converts a PyArrow DataType to an equivalent SQLGlot DataType.
    """
    if pa.types.is_boolean(pa_type):
        return exp.DataType.build(exp.DataType.Type.BOOLEAN)
    elif pa.types.is_int8(pa_type):
        return exp.DataType.build(exp.DataType.Type.TINYINT)
    elif pa.types.is_int16(pa_type):
        return exp.DataType.build(exp.DataType.Type.SMALLINT)
    elif pa.types.is_int32(pa_type):
        return exp.DataType.build(exp.DataType.Type.INT)
    elif pa.types.is_int64(pa_type):
        return exp.DataType.build(exp.DataType.Type.BIGINT)
    elif pa.types.is_uint8(pa_type):
        return exp.DataType.build(exp.DataType.Type.UTINYINT)
    elif pa.types.is_uint16(pa_type):
        return exp.DataType.build(exp.DataType.Type.USMALLINT)
    elif pa.types.is_uint32(pa_type):
        return exp.DataType.build(exp.DataType.Type.UINT)
    elif pa.types.is_uint64(pa_type):
        return exp.DataType.build(exp.DataType.Type.UBIGINT)
    elif pa.types.is_float16(pa_type) or pa.types.is_float32(pa_type):
        return exp.DataType.build(exp.DataType.Type.FLOAT)
    elif pa.types.is_float64(pa_type):
        return exp.DataType.build(exp.DataType.Type.DOUBLE)
    elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return exp.DataType.build(exp.DataType.Type.TEXT)
    elif pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
        return exp.DataType.build(exp.DataType.Type.BINARY)
    elif pa.types.is_date(pa_type):
        return exp.DataType.build(exp.DataType.Type.DATE)
    elif pa.types.is_timestamp(pa_type):
        return (
            exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ)
            if getattr(pa_type, "tz", None)
            else exp.DataType.build(exp.DataType.Type.TIMESTAMP)
        )
    elif pa.types.is_duration(pa_type):
        return exp.DataType.build(exp.DataType.Type.INTERVAL)
    elif pa.types.is_decimal(pa_type):
        return exp.DataType.build(exp.DataType.Type.DECIMAL)
    elif pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        return exp.DataType.build(exp.DataType.Type.ARRAY)
    elif pa.types.is_map(pa_type):
        return exp.DataType.build(exp.DataType.Type.MAP)
    elif pa.types.is_struct(pa_type):
        return exp.DataType.build(exp.DataType.Type.STRUCT)
    elif pa.types.is_null(pa_type):
        return exp.DataType.build(exp.DataType.Type.NULL)

    return exp.DataType.build(exp.DataType.Type.USERDEFINED)

def columns_to_types_from_dtypes(
    dtypes: t.Iterable[t.Tuple[t.Hashable, t.Any]],
    df: t.Optional[pd.DataFrame] = None,
) -> t.Dict[str, exp.DataType]:
    result = OrderedDict()
    type_mappings = get_pandas_type_mappings()
    for column_name, column_type in dtypes:
        exp_type: t.Optional[exp.DataType] = None
        # Check DatetimeTZDtype explicitly
        if hasattr(pd, "DatetimeTZDtype") and isinstance(column_type, pd.DatetimeTZDtype):
            exp_type = exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ)
        # Check ArrowDtype dynamically
        elif hasattr(pd, "ArrowDtype") and isinstance(column_type, pd.ArrowDtype):
            exp_type = _arrow_type_to_sqlglot(column_type.pyarrow_dtype)
        # Check Object dtype with sample inference if DataFrame is available
        elif (
            column_type == np.dtype("O")
            or column_type == object
            or getattr(column_type, "kind", None) == "O"
            or isinstance(column_type, getattr(np.dtypes, "ObjectDType", ()))
        ):
            if df is not None and column_name in df.columns:
                series = df[column_name]
                series_not_null = series[~series.isnull()]
                if series_not_null.shape[0] > 0:
                    sample_val = series_not_null.iat[0]
                    exp_type = type_mappings.get(
                        type(sample_val), exp.DataType.build(exp.DataType.Type.TEXT)
                    )
                else:
                    exp_type = exp.DataType.build(exp.DataType.Type.TEXT)
            else:
                exp_type = exp.DataType.build(exp.DataType.Type.TEXT)
        else:
            exp_type = type_mappings.get(column_type) or type_mappings.get(type(column_type))

        if not exp_type:
            raise ValueError(f"Unsupported pandas type '{column_type}'")
        result[str(column_name)] = exp_type
    return result

def columns_to_types_from_df(df: DataFrameType) -> t.Dict[str, exp.DataType]:
    if isinstance(df, pd.DataFrame):
        return columns_to_types_from_dtypes(df.dtypes.items(), df=df)
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

def to_pandas_dtype(data_type: exp.DataType | exp.DATA_TYPE) -> str:
    """
    Deterministically maps any SQLGlot DataType to a standard Pandas/NumPy dtype string.
    """
    dtype = exp.DataType.build(data_type) if not isinstance(data_type, exp.DataType) else data_type
    t: exp.DataType.Type = dtype.this

    if t in (exp.DataType.Type.BOOLEAN, exp.DataType.Type.BIT):
        return "bool"

    if t in exp.DataType.SIGNED_INTEGER_TYPES or t in (
        exp.DataType.Type.SERIAL,
        exp.DataType.Type.SMALLSERIAL,
        exp.DataType.Type.BIGSERIAL,
    ):
        int_width_map = {
            "tinyint": "int8",
            "smallint": "int16",
            "smallserial": "int16",
            "int": "int32",
            "mediumint": "int32",
            "serial": "int32",
        }
        return int_width_map.get(t.value.lower(), "int64")

    if t in exp.DataType.UNSIGNED_INTEGER_TYPES:
        uint_width_map = {
            "utinyint": "uint8",
            "usmallint": "uint16",
            "uint": "uint32",
            "umediumint": "uint32",
        }
        return uint_width_map.get(t.value.lower(), "uint64")

    if t in exp.DataType.REAL_TYPES:
        return "float32" if t == exp.DataType.Type.FLOAT else "float64"

    # Pure Time-of-day (TIME, TIMETZ, TIME_NS) has no native NumPy date type, so it remains 'object'
    if t in (exp.DataType.Type.TIME, exp.DataType.Type.TIMETZ, getattr(exp.DataType.Type, "TIME_NS", None)):
        return "object"

    if t in (exp.DataType.Type.TIMESTAMPTZ, exp.DataType.Type.TIMESTAMPLTZ):
        return "datetime64[ns, UTC]"

    if t in exp.DataType.TEMPORAL_TYPES:
        return "datetime64[ns]"

    if t == exp.DataType.Type.INTERVAL:
        return "timedelta64[ns]"

    # Fallback for Strings, Binary, JSON, Arrays, Structs, Maps, Objects, etc.
    return "object"

def empty_pandas_df(schema: t.Dict[str, exp.DataType | exp.DATA_TYPE]) -> pd.DataFrame:
    return pd.DataFrame({
        col: pd.Series(dtype=to_pandas_dtype(dtype))
        for col, dtype in schema.items()
    })

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

def get_spark_data_type(data_type: str, data_type_params: t.Optional[t.List[t.Any]] = None):
    from pyspark.sql.types import _parse_datatype_string

    if data_type_params is None:
        data_type_params = []

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