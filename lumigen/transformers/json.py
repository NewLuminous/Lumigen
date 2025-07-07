import typing as t

from collections import OrderedDict
import pyspark
from pyspark.sql.functions import col, from_json, coalesce
from pyspark.sql.types import StructType, StructField
import pandas as pd
from sqlglot import exp

from transformers.base import AbstractTransformerDecorator
from transformers.sql import Literal
from utils.errors import MissingColumnError, UnsupportedDataFrameError
from utils.dataset import *


class JSONFlattener(AbstractTransformerDecorator):
    spark_struct: StructType = None
    columns_to_types: t.Dict[str, exp.DataType] = None

    def transform(self, df, column: str, schema_type: t.List[t.Dict[str, t.Any]], in_place: bool=True, *args, **kwargs):
        """
        Flatten a column in JSON format. Only one level of nesting is removed.

        Parameters
        ----------
        df: A data frame.
        column: a column in JSON format
        schema_type: schema when parsing the JSON column
        in_place: whether to remove the JSON format after flattening

        Returns
        -------
        A data frame with each added column corresponding to one field of the JSON value.
        """
        df = super().transform(df, *args, **kwargs)

        if column not in df.columns:
            raise MissingColumnError(f"Column `{column}` is missing.")
        
        if isinstance(df, pyspark.sql.DataFrame):
            if self.spark_struct is None:
                self.spark_struct = self._build_spark_struct(schema_type=schema_type)
            df = df.withColumn(column, from_json(col(column), self.spark_struct))

            col_map = OrderedDict()
            for c in df.columns:
                col_map[c] = col(c)
            for field_schema in schema_type:
                field_name = field_schema["name"]
                col_map[field_name] = col(f"{column}.{field_name}")
                if field_schema.get("default_value"):
                    lit_instance = Literal.build_spark_instance(field_schema["data_type"], field_schema.get("data_type_params", []), field_schema["default_value"])
                    col_map[field_name] = coalesce(col(f"{column}.{field_name}"), lit_instance)
            df = df.select([col_map[c].alias(c) for c in col_map])
            if in_place:
                df = df.drop(column)
        elif isinstance(df, pd.DataFrame):
            if in_place:
                df = df.join(pd.DataFrame(df.pop(column).tolist()))
            else:
                df = df.join(pd.DataFrame(df[column].tolist()))

            col_map = case_insensitive_column_name_mapping(df)
            for field_schema in schema_type:
                field_name = col_map[field_schema["name"]]
                default_value = field_schema.get("default_value")
                if default_value is not None:
                    df[field_name] = df[field_name].fillna(default_value)
            if self.columns_to_types is None:
                self.columns_to_types = columns_to_types_from_df(df)
            for field_schema in schema_type:
                field_name = col_map[field_schema["name"]]
                old_dtype = self.columns_to_types[field_name]
                new_dtype = field_schema.get("data_type")
                if new_dtype is not None:
                    if needs_conversion(old_dtype, new_dtype):
                        df[field_name] = cast_pandas(df, field_name, new_dtype)
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df
    
    def _build_spark_struct(self, schema_type: t.List[t.Dict[str, t.Any]]) -> StructType:
        schema_struct = []
        for field_schema in schema_type:
            data_type_instance = get_spark_data_type(field_schema["data_type"], field_schema.get("data_type_params", []))
            schema_struct.append(StructField(field_schema["name"], data_type_instance, True))
        return StructType(schema_struct)