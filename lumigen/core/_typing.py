from __future__ import annotations

import sys
import typing as t
import pandas as pd
from sqlglot import exp

if t.TYPE_CHECKING:
    import pyspark.sql

    TableName = t.Union[str, exp.Table]
    SchemaName = t.Union[str, exp.Table]
    PySparkDataFrame = t.Union[pyspark.sql.DataFrame, pyspark.sql.connect.dataframe.DataFrame]
    DataFrameType = t.Union[pd.DataFrame, PySparkDataFrame]

if sys.version_info >= (3, 11):
    from typing import Self as Self
else:
    from typing_extensions import Self as Self
