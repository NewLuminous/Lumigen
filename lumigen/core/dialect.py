import typing as t
from sqlglot import exp
import numpy as np
import pandas as pd


def select_from_values_for_batch_range(
    values: t.List[t.Tuple[t.Any, ...]],
    columns_to_types: t.Dict[str, exp.DataType],
    batch_start: int,
    batch_end: int,
    alias: str = "t",
) -> exp.Select:
    casted_columns = [
        exp.alias_(exp.cast(exp.column(column), to=kind), column, copy=False)
        for column, kind in columns_to_types.items()
    ]

    if not values:
        # Ensures we don't generate an empty VALUES clause & forces a zero-row output
        where = exp.false()
        expressions = [tuple(exp.cast(exp.null(), to=kind) for kind in columns_to_types.values())]
    else:
        where = None
        expressions = [
            tuple(transform_values(v, columns_to_types)) for v in values[batch_start:batch_end]
        ]

    values_exp = exp.values(expressions, alias=alias, columns=columns_to_types)
    if values:
        # BigQuery crashes on `SELECT CAST(x AS TIMESTAMP) FROM UNNEST([NULL]) AS x`, but not
        # on `SELECT CAST(x AS TIMESTAMP) FROM UNNEST([CAST(NULL AS TIMESTAMP)]) AS x`. This
        # ensures nulls under the `Values` expression are cast to avoid similar issues.
        for value, kind in zip(values_exp.expressions[0].expressions, columns_to_types.values()):
            if isinstance(value, exp.Null):
                value.replace(exp.cast(value, to=kind))

    return exp.select(*casted_columns).from_(values_exp, copy=False).where(where, copy=False)


def transform_value(value: t.Any, dtype: t.Optional[exp.DataType] = None) -> t.Any:
        if value is pd.NA:
            return exp.null()
        
        # Ref: https://sqlglot.com/sqlglot/expressions.html#convert
        if isinstance(value, bytearray):
            return exp.HexString(this=value.hex())

        if dtype:
            if (
                isinstance(value, (list, np.ndarray))
                and dtype.is_type(*exp.DataType.ARRAY_TYPES)
                and len(dtype.expressions) == 1
            ):
                element_type = dtype.expressions[0]
                return exp.convert([transform_value(v, element_type) for v in value])

            if (
                isinstance(value, dict)
                and dtype.is_type(*exp.DataType.STRUCT_TYPES)
                and len(value) == len(dtype.expressions)
            ):
                expressions = []
                for (field_name, field_value), field_type in zip(value.items(), dtype.expressions):
                    if isinstance(field_type, exp.ColumnDef):
                        field_type = field_type.kind
                    else:
                        field_type = exp.DataType.build(exp.DataType.Type.UNKNOWN)

                    expressions.append(
                        exp.PropertyEQ(
                            this=exp.to_identifier(field_name),
                            expression=transform_value(field_value, field_type),
                        )
                    )

                return exp.Struct(expressions=expressions)

            if dtype.is_type(exp.DataType.Type.JSON):
                return exp.func("PARSE_JSON", f"'{value}'")

        return exp.convert(value)


def transform_values(
    values: t.Tuple[t.Any, ...], columns_to_types: t.Dict[str, exp.DataType]
) -> t.Iterator[t.Any]:
    """Perform transformations on values given columns_to_types."""
    for col_value, col_type in zip(values, columns_to_types.values()):
        yield transform_value(col_value, col_type)