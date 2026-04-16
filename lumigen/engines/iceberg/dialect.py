from sqlglot import exp
from sqlglot.dialects.databricks import Databricks

class Iceberg(Databricks):
    class Generator(Databricks.Generator):
        # Reference: https://github.com/apache/iceberg-python/blob/main/pyiceberg/types.py
        ICEBERG_TYPE_MAPPING = {
            exp.DataType.Type.BIGINT: "long",
            exp.DataType.Type.INT: "int",
            exp.DataType.Type.SMALLINT: "int",
            exp.DataType.Type.TINYINT: "int",
            exp.DataType.Type.VARCHAR: "string",
            exp.DataType.Type.NVARCHAR: "string",
            exp.DataType.Type.TEXT: "string",
            exp.DataType.Type.CHAR: "string",
            exp.DataType.Type.FLOAT: "float",
            exp.DataType.Type.DOUBLE: "double",
            exp.DataType.Type.BOOLEAN: "boolean",
            exp.DataType.Type.DATE: "date",
            exp.DataType.Type.TIME: "time",
            exp.DataType.Type.TIMESTAMP: "timestamp",
            exp.DataType.Type.TIMESTAMPTZ: "timestamptz",
            exp.DataType.Type.BINARY: "binary",
            exp.DataType.Type.VARBINARY: "binary",
            exp.DataType.Type.UUID: "uuid",
            exp.DataType.Type.DECIMAL: "decimal",
        }

        TRANSFORMS = {
            **Databricks.Generator.TRANSFORMS,
            exp.DataType: lambda self, e: self.iceberg_datatype_sql(e),
        }

        def iceberg_datatype_sql(self, expression: exp.DataType) -> str:
            t = expression.this
            
            # Get mapped type or default to lowercase string of the type
            base_type = self.ICEBERG_TYPE_MAPPING.get(
                t, 
                t.value.lower() if hasattr(t, "value") else str(t).lower()
            )
            
            # PyIceberg maps fixed-length binaries to 'fixed(L)' 
            # (e.g., BINARY(16) -> fixed(16))
            if base_type == "binary" and expression.expressions:
                base_type = "fixed"

            # Iceberg's type parser only accepts arguments for 'decimal' and 'fixed'.
            # Explicitly drop lengths from types like VARCHAR(255) -> "string"
            if base_type in ("decimal", "fixed") and expression.expressions:
                params = ", ".join(self.sql(e) for e in expression.expressions)
                return f"{base_type}({params})"
            
            return base_type