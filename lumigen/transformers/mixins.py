from __future__ import annotations
import typing as t

ContextType = t.TypeVar("ContextType", "duckdb.DuckDBPyConnection", "SparkSession")

# To avoid circular imports, use string forward-references
if t.TYPE_CHECKING:
    import duckdb
    from pyspark.sql import SparkSession


class SQLFunctionMixin:
    """
    A mixin for transformers that can be registered as a user-defined
    function (UDF) in various SQL engines.
    
    Subclasses should implement the engine-specific `_register_*_udf`
    methods for the engines they support. These methods are expected to
    return the name or names of the UDFs they create.
    """
    
    @staticmethod
    def _register_spark_udf(session: "SparkSession") -> t.Optional[t.Union[str, t.Iterable[str]]]:
        """Registers the function with a Spark session."""
        raise NotImplementedError("This transformer does not support Spark SQL registration.")

    @staticmethod
    def _register_duckdb_udf(conn: "duckdb.DuckDBPyConnection") -> t.Optional[t.Union[str, t.Iterable[str]]]:
        """Registers the function with a DuckDB connection."""
        raise NotImplementedError("This transformer does not support DuckDB SQL registration.")

    @classmethod
    def register_as_sql_function(cls, context: ContextType) -> t.Optional[t.Union[str, t.Iterable[str]]]:
        """
        Registers this transformer as a UDF in the given SQL context.

        This method dispatches to the appropriate engine-specific
        registration logic.

        Args:
            context: An active SparkSession, DuckDBPyConnection, or Polars SQLContext.

        Raises:
            TypeError: If the provided context is of an unsupported type.
            NotImplementedError: If the transformer doesn't support the given engine.
        """
        from pyspark.sql import SparkSession
        from duckdb import DuckDBPyConnection
        
        if isinstance(context, SparkSession):
            return cls._register_spark_udf(context)
        elif isinstance(context, DuckDBPyConnection):
            return cls._register_duckdb_udf(context)
        else:
            raise TypeError(f"Unsupported SQL context type for UDF registration: {type(context).__name__}. ")