import typing as t
import sys
from sqlglot import exp, parse_one, Dialects
from sqlglot.errors import ParseError
import pyspark.sql as pyspark_sql
from pyspark.sql.functions import col, current_timestamp, lit, array, create_map
from pyspark.sql.utils import CapturedException
import duckdb
from duckdb import BinderException
import pandas as pd
import uuid

from transformers.base import AbstractTransformerDecorator
from transformers.mixins import SQLFunctionMixin
from utils.errors import UnsupportedDataFrameError, InvalidParameterError
from utils.parsing import extract_params, extract_name_wo_params
from utils.dataset import get_spark_data_type, cast_pandas, case_insensitive_column_name_mapping
from core.engine_context import SQLContextManager
from core.engine_adapter import EngineAdapter
from core.config.job import JobMode, JobSourceRelatedFields


class SQL(AbstractTransformerDecorator, SQLFunctionMixin):
    @staticmethod
    def _register_duckdb_udf(conn: "duckdb.DuckDBPyConnection") -> t.Optional[t.Union[str, t.Iterable[str]]]:
        return None

    @staticmethod
    def parse_and_validate_query(query: str, source: EngineAdapter, **kwargs) -> exp.Select:
        # --- Validation and Parsing Logic ---
        try:
            select_exp = parse_one(query, dialect=source.dialect, into=exp.Select)
        except:
            try:
                select_exp = parse_one(query, dialect=Dialects.SPARK, into=exp.Select)
            except ParseError as e:
                err_msg = str(e)
                if err_msg.startswith("Failed ") and "'" in err_msg:
                    raise InvalidParameterError(f"Not a valid SELECT statement: {query}")
                else:
                    raise e
        
        # --- DUAL Table Validation Logic ---
        dual_exists = False
        source_table = JobSourceRelatedFields(**kwargs).get_source_path()
        for table_exp in select_exp.find_all(exp.Table):
            if source:
                origin_table = source.get_qualified_table_name(table_exp.name, table_exp.db)
                if table_exp.catalog:
                    origin_table = table_exp.catalog + "." + origin_table
            else:
                origin_table = table_exp.sql(dialect=Dialects.SPARK, identify=False)
            if table_exp.name.upper() == "DUAL" and not table_exp.db:
                dual_exists = True
            if kwargs.get("job_mode", JobMode.BATCH) == JobMode.BATCH:
                try:
                    tbl_wo_alias = table_exp.copy()
                    tbl_wo_alias.args["alias"] = None
                    if exp.to_table(source_table).sql() == tbl_wo_alias.sql():
                        dual_exists = True
                except:
                    pass
        if not dual_exists:
            if kwargs.get("job_mode", JobMode.BATCH) == JobMode.BATCH:
                raise InvalidParameterError("Query must select from `DUAL` table or provided source table at least once." \
                    " `DUAL` table represents the main table. All tables except `DUAL` are read in batch mode."
                )
            else:
                raise InvalidParameterError("Query must select from `DUAL` table at least once." \
                    " `DUAL` table represents the main table. All tables except `DUAL` are read in batch mode."
                )
            
        return select_exp

    @staticmethod
    def _rewrite_query_using_temp_views(select_exp: exp.Select, source: EngineAdapter,
                                        is_global_temp: t.Optional[bool] = True) -> t.Tuple[t.Dict, t.Dict]:
        # --- Query Rewriting Logic ---
        origin2tmp = {}
        wo_alias2tmp = {}
        origin2fetch_query = {}
        for table_exp in select_exp.find_all(exp.Table):
            if source:
                origin_table = source.get_qualified_table_name(table_exp.name, table_exp.db)
                if table_exp.catalog:
                    origin_table = table_exp.catalog + "." + origin_table
            else:
                origin_table = table_exp.sql(dialect=Dialects.SPARK, identify=False)

            if table_exp.name.upper() == "DUAL" and not table_exp.db:
                origin_table = "DUAL"
                origin2fetch_query[origin_table] = None
            elif source:
                if source.dialect == "kafka":
                    fetch_query = table_exp.copy()
                else:
                    fetch_query = exp.select('*').from_(table_exp.copy())

                origin2fetch_query[origin_table] = fetch_query
            else:
                continue

            if origin_table not in origin2tmp:
                temp_table = f"temp_{uuid.uuid4().hex}"
                origin2tmp[origin_table] = temp_table
                if not table_exp.alias:
                    wo_alias2tmp[table_exp.alias_or_name] = temp_table
            else:
                temp_table = origin2tmp[origin_table]

            table_exp.args["catalog"] = None
            if is_global_temp:
                table_exp.args["db"] = exp.Identifier(this="global_temp", quoted=False)
            else:
                table_exp.args["db"] = None
            table_exp.args["this"] = exp.Identifier(this=temp_table, quoted=False)

        for column_exp in select_exp.find_all(exp.Column):
            if column_exp.table in wo_alias2tmp:
                column_exp.args["catalog"] = None
                column_exp.args["db"] = None
                column_exp.args["table"] = wo_alias2tmp[column_exp.table]

        return origin2tmp, origin2fetch_query

    @staticmethod
    def _prepare_spark_query(
        df: pyspark_sql.DataFrame,
        query: str,
        spark: pyspark_sql.SparkSession,
        source: EngineAdapter,
        is_global: t.Optional[bool] = True,
        **kwargs
    ) -> t.Tuple[str, t.Dict]:
        """Parses the SQL, validates it, and rewrites it to use temporary views."""
        select_exp = SQL.parse_and_validate_query(query, source, **kwargs)
        origin2tmp, origin2fetch_query = SQL._rewrite_query_using_temp_views(select_exp, source, is_global_temp=is_global)
            
        # --- Temp Table Creation and Query Rewriting Logic ---
        for origin_table, fetch_query in origin2fetch_query.items():
            if fetch_query is None:
                sub_df = df
            else:
                try:
                    if source:
                        sub_df = source.fetch_pyspark_df(fetch_query)
                    else:
                        sub_df = spark.sql(fetch_query)
                except Exception as e:
                    raise Exception(f"Failed to fetch data from {origin_table}: {e}")
                
            temp_table = origin2tmp[origin_table]
            if is_global:
                sub_df.createOrReplaceGlobalTempView(temp_table)
            else:
                sub_df.createOrReplaceTempView(temp_table)
        
        query_spark = select_exp.sql(dialect=Dialects.SPARK)
        return query_spark, origin2tmp
    
    @staticmethod
    def _prepare_duckdb_query(
        df: pd.DataFrame,
        query: str,
        conn: duckdb.DuckDBPyConnection,
        source: EngineAdapter,
        **kwargs
    ) -> t.Tuple[str, t.Dict]:
        """Parses the SQL, validates it, and rewrites it to use temporary views."""
        select_exp = SQL.parse_and_validate_query(query, source, **kwargs)
        origin2tmp, origin2fetch_query = SQL._rewrite_query_using_temp_views(select_exp, source, is_global_temp=False)
            
        # --- Temp Table Creation and Query Rewriting Logic ---
        for origin_table, fetch_query in origin2fetch_query.items():
            if fetch_query is None:
                sub_df = df
            else:
                try:
                    if source:
                        if not kwargs.get("source_limit"):
                            sub_df = source.fetchdf(fetch_query)
                        else:
                            sub_df = next(source.fetch_batch_df(fetch_query, kwargs.get("source_limit")))
                    else:
                        sub_df = conn.execute(fetch_query).df()
                except Exception as e:
                    raise Exception(f"Failed to fetch data from {origin_table}: {e}")
                
            temp_table = origin2tmp[origin_table]
            conn.register(temp_table, sub_df)
        
        query_duckdb = select_exp.sql(dialect=Dialects.DUCKDB)
        return query_duckdb, origin2tmp

    @staticmethod
    def _execute_spark_query(
        query: str,
        temp_views: t.Dict,
        spark_session: pyspark_sql.SparkSession,
        is_global: t.Optional[bool] = True,
    ) -> pyspark_sql.DataFrame:
        """Executes the query and cleans up temp views."""
        try:
            df = spark_session.sql(query)
            return df
        except CapturedException as e:
            exc_desc = e.desc
            for origin, temp in temp_views.items():
                exc_desc = exc_desc.replace(temp, origin)
            e.desc = exc_desc
            raise e
        except Exception as e:
            # Remap error messages from temp names back to original names
            exc_type, _, exc_traceback = sys.exc_info()
            exc_desc = str(e)
            for origin, temp in temp_views.items():
                exc_desc = exc_desc.replace(temp, origin)
            new_exception = exc_type(exc_desc)
            raise new_exception.with_traceback(exc_traceback) from None
        finally:
            # Ensure cleanup happens even if the query fails
            for view_name in temp_views.values():
                if is_global:
                    spark_session.catalog.dropGlobalTempView(view_name)
                else:
                    spark_session.catalog.dropTempView(view_name)

    @staticmethod
    def _execute_duckdb_query(
        query: str,
        temp_views: t.Dict,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> pd.DataFrame:
        """Executes the query and cleans up temp views."""
        try:
            df = duckdb_conn.execute(query).df()
            return df
        except Exception as e: # Should be BinderException
            exc_type, _, exc_traceback = sys.exc_info()
            exc_desc = str(e)
            for origin, temp in temp_views.items():
                exc_desc = exc_desc.replace(temp, origin)
            new_exception = exc_type(exc_desc)
            raise new_exception.with_traceback(exc_traceback) from None
        finally:
            for view_name in temp_views.values():
                duckdb_conn.unregister(view_name)

    def transform(
        self, df,
        query: str,
        source: EngineAdapter = None,
        spark_session: pyspark_sql.SparkSession = None,
        duckdb_conn: duckdb.DuckDBPyConnection = None,
        *args, **kwargs
    ):
        """
        Executes a SQL query against a DataFrame, allowing joins with other sources.

        Parameters
        ----------
        df: The primary input DataFrame, accessible as `DUAL` in the query.
        query: The SQL query to execute.
        source: An EngineAdapter for fetching data from other tables mentioned in the query.
        spark_session: (Optional) An active SparkSession. If not provided, one will be
                       retrieved from the global SQLContextManager.
        """
        df = super().transform(df, *args, **kwargs)

        if isinstance(df, pyspark_sql.DataFrame):
            # The dependency injection pattern: use provided session or get the global one.
            spark = spark_session or SQLContextManager.get_instance().get_spark_session()
            spark_query, temp_views = self.__class__._prepare_spark_query(df, query, spark, source, **kwargs)
            df = self.__class__._execute_spark_query(spark_query, temp_views, spark)
        elif isinstance(df, pd.DataFrame):
            conn = duckdb_conn or SQLContextManager.get_instance().get_duckdb_conn()
            duckdb_query, temp_views = self.__class__._prepare_duckdb_query(df, query, conn, source, **kwargs)
            df = self.__class__._execute_duckdb_query(duckdb_query, temp_views, conn)
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df


class Select(AbstractTransformerDecorator):
    def transform(self, 
                  df, 
                  exclude_fields: t.Optional[t.List[str]] = [],
                  include_fields: t.Optional[t.List[str]] = [],
                  rename_fields: t.Optional[t.Dict[str, str]] = {},
                  *args, **kwargs):
        """
        Parameters
        ----------
        df: A data frame.
        exclude_fields: list of columns to exclude.
        include_fields: list of columns to include. Defaults to all if not provided.
        rename_fields: a dict of existing column names and corresponding desired column names.
        """
        df = super().transform(df, *args, **kwargs)

        if isinstance(df, pyspark_sql.DataFrame):
            if len(exclude_fields) > 0:
                df = df.drop(*exclude_fields)
            if len(include_fields) > 0:
                df = df.select(*include_fields)
            if len(rename_fields) > 0:
                df = df.select([col(c).alias(rename_fields.get(c, c)) for c in df.columns])
        elif isinstance(df, pd.DataFrame):
            col_map = case_insensitive_column_name_mapping(df)
            if len(exclude_fields) > 0:
                df = df.drop(columns=[col_map[c] for c in exclude_fields])
            if len(include_fields) > 0:
                df = df[[col_map[c] for c in include_fields]]
            if len(rename_fields) > 0:
                df = df.rename(columns={col_map[k]: v for k, v in rename_fields.items()})
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df
    

class Copy(AbstractTransformerDecorator):
    def transform(self, df, field_mapping: t.Optional[t.Dict[str, str]], *args, **kwargs):
        """
        Parameters
        ----------
        df: A data frame.
        field_mapping: a dict of new column names and corresponding copied column names.
        """
        df = super().transform(df, *args, **kwargs)

        if isinstance(df, pyspark_sql.DataFrame):
            df = df.withColumns({k: col(v) for k, v in field_mapping.items()})
        elif isinstance(df, pd.DataFrame):
            col_map = case_insensitive_column_name_mapping(df)
            for new_col, old_col in field_mapping.items():
                df[new_col] = df[col_map[old_col]].values
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df
    

class Literal(AbstractTransformerDecorator):
    def transform(self,
                  df,
                  as_column: str,
                  data_type: str, data_type_params: t.Optional[t.List[t.Any]] = [],
                  default_value: t.Optional[t.Any] = None,
                  *args, **kwargs):
        df = super().transform(df, *args, **kwargs)

        if isinstance(df, pyspark_sql.DataFrame):
            df = df.withColumn(as_column, self.build_spark_instance(data_type, data_type_params, default_value))
        elif isinstance(df, pd.DataFrame):
            df[as_column] = default_value
            df[as_column] = cast_pandas(df, as_column, data_type)
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df
    
    @staticmethod
    def build_spark_instance(data_type: str, data_type_params: t.Optional[t.List[t.Any]] = [],
                       default_value: t.Optional[t.Any] = None):
        params = extract_params(data_type)
        if params:
            data_type_params = params
        data_type = extract_name_wo_params(data_type)
        if data_type.startswith("array"):
            if len(data_type_params) > 0:
                element_dtype = data_type_params[0]
            else:
                raise InvalidParameterError("Data type of array element not found in `data_type_params`")
            
            element_values = default_value or []
            default_value = []
            for element_value in element_values:
                default_value.append(Literal.build_spark_instance(element_dtype, [], element_value))
            default_value = array(default_value)
        elif data_type.startswith("map"):
            if len(data_type_params) > 0:
                key_dtype = data_type_params[0]
            else:
                raise InvalidParameterError("Data type of key and value not found in `data_type_params`")
            if len(data_type_params) > 1:
                value_dtype = data_type_params[1]
            else:
                raise InvalidParameterError("Data type of value not found in `data_type_params`")
            
            map_values = (default_value or {})
            default_value = []
            for k, v in map_values.items():
                default_key_value = Literal.build_spark_instance(key_dtype, [], k)
                default_val_value = Literal.build_spark_instance(value_dtype, [], v)
                default_value.extend([default_key_value, default_val_value])
            default_value = create_map(*default_value)
        else:
            ddl = get_spark_data_type(data_type, data_type_params)
            default_value = lit(default_value).cast(ddl)

        return default_value


class CurrentTimestamp(AbstractTransformerDecorator):
    def transform(self, df, as_column: str, *args, **kwargs):
        df = super().transform(df, *args, **kwargs)

        if isinstance(df, pyspark_sql.DataFrame):
            df = df.withColumn(as_column, current_timestamp())
        elif isinstance(df, pd.DataFrame):
            df[as_column] = pd.Timestamp.now()
        else:
            raise UnsupportedDataFrameError(f"DataFrame type `{type(df).__name__}` not supported.")

        return df