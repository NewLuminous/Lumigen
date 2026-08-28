import typing as t
from enum import Enum
import threading
from collections import Counter
import logging

if t.TYPE_CHECKING:
    import duckdb
    import polars as pl
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf

logger = logging.getLogger(__name__)

class SparkStatus(str, Enum):
    UNINITIALIZED = "UNINITIALIZED"
    INITIALIZING = "INITIALIZING"
    READY = "READY"
    FAILED = "FAILED"

    def __str__(self) -> str:
        return self.value

class SQLContextManager:
    """
    A thread-safe singleton that lazily initializes and manages Spark, DuckDB,
    and Polars contexts.

    Upon first creation of a context, it automatically discovers and registers
    all transformers that implement the SQLFunctionMixin.
    """
    _instance: t.Optional['SQLContextManager'] = None
    _singleton_lock: threading.Lock = threading.Lock()
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._singleton_lock:
                # Double-checked locking
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        with self._singleton_lock:
            if self._initialized:
                return
            
            self._spark_session: t.Optional['SparkSession'] = None
            self._spark_lock: threading.Lock = threading.Lock()
            self._spark_status: SparkStatus = SparkStatus.UNINITIALIZED
            self._spark_functions: t.Set[str] = set()
            self._spark_udfs: t.Set[str] = set()
            self._persisted_spark_conf: t.Dict[str, str] = {}

            self._duckdb_conn: t.Optional['duckdb.DuckDBPyConnection'] = None
            self._duckdb_lock: threading.Lock = threading.Lock()
            self._duckdb_udfs: t.Set[str] = set()
            
            self._polars_context: t.Optional['pl.SQLContext'] = None
            self._polars_lock: threading.Lock = threading.Lock()

            self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'SQLContextManager':
        return cls()
    
    def _register_all_udfs(self, context: t.Any) -> t.List[str]:
        """Discovers and registers all SQL-compatible transformers for the given context."""
        from duckdb import DuckDBPyConnection
        from pyspark.sql import SparkSession

        if not isinstance(context, (SparkSession, DuckDBPyConnection)):
            logger.info(f"Skipping UDF registration: {type(context).__name__} does not support custom Python UDFs.")
            return []
        logger.info(f"Searching for UDFs to register for {type(context).__name__}...")
        
        # Local imports to avoid circular dependencies at the module level
        from transformers.factory import get_transformer_registry
        from transformers.mixins import SQLFunctionMixin

        registry = get_transformer_registry()
        udfs = set()
        for name, transformer_class in registry.items():
            if issubclass(transformer_class, SQLFunctionMixin):
                try:
                    # The mixin handles dispatching to the correct engine
                    registered_udfs = transformer_class.register_as_sql_function(context)
                    if registered_udfs:
                        registered_udfs = [registered_udfs] if isinstance(registered_udfs, str) else registered_udfs
                        udfs.update(registered_udfs)
                except NotImplementedError:
                    # This is expected if a mixin-class doesn't support the current engine
                    pass
                except Exception as e:
                    logger.error(
                        f"Failed to register UDF for transformer '{name}': {e}",
                        exc_info=True
                    )
        return list(udfs)
    
    def is_spark_alive(self) -> bool:
        """Checks if the Spark Session and its underlying JVM gateway are active."""
        if not self._spark_session:
            return False
        try:
            # Query the Java Spark Context directly to see if it has been stopped
            sc = self._spark_session.sparkContext
            if sc._jsc is None or sc._jsc.sc().isStopped():
                return False

            # Ping the JVM directly across the Py4J gateway
            sc._jvm.java.lang.System.currentTimeMillis()
            return True
        except Exception:
            # If accessing the JVM throws an error (e.g. Py4J Network Error), the session is dead
            return False

    def _stop_and_reset_spark(self):
        """
        Safely stops the Spark session and aggressively clears PySpark's internal singletons
        to ensure the next getOrCreate() spawns a fresh JVM.
        MUST BE CALLED INSIDE `self._spark_lock`.
        """
        if self._spark_session:
            try:
                self._spark_session.stop()
            except Exception as e:
                logger.debug(f"Ignored error while stopping Spark session: {e}")
        
        try:
            from pyspark.sql import SparkSession
            # Reset PySpark's internal singleton references so a fresh JVM can spawn
            SparkSession._instantiatedSession = None
            # Also clear builder options so old configs don't leak
            if hasattr(SparkSession.builder, "_options"):
                SparkSession.builder._options.clear()
        except ImportError:
            pass

        self._spark_status = SparkStatus.UNINITIALIZED
        self._spark_session = None

    def get_spark_session(
            self,
            app_name: t.Optional[str] = None,
            conf: t.Optional[t.Union['SparkConf', t.Dict[str, t.Any]]] = None,
            hive_support: t.Optional[bool] = True,
            log_level: t.Optional[str] = "INFO",
            force_restart: bool = False,
        ) -> 'SparkSession':
        """
        Lazily initializes and returns a SparkSession.

        This method is thread-safe. On first call, it will build the SparkSession,
        which can be time-consuming, and register all discovered UDFs.

        Args:
            app_name: The name for the Spark application.
            conf: A SparkConf object or dictionary for custom configuration.
            hive_support: Whether to enable Hive support. Defaults to True.
            log_level: Defaults to INFO.
            force_restart: If True, terminates the active Spark session, clears previous configurations, and builds a completely new session. 
                           If False, retains the active session and merges new configurations.

        Returns:
            An active SparkSession instance.
            
        Raises:
            SparkTimeoutError: If the session creation exceeds the specified timeout.
            Exception: Any other exception raised during Spark initialization.
        """
        def _extract_conf_dict(c: t.Any) -> t.Dict[str, str]:
            if c is None:
                return {}
            if hasattr(c, "getAll"):
                return dict(c.getAll())
            if isinstance(c, dict):
                return {str(k): str(v) for k, v in c.items()}
            return {}

        if force_restart:
            with self._spark_lock:
                logger.info("Force restart requested. Stopping current Spark session...")
                self._stop_and_reset_spark()
                self._persisted_spark_conf = _extract_conf_dict(conf)
        else:
            # Incremental configuration updates
            if conf is not None:
                new_configs = dict(conf.getAll()) if hasattr(conf, "getAll") else conf
                with self._spark_lock:
                    self._persisted_spark_conf.update(new_configs)
                    if self._spark_session and self.is_spark_alive():
                        for k, v in new_configs.items():
                            try:
                                self._spark_session.conf.set(k, str(v))
                            except Exception as e:
                                logger.debug(f"Could not apply config '{k}' at runtime. Reason: {e}")

        if self._spark_session and not self.is_spark_alive():
            logger.warning("Active Spark session found to be dead or stopped. Resetting state for re-initialization.")
            with self._spark_lock:
                # Re-check inside the lock to avoid race conditions
                if self._spark_session and not self.is_spark_alive():
                    self._stop_and_reset_spark()
        
        if self._spark_session:
            return self._spark_session
        
        with self._spark_lock:
            if self._spark_session:
                return self._spark_session
            
            self._spark_status = SparkStatus.INITIALIZING
            try:
                from pyspark.sql import SparkSession
                from pyspark.conf import SparkConf
                logger.info(f"Initializing Spark session '{app_name or 'Default'}' for the first time...")
                spark_conf = SparkConf()
                for k, v in self._persisted_spark_conf.items():
                    spark_conf.set(k, str(v))

                builder = SparkSession.builder
                if app_name:
                    builder = builder.appName(app_name)
                builder = builder.config(conf=spark_conf)
                if hive_support:
                    builder = builder.enableHiveSupport()
                session = builder.getOrCreate()
                session.sparkContext.setLogLevel(log_level)

                builtin_funcs = Counter(self._list_spark_functions(session))
                registered_udfs = self._register_all_udfs(session)
                all_funcs = Counter(self._list_spark_functions(session))
                udf_counter = all_funcs - builtin_funcs
                self._spark_functions = set(k.lower() for k, v in all_funcs.items() if v > 0)
                self._spark_udfs = set(k.lower() for k, v in udf_counter.items() if v > 0)
                registered_count = sum(udf_counter.values())
                for udf in registered_udfs:
                    udf_name = udf.strip().lower()
                    if udf_name not in self._spark_udfs:
                        self._spark_udfs.add(udf_name)
                        registered_count += 1
                logger.info(f"Completed Spark UDF registration. {registered_count} functions registered.")

                self._spark_status = SparkStatus.READY
                self._spark_session = session
            except Exception as e:
                logger.error(f"An error occurred during Spark session initialization: {e}", exc_info=True)
                self._spark_status = SparkStatus.FAILED
                self._spark_session = None
                raise e

        return self._spark_session
    
    def get_spark_status(self) -> SparkStatus:
        """Returns the current status of the Spark session initialization."""
        return self._spark_status
    
    def get_duckdb_conn(self) -> 'duckdb.DuckDBPyConnection':
        if self._duckdb_conn:
            return self._duckdb_conn

        with self._duckdb_lock:
            if self._duckdb_conn is None:
                import duckdb
                logger.info("Initializing DuckDB connection for the first time...")
                conn = duckdb.connect()
                
                builtin_funcs = Counter(self._list_duckdb_functions(conn))
                registered_udfs = self._register_all_udfs(conn)
                all_funcs = Counter(self._list_duckdb_functions(conn))
                udf_counter = all_funcs - builtin_funcs
                self._duckdb_udfs = set(k.lower() for k, v in udf_counter.items() if v > 0)
                registered_count = sum(udf_counter.values())
                for udf in registered_udfs:
                    udf_name = udf.strip().lower()
                    if udf_name not in self._duckdb_udfs:
                        self._duckdb_udfs.add(udf_name)
                        registered_count += 1
                logger.info(f"Completed DuckDB UDF registration. {registered_count} functions registered.")

                # Manually add universally supported functions here
                self._duckdb_udfs.update({})

                self._duckdb_conn = conn

        return self._duckdb_conn

    def get_polars_context(self) -> 'pl.SQLContext':
        if self._polars_context is None:
            with self._polars_lock:
                if self._polars_context is None:
                    import polars as pl
                    logger.info("Initializing Polars context for the first time...")
                    self._polars_context = pl.SQLContext()
                    # No UDF registration
        return self._polars_context
    
    def _list_spark_functions(self, session: "SparkSession") -> t.List[str]:
        """Lists all function names (lowercase) from a Spark session's catalog."""
        return [f.name.lower() for f in session.catalog.listFunctions()]
    
    def _list_duckdb_functions(self, conn: "duckdb.DuckDBPyConnection") -> t.List[str]:
        """Lists all function names (lowercase) from a DuckDB connection."""
        result = conn.execute("SELECT function_name FROM duckdb_functions()").fetchall()
        func_list = [row[0] for row in result]
        return func_list

    def _spark_func_exists(self, func_name: str) -> bool:
        self.get_spark_session()
        return func_name.lower() in self._spark_functions
    
    def _spark_udf_exists(self, udf_name: str) -> bool:
        """Checks if a UDF exists in the managed Spark session's catalog."""
        self.get_spark_session()
        return udf_name.lower() in self._spark_udfs
    
    def _duckdb_func_exists(self, func_name: str) -> bool:
        conn = self.get_duckdb_conn()
        result = conn.execute(
            "SELECT 1 FROM duckdb_functions() WHERE lower(function_name) = ?",
            [func_name.lower()]
        ).fetchone()
        return result is not None

    def _duckdb_udf_exists(self, udf_name: str) -> bool:
        """Checks if a UDF exists in the managed DuckDB connection's catalog."""
        self.get_duckdb_conn()
        return udf_name.lower() in self._duckdb_udfs

    def udf_exists(self, udf_name: str, engine: str) -> bool:
        """
        Checks if a UDF with the given name is registered in the specified engine.

        This will lazily initialize the engine's context if it hasn't been
        created yet.

        Args:
            udf_name: The name of the function to check.
            engine: The target engine. Must be one of 'spark' or 'duckdb'.

        Returns:
            True if the UDF exists, False otherwise.

        Raises:
            ValueError: If an unsupported engine name is provided.
        """
        engine = engine.lower()
        if engine == 'spark':
            return self._spark_udf_exists(udf_name)
        elif engine == 'duckdb':
            return self._duckdb_udf_exists(udf_name)
        elif engine == 'polars':
            logger.warning("Polars does not support custom Python UDFs; 'udf_exists' will always be False for engine 'polars'.")
            return False
        else:
            raise ValueError(f"Unsupported engine: '{engine}'. Must be 'spark' or 'duckdb'.")
    
    def close(self):
        """Gracefully closes all initialized resources."""
        logger.info("Closing initialized SQL context resources.")
        with self._duckdb_lock:
            if self._duckdb_conn:
                self._duckdb_conn.close()
                logger.info("DuckDB connection closed.")
                self._duckdb_conn = None

        with self._spark_lock:
            if self._spark_session:
                self._stop_and_reset_spark()
                logger.info("Spark session stopped and cleaned up.")

        with self._polars_lock:
            if self._polars_context:
                self._polars_context = None

    def __enter__(self) -> 'SQLContextManager':
        """Allows the manager to be used as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are closed when exiting the 'with' block."""
        self.close()