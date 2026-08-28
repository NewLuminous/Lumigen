import logging
import threading
from core.engine_context import SQLContextManager

logger = logging.getLogger(__name__)


def start_embedded_spark_session():
    """
    Initializes an embedded, in-process Spark session tailored specifically for the API server.
    """
    def _init_spark():
        from pyspark.conf import SparkConf

        spark_conf = SparkConf(loadDefaults=False)
        spark_conf.setMaster("local[*]")
        spark_conf.set("spark.eventLog.enabled", False)
        spark_conf.set("spark.scheduler.mode", "FAIR")

        # Bind strictly to loopback to avoid container hostname DNS resolution issues
        spark_conf.set("spark.driver.host", "127.0.0.1")
        spark_conf.set("spark.driver.bindAddress", "127.0.0.1")

        # Relax timeouts for web server background idle periods
        spark_conf.set("spark.network.timeout", "800s")
        spark_conf.set("spark.executor.heartbeatInterval", "60s")
        spark_conf.set("spark.rpc.askTimeout", "120s")
        spark_conf.set("spark.rpc.lookupTimeout", "120s")

        context_manager = SQLContextManager.get_instance()
        spark = context_manager.get_spark_session(app_name="Job runner API", conf=spark_conf, hive_support=False, log_level="WARN")
        try:
            jvm = spark.sparkContext._jvm
            Level = jvm.org.apache.logging.log4j.Level
            Configurator = jvm.org.apache.logging.log4j.core.config.Configurator
            
            Configurator.setLevel("org.apache.spark.scheduler.FairSchedulableBuilder", Level.ERROR)
            Configurator.setLevel("org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD", Level.ERROR)
            logger.debug("Successfully configured embedded Spark loggers.")
        except Exception as e:
            logger.warning(f"Failed to configure specific Spark log levels: {e}")

    # Run in a background daemon thread so it does not block FastAPI startup
    spark_init_thread = threading.Thread(
        target=_init_spark, 
        daemon=True, 
        name="EmbeddedSparkInitThread"
    )
    spark_init_thread.start()