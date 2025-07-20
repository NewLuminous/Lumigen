from __future__ import annotations
import typing as t
import os
import re
import subprocess
import tempfile
import logging

if t.TYPE_CHECKING:
    from core.config.root import Config

from config.environment import settings
from core.runner import JobRunner
from utils import hocon
from entry_points.launchers import submit_job

logger = logging.getLogger(__name__)


class SeaTunnelJobRunner(JobRunner):
    """
    Runs the ETL job by generating a SeaTunnel config and launching the
    appropriate SeaTunnel script.
    """
    def __init__(self, config: Config):
        self.config = config

    def run(self) -> t.Optional[int]:
        self._verify_environment()
        with self._generate_temp_config_file() as config_filepath:
            cmd = self._build_run_command(config_filepath)
            logger.info(f"Submitting SeaTunnel job with command: {' '.join(cmd)}")
            return submit_job(cmd)
    
    def _verify_environment(self):
        if not settings.SEATUNNEL_HOME or not os.path.exists(settings.SEATUNNEL_HOME):
            logger.critical(f"Could not find valid SEATUNNEL_HOME={settings.SEATUNNEL_HOME}", exit_on_call=True)
            return 1
        
    def _generate_temp_config_file(self) -> t.ContextManager[str]:
        """
        Generates the HOCON .conf file for SeaTunnel as a managed temporary file.

        Returns:
            A context manager that yields the path to the temporary file.
        """
        logger.info("Generating temporary config file for SeaTunnel job...")
        
        seatunnel_cfg = {
            "env": self.config.job_properties.get("seatunnel_env", {}),
            "source": {
                self.config.source.type_: self.config.source.options,
            },
            "transform": self.config.job_properties.get("seatunnel_transform", {}),
            "sink": {
                self.config.sink.type_: self.config.sink.options,
            },
        }
        cfg_string = hocon.dumps(seatunnel_cfg)

        # Create a named temporary file that is deleted on close.
        temp_config_file = tempfile.NamedTemporaryFile(
            mode='w+', 
            suffix='.conf', 
            prefix='seatunnel_job_', 
            delete=False,
            encoding='utf-8'
        )

        @t.contextmanager
        def file_manager():
            try:
                temp_config_file.write(cfg_string)
                temp_config_file.flush()
                temp_config_file.close()
                logger.info(f"SeaTunnel config written to temporary file: {temp_config_file.name}")
                yield temp_config_file.name
            finally:
                os.remove(temp_config_file.name)

        return file_manager()
    
    def _build_run_command(self, config_filepath: str) -> t.List[str]:
        """Builds the final shell command to execute SeaTunnel."""
        exec_cfg = self.config.get_executor()

        if exec_cfg is None:
            return [
                os.path.join(settings.SEATUNNEL_HOME, "bin", "seatunnel.sh"),
                "--config", config_filepath,
                "-m", "local"
            ]
        
        if exec_cfg.type_ == "spark":
            spark_version = self._get_spark_version()
            if not spark_version:
                spark_version = self._get_spark_version_from_context()

            if spark_version:
                spark_major_version = int(spark_version.split(".")[0])
            else:
                spark_major_version = 3
            if spark_major_version <= 2:
                script_filename = "start-seatunnel-spark-2-connector-v2.sh"
            else:
                script_filename = "start-seatunnel-spark-3-connector-v2.sh"
            
            spark_opts = []
            for k, v in self.config.job_properties.get("spark_submit_options", {}).items():
                spark_opts.extend([f"--{k}", str(v)])
            if exec_cfg.app_name:
                spark_opts.extend(["--name", exec_cfg.app_name.replace(' ', '_')])

            return [
                os.path.join(settings.SEATUNNEL_HOME, "bin", script_filename),
                *spark_opts,
                "--config", config_filepath,
            ]

        e = NotImplementedError(f"Execution engine '{exec_cfg.type_}' not supported by SeaTunnel runner.")
        logger.critical(e, exit_on_call=True)
        raise e
    
    def _get_spark_version(self) -> str:
        """Determines the Spark version for choosing the correct script."""
        if not settings.SPARK_HOME or not os.path.exists(settings.SPARK_HOME):
            e = FileNotFoundError("SPARK_HOME not set or invalid, cannot determine Spark version for SeaTunnel.")
            logger.critical(e, exit_on_call=True)
            raise e

        try:
            output = subprocess.run(
                [os.path.join(settings.SPARK_HOME, "bin", "spark-submit"), '--version'],
                capture_output=True, text=True, check=True, stderr=subprocess.STDOUT
            )
            regex = r"version\s+(\d+\.\d+(?:\.\d+)?)"
            match = re.search(regex, output.stdout)
            if match:
                version = match.group(1)
                logger.info(f"Detected Spark version: {version}")
                return version
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error(f"Could not check Spark version via spark-submit. Error: {e}")
        
        logger.warning("Failed to determine Spark version via spark-submit.")
        return None
    
    def _get_spark_version_from_context(self) -> str:
        from pyspark.sql import SparkSession
        import findspark

        findspark.init(settings.SPARK_HOME)
        spark = SparkSession.builder.getOrCreate()
        spark_ver = spark.version
        spark.stop()
        return spark_ver