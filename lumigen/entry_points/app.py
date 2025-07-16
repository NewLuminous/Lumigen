from __future__ import annotations
import logging
import os
import argparse
from config.environment import settings
from config.logging import setup_logger, ExitCapableLogger
from core.config.loader import load_config
from core.engine_context import SQLContextManager
from core.runner import JobRunner, get_runner
from entry_points.launchers import submit_job


def run_spark_job(runner: JobRunner, config_path: str, logger: ExitCapableLogger) -> int | None:
    """Constructs and runs the spark-submit command."""
    if not settings.SPARK_HOME or not os.path.exists(settings.SPARK_HOME):
        logger.critical(f"Could not find valid SPARK_HOME: '{settings.SPARK_HOME}'", exit_on_call=True)
        return 1
    
    if not os.path.isfile(os.path.join(settings.SPARK_HOME, "bin", settings.SPARK_EXECUTABLE)):
        logger.critical(f"Could not find valid SPARK_EXECUTABLE={settings.SPARK_EXECUTABLE} in $SPARK_HOME/bin/", exit_on_call=True)
        return 1

    spark_submit_options = []
    for k, v in runner.config.job_properties.get("spark_submit_options", {}).items():
        spark_submit_options.append(f"--{k}")
        spark_submit_options.append(str(v))

    pyspark_app_path = os.path.join(os.path.dirname(__file__), "app_pyspark.py")

    spark_submit_cmd = [
        os.path.join(settings.SPARK_HOME, "bin", settings.SPARK_EXECUTABLE),
        *spark_submit_options,
        pyspark_app_path,
    ]
    if config_path:
        spark_submit_cmd.extend(["--config", config_path])
    
    logger.info(f"Submitting Spark job with command: {' '.join(spark_submit_cmd)}")
    return submit_job(spark_submit_cmd)

def main():
    setup_logger()
    logger = logging.getLogger(__name__)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", type=str, required=False, default="", help="Config file")
    args = arg_parser.parse_args()
    job_cfg = load_config(path=args.config)
    with SQLContextManager.get_instance() as manager:
        runner = get_runner(job_cfg)

        # Check the type of the runner to decide how to execute it
        from entry_points.runners.spark_runner import SparkJobRunner
        if isinstance(runner, SparkJobRunner):
            # Spark jobs should be submitted as a separate process
            run_spark_job(runner, args.config, logger)
        else:
            # Other runners (Python, etc.) can run in the current process
            runner.run()

if __name__ == '__main__':
    main()