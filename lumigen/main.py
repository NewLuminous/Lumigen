from __future__ import annotations
import logging
import os
import argparse
import sys

from config.environment import settings
from config.logging import LOG_LEVEL_MAP, setup_logger
setup_logger(LOG_LEVEL_MAP.get(settings.LOG_LEVEL.upper(), logging.INFO))
logger = logging.getLogger(__name__)

from config.environment import settings
from core.config.loader import load_config
from core.engine_context import SQLContextManager
from core.runner import JobRunner, get_runner
from utils.processes import submit_job
from runners.spark_runner import SparkJobRunner


def run_spark_job(runner: JobRunner, config_path: str) -> int | None:
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

    main_script_path = os.path.abspath(__file__)

    spark_submit_cmd = [
        os.path.join(settings.SPARK_HOME, "bin", settings.SPARK_EXECUTABLE),
        *spark_submit_options,
        main_script_path,
        "--spark-driver",
    ]
    if config_path:
        spark_submit_cmd.extend(["--config", config_path])
    
    logger.info(f"Submitting Spark job with command: {' '.join(spark_submit_cmd)}")
    return submit_job(spark_submit_cmd)

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", type=str, required=False, default="", help="Config file")

    # internal flag, MUST NOT be provided by user
    arg_parser.add_argument("--spark-driver", action="store_true", help=argparse.SUPPRESS)
    
    args = arg_parser.parse_args()

    if args.spark_driver:
        # --- SPARK DRIVER MODE ---
        logger.info("Running in Spark Driver mode.")
        job_cfg = load_config(path=args.config)
        with SQLContextManager.get_instance() as manager:
            runner = SparkJobRunner(job_cfg)
            runner.run()
        sys.exit(0)

    # --- LAUNCHER MODE (default) ---
    exit_code = 0
    try:
        logger.info("Running in Launcher mode.")
        job_cfg = load_config(path=args.config)
        with SQLContextManager.get_instance() as manager:
            runner = get_runner(job_cfg)

            # Check the type of the runner to decide how to execute it
            if isinstance(runner, SparkJobRunner):
                # Spark jobs should be submitted as a separate process with the --spark-driver flag.
                rc = run_spark_job(runner, args.config)
                exit_code = rc if rc is not None else 1
            else:
                # Other runners (Python, etc.) can run in the current process
                runner.run()

        if exit_code == 0:
            logger.info("Job completed successfully.")
        else:
            logger.error(f"Job failed with exit code {exit_code}.")
    except Exception as e:
        logger.critical("Job execution failed with an unhandled exception.", exc_info=True)
        exit_code = 1
    finally:
        logger.info(f"Application shutting down with exit code {exit_code}.")
        sys.exit(exit_code)

if __name__ == '__main__':
    main()