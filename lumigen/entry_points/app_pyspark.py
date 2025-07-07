from __future__ import annotations
import logging
import argparse
from config.logging import setup_logger
from core.config.loader import load_config
from core.engine_context import SQLContextManager
from entry_points.runners.spark_runner import SparkJobRunner


if __name__ == '__main__':
    setup_logger()
    logger = logging.getLogger(__name__)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", type=str, required=False, default="", help="Config file")
    args = arg_parser.parse_args()
    job_cfg = load_config(path=args.config)
    with SQLContextManager.get_instance() as manager:
        runner = SparkJobRunner(job_cfg)
        runner.run()