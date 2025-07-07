import typing as t
import logging
from pyspark.sql import DataFrame

from core.runner import JobRunner
from core.config.job import JobMode
from core.engine_adapter.mixins import FetchStreamMixin
from core.job_context import JobContext

logger = logging.getLogger(__name__)


class SparkJobRunner(JobRunner):
    """Runs the ETL job within a running Spark context."""
    
    def _read_data(self, context: JobContext) -> t.Iterator[DataFrame]:
        """Implements the read logic specifically for PySpark DataFrames."""
        reader = context.source_adapter
        query = context.build_source_expression()
        job_mode = context.job_props.get("job_mode")

        if job_mode == JobMode.STREAMING:
            try:
                reader: FetchStreamMixin = reader
                yield from reader.fetch_stream_df(query)
            except AttributeError:
                e = NotImplementedError(f"Streaming mode is not supported by {reader.__class__.__name__}.")
                logger.exception(e, exit_on_call=True)
        else:
            yield reader.fetch_pyspark_df(query)
        return