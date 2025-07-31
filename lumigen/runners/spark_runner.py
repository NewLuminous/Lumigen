from __future__ import annotations
import typing as t
import logging
from pyspark.sql import DataFrame

if t.TYPE_CHECKING:
    from core.job_context import JobContext
    from core.engine_adapter.mixins import FetchStreamMixin

from core.job_context import RuntimeEnvironment
from core.runner import JobRunner
from core.config.job import JobMode

logger = logging.getLogger(__name__)


class SparkJobRunner(JobRunner):
    """Runs the ETL job within a running Spark context."""

    @property
    def runtime_environment(self) -> RuntimeEnvironment:
        return RuntimeEnvironment.SPARK
    
    def _read_data(self, context: JobContext) -> t.Iterator[DataFrame]:
        """Implements the read logic specifically for PySpark DataFrames."""
        reader = context.source_adapter
        job_props = context.job_props_model
        job_mode = job_props.job_mode
        query = self._get_source_query(context)

        if job_mode == JobMode.STREAMING:
            try:
                reader = t.cast("FetchStreamMixin", reader)
                yield from reader.fetch_stream_df(query)
            except AttributeError:
                e = NotImplementedError(f"Streaming mode is not supported by {reader.__class__.__name__}.")
                logger.exception(e, exit_on_call=True)
        else:
            yield reader.fetch_pyspark_df(query)
        return
    
    @property
    def requires_sequential_watermarks(self) -> bool:
        # Spark processes the entire incremental dataset as one atomic
        # DataFrame, so it can calculate the true max watermark without
        # needing the source rows to be pre-sorted.
        return False