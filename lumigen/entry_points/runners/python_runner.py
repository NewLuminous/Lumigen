import typing as t
import logging
from pandas import DataFrame

from core.runner import JobRunner
from core.config.job import JobMode
from core.job_context import JobContext
from core.engine_adapter.mixins import FetchStreamMixin

logger = logging.getLogger(__name__)


class PythonJobRunner(JobRunner):
    """Runs the ETL job using standard Python and Pandas."""

    def _read_data(self, context: JobContext) -> t.Iterator[DataFrame]:
        """Implements the read logic specifically for Pandas DataFrames."""
        reader = context.source_adapter
        query = context.build_source_expression()
        job_mode = context.job_props.get("job_mode")
        batch_size = context.job_props.get("source_batch_size")

        if job_mode == JobMode.STREAMING:
            try:
                reader: FetchStreamMixin = reader
                yield from reader.fetch_stream_df(query, batch_size)
            except AttributeError:
                e = NotImplementedError(f"Streaming mode is not supported by {reader.__class__.__name__}.")
                logger.exception(e, exit_on_call=True)
        else: # BATCH
            if batch_size:
                try:
                    yield from reader.fetch_batch_df(query, batch_size)
                    return
                except AttributeError:
                    logger.error(NotImplementedError(f"Batch fetching is not supported by {reader.__class__.__name__}."))
            
            # Fetch all by default
            yield reader.fetchdf(query)
        return