from __future__ import annotations
import typing as t
import logging
from pandas import DataFrame

if t.TYPE_CHECKING:
    from core.job_context import JobContext
    from core.engine_adapter.mixins import FetchBatchMixin, FetchStreamMixin

from core.job_context import RuntimeEnvironment
from core.runner import JobRunner
from core.config.job import JobMode

logger = logging.getLogger(__name__)


class PythonJobRunner(JobRunner):
    """Runs the ETL job using standard Python and Pandas."""

    @property
    def runtime_environment(self) -> RuntimeEnvironment:
        return RuntimeEnvironment.PYTHON

    def _read_data(self, context: JobContext) -> t.Iterator[DataFrame]:
        """Implements the read logic specifically for Pandas DataFrames."""
        reader = context.source_adapter
        job_props = context.job_props_model
        job_mode = job_props.job_mode
        batch_size = job_props.source_batch_size
        query = self._get_source_query(context)

        if job_mode == JobMode.STREAMING:
            try:
                reader = t.cast("FetchStreamMixin", reader)
                yield from reader.fetch_stream_df(query, batch_size)
            except AttributeError:
                e = NotImplementedError(f"Streaming mode is not supported by {reader.__class__.__name__}.")
                logger.exception(e, exit_on_call=True)
        else: # BATCH
            if batch_size:
                try:
                    reader = t.cast("FetchBatchMixin", reader)
                    yield from reader.fetch_batch_df(query, batch_size)
                    return
                except AttributeError:
                    logger.error(NotImplementedError(f"Batch fetching is not supported by {reader.__class__.__name__}."))
            
            # Fetch all by default
            yield reader.fetchdf(query)
        return