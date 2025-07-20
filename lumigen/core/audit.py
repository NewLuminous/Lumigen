from __future__ import annotations
import typing as t
import logging
from sqlglot import exp
import pandas as pd
from sqlmesh.core.engine_adapter.base import (
    EngineAdapter, MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
)

if t.TYPE_CHECKING:
    from core.job_context import JobContext

logger = logging.getLogger(__name__)


class AuditManager:
    """
    Manages the persistent audit trail for ETL jobs.
    
    This includes:
    - State for incremental loads (watermarks).
    - (Future) Performance metrics.
    """
    
    STATE_TABLE = "lumigen_job_state"
    STATE_TABLE_SCHEMA: t.ClassVar[t.Dict[str, exp.DataType]] = {
        "job_id": exp.DataType.build("varchar"),
        "watermark_field": exp.DataType.build("varchar"),
        "watermark_value": exp.DataType.build("varchar"),
        "source_type": exp.DataType.build("varchar"),
        "source_path": exp.DataType.build("text"),
        "source_query": exp.DataType.build("text"),
        "updated_at": exp.DataType.build("timestamp"),
    }

    METRICS_TABLE = "lumigen_job_metrics"

    def __init__(self, audit_adapter: EngineAdapter):
        self.adapter = audit_adapter
        self._ensure_all_tables_exist()

    # ===================================================================
    # Setup and Initialization
    # ===================================================================
    def _ensure_all_tables_exist(self):
        self._create_or_migrate_table(
            table_name=self.STATE_TABLE,
            expected_schema=self.STATE_TABLE_SCHEMA,
            primary_key=("job_id",)
        )
        # self._create_or_migrate_table(table_name=self.METRICS_TABLE)

    def _create_or_migrate_table(
        self,
        table_name: str,
        expected_schema: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ):
        if not self.adapter.table_exists(table_name):
            logger.info(f"Audit table '{table_name}' not found. Creating it.")
            self.adapter.create_table(
                table_name, 
                expected_schema, 
                primary_key=primary_key
            )
            return
        
        logger.debug(f"Validating schema for existing audit table '{table_name}'.")
        try:
            current_columns = self.adapter.columns(table_name)
            expected_columns = expected_schema
            alter_expressions = self.adapter.SCHEMA_DIFFER.compare_columns(
                table_name=table_name,
                current=current_columns,
                new=expected_columns,
            )
            if not alter_expressions:
                logger.debug(f"Schema for '{table_name}' is valid and up-to-date.")
                return

            logger.warning(
                f"Schema of audit table '{table_name}' is outdated. "
                f"Applying {len(alter_expressions)} migration(s)..."
            )
            self.adapter.alter_table(alter_expressions)
            logger.info(f"Successfully migrated schema for '{table_name}'.")
        except Exception as e:
            logger.exception(
                f"Failed to validate or migrate schema for table '{table_name}'. "
                "Manual intervention may be required."
            )
            raise e

    # ===================================================================
    # State Management (for Incremental Loads)
    #
    # This entire block will be moved to `StateManager` later.
    # ===================================================================
    def get_high_watermark(self, job_id: str) -> t.Optional[str]:
        return self._get_high_watermark_internal(job_id)
    
    def _get_high_watermark_internal(self, job_id: str) -> t.Optional[str]:
        if not job_id:
            return None
        
        query = exp.select("watermark_value").from_(self.STATE_TABLE).where(
            exp.column("job_id").eq(exp.Literal.string(job_id))
        )
        result = self.adapter.fetchone(query)
        if result:
            watermark = result[0]
            return watermark
        
        logger.debug(f"No previous high watermark found for job '{job_id}'. This is expected for the first run.")
        return None

    def update_high_watermark(self, job_context: JobContext, new_watermark_value: t.Any):
        self._update_high_watermark_internal(job_context, new_watermark_value)

    def _update_high_watermark_internal(self, job_context: JobContext, new_watermark_value: t.Any):
        job_id = job_context.job_props_model.job_id
        if not job_id:
            logger.warning("Cannot update high watermark without a `job_id`. Skipping state update.")
            return

        job_props = job_context.job_props_model
        source_dialect = job_context.source_adapter.dialect
        
        source_row_dict = {
            "job_id": job_id,
            "watermark_field": job_props.source_watermark,
            "watermark_value": str(new_watermark_value),
            "source_type": source_dialect,
            "source_path": job_props.get_source_path(),
            "source_query": job_props.source_query,
            "updated_at": pd.Timestamp.now(tz='UTC'),
        }
        # Filter out any None values to avoid inserting NULLs where not intended
        source_row_dict = {k: v for k, v in source_row_dict.items() if v is not None}
        source_df = pd.DataFrame([source_row_dict])
        columns_to_types = {
            col: self.STATE_TABLE_SCHEMA[col] for col in source_df.columns
        }
        unique_key = [exp.to_column("job_id")]
        update_expressions = [
            exp.column(col, MERGE_TARGET_ALIAS).eq(exp.column(col, MERGE_SOURCE_ALIAS))
            for col in source_df.columns if col != 'job_id'
        ]
        whens = exp.Whens(expressions=[
            exp.When(matched=True, source=False, then=exp.Update(expressions=update_expressions))
        ])
        
        try:
            self.adapter.merge(
                target_table=self.STATE_TABLE,
                source_table=source_df,
                columns_to_types=columns_to_types,
                unique_key=unique_key,
                when_matched=whens,
            )
            logger.debug(f"Successfully updated state for job_id '{job_id}' with watermark '{new_watermark_value}'.")
        except Exception:
            logger.exception(f"Failed to merge state for job_id '{job_id}'. The high watermark was not updated.")
            # It's unsafe to continue the job if state cannot be updated.
            raise

    # ===================================================================
    # Metrics Logging (Future Implementation)
    #
    # This entire block will be moved to `MetricsManager` later.
    # ===================================================================
    def _ensure_metrics_table_exists(self):
        raise NotImplementedError()

    def log_job_metrics(self, job_id: str, metrics: dict):
        self._log_job_metrics_internal(job_id, metrics)

    def _log_job_metrics_internal(self, job_id: str, metrics: dict):
        raise NotImplementedError()