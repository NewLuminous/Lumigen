from __future__ import annotations
import typing as t
import logging
from sqlglot import exp
import pandas as pd

if t.TYPE_CHECKING:
    from core.job_context import JobContext
    from sqlmesh.core.engine_adapter.base import EngineAdapter

from core.config.job import DataLoadMode
from utils.json import encode_typed_value, decode_typed_value
from utils.collections import FallbackSet
from utils.errors import ConfigError

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
        "job_id": exp.DataType.build("varchar(255)"),
        "watermark_field": exp.DataType.build("varchar(255)"),
        "watermark_value": exp.DataType.build("text"),
        "watermark_value_type": exp.DataType.build("varchar(255)"),
        "source_type": exp.DataType.build("varchar(255)"),
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
                primary_key=primary_key,
                exists=False,
            )
            return
        
        logger.debug(f"Validating schema for existing audit table '{table_name}'.")
        try:
            current_schema = self.adapter.columns(table_name)
            current_columns = FallbackSet(current_schema.keys())
            
            # This migration logic is additive-only to prevent accidental data loss.
            # It checks for any columns that are expected by the application but are
            # missing from the database, and then adds them without altering existing columns.
            new_schema = current_schema.copy()
            for expected_col, expected_dtype in expected_schema.items():
                if not current_columns.get(expected_col):
                    new_schema[expected_col] = expected_dtype

            alter_expressions = self.adapter.schema_differ.compare_columns(
                table_name=table_name,
                current=current_schema,
                new=new_schema,
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
    def get_validated_high_watermark(self, job_context: JobContext, silent: t.Optional[bool] = False) -> t.Optional[t.Any]:
        """
        Retrieves the high watermark for a job and validates the current job
        configuration against the stored state.
        """
        return self._get_high_watermark_internal(
            job_id=job_context.job_props_model.job_id,
            job_context_for_validation=job_context,
            silent=silent,
        )

    def get_high_watermark_by_id(self, job_id: str) -> t.Optional[t.Any]:
        """
        Retrieves a job's high watermark by its ID without performing any
        validation against a current configuration.
        """
        return self._get_high_watermark_internal(job_id=job_id, silent=True)
    
    def _get_high_watermark_internal(self,
                                     job_id: str,
                                     job_context_for_validation: t.Optional[JobContext] = None,
                                     silent: t.Optional[bool] = False,
                                     ) -> t.Optional[t.Any]:
        """
        Retrieves the last high watermark for a job, performing critical
        validation checks against the current job configuration.
        """
        if not job_id:
            return None
        
        # ==================================================================
        # RULE 1: Handle FULL load mode explicitly.
        # ==================================================================
        if job_context_for_validation:
            current_props = job_context_for_validation.job_props_model
            if current_props.data_load_mode == DataLoadMode.FULL:
                if not silent:
                    logger.warning(
                        f"Job '{job_id}' is in 'FULL' load mode. "
                        "Any existing watermark will be ignored and overwritten upon completion."
                    )
                return None
        
        query = (
            exp.select(
                "watermark_value", 
                "watermark_value_type",
                "watermark_field",
                "source_type",
                "source_path",
                "source_query"
            )
            .from_(exp.to_table(self.STATE_TABLE))
            .where(exp.column("job_id").eq(exp.Literal.string(job_id)))
        )
        result = self.adapter.fetchone(query, quote_identifiers=True)
        if not result:
            if not silent:
                logger.debug(f"No previous high watermark found for job '{job_id}'. This is expected for the first run.")
            return None
        
        (
            prev_watermark_value_str,
            prev_watermark_type_str,
            prev_watermark_field,
            prev_source_type,
            prev_source_path,
            prev_source_query,
        ) = result

        if job_context_for_validation:
            # ==================================================================
            # RULE 2: Source Type (dialect) MUST NOT change. This is a critical error.
            # ==================================================================
            current_source_dialect = job_context_for_validation.source_adapter.dialect
            if prev_source_type and prev_source_type != current_source_dialect:
                error_msg = (
                    f"Critical configuration change detected for incremental job '{job_id}'. "
                    f"Source type cannot be changed. Previous: '{prev_source_type}', Current: '{current_source_dialect}'. "
                    "To proceed, either revert the source connection type or assign a new `job_id` to start a new incremental history."
                )
                if not silent:
                    logger.error(error_msg)
                raise ConfigError(error_msg)

            # ==================================================================
            # RULE 3: Source Path or Query change should trigger a WARNING.
            # ==================================================================
            current_source_path = current_props.get_source_path()
            current_source_query = current_props.source_query
            if prev_source_path and prev_source_path != current_source_path:
                if not silent:
                    logger.warning(
                        f"Source path may have changed for job '{job_id}'.\n"
                        f"  - Previous: '{prev_source_path or 'N/A'}'\n"
                        f"  - Current: '{current_source_path or 'N/A'}'\n"
                        "Continuing with the stored watermark, but ensure this change was intentional."
                    )
        
            if prev_source_query and prev_source_query != current_source_query:
                if not silent:
                    logger.warning(
                        f"Source query may have changed for job '{job_id}'.\n"
                        f"  - Previous: '{prev_source_query or 'N/A'}'\n"
                        f"  - Current: '{current_source_query or 'N/A'}'\n"
                        "Continuing with the stored watermark, but ensure this change was intentional."
                    )

            # ==================================================================
            # RULE 4: Watermark Field change should trigger a WARNING.
            # ==================================================================
            current_watermark_field = current_props.source_watermark
            if prev_watermark_field and prev_watermark_field != current_watermark_field:
                if not silent:
                    logger.warning(
                        f"Watermark field has changed for job '{job_id}'.\n"
                        f"  - Previous: '{prev_watermark_field}'\n"
                        f"  - Current: '{current_watermark_field}'\n"
                        "The incremental load will proceed using the old watermark value against the NEW field. "
                        "This is usually incorrect unless the column was simply renamed."
                    )

        # If all checks pass, decode and return the watermark value.
        try:
            watermark = decode_typed_value(prev_watermark_value_str, prev_watermark_type_str)
            if not silent:
                logger.info(f"Found previous high watermark for job '{job_id}' (Type: {prev_watermark_type_str})")
            return watermark
        except Exception as e:
            if not silent:
                logger.error(f"Could not decode stored watermark for job '{job_id}'. This may indicate data corruption in the audit store.", exc_info=True)
            raise e

    def update_high_watermark(self, job_context: JobContext, new_watermark_value: t.Any, record_exists: bool):
        self._update_high_watermark_internal(job_context, new_watermark_value, record_exists)

    def _update_high_watermark_internal(self, job_context: JobContext, new_watermark_value: t.Any, record_exists: bool):
        job_id = job_context.job_props_model.job_id
        if not job_id:
            logger.warning("Cannot update high watermark without a `job_id`. Skipping state update.")
            return

        job_props = job_context.job_props_model
        source_dialect = job_context.source_adapter.dialect
        
        try:
            watermark_string, watermark_type_string = encode_typed_value(new_watermark_value)
        except TypeError as e:
            error_message = (
                f"Unsupported watermark type '{new_watermark_value.__class__.__name__}' for incremental load. "
            )
            raise ValueError(error_message) from e
        
        source_row_dict = {
            "job_id": job_id,
            "watermark_field": job_props.source_watermark,
            "watermark_value": watermark_string,
            "watermark_value_type": watermark_type_string,
            "source_type": source_dialect,
            "source_path": job_props.get_source_path(),
            "source_query": job_props.source_query,
            "updated_at": pd.Timestamp.now(tz='UTC'),
        }
        # Filter out any None values to avoid inserting NULLs where not intended
        source_row_dict = {k: v for k, v in source_row_dict.items() if v is not None}
        try:
            if not record_exists:
                source_df = pd.DataFrame([source_row_dict])
                columns_to_types = {
                    col: self.STATE_TABLE_SCHEMA[col] for col in source_df.columns
                }
                unique_key = [exp.column("job_id")]

                if record_exists is None:
                    self.adapter.merge(
                        self.STATE_TABLE,
                        source_df,
                        columns_to_types,
                        unique_key,
                        when_matched=None,
                    )
                else:
                    self.adapter.insert_append(
                        self.STATE_TABLE,
                        source_df, 
                        columns_to_types
                    )
            else:
                update_payload = source_row_dict.copy()
                update_payload.pop("job_id")
                self.adapter.update_table(
                    self.STATE_TABLE,
                    update_payload,
                    where=exp.column("job_id").eq(exp.Literal.string(job_id)),
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