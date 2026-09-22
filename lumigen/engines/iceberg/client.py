from __future__ import annotations
import typing as t
import logging
from threading import Lock
import re
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from sqlglot import exp, parse_one, parse
from sqlglot.dialects.dialect import Dialects
from pyiceberg.catalog import WAREHOUSE_LOCATION, URI
from pyiceberg.io import load_file_io, AWS_REGION
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import upsert_util, ALWAYS_TRUE
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, NoSuchViewError
import pyarrow as pa
import duckdb

if t.TYPE_CHECKING:
    from pandas import DataFrame
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table as PyIcebergTable
    from pyiceberg.table.update.schema import UpdateSchema
    from pyiceberg.expressions import BooleanExpression
    from duckdb import DuckDBPyConnection

from engines.iceberg.catalog import load_catalog
from engines.iceberg.parser import IcebergSQLParser
from utils.decorators import TimeoutProxy
from utils.sql import TablePushdownExtractor
from utils.collections import SQLIdentifierSet

logger = logging.getLogger(__name__)


class DMLOperation(Enum):
    APPEND = auto()
    OVERWRITE = auto()
    TRUNCATE = auto()

@dataclass
class StagedOperation:
    action: DMLOperation
    table: str
    data: t.Optional[pa.Table] = None
    overwrite_filter: t.Union[str, BooleanExpression] = ALWAYS_TRUE

@dataclass
class TableTxnState:
    table_name: str
    initial_snapshot_id: t.Optional[int]
    ops_queue: t.List[StagedOperation] = field(default_factory=list)
    wap_branch_created: bool = False
    
    @property
    def queued_memory_bytes(self) -> int:
        return sum(op.data.nbytes for op in self.ops_queue if op.data is not None)

class IcebergCursor:
    BATCH_SIZE = 8192

    def __init__(self, catalog: Catalog, flush_threshold_bytes: int):
        self._catalog = catalog
        self.dialect = Dialects.DATABRICKS # also supported by DataFusion
        self._duckdb_conn = duckdb.connect()

        self._is_in_transaction: bool = False
        self.txn_id: t.Optional[str] = None
        self.flush_threshold_bytes = flush_threshold_bytes
        self._txn_states: t.Dict[str, TableTxnState] = {}
        
        self._last_df = None
        self._registered_tables = set()

    def _load_table(self, identifier: str) -> PyIcebergTable:
        if "." not in identifier:
            namespaces = (ns[0] for ns in self._catalog.list_namespaces() if ns)
            for ns in namespaces:
                if self._catalog.table_exists(ns + "." + identifier):
                    return self._catalog.load_table(ns + "." + identifier)
            for ns in namespaces:
                if self._catalog.view_exists(ns + "." + identifier):
                    return self._catalog.load_table(ns + "." + identifier)
        return self._catalog.load_table(identifier)

    def _get_empty_arrow_table(self, iceberg_tbl: PyIcebergTable) -> pa.Table:
        return pa.Table.from_pylist([], schema=iceberg_tbl.schema().as_arrow())

    def begin(self) -> None:
        if self._is_in_transaction:
            logger.debug("begin() called on an already active transaction. Ignoring.")
            return
        
        self.txn_id = f"wap_txn_{uuid.uuid4().hex}"
        self._txn_states.clear()
        self._is_in_transaction = True
        logger.debug(f"Iceberg WAP transaction started: {self.txn_id}")

    def _flush_table_to_wap(self, table_name: str) -> None:
        """Flushes in-memory staged operations to the Iceberg WAP branch to prevent OOM."""
        state = self._txn_states.get(table_name)
        if not state or not state.ops_queue:
            return
            
        logger.debug(f"Flushing {state.queued_memory_bytes} bytes for {table_name} to WAP branch.")
        iceberg_tbl = self._load_table(table_name)
        
        # Create WAP branch if it doesn't exist
        if not state.wap_branch_created:
            if state.initial_snapshot_id is not None:
                with iceberg_tbl.manage_snapshots() as ms:
                    ms.create_branch(snapshot_id=state.initial_snapshot_id, branch_name=self.txn_id)
                state.wap_branch_created = True
            else:
                # Edge Case: Table is brand new with 0 snapshots.
                logger.warning(f"Table {table_name} is empty. Cannot create branch. Will write to main.")
        
        # Write to the branch (or main if branching failed on empty table)
        branch_kwarg = {"branch": self.txn_id} if state.wap_branch_created else {}
        
        for op in state.ops_queue:
            if op.action == DMLOperation.APPEND:
                iceberg_tbl.append(op.data, **branch_kwarg)
            elif op.action == DMLOperation.OVERWRITE:
                iceberg_tbl.overwrite(op.data, overwrite_filter=op.overwrite_filter, **branch_kwarg)
            elif op.action == DMLOperation.TRUNCATE:
                iceberg_tbl.overwrite(self._get_empty_arrow_table(iceberg_tbl), overwrite_filter=op.overwrite_filter, **branch_kwarg)
        
        state.ops_queue.clear()

    def commit(self) -> None:
        """Publishes the transaction by fast-forwarding main to the WAP branch."""
        # Protection against manually calling commit when no operations were queued
        if not self._is_in_transaction:
            return

        logger.debug(f"Publishing WAP transaction {self.txn_id}...")
        try:
            # Phase 1: Flush all uncommitted memory queues for ALL tables first.
            for table_name, state in self._txn_states.items():
                self._flush_table_to_wap(table_name)
                
            # Phase 2: ONLY once all data is safely written to disk, update `main` pointers.
            for table_name, state in self._txn_states.items():
                # Publish the WAP branch to main
                if state.wap_branch_created:
                    iceberg_tbl = self._load_table(table_name)
                    wap_snapshot = iceberg_tbl.metadata.snapshot_by_name(self.txn_id)
                    if wap_snapshot:
                        with iceberg_tbl.manage_snapshots() as ms:
                            ms.set_current_snapshot(snapshot_id=wap_snapshot.snapshot_id)
                            ms.remove_branch(self.txn_id)
            
            logger.debug("Iceberg transaction committed successfully.")
        except Exception as e:
            logger.error(f"Commit failed mid-execution. Initiating rollback.", exc_info=e)
            self._perform_compensating_rollback()
            raise
        finally:
            self._cleanup_transaction()

    def _perform_compensating_rollback(self) -> None:
        """Gracefully removes temporary WAP branches to ensure the Data Lake remains clean on failure."""
        if (not self._is_in_transaction) and (not self._txn_states):
            return

        logger.debug(f"Rolling back WAP transaction {self.txn_id}.")
        for table_name, state in self._txn_states.items():
            if state.wap_branch_created:
                try:
                    iceberg_tbl = self._load_table(table_name)
                    with iceberg_tbl.manage_snapshots() as ms:
                        ms.remove_branch(self.txn_id)
                    logger.info(f"Dropped orphaned WAP branch {self.txn_id} for {table_name}")
                except Exception as e:
                    logger.error(f"Failed to drop WAP branch {self.txn_id} for {table_name}: {e}")
            elif not state.wap_branch_created and state.initial_snapshot_id is None:
                # Edge Case handling: it was an empty table and we flushed to main.
                try:
                    iceberg_tbl = self._load_table(table_name)
                    if iceberg_tbl.current_snapshot() is not None:
                        iceberg_tbl.overwrite(self._get_empty_arrow_table(iceberg_tbl))
                except Exception:
                    pass

        self._cleanup_transaction()

    def rollback(self) -> None:
        if not self._is_in_transaction:
            return
        self._perform_compensating_rollback()

    def _cleanup_transaction(self):
        self._txn_states.clear()
        self._is_in_transaction = False
        self.txn_id = None
        # Do not call clear cursor state here
        # so result from the last query can still be fetched after transaction ends.

    def _clear_cursor_state(self) -> None:
        """Clears the result set and temporary tables from the previous query."""
        self._last_df = None
        for tbl in list(self._registered_tables):
            try:
                self._duckdb_conn.unregister(tbl)
            except duckdb.CatalogException:
                pass
        self._registered_tables.clear()

    def close(self) -> None:
        if self._is_in_transaction:
            logger.warning("Cursor closed with an active transaction. Attempting to roll back.")
            self.rollback()

        self._clear_cursor_state()
        self._duckdb_conn.close()

    def _validate_table_and_schema(self, table_name: str, pa_df: t.Optional[pa.Table] = None, is_positional_insert: bool = False) -> None:
        """Fast-fails if the table doesn't exist or if the DataFrame schema is fundamentally incompatible."""
        iceberg_tbl = self._load_table(table_name)

        if pa_df is not None:
            iceberg_schema = iceberg_tbl.schema()
            if is_positional_insert:
                if len(pa_df.column_names) != len(iceberg_schema.fields):
                    raise ValueError(
                        f"Fail Fast: INSERT has {len(pa_df.column_names)} columns, but table '{table_name}' has {len(iceberg_schema.fields)} columns."
                    )
            else:
                iceberg_fields_set = SQLIdentifierSet(f.name for f in iceberg_schema.fields)
                required_fields_set = SQLIdentifierSet(f.name for f in iceberg_schema.fields if f.required)
                df_fields_set = SQLIdentifierSet(pa_df.column_names)

                missing_required = set()
                for req_field in required_fields_set:
                    if req_field not in df_fields_set:
                        missing_required.add(req_field)
                if missing_required:
                    raise ValueError(
                        f"Fail Fast: Cannot INSERT into '{table_name}'. "
                        f"DataFrame is missing required columns: {missing_required}"
                    )

                extra_fields = set()
                for col in pa_df.column_names:
                    if col not in iceberg_fields_set:
                        extra_fields.add(col)
                if extra_fields:
                    raise ValueError(
                        f"Fail Fast: Cannot INSERT into '{table_name}'. "
                        f"DataFrame contains extra columns not present in the table schema: {extra_fields}"
                    )
            
    def _align_arrow_table_to_schema(self, pa_df: pa.Table, iceberg_tbl: PyIcebergTable, is_positional_insert: bool = False) -> pa.Table:
        """
        Aligns a PyArrow Table to match the target Iceberg table's schema.
        """
        arrow_schema = iceberg_tbl.schema().as_arrow()
        
        target_column_names = [field.name for field in arrow_schema]
        if is_positional_insert:
            renamed_pa_df = pa_df.rename_columns(target_column_names)
            return renamed_pa_df.cast(arrow_schema)
        else:
            iceberg_fields_set = SQLIdentifierSet(target_column_names)
            normalized_cols = []
            for col in pa_df.column_names:
                matched_col = iceberg_fields_set.get(col)
                if matched_col is not None:
                    normalized_cols.append(matched_col)
                else:
                    normalized_cols.append(col)

            aligned_pa_df = pa_df.rename_columns(normalized_cols) if normalized_cols != pa_df.column_names else pa_df
            df_columns_set = SQLIdentifierSet(aligned_pa_df.column_names)
            for field in arrow_schema:
                # If an optional column is missing from the dataframe, append it as nulls
                if field.name not in df_columns_set:
                    null_array = pa.nulls(aligned_pa_df.num_rows, type=field.type)
                    aligned_pa_df = aligned_pa_df.append_column(field.name, null_array)
        
            # Reorder columns by name to match the target schema and strictly cast types
            return aligned_pa_df.select(target_column_names).cast(arrow_schema)
        
    def _register_single_table(self, origin_table: str, temp_table: str,
                               condition: t.Union[str, BooleanExpression, None] = None,
                               read_unflushed: bool = True,
                               force_flush: bool = False,
                               materialize: bool = False) -> None:
        state = self._txn_states.get(origin_table)
        if state and read_unflushed:
            # JIT WAP flush for partial overwrites
            has_partial_ops = any(
                op.action in (DMLOperation.OVERWRITE, DMLOperation.TRUNCATE) 
                and op.overwrite_filter is not ALWAYS_TRUE
                for op in state.ops_queue
            )
            if has_partial_ops:
                self._flush_table_to_wap(origin_table)
        
        iceberg_tbl = self._load_table(origin_table)
        arrow_schema = iceberg_tbl.schema().as_arrow()
        
        is_overwritten = False
        uncommitted_pa_tables = []
        if state and read_unflushed:
            for op in state.ops_queue:
                if op.action in (DMLOperation.OVERWRITE, DMLOperation.TRUNCATE):
                    is_overwritten = True
                    uncommitted_pa_tables.clear()
                if op.action in (DMLOperation.OVERWRITE, DMLOperation.APPEND) and op.data:
                    uncommitted_pa_tables.append(op.data)

        base_reader = None
        if not is_overwritten:
            # Scan the WAP branch if it exists, otherwise main
            branch_target = self.txn_id if (state and state.wap_branch_created) else "main"
            if condition:
                try:
                    data_scan = iceberg_tbl.scan(row_filter=condition)
                except:
                    data_scan = iceberg_tbl.scan()
            else:
                data_scan = iceberg_tbl.scan()

            if branch_target != "main":
                data_scan = data_scan.use_ref(branch_target)

            # Materialize prevents stream exhaustion when evaluating MERGE conditions sequentially
            if materialize:
                base_reader = data_scan.to_arrow()
            else:
                base_reader = data_scan.to_arrow_batch_reader()

        if not uncommitted_pa_tables and base_reader:
            self._duckdb_conn.register(temp_table, base_reader)
        else:
            if materialize:
                tables_to_concat = []
                if base_reader is not None:
                    tables_to_concat.append(base_reader)
                tables_to_concat.extend(uncommitted_pa_tables)
                if tables_to_concat:
                    combined_reader = pa.concat_tables([tbl.cast(arrow_schema) for tbl in tables_to_concat])
                else:
                    combined_reader = self._get_empty_arrow_table(iceberg_tbl)
            else:
                # Combine the Iceberg base stream and uncommitted in-memory batches
                def combined_batches():
                    if base_reader:
                        for batch in base_reader:
                            yield batch
                    for tbl in uncommitted_pa_tables:
                        # max_chunksize matches typical Arrow page sizes to prevent huge unified allocations
                        for batch in tbl.to_batches(max_chunksize=self.BATCH_SIZE):
                            yield batch.cast(arrow_schema)

                combined_reader = pa.RecordBatchReader.from_batches(arrow_schema, combined_batches())
        
            self._duckdb_conn.register(temp_table, combined_reader)
        self._registered_tables.add(temp_table)

        if state and force_flush:
            self._flush_table_to_wap(origin_table)

    def _register_tables_for_ast(self, parsed_ast: exp.Expression, exclude_table_exp_ids: t.Set[int] = {},
                                 row_filter_overrides: t.Dict[str, t.Any] = {},
                                 read_unflushed: bool = True,
                                 force_flush: bool = False,
                                 materialize: bool = False) -> None:
        tbl_dict = {}
        extractor = TablePushdownExtractor(parsed_ast)
        tbl_pushdowns = extractor.extract()
        for tbl_pushdown in tbl_pushdowns:
            table_exp: exp.Table = tbl_pushdown.table_node
            if id(table_exp) in exclude_table_exp_ids:
                continue

            origin_table = IcebergSQLParser.extract_table_name(table_exp, dialect=self.dialect)
            condition = tbl_pushdown.condition_sql

            temp_table = f"temp_{uuid.uuid4().hex}"
            tbl_dict[id(table_exp)] = (origin_table, temp_table, condition)

        for column_exp in parsed_ast.find_all(exp.Column):
            if column_exp.table:
                table_exp = extractor.get_table_for_column(column_exp)
                if table_exp and id(table_exp) not in exclude_table_exp_ids:
                    temp_name = tbl_dict[id(table_exp)][1]
                    if not column_exp.table == table_exp.alias:
                        # Note: columns in AST are modified in-place
                        column_exp.args["catalog"] = None
                        column_exp.args["db"] = None
                        column_exp.args["table"] = exp.Identifier(this=temp_name, quoted=False)

        for tbl_pushdown in tbl_pushdowns:
            table_exp: exp.Table = tbl_pushdown.table_node
            if id(table_exp) in exclude_table_exp_ids:
                continue

            temp_table = tbl_dict[id(table_exp)][1]

            # Note: tables in AST are modified in-place
            table_exp.args["catalog"] = None
            table_exp.args["db"] = None
            table_exp.args["this"] = exp.Identifier(this=temp_table, quoted=False)

        for origin_table, temp_table, condition in tbl_dict.values():
            if temp_table not in self._registered_tables:
                self._register_single_table(origin_table, temp_table, row_filter_overrides.get(origin_table, condition),
                                            read_unflushed=read_unflushed, force_flush=force_flush, materialize=materialize,
                                            )

    def execute(self, query: str, df: t.Optional[DataFrame] = None, **kwargs) -> None:
        parsed_exps = parse(query, dialect=self.dialect)
        if not parsed_exps:
            return None
        
        if len(parsed_exps) == 1:
            self._execute_single(query, df=df, **kwargs)
        else:
            for parsed_exp in parsed_exps:
                if parsed_exp is not None:
                    self._execute_single(parsed_exp.sql(dialect=self.dialect), df=df, **kwargs)
    
    def _execute_single(self, query: str, df: t.Optional[DataFrame] = None, **kwargs) -> None:
        self._clear_cursor_state()
        parsed_exp = parse_one(query, dialect=self.dialect)

        # DESCRIBE
        if isinstance(parsed_exp, exp.Describe):
            self._execute_describe(parsed_exp)
            return

        # SHOW ...
        is_show_cmd = isinstance(parsed_exp, exp.Show) or (isinstance(parsed_exp, exp.Command) and str(parsed_exp.this).upper() == "SHOW")
        if is_show_cmd:
            self._execute_show(query)
            return

        # CREATE DATABASE
        if (isinstance(parsed_exp, exp.Create) and parsed_exp.kind == "DATABASE") \
        or (isinstance(parsed_exp, exp.Command) and str(parsed_exp.expression).strip().upper().startswith("DATABASE")):
            create_meta = IcebergSQLParser.parse_create_database(query, self.dialect)
            if create_meta.if_not_exists:
                self._catalog.create_namespace_if_not_exists(create_meta.namespace, properties=create_meta.properties)
            else:
                self._catalog.create_namespace(create_meta.namespace, properties=create_meta.properties)
            return

        # CREATE TABLE ... AS SELECT
        if isinstance(parsed_exp, exp.Create) and isinstance(parsed_exp.expression, exp.Select):
            is_ctas = True
        elif isinstance(parsed_exp, exp.Create) and df is not None:
            is_ctas = True
        else:
            is_ctas = False
        is_dml = isinstance(parsed_exp, (exp.Insert, exp.Update, exp.Delete, exp.TruncateTable, exp.Merge)) or is_ctas
        
        is_auto_commit = False
        if is_dml and not self._is_in_transaction:
            self.begin()
            is_auto_commit = True
        
        try:
            # DDL operations
            reader = None
            is_ddl = True
            if isinstance(parsed_exp, exp.Create):
                inferred_schema = None
                if is_ctas:
                    if isinstance(parsed_exp.expression, exp.Select):
                        self._execute_select(parsed_exp.expression)
                        reader = t.cast("DuckDBPyConnection", self._last_df).to_arrow_reader(self.BATCH_SIZE)
                        pa_schema = reader.schema
                    elif df is not None:
                        pa_schema = pa.Table.from_pandas(df).schema

                    if pa_schema:
                        raw_schema = _pyarrow_to_schema_without_ids(pa_schema)
                        inferred_schema = assign_fresh_schema_ids(raw_schema)

                create_meta = IcebergSQLParser.parse_create_table(parsed_exp, self.dialect, inferred_schema=inferred_schema)
                namespace_tuple = self._catalog.namespace_from(create_meta.table_name)
                if namespace_tuple and not self._catalog.namespace_exists(namespace_tuple):
                    namespace_str = self._catalog.namespace_to_string(namespace_tuple)
                    raise NoSuchNamespaceError(
                        f"Cannot create table '{create_meta.table_name}'. "
                        f"Namespace '{namespace_str}' does not exist."
                    )
                
                # CREATE OR REPLACE TABLE
                if parsed_exp.args.get("replace"):
                    target_table = IcebergSQLParser.extract_table_name(parsed_exp, self.dialect)
                    try:
                        self._catalog.drop_table(target_table)
                    except NoSuchTableError:
                        pass
                if create_meta.if_not_exists:
                    self._catalog.create_table_if_not_exists(
                        create_meta.table_name,
                        create_meta.schema,
                        location=create_meta.location,
                        partition_spec=create_meta.partition_spec,
                        properties=create_meta.properties,
                    )
                else:
                    self._catalog.create_table(
                        create_meta.table_name,
                        create_meta.schema,
                        location=create_meta.location,
                        partition_spec=create_meta.partition_spec,
                        properties=create_meta.properties,
                    )
            elif isinstance(parsed_exp, exp.Drop):
                self._execute_drop(parsed_exp)
            elif isinstance(parsed_exp, exp.Alter) or (isinstance(parsed_exp, exp.Command) and str(parsed_exp.this).upper() == "ALTER"):
                self._execute_alter(query)
            else:
                is_ddl = False
            
            # DML operations
            if is_dml:
                target_table = IcebergSQLParser.extract_table_name(parsed_exp, self.dialect)
                
                # Setup Transaction State Tracking for this table
                if target_table not in self._txn_states:
                    iceberg_tbl = self._load_table(target_table)
                    curr = iceberg_tbl.current_snapshot()
                    self._txn_states[target_table] = TableTxnState(
                        table_name=target_table,
                        initial_snapshot_id=curr.snapshot_id if curr else None
                    )
                state = self._txn_states[target_table]

                if isinstance(parsed_exp, (exp.Insert, exp.Create)):
                    iceberg_tbl = self._load_table(target_table)
                    overwrite_filter = ALWAYS_TRUE

                    if isinstance(parsed_exp, exp.Create):
                        # CREATE OR REPLACE TABLE ...
                        is_overwrite = is_ctas or parsed_exp.args.get("replace")
                    else:
                        # INSERT OVERWRITE ...
                        is_overwrite = bool(parsed_exp.args.get("overwrite"))
                        if is_overwrite:
                            # Static overwrite: INSERT OVERWRITE ... PARTITION ...
                            partition_exp = parsed_exp.args.get("partition") or t.cast("exp.Table", parsed_exp.this).find(exp.Partition)
                            if partition_exp and partition_exp.expressions:
                                overwrite_filter = exp.and_(*partition_exp.expressions).sql(self.dialect)

                    # INSERT ... SELECT ...
                    if isinstance(parsed_exp.expression, exp.Select):
                        if reader is None:
                            self._execute_select(parsed_exp.expression)
                            reader = t.cast("DuckDBPyConnection", self._last_df).to_arrow_reader(self.BATCH_SIZE)

                        first_chunk = True
                        has_data = False
                        for batch in reader:
                            has_data = True
                            chunk_pa_df = pa.Table.from_batches([batch])
                            if first_chunk:
                                self._validate_table_and_schema(target_table, chunk_pa_df, is_positional_insert=True)
                            aligned_chunk = self._align_arrow_table_to_schema(chunk_pa_df, iceberg_tbl, is_positional_insert=True)

                            if is_overwrite and first_chunk:
                                current_action = DMLOperation.OVERWRITE
                                current_filter = overwrite_filter
                            else:
                                current_action = DMLOperation.APPEND
                                current_filter = ALWAYS_TRUE
                            state.ops_queue.append(StagedOperation(
                                action=current_action,
                                table=target_table,
                                data=aligned_chunk,
                                overwrite_filter=current_filter,
                            ))
                            first_chunk = False

                            # Trigger WAP Flush if Memory Exceeds Threshold
                            if state.queued_memory_bytes >= self.flush_threshold_bytes:
                                self._flush_table_to_wap(target_table)

                        if is_overwrite and not has_data:
                            state.ops_queue.append(StagedOperation(
                                action=DMLOperation.TRUNCATE,
                                table=target_table,
                                overwrite_filter=overwrite_filter,
                            ))

                        # Clear the SELECT result from the cursor state
                        self._clear_cursor_state()
                    else:
                        if df is None:
                            raise ValueError("DataFrame must be provided for INSERT statements without a SELECT clause")
                    
                        pa_df = pa.Table.from_pandas(df)
                        self._validate_table_and_schema(target_table, pa_df)
                        aligned_pa_df = self._align_arrow_table_to_schema(pa_df, iceberg_tbl)
                        action = DMLOperation.OVERWRITE if is_overwrite else DMLOperation.APPEND
                        state.ops_queue.append(StagedOperation(
                            action=action,
                            table=target_table,
                            data=aligned_pa_df,
                            overwrite_filter=overwrite_filter if action == DMLOperation.OVERWRITE else ALWAYS_TRUE
                        ))

                        if state.queued_memory_bytes >= self.flush_threshold_bytes:
                            self._flush_table_to_wap(target_table)
                
                # UPDATE
                elif isinstance(parsed_exp, exp.Update):
                    self._execute_update(parsed_exp)
                # DELETE
                elif isinstance(parsed_exp, exp.Delete):
                    self._execute_delete(parsed_exp)
                # TRUNCATE TABLE
                elif isinstance(parsed_exp, exp.TruncateTable):
                    self._validate_table_and_schema(target_table)
                    state.ops_queue.append(StagedOperation(action=DMLOperation.TRUNCATE, table=target_table))
                # MERGE INTO
                elif isinstance(parsed_exp, exp.Merge):
                    self._execute_merge(parsed_exp, df=df)
                else:
                    raise ValueError(f"Unsupported SQL statement of type: '{type(parsed_exp).__name__.upper()}'")
            # DQL operation
            elif isinstance(parsed_exp, exp.Select):
                self._execute_select(parsed_exp)
            elif not is_ddl:
                if not isinstance(parsed_exp, exp.Command):
                    raise ValueError(f"Unsupported SQL statement of type: '{type(parsed_exp).__name__.upper()}'")
                else:
                    raise ValueError(f"Unsupported SQL statement: {query}")
            
            if is_auto_commit:
                self.commit()
        except Exception:
            if is_auto_commit:
                self.rollback()
            raise

    def _execute_drop(self, drop_exp: exp.Drop) -> None:
        target_table = IcebergSQLParser.extract_table_name(drop_exp, self.dialect, lowercase=False)
        if drop_exp.args.get("purge"):
            drop_func = self._catalog.purge_table
        elif drop_exp.kind == "TABLE":
            drop_func = self._catalog.drop_table
        elif drop_exp.kind == "VIEW":
            drop_func = self._catalog.drop_view
        elif drop_exp.kind in ("DATABASE", "SCHEMA"):
            drop_func = self._catalog.drop_namespace
        else:
            raise NotImplementedError(f"DROP {drop_exp.kind} unsupported.")
        
        try:
            if drop_exp.args.get("exists"):
                try:
                    drop_func(target_table)
                except (NoSuchTableError, NoSuchViewError):
                    pass
            else:
                drop_func(target_table)
        except NotImplementedError:
            raise ValueError(f"DROP {drop_exp.kind} unsupported.")

    def _execute_alter(self, query: str) -> None:
        # ALTER TABLE table_identifier UNSET TBLPROPERTIES ( key1, key2, ... )
        props_to_remove = []
        if re.search(r"UNSET\s+TBLPROPERTIES", query, re.IGNORECASE):
            m = re.search(r"UNSET\s+TBLPROPERTIES\s*\(([^)]+)\)", query, re.IGNORECASE)
            if m:
                keys = re.findall(r"['\"]([^'\"]+)['\"]", m.group(1))
                props_to_remove.extend(keys)
            query = re.sub(r"UNSET\s+TBLPROPERTIES\s*\([^)]+\)", "SET TBLPROPERTIES ()", query, flags=re.IGNORECASE)

        # Extract FIRST / AFTER clauses before parsing
        col_positions: t.Dict[str, str] = {}
        def replace_pos_clause(m):
            op = m.group(1).upper()
            col_name = m.group(2).strip(" `'\"")
            col_positions[col_name] = m.group(4).strip()
            intermediate = (m.group(3) or "").strip()
            if not intermediate:
                # Replace with a dummy rename to keep SQL AST valid for sqlglot
                if op in ("ALTER", "CHANGE"):
                    return f"RENAME COLUMN {m.group(2)} TO {m.group(2)}"
                return f"{op} COLUMN {m.group(2)}"
            return f"{op} COLUMN {m.group(2)} {intermediate}"
        
        # Matches strictly: (ALTER|CHANGE) COLUMN <col> <intermediate> (FIRST|AFTER <col>)
        intermediate_pattern = r"""((?:[^,()'"]+|\([^)]*\)|'[^']*'|"[^"]*")*?)"""
        pattern = r"(?i)\b(ALTER|CHANGE)\s+(?!TABLE\b)(?:COLUMN\s+)?(?!\bCOLUMN\b)([`'\"]?[\w.]+[`'\"]?)(?:\s+" + intermediate_pattern + r")?\s+(FIRST|AFTER\s+[`'\"]?[\w.]+[`'\"]?)(?=\s*[,;]|\s*$)"
        query = re.sub(pattern, replace_pos_clause, query)

        alter_exp = parse_one(query, dialect=self.dialect)
        if not isinstance(alter_exp, exp.Alter):
            # Fallback to MySQL dialect for ALTER TABLE DROP COLUMN
            alter_exp = parse_one(query, dialect=Dialects.MYSQL)
            if not isinstance(alter_exp, exp.Alter):
                raise ValueError(f"Unsupported SQL statement: {query}\nYou may need to quote field names that contain \".\"")

        if not alter_exp.kind == "TABLE":
            raise NotImplementedError(f"ALTER {alter_exp.kind} unsupported.")

        rename_tbl_to = None
        props_to_set = {}
        target_table = IcebergSQLParser.extract_table_name(alter_exp.this, dialect=self.dialect)
        iceberg_tbl = self._load_table(target_table)
        with iceberg_tbl.transaction() as txn:
            schema_txn = None
            def get_schema_txn():
                nonlocal schema_txn
                if schema_txn is None:
                    schema_txn = txn.update_schema(allow_incompatible_changes=True)
                return schema_txn

            for action in alter_exp.actions:
                if isinstance(action, exp.AlterRename):
                    rename_tbl_to = IcebergSQLParser.extract_table_name(action.this, self.dialect, lowercase=False)
                elif isinstance(action, exp.AlterSet):
                    for set_exp in action.expressions:
                        if isinstance(set_exp, exp.Properties):
                            for property_exp in t.cast("t.List[exp.Property]", set_exp.expressions):
                                if "value" in property_exp.args:
                                    key = t.cast("exp.Literal", property_exp.this).name
                                    value = t.cast("exp.Literal", property_exp.args["value"]).name
                                    props_to_set[key] = value
                                else:
                                    raise NotImplementedError(f"Unsupported table property type: {type(property_exp).__name__}")
                # ALTER TABLE ADD COLUMN(S)
                elif isinstance(action, (exp.Schema, exp.ColumnDef)):
                    col_defs: t.List[exp.ColumnDef] = [action] if isinstance(action, exp.ColumnDef) else action.expressions
                    for col_def in col_defs:
                        new_col = IcebergSQLParser.parse_column_def(col_def)
                        col_path = tuple(new_col.name.split('.')) if '.' in new_col.name else new_col.name

                        st = get_schema_txn()
                        st.add_column(
                            path=col_path,
                            field_type=new_col.field_type,
                            doc=new_col.doc,
                            required=new_col.required,
                            default_value=new_col.initial_default,
                        )

                        position: exp.ColumnPosition = col_def.args.get("position")
                        if position:
                            if position.args["position"] == "FIRST":
                                st.move_first(col_path)
                            elif position.args["position"] == "AFTER":
                                after_col: exp.Column = position.this
                                st.move_after(col_path, after_col.name)
                            else:
                                raise ValueError(f"Unsupported column position: {position.args['position']}")
                elif isinstance(action, exp.RenameColumn):
                    rename_col_from: exp.Column = action.this
                    rename_col_to: exp.Column = action.args["to"]
                    if rename_col_from.name != rename_col_to.name:
                        get_schema_txn().rename_column(rename_col_from.name, rename_col_to.name)
                elif isinstance(action, exp.AlterColumn):
                    col_name = t.cast("exp.Identifier", action.this).name
                    col_dtype = IcebergSQLParser.convert_data_type(action.args.get("dtype")) if action.args.get("dtype") else None
                    comment_lit: exp.Literal = action.args.get("comment")
                    get_schema_txn().update_column(
                        path=col_name,
                        field_type=col_dtype,
                        required=action.args.get("allow_null"),
                        doc=comment_lit.to_py() if comment_lit else None,
                    )
                elif isinstance(action, exp.Drop) and action.kind == "COLUMN":
                    col_name = t.cast("exp.Identifier", action.this).name
                    get_schema_txn().delete_column(col_name)
                else:
                    raise ValueError(f"Unsupported SQL statement: {query}")
                                
            if props_to_set:
                txn.set_properties(props_to_set)
            if props_to_remove:
                txn.remove_properties(*props_to_remove)

            for col_name, pos_clause in col_positions.items():
                st = get_schema_txn()
                pos_upper = pos_clause.upper()
                if pos_upper.startswith("FIRST"):
                    st.move_first(col_name)
                elif pos_upper.startswith("AFTER"):
                    after_col_name: str = re.sub(r"(?i)AFTER\s+", "", pos_clause).strip(" `'\"")
                    st.move_after(col_name, after_col_name)
            if schema_txn is not None:
                t.cast("UpdateSchema", schema_txn).commit()

        if rename_tbl_to is not None:
            self._catalog.rename_table(target_table, rename_tbl_to)
        
    def _execute_scoped_cow(
        self, 
        parsed_exp: t.Union[exp.Update, exp.Delete], 
        target_table: str, 
        iceberg_tbl: PyIcebergTable, 
        where_exp: t.Optional[exp.Where] = None,
        update_assignments: t.Optional[t.Dict[str, exp.Expression]] = None
    ) -> None:
        """
        Fallback implementation for UPDATE and DELETE on tables without Primary Keys.
        Safely isolates the blast radius of affected rows, resolves the updates/deletes, 
        and preserves unchanged duplicate rows before overwriting the radius.
        """
        state = self._txn_states[target_table]
        target_node: exp.Table = parsed_exp.this
        target_alias = target_node.alias_or_name
        target_columns = [field.name for field in iceberg_tbl.schema().fields]

        # 1. Isolate Blast Radius
        join_cols = target_columns
        select_keys = exp.select(*[exp.column(c, table=target_alias) for c in join_cols]).distinct().from_(target_node.copy())
        if where_exp:
            select_keys = select_keys.where(where_exp.copy())

        # 2. Disambiguate column references in the WHERE clause.
        #    This prevents 'ambiguous column name' errors
        #    when we later join the target table against the blast_radius temp table.
        where_cond = t.cast("exp.Condition", t.cast("exp.Where", select_keys.args.get("where")).this)
        extractor = TablePushdownExtractor(select_keys)
        for column_exp in where_cond.find_all(exp.Column):
            if not column_exp.table:
                table_exp = extractor.get_table_for_column(column_exp)
                if table_exp:
                    column_exp.args["table"] = exp.Identifier(this=table_exp.alias_or_name, quoted=False)
        where_cond = where_cond.copy()

        self._execute_select(select_keys)
        blast_radius_df = t.cast("DuckDBPyConnection", self._last_df).to_arrow_table()
        self._clear_cursor_state()

        if len(blast_radius_df) == 0:
            return
        
        overwrite_filter = upsert_util.create_match_filter(blast_radius_df, join_cols)

        # 3. Register blast temp table
        blast_temp_name = f"__blast_{uuid.uuid4().hex}"
        self._duckdb_conn.register(blast_temp_name, blast_radius_df)
        self._registered_tables.add(blast_temp_name)

        # 4. Formulate NULL-safe join conditions to pull all matching survivor rows
        join_conds = []
        for c in join_cols:
            left_col = exp.column(c, table=target_alias)
            right_col = exp.column(c, table=blast_temp_name)
            eq = exp.EQ(this=left_col.copy(), expression=right_col.copy())
            null_eq = exp.and_(exp.Is(this=left_col.copy(), expression=exp.Null()), exp.Is(this=right_col.copy(), expression=exp.Null()))
            join_conds.append(exp.or_(eq, null_eq))
        join_node = exp.Join(this=exp.table_(blast_temp_name), on=exp.and_(*join_conds), kind="INNER")

        # 5. Build the Final Query
        if isinstance(parsed_exp, exp.Delete):
            select_exprs = [exp.column(col, table=target_alias) for col in target_columns]
            final_query = exp.select(*select_exprs).from_(target_node.copy()).join(join_node)
            if where_exp:
                final_query = final_query.where(exp.not_(where_cond.copy()))
        else: # exp.Update
            select_exprs = []
            for col in target_columns:
                if update_assignments and col in update_assignments:
                    if where_exp:
                        case_node = exp.Case().when(where_cond.copy(), update_assignments[col].copy()).else_(exp.column(col, table=target_alias))
                        select_exprs.append(exp.alias_(case_node, col))
                    else:
                        select_exprs.append(exp.alias_(update_assignments[col].copy(), col))
                else:
                    select_exprs.append(exp.column(col, table=target_alias))
            final_query = exp.select(*select_exprs).from_(target_node.copy()).join(join_node)

        blast_node = t.cast("exp.Join", final_query.args["joins"][0]).this
        self._register_tables_for_ast(final_query, exclude_table_exp_ids={id(blast_node)})
        final_df = self._duckdb_conn.execute(final_query.sql(Dialects.DUCKDB)).to_arrow_table()
        aligned_df = self._align_arrow_table_to_schema(final_df, iceberg_tbl)
        state.ops_queue.append(StagedOperation(
            action=DMLOperation.OVERWRITE,
            table=target_table,
            data=aligned_df,
            overwrite_filter=overwrite_filter,
        ))

        if state.queued_memory_bytes >= self.flush_threshold_bytes:
            self._flush_table_to_wap(target_table)

        self._clear_cursor_state()

    def _execute_update(self, update_exp: exp.Update) -> None:
        target_table = IcebergSQLParser.extract_table_name(update_exp.this, self.dialect)
        state = self._txn_states[target_table]

        target_node: exp.Table = update_exp.this
        target_alias = target_node.alias_or_name
        where_exp = update_exp.args["where"]
        iceberg_tbl = self._load_table(target_table)
        pk_fields = iceberg_tbl.schema().identifier_field_ids
        has_pk = bool(pk_fields)

        update_assignments: t.Dict[str, exp.Expression] = {}
        assignment: exp.EQ
        for assignment in update_exp.expressions:
            col = t.cast("exp.Column", assignment.this).name
            update_assignments[col] = assignment.expression

        if not has_pk:
            self._execute_scoped_cow(update_exp, target_table, iceberg_tbl, where_exp, update_assignments)
            return
        
        join_cols = [iceberg_tbl.schema().find_column_name(fid) for fid in pk_fields]
        target_columns = [field.name for field in iceberg_tbl.schema().fields]

        select_exprs = []
        old_join_cols = []
        for col in join_cols:
            alias_name = f"__old_{col}"
            old_join_cols.append(alias_name)
            select_exprs.append(exp.alias_(exp.column(col, table=target_alias), alias_name))
        for col in target_columns:
            if col in update_assignments:
                select_exprs.append(exp.alias_(update_assignments[col].copy(), col))
            else:
                select_exprs.append(exp.column(col, table=target_alias))
        select_exp = exp.select(*select_exprs).from_(target_node.copy())
        if where_exp:
            select_exp = select_exp.where(where_exp)

        self._execute_select(select_exp)
        result_df = t.cast("DuckDBPyConnection", self._last_df).to_arrow_table()
        if len(result_df) > 0:
            old_df = result_df.select(old_join_cols).rename_columns(join_cols)
            overwrite_filter = upsert_util.create_match_filter(old_df, join_cols)
            updated_df = result_df.select(target_columns)
            aligned_df = self._align_arrow_table_to_schema(updated_df, iceberg_tbl)
            state.ops_queue.append(StagedOperation(
                action=DMLOperation.OVERWRITE,
                table=target_table,
                data=aligned_df,
                overwrite_filter=overwrite_filter,
            ))

            if state.queued_memory_bytes >= self.flush_threshold_bytes:
                self._flush_table_to_wap(target_table)
        self._clear_cursor_state()

    def _execute_delete(self, delete_exp: exp.Delete) -> None:
        target_table = IcebergSQLParser.extract_table_name(delete_exp.this, self.dialect)
        state = self._txn_states[target_table]

        where_exp = delete_exp.args["where"]
        if not where_exp:
            self._validate_table_and_schema(target_table)
            state.ops_queue.append(StagedOperation(action=DMLOperation.TRUNCATE, table=target_table))
            self._clear_cursor_state()
            return

        target_node: exp.Table = delete_exp.this
        target_alias = target_node.alias_or_name
        iceberg_tbl = self._load_table(target_table)
        pk_fields = iceberg_tbl.schema().identifier_field_ids
        has_pk = bool(pk_fields)

        if not has_pk:
            self._execute_scoped_cow(delete_exp, target_table, iceberg_tbl, where_exp)
            return
        
        join_cols = [iceberg_tbl.schema().find_column_name(fid) for fid in pk_fields]

        select_exprs = [exp.column(col, table=target_alias) for col in join_cols]
        select_exp = exp.select(*select_exprs).from_(target_node.copy()).where(where_exp)

        self._execute_select(select_exp)
        deleted_df = t.cast("DuckDBPyConnection", self._last_df).to_arrow_table()
        if len(deleted_df) > 0:
            overwrite_filter = upsert_util.create_match_filter(deleted_df, join_cols)
            state.ops_queue.append(StagedOperation(
                action=DMLOperation.TRUNCATE,
                table=target_table,
                overwrite_filter=overwrite_filter,
            ))

            if state.queued_memory_bytes >= self.flush_threshold_bytes:
                self._flush_table_to_wap(target_table)
        self._clear_cursor_state()
    
    def _execute_merge(self, merge_exp: exp.Merge, df: t.Optional[DataFrame] = None) -> None:
        target_table = IcebergSQLParser.extract_table_name(merge_exp.this, self.dialect)
        state = self._txn_states[target_table]
        
        target_node: exp.Table = merge_exp.this
        target_alias = target_node.alias_or_name
        source_node: exp.Expression = merge_exp.args.get("using")
        source_alias = source_node.alias_or_name
        on_condition: exp.Condition = merge_exp.args.get("on")
        whens_exp: exp.Whens = merge_exp.args.get("whens")

        join_cols = []
        for node in on_condition.find_all(exp.EQ):
            if isinstance(node.left, exp.Column) and isinstance(node.right, exp.Column):
                if node.left.table == target_alias:
                    join_cols.append(node.left.name)
                elif node.right.table == target_alias:
                    join_cols.append(node.right.name)

        iceberg_tbl = self._load_table(target_table)
        if not join_cols:
            # Find explicit row-level identifier (like Primary Key)
            join_cols = [iceberg_tbl.schema().find_column_name(fid) for fid in iceberg_tbl.schema().identifier_field_ids]

        if not join_cols:
            raise ValueError(
                f"Cannot execute MERGE INTO '{target_table}'. "
                f"Target table has no Primary Key identifier fields, and no simple equality join keys (e.g. 't.id = s.id') were found in the ON clause."
            )
            
        if df is None:
            source_query = exp.select("*").from_(source_node.copy())
            self._execute_select(source_query)
            source_df = t.cast("DuckDBPyConnection", self._last_df).to_arrow_table()
            self._clear_cursor_state()
        else:
            temp_table = f"temp_{uuid.uuid4().hex}"
            sub_tbl = exp.table_(temp_table, alias=source_alias)
            source_node.replace(sub_tbl)
            source_node = sub_tbl
            source_df = pa.Table.from_pandas(df)
            self._duckdb_conn.register(temp_table, source_df)
            self._registered_tables.add(temp_table)

        match_filter = upsert_util.create_match_filter(source_df, join_cols)
        register_kwargs = {}
        not_matched_by_source_exists = any(t.cast("exp.When", when_exp).args["source"] for when_exp in whens_exp.expressions)
        if not not_matched_by_source_exists:
            register_kwargs["row_filter_overrides"] = {target_table: match_filter}

        target_columns = [field.name for field in iceberg_tbl.schema().fields]
        matched_handled_conds: t.List[exp.Condition] = []
        not_matched_handled_conds: t.List[exp.Condition] = []
        op_started = False
        when_exp: exp.When
        for when_exp in whens_exp.expressions:
            is_matched = when_exp.args.get("matched")
            by_source = bool(when_exp.args["source"])
            cond: t.Optional[exp.Condition] = when_exp.args.get("condition")
            then_node: exp.Expression = when_exp.args.get("then")

            effective_conds = []
            if cond:
                effective_conds.append(cond.copy())

            prev_conds = matched_handled_conds if is_matched else not_matched_handled_conds
            for prev_cond in prev_conds:
                if prev_cond is None:
                    effective_conds.append(exp.false())
                else:
                    effective_conds.append(exp.not_(prev_cond.copy()))

            if cond is None:
                prev_conds.append(None)
            else:
                prev_conds.append(cond.copy())

            register_kwargs["read_unflushed"] = not op_started
            register_kwargs["force_flush"] = not op_started
            op_started = True
            final_condition = exp.and_(*effective_conds) if effective_conds else None
            if isinstance(then_node, exp.Delete) or then_node.sql(self.dialect).upper().startswith("DELETE"):
                join_exp = exp.Join(this=source_node.copy(), on=on_condition.copy(), kind="INNER" if not by_source else "ANTI")
                select_exp = exp.select(f"{target_alias}.*").from_(target_node.copy()).join(join_exp)
                if final_condition:
                    select_exp = select_exp.where(final_condition)
                
                if df is not None:
                    source_in_select = t.cast("exp.Join", select_exp.args["joins"][0]).this
                    register_kwargs["exclude_table_exp_ids"] = {id(source_in_select)}
                self._register_tables_for_ast(select_exp, **register_kwargs)
                deleted_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB)).to_arrow_table()
                if len(deleted_df) > 0:
                    overwrite_filter = upsert_util.create_match_filter(deleted_df, join_cols)
                    state.ops_queue.append(StagedOperation(
                        action=DMLOperation.TRUNCATE,
                        table=target_table,
                        overwrite_filter=overwrite_filter,
                    ))
            elif isinstance(then_node, exp.Update) or then_node.sql(self.dialect).upper().startswith("UPDATE"):
                update_assignments: t.Dict[str, exp.Expression] = {}
                assignment: exp.EQ
                for assignment in then_node.expressions:
                    if isinstance(assignment, exp.Star):
                        # UPDATE SET * [ EXCEPT ( column [, ...] ) ]
                        except_cols = set(c.name for c in t.cast("t.List[exp.Column]", assignment.args["except"] or []))
                        for col in target_columns:
                            if col not in except_cols:
                                update_assignments[col] = exp.column(col, table=source_alias)
                            else:
                                update_assignments[col] = exp.Null()
                        insert_cols = [col for col in target_columns if col not in except_cols]
                    else:
                        # UPDATE SET { column = { expr | DEFAULT } }
                        col = t.cast("exp.Column", assignment.this).name
                        if isinstance(assignment.expression, exp.Var) and assignment.expression.this == "DEFAULT":
                            update_assignments[col] = exp.Null()
                        else:
                            update_assignments[col] = assignment.expression
                    
                select_exprs = []
                for col in target_columns:
                    if col in update_assignments:
                        select_exprs.append(exp.alias_(update_assignments[col].copy(), col))
                    else:
                        select_exprs.append(exp.column(col, table=target_alias))

                join_exp = exp.Join(this=source_node.copy(), on=on_condition.copy(), kind="INNER" if not by_source else "ANTI")
                select_exp = exp.select(*select_exprs).from_(target_node.copy()).join(join_exp)
                if final_condition:
                    select_exp = select_exp.where(final_condition)
                
                if df is not None:
                    source_in_select = t.cast("exp.Join", select_exp.args["joins"][0]).this
                    register_kwargs["exclude_table_exp_ids"] = {id(source_in_select)}
                self._register_tables_for_ast(select_exp, **register_kwargs)
                updated_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB)).to_arrow_table()
                if len(updated_df) > 0:
                    overwrite_filter = upsert_util.create_match_filter(updated_df, join_cols)
                    aligned_df = self._align_arrow_table_to_schema(updated_df, iceberg_tbl)
                    state.ops_queue.append(StagedOperation(
                        action=DMLOperation.OVERWRITE,
                        table=target_table,
                        data=aligned_df,
                        overwrite_filter=overwrite_filter,
                    ))
            elif isinstance(then_node, exp.Insert) or then_node.sql(self.dialect).upper().startswith("INSERT"):
                if isinstance(then_node.this, (exp.Tuple, exp.Schema)):
                    insert_cols = [c.name for c in t.cast("t.List[exp.Column]", then_node.this.expressions)]
                elif isinstance(then_node.this, exp.Star):
                    except_cols = set(c.name for c in t.cast("t.List[exp.Column]", then_node.this.args["except"] or []))
                    insert_cols = [col for col in target_columns if col not in except_cols]
                else:
                    insert_cols = target_columns

                if isinstance(then_node.this, exp.Star):
                    # INSERT * [ EXCEPT ( column [, ...] ) ]
                    insert_vals = [exp.column(col) for col in target_columns if col not in except_cols]
                elif isinstance(then_node.expression, exp.Tuple):
                    # INSERT (column1 [, ...] ) VALUES ( expr | DEFAULT )
                    insert_vals = []
                    for val in then_node.expression.expressions:
                        if isinstance(val, exp.Var) and val.this == "DEFAULT":
                            insert_vals.append(exp.Null())
                        else:
                            insert_vals.append(val)
                else:
                    raise ValueError(f"Unsupported INSERT expression in MERGE: {then_node.expression}")
                    
                select_exprs = []
                val: exp.Expression
                for col, val in zip(insert_cols, insert_vals):
                    select_exprs.append(exp.alias_(val.copy(), col))
                    
                for col in target_columns:
                    if col not in insert_cols:
                        select_exprs.append(exp.alias_(exp.Null(), col))

                join_exp = exp.Join(this=target_node.copy(), on=on_condition.copy(), kind="ANTI")
                select_exp = exp.select(*select_exprs).from_(source_node.copy()).join(join_exp)
                if final_condition:
                    select_exp = select_exp.where(final_condition)
                
                if df is not None:
                    source_in_select = t.cast("exp.From", select_exp.args["from"]).this
                    register_kwargs["exclude_table_exp_ids"] = {id(source_in_select)}
                self._register_tables_for_ast(select_exp, **register_kwargs)
                inserted_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB)).to_arrow_table()
                if len(inserted_df) > 0:
                    aligned_df = self._align_arrow_table_to_schema(inserted_df, iceberg_tbl)
                    state.ops_queue.append(StagedOperation(
                        action=DMLOperation.APPEND,
                        table=target_table,
                        data=aligned_df,
                    ))

        # Flush MERGE operations at once
        if state.queued_memory_bytes >= self.flush_threshold_bytes:
            self._flush_table_to_wap(target_table)
        self._clear_cursor_state()
    
    def _execute_select(self, select_exp: exp.Select) -> None:
        select_exp = select_exp.copy()
        self._register_tables_for_ast(select_exp)
        self._last_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB))

    def _execute_describe(self, describe_exp: exp.Describe) -> None:
        if describe_exp.args.get("kind") and not describe_exp.args["kind"] == "TABLE":
            raise NotImplementedError(f'DESCRIBE {describe_exp.args["kind"]} unsupported.')

        target_table = IcebergSQLParser.extract_table_name(describe_exp, dialect=self.dialect)
        iceberg_tbl = self._load_table(target_table)
        schema = iceberg_tbl.schema()

        records = []
        for field in schema.fields:
            records.append((
                field.name,
                str(field.field_type),
                "NO" if field.required else "YES",
                field.write_default,
                field.doc or "",
                field.initial_default,
            ))
        pa_table = pa.Table.from_pydict({
            "field": [r[0] for r in records],
            "type": [r[1] for r in records],
            "nullable": [r[2] for r in records],
            "write_default": [r[3] for r in records],
            "comment": [r[4] for r in records],
            "initial_default": [r[5] for r in records]
        })

        temp_view = "__iceberg_describe_table"
        self._duckdb_conn.register(temp_view, pa_table)
        self._registered_tables.add(temp_view)

        self._last_df = self._duckdb_conn.execute(exp.select("*").from_(exp.table_(temp_view)).sql(Dialects.DUCKDB))

    def _execute_show(self, query: str) -> None:
        # Fallback to MySQL dialect for consistent SHOW command parsing 
        parsed_exp = parse_one(query, dialect=Dialects.MYSQL)
        if isinstance(parsed_exp, exp.Show):
            if str(parsed_exp.this).upper() == "DATABASES":
                namespaces = [ns[0] for ns in self._catalog.list_namespaces() if ns]
                arrow_df = pa.Table.from_arrays([
                    pa.array(namespaces, type=pa.string())
                ], names=["database"])

                temp_view = "__iceberg_show_databases"
                self._duckdb_conn.register(temp_view, arrow_df)
                self._registered_tables.add(temp_view)

                select_exp = (
                    exp.select("*").from_(exp.table_(temp_view))
                )
                like_lit = parsed_exp.args["like"]
                if like_lit:
                    select_exp = select_exp.where(exp.column("database").ilike(like_lit))
                where_exp: exp.Where = parsed_exp.args["where"]
                if where_exp:
                    select_exp = select_exp.where(where_exp.this)
    
                self._last_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB))
                return
            elif str(parsed_exp.this).upper() == "TABLES":
                namespaces = set()
                db_lit: t.Optional[exp.Literal] = parsed_exp.args["db"]
                if db_lit:
                    db_lit.set("quoted", False)
                    namespaces.add(db_lit.sql())
                else:
                    namespaces.update(ns[0] for ns in self._catalog.list_namespaces() if ns)
                
                table_names = []
                table_schemas = []
                table_types = []
                for ns in namespaces:
                    for table in self._catalog.list_tables(ns):
                        table_names.append(table[-1])
                        table_schemas.append(".".join(table[:-1]))
                        table_types.append("TABLE")
                    try:
                        for view in self._catalog.list_views(ns):
                            table_names.append(view[-1])
                            table_schemas.append(".".join(view[:-1]))
                            table_types.append("VIEW")
                    except NotImplementedError:
                        pass
                arrow_df = pa.Table.from_arrays([
                    pa.array(table_names, type=pa.string()),
                    pa.array(table_schemas, type=pa.string()),
                    pa.array(table_types, type=pa.string())
                ], names=["table_name", "table_schema", "table_type"])

                temp_view = "__iceberg_show_tables"
                self._duckdb_conn.register(temp_view, arrow_df)
                self._registered_tables.add(temp_view)

                select_exp = (
                    exp.select("*").from_(exp.table_(temp_view))
                )
                like_lit = parsed_exp.args["like"]
                if like_lit:
                    select_exp = select_exp.where(exp.column("table_name").ilike(like_lit))
                where_exp: exp.Where = parsed_exp.args["where"]
                if where_exp:
                    select_exp = select_exp.where(where_exp.this)
    
                self._last_df = self._duckdb_conn.execute(select_exp.sql(Dialects.DUCKDB))
                return

        raise ValueError(f"Unsupported SHOW command: {query}")
        
    def fetchone(self) -> t.Optional[t.Tuple]:
        return self._last_df.fetchone() if self._last_df else None
    
    def fetchmany(self, size: int = 1) -> t.List[t.Tuple]:
        return self._last_df.fetchmany(size=size) if self._last_df else []

    def fetchall(self) -> t.List[t.Tuple]:
        return self._last_df.fetchall() if self._last_df else []
    
    def fetchdf(self) -> t.Optional[DataFrame]:
        return self._last_df.df() if self._last_df else None
    
    @property
    def description(self) -> t.Optional[t.List[t.Tuple]]:
        return self._last_df.description if self._last_df else None
    
    @property
    def rowcount(self) -> int:
        return self._last_df.rowcount if self._last_df else 0
    
class IcebergConnection:
    DEFAULT_FLUSH_THRESHOLD_BYTES = 128 * 1024 * 1024
    DEFAULT_TIMEOUT_SECONDS = 10

    def __init__(self, catalog_type: str, catalog_properties: t.Optional[t.Dict[str, t.Any]] = None,
                 connect_timeout: t.Optional[int] = None):
        self.catalog_type = catalog_type
        self.catalog_props = catalog_properties
        self.connect_timeout = connect_timeout or self.DEFAULT_TIMEOUT_SECONDS

        self._catalog: t.Optional[Catalog] = None
        self._catalog_proxy: t.Optional[Catalog] = None
        self._catalog_lock = Lock()

    @property
    def catalog(self) -> Catalog:
        if self._catalog_proxy is None:
            with self._catalog_lock:
                if self._catalog_proxy is None:
                    self._catalog = load_catalog("default", type=self.catalog_type, **self.catalog_props)
                    self._catalog_proxy = TimeoutProxy(self._catalog, self.connect_timeout)

        return self._catalog_proxy
    
    def get_catalog(self) -> Catalog:
        return self.catalog
    
    def ping(self) -> None:
        namespaces = self.catalog.list_namespaces()

        warehouse = self.catalog.properties.get(WAREHOUSE_LOCATION)
        # If warehouse isn't explicitly configured locally, try to fetch it from the server
        if (not warehouse) and namespaces:
            try:
                ns_props = self.catalog.load_namespace_properties(namespaces[0])
                warehouse = ns_props.get("location")
            except Exception:
                pass
        if warehouse:
            io_props = self.catalog.properties.copy()
            timeout_ms = str((self.connect_timeout or self.DEFAULT_TIMEOUT_SECONDS) * 1000)
            io_props.setdefault("s3.connect-timeout", timeout_ms)
            io_props.setdefault("s3.request-timeout", timeout_ms)
            
            try:
                io = load_file_io(io_props)
                # Calling exists() forces a HEAD request to the storage API.
                io.new_input(warehouse).exists()
            except Exception as e:
                raise ConnectionError(
                    f"Catalog connected successfully, but Storage ping failed for '{warehouse}'. "
                    f"Check your S3 endpoints, region, and credentials. Details: {str(e)}"
                ) from e

    def table_exists(self, table: str, namespace: t.Optional[str] = None) -> bool:
        namespaces = {namespace} if namespace else (ns[0] for ns in self.catalog.list_namespaces() if ns)
        return any(self.catalog.table_exists(ns + "." + table) for ns in namespaces)
    
    def view_exists(self, table: str, namespace: t.Optional[str] = None) -> bool:
        namespaces = {namespace} if namespace else (ns[0] for ns in self.catalog.list_namespaces() if ns)
        try:
            return any(self.catalog.view_exists(ns + "." + table) for ns in namespaces)
        except NotImplementedError:
            return False

    def cursor(self, flush_threshold_bytes: t.Optional[int] = None) -> IcebergCursor:
        threshold = flush_threshold_bytes or self.DEFAULT_FLUSH_THRESHOLD_BYTES
        return IcebergCursor(catalog=self.catalog, flush_threshold_bytes=threshold)
    
    def commit(self) -> None:
        """
        Satisfies PEP-249 DB-API requirements for Pandas.
        Note: Actual WAP transactions in this engine are managed and committed locally by the IcebergCursor.
        """
        pass

    def rollback(self) -> None:
        """
        Satisfies PEP-249 DB-API requirements for Pandas error-handling.
        Note: Actual WAP rollbacks are handled locally by the IcebergCursor.
        """
        pass

    def close(self) -> None:
        if self._catalog_proxy:
            self._catalog_proxy.close()
            self._catalog_proxy = None

def connect(catalog_type: str,
            warehouse: t.Optional[str] = None, uri: t.Optional[str] = None,
            catalog_properties: t.Optional[t.Dict[str, t.Any]] = None,
            hadoop_config: t.Optional[t.Dict[str, t.Any]] = None,
            connect_timeout: t.Optional[int] = None) -> IcebergConnection:
    props = (catalog_properties or {}).copy()

    # Explicit arguments take precedence if they are not None
    if warehouse is not None:
        props[WAREHOUSE_LOCATION] = warehouse
    if uri is not None:
        props[URI] = uri
    if hadoop_config:
        for k, v in hadoop_config.items():
            if v is not None:
                props["hadoop." + k] = v
    if AWS_REGION not in props:
        props[AWS_REGION] = "us-east-1"

    filtered_props = {k: v for k, v in props.items() if v is not None}
    return IcebergConnection(catalog_type, filtered_props, connect_timeout)