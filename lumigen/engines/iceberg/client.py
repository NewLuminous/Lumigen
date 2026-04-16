from __future__ import annotations
import typing as t
import logging
from threading import Lock
import uuid
from dataclasses import dataclass
from collections import defaultdict
from enum import Enum, auto
from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import Dialects
from pyiceberg.catalog import load_catalog, WAREHOUSE_LOCATION, URI
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField, PARTITION_FIELD_ID_START, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.transforms import IdentityTransform, YearTransform, MonthTransform, DayTransform, HourTransform, BucketTransform, TruncateTransform
import pyarrow as pa
import duckdb

if t.TYPE_CHECKING:
    from pandas import DataFrame
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table as PyIcebergTable

from engines.iceberg.dialect import Iceberg
from utils.sql import TablePushdownExtractor

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

class IcebergCursor:
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self.dialect = Dialects.DATABRICKS # also supported by DataFusion
        self._duckdb_conn = duckdb.connect()

        self._is_in_transaction: bool = False
        self._staged_operations: t.List[StagedOperation] = []
        
        self._last_df = None
        self._registered_tables = set()

    def _get_empty_arrow_table(self, iceberg_tbl: PyIcebergTable) -> pa.Table:
        return pa.Table.from_pylist([], schema=iceberg_tbl.schema().as_arrow())

    def begin(self) -> None:
        if self._is_in_transaction:
            logger.debug("begin() called on an already active transaction. Ignoring.")
            return
        
        self._is_in_transaction = True
        logger.debug("Iceberg transaction started.")

    def commit(self) -> None:
        """Applies all staged operations. Performs a compensating rollback if an error occurs mid-transaction."""
        logger.debug(f"Committing {len(self._staged_operations)} staged operations to Iceberg.")
        initial_snapshots = {}
        try:
            for op in self._staged_operations:
                if op.table not in initial_snapshots:
                    iceberg_tbl = self._catalog.load_table(op.table)
                    current_snap = iceberg_tbl.current_snapshot()
                    initial_snapshots[op.table] = current_snap.snapshot_id if current_snap else None
        except Exception as e:
            logger.error(f"Pre-flight check failed: Could not prepare transaction.", exc_info=e)
            self.rollback()
            raise
            
        try:
            for op in self._staged_operations:
                iceberg_tbl = self._catalog.load_table(op.table)
                if op.action == DMLOperation.APPEND:
                    iceberg_tbl.append(op.data)
                elif op.action == DMLOperation.OVERWRITE:
                    iceberg_tbl.overwrite(op.data)
                elif op.action == DMLOperation.TRUNCATE:
                    iceberg_tbl.overwrite(self._get_empty_arrow_table(iceberg_tbl))
            
            logger.debug("Iceberg transaction committed successfully.")
        except Exception as e:
            logger.error(f"Commit failed mid-execution. Initiating rollback of applied operations.", exc_info=e)
            self._perform_compensating_rollback(initial_snapshots)
            raise
        finally:
            self._staged_operations.clear()
            self._is_in_transaction = False

    def _perform_compensating_rollback(self, initial_snapshots: t.Dict[str, t.Optional[int]]) -> None:
        """Helper method to revert tables to their pre-commit state if a transaction fails."""
        for table_name, initial_snapshot_id in initial_snapshots.items():
            try:
                iceberg_tbl = self._catalog.load_table(table_name)
                current_snap = iceberg_tbl.current_snapshot()
                current_snapshot_id = current_snap.snapshot_id if current_snap else None
                
                if current_snapshot_id != initial_snapshot_id:
                    if initial_snapshot_id is None:
                        # Edge case: Table was empty/new and had no snapshots before this transaction.
                        logger.warning(f"Cannot cleanly rollback {table_name}: it had no initial snapshot. Truncating instead.")
                        iceberg_tbl.overwrite(self._get_empty_arrow_table(iceberg_tbl))
                    else:
                        with iceberg_tbl.manage_snapshots() as ms:
                            ms.set_current_snapshot(initial_snapshot_id)
                        logger.info(f"Successfully rolled back {table_name} to snapshot {initial_snapshot_id}")
            except Exception as rollback_err:
                logger.critical(f"FATAL: Failed to rollback {table_name} during compensating transaction: {rollback_err}")

    def rollback(self) -> None:
        if self._staged_operations:
            logger.debug(f"Rolling back! Discarding {len(self._staged_operations)} staged operations.")
            self._staged_operations.clear()
            
        self._is_in_transaction = False
        logger.debug("Iceberg transaction rolled back.")

    def close(self) -> None:
        if self._is_in_transaction:
            logger.warning("Cursor closed with an active transaction. Attempting to roll back.")
            self.rollback()

        self._duckdb_conn.close()

    def _validate_table_and_schema(self, table_name: str, pa_df: t.Optional[pa.Table] = None) -> None:
        """Fast-fails if the table doesn't exist or if the DataFrame schema is fundamentally incompatible."""
        iceberg_tbl = self._catalog.load_table(table_name)

        if pa_df is not None:
            iceberg_schema = iceberg_tbl.schema()
            
            iceberg_fields = {f.name for f in iceberg_schema.fields}
            required_fields = {f.name for f in iceberg_schema.fields if f.required}
            df_fields = set(pa_df.column_names)

            missing_required = required_fields - df_fields
            if missing_required:
                raise ValueError(
                    f"Fail Fast: Cannot INSERT into '{table_name}'. "
                    f"DataFrame is missing required columns: {missing_required}"
                )

            extra_fields = df_fields - iceberg_fields
            if extra_fields:
                raise ValueError(
                    f"Fail Fast: Cannot INSERT into '{table_name}'. "
                    f"DataFrame contains extra columns not present in the table schema: {extra_fields}"
                )
            
    def _align_arrow_table_to_schema(self, pa_df: pa.Table, iceberg_tbl: PyIcebergTable) -> pa.Table:
        """
        Aligns a PyArrow Table to match the target Iceberg table's schema.
        """
        arrow_schema = iceberg_tbl.schema().as_arrow()
        
        target_column_names = [field.name for field in arrow_schema]
        df_columns = set(pa_df.column_names)
        aligned_pa_df = pa_df
        for field in arrow_schema:
            # If an optional column is missing from the dataframe, append it as nulls
            if field.name not in df_columns:
                null_array = pa.nulls(aligned_pa_df.num_rows, type=field.type)
                aligned_pa_df = aligned_pa_df.append_column(field.name, null_array)
        
        # Reorder columns by name to match the target schema and strictly cast types
        return aligned_pa_df.select(target_column_names).cast(arrow_schema)

    def execute(self, query: str, df: t.Optional[DataFrame] = None, **kwargs) -> None:
        parsed_exp = parse_one(query, dialect=self.dialect)
        if isinstance(parsed_exp, exp.Create):
            schema_exp = t.cast("exp.Schema", parsed_exp.this)
            target_table = t.cast("exp.Expression", schema_exp.this).find(exp.Table).sql(dialect=self.dialect)

            fields = []
            col_def: exp.ColumnDef
            for i, col_def in enumerate(schema_exp.expressions, start=1):
                not_null_constr = False
                comment_col_constr = None
                default_col_constr = None
                for constr in col_def.constraints:
                    if isinstance(constr.kind, exp.NotNullColumnConstraint) and not constr.kind.args.get("allow_null"):
                        not_null_constr = True
                    elif isinstance(constr.kind, exp.CommentColumnConstraint):
                        comment_col_constr = t.cast("exp.Literal", constr.kind.this).to_py()
                    elif isinstance(constr.kind, exp.DefaultColumnConstraint):
                        default_col_constr = t.cast("exp.Literal", constr.kind.this).to_py()

                fields.append(
                    NestedField(
                        field_id=i,
                        name=col_def.name,
                        field_type=col_def.kind.sql(dialect="iceberg").lower(),
                        required=not_null_constr,
                        doc=comment_col_constr,
                        initial_default=default_col_constr,
                    )
                )
            schema = Schema(*fields)

            tblproperties = {}
            tbl_location = None
            partition_fields = []
            partition_field_id = PARTITION_FIELD_ID_START
            properties_exp = t.cast(exp.Properties, parsed_exp.args.get("properties"))
            if properties_exp:
                for property_exp in properties_exp.expressions:
                    if isinstance(property_exp, exp.FileFormatProperty):
                        pass
                    elif isinstance(property_exp, exp.LocationProperty):
                        tbl_location = t.cast("exp.Literal", property_exp.this).to_py()
                    elif isinstance(property_exp, exp.PartitionedByProperty):
                        for part_exp in t.cast("exp.Schema", property_exp.this).expressions:
                            if isinstance(part_exp, exp.Identifier):
                                col_name = part_exp.name
                                transform = IdentityTransform()
                                partition_field_name = part_exp.name
                            elif isinstance(part_exp, exp.Anonymous):
                                transform_name = part_exp.name.lower()
                                if transform_name in ("hour", "date_hour", "hours"):
                                    transform = HourTransform()
                                elif transform_name == "years":
                                    transform = YearTransform()
                                elif transform_name == "months":
                                    transform = MonthTransform()
                                elif transform_name == "days":
                                    transform = DayTransform()
                                else:
                                    raise NotImplementedError(f"Unsupported partition transform: {transform_name}")
                                
                                col_name = t.cast("exp.Column", part_exp.expressions[0]).name
                                partition_field_name = f"{col_name}_{transform_name}"
                            else:
                                col_name = t.cast("exp.Column", part_exp.this).name
                                partition_field_name = f"{col_name}_{type(part_exp).__name__.lower()}"

                                # Reference: https://iceberg.apache.org/docs/latest/spark-ddl/#partitioned-by
                                if isinstance(part_exp, exp.Year):
                                    transform = YearTransform()
                                elif isinstance(part_exp, exp.Month):
                                    transform = MonthTransform()
                                elif isinstance(part_exp, (exp.Day, exp.Date)):
                                    transform = DayTransform()
                                elif isinstance(part_exp, exp.PartitionedByBucket):
                                    num_buckets = int(t.cast("exp.Literal", part_exp.expression).to_py())
                                    transform=BucketTransform(num_buckets)
                                    partition_field_name = f"{col_name}_bucket_{num_buckets}"
                                elif isinstance(part_exp, exp.PartitionByTruncate):
                                    width = int(t.cast("exp.Literal", part_exp.expression).to_py())
                                    transform=TruncateTransform(width)
                                    partition_field_name = f"{col_name}_truncate_{width}"
                                else:
                                    raise NotImplementedError(f"Unsupported partition transform: {type(part_exp).__name__}")

                            partition_fields.append(PartitionField(
                                source_id=schema.find_field(col_name).field_id,
                                field_id=partition_field_id,
                                transform=transform,
                                name=partition_field_name,
                            ))
                            partition_field_id += 1
                    elif "value" in property_exp.args:
                        key = t.cast("exp.Literal", property_exp.this).name
                        value = t.cast("exp.Literal", property_exp.args["value"]).name
                        tblproperties[key] = value
                    else:
                        raise NotImplementedError(f"Unsupported table property type: {type(property_exp).__name__}")
            partition_spec = PartitionSpec(*partition_fields) if partition_fields else UNPARTITIONED_PARTITION_SPEC

            if parsed_exp.args.get("exists"):
                self._catalog.create_table_if_not_exists(
                    target_table, schema,
                    location=tbl_location,
                    partition_spec=partition_spec,
                    properties=tblproperties,
                )
            else:
                self._catalog.create_table(
                    target_table, schema,
                    location=tbl_location,
                    partition_spec=partition_spec,
                    properties=tblproperties,
                )
        elif isinstance(parsed_exp, exp.Drop):
            target_table = t.cast("exp.Expression", parsed_exp.this).find(exp.Table).sql(dialect=self.dialect)
            if parsed_exp.args.get("purge"):
                self._catalog.purge_table(target_table)
            else:
                self._catalog.drop_table(target_table)
        elif isinstance(parsed_exp, exp.Select):
            self._execute_select(parsed_exp)
        elif isinstance(parsed_exp, exp.Insert):
            if df is None:
                raise ValueError("DataFrame must be provided for INSERT statements")
            
            target_table = t.cast("exp.Expression", parsed_exp.this).find(exp.Table).sql(dialect=self.dialect)
            pa_df = pa.Table.from_pandas(df)
            self._validate_table_and_schema(target_table, pa_df)
            iceberg_tbl = self._catalog.load_table(target_table)
            aligned_pa_df = self._align_arrow_table_to_schema(pa_df, iceberg_tbl)
            action = DMLOperation.OVERWRITE if parsed_exp.args.get("overwrite") else DMLOperation.APPEND
            self._staged_operations.append(StagedOperation(action=action, table=target_table, data=aligned_pa_df))
            if not self._is_in_transaction:
                logger.debug("Auto-committing INSERT operation.")
                self.commit()
        elif isinstance(parsed_exp, exp.TruncateTable):
            target_table = parsed_exp.find(exp.Table).sql(dialect=self.dialect)
            self._validate_table_and_schema(target_table)
            self._staged_operations.append(StagedOperation(action=DMLOperation.TRUNCATE, table=target_table))
            if not self._is_in_transaction:
                logger.debug("Auto-committing TRUNCATE operation.")
                self.commit()
        else:
            raise ValueError(f"Unsupported SQL statement of type: '{type(parsed_exp).__name__.upper()}'")
    
    def _execute_select(self, select_exp: exp.Select) -> None:
        tbl_dict = {}
        full_scan_table_dict = {}
        extractor = TablePushdownExtractor(select_exp)
        tbl_pushdowns = extractor.extract()
        for tbl_pushdown in tbl_pushdowns:
            table_exp: exp.Table = tbl_pushdown[extractor.TABLE_NODE]
            origin_table = table_exp.sql(dialect=self.dialect)
            condition = tbl_pushdown[extractor.CONDITION]

            # Full scan each table at most once
            if origin_table in full_scan_table_dict and not condition:
                temp_table = full_scan_table_dict[origin_table]
            else:
                temp_table = f"temp_{uuid.uuid4().hex}"
            tbl_dict[id(table_exp)] = (origin_table, temp_table, condition)
            if not condition:
                full_scan_table_dict[origin_table] = temp_table

            table_exp.args["catalog"] = None
            table_exp.args["db"] = None
            table_exp.args["this"] = exp.Identifier(this=temp_table, quoted=False)

        for column_exp in select_exp.find_all(exp.Column):
            if column_exp.table:
                table_exp = extractor.get_table_for_column(column_exp)
                if table_exp:
                    column_exp.args["catalog"] = None
                    column_exp.args["db"] = None
                    column_exp.args["table"] = tbl_dict[id(table_exp)][1]

        # Unregister tables from previous fetch
        for tbl in self._registered_tables:
            self._duckdb_conn.unregister(tbl)
        self._registered_tables = set()

        ops_by_table: t.Dict[str, t.List[StagedOperation]] = defaultdict(list)
        for op in self._staged_operations:
            ops_by_table[op.table].append(op)

        for origin_table, temp_table, condition in tbl_dict.values():
            if temp_table not in self._registered_tables:
                iceberg_tbl = self._catalog.load_table(origin_table)
                arrow_schema = iceberg_tbl.schema().as_arrow()
                
                is_overwritten = False
                uncommitted_pa_tables = []
                for op in ops_by_table.get(origin_table, []):
                    if op.action in (DMLOperation.OVERWRITE, DMLOperation.TRUNCATE):
                        is_overwritten = True
                        uncommitted_pa_tables.clear()
                    if op.action in (DMLOperation.OVERWRITE, DMLOperation.APPEND) and op.data:
                        uncommitted_pa_tables.append(op.data)

                base_reader = None
                if not is_overwritten:
                    if condition:
                        data_scan = iceberg_tbl.scan(row_filter=condition)
                    else:
                        data_scan = iceberg_tbl.scan()
                    base_reader = data_scan.to_arrow_batch_reader()

                if not uncommitted_pa_tables and base_reader:
                    self._duckdb_conn.register(temp_table, base_reader)
                else:
                    # Combine the Iceberg base stream and uncommitted in-memory batches
                    def combined_batches():
                        if base_reader:
                            for batch in base_reader:
                                yield batch
                        for tbl in uncommitted_pa_tables:
                            # max_chunksize matches typical Arrow page sizes to prevent huge unified allocations
                            for batch in tbl.to_batches(max_chunksize=8192):
                                yield batch.cast(arrow_schema)

                    combined_reader = pa.RecordBatchReader.from_batches(arrow_schema, combined_batches())
                    self._duckdb_conn.register(temp_table, combined_reader)

                self._registered_tables.add(temp_table)

        self._last_df = self._duckdb_conn.execute(select_exp.sql(self.dialect))
        
    def fetchone(self) -> t.Optional[t.Tuple]:
        return self._last_df.fetchone() if self._last_df else None

    def fetchall(self) -> t.List[t.Tuple]:
        return self._last_df.fetchall() if self._last_df else []
    
    def fetchdf(self) -> t.Optional[DataFrame]:
        return self._last_df.df() if self._last_df else None
    
class IcebergConnection:
    def __init__(self, catalog_type: str, catalog_properties: t.Optional[t.Dict[str, t.Any]] = None):
        self.catalog_type = catalog_type
        self.catalog_props = catalog_properties

        self._catalog: t.Optional[Catalog] = None
        self._catalog_lock = Lock()

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            with self._catalog_lock:
                if self._catalog is None:
                    self._catalog = load_catalog("default", type=self.catalog_type, **self.catalog_props)
        return self._catalog
    
    def get_catalog(self) -> Catalog:
        return self.catalog

    def cursor(self) -> IcebergCursor:
        return IcebergCursor(catalog=self.catalog)

    def close(self) -> None:
        pass

def connect(catalog_type: str,
            warehouse: t.Optional[str] = None, uri: t.Optional[str] = None,
            catalog_properties: t.Optional[t.Dict[str, t.Any]] = None) -> IcebergConnection:
    props = (catalog_properties or {}).copy()

    # Explicit arguments take precedence if they are not None
    if warehouse is not None:
        props[WAREHOUSE_LOCATION] = warehouse
    if uri is not None:
        props[URI] = uri

    filtered_props = {k: v for k, v in props.items() if v is not None}
    return IcebergConnection(catalog_type, filtered_props)