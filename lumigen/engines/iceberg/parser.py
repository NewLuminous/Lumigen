from __future__ import annotations
import typing as t
import re
import itertools
from dataclasses import dataclass
from sqlglot import exp, parse_one

from pyiceberg.schema import Schema
from pyiceberg.types import IcebergType, NestedField, ListType, MapType, StructType
from pyiceberg.partitioning import (
    PartitionSpec, 
    PartitionField, 
    PARTITION_FIELD_ID_START, 
    UNPARTITIONED_PARTITION_SPEC
)
from pyiceberg.transforms import (
    IdentityTransform, 
    YearTransform, 
    MonthTransform, 
    DayTransform, 
    HourTransform, 
    BucketTransform, 
    TruncateTransform
)

if t.TYPE_CHECKING:
    from sqlglot import DialectType

from engines.iceberg.dialect import Iceberg
from utils.sql import extract_main_table


@dataclass
class CreateDatabaseMetadata:
    namespace: str
    properties: t.Dict[str, str]
    if_not_exists: bool

@dataclass
class CreateTableMetadata:
    table_name: str
    schema: Schema
    location: t.Optional[str]
    partition_spec: PartitionSpec
    properties: t.Dict[str, str]
    if_not_exists: bool

@dataclass
class ColumnDefMetadata:
    name: str
    field_type: t.Union[IcebergType, str]
    required: bool
    doc: str
    initial_default: t.Any


class IcebergSQLParser:
    """Utility class to extract Iceberg metadata and routing info from sqlglot ASTs."""

    @staticmethod
    def extract_table_name(parsed_exp: exp.Expression, dialect: DialectType, lowercase: bool = True) -> str:
        """
        Extracts the fully qualified table name from a DML or DDL statement and normalizes it.
        Normalization is required by some case-insensitive catalogs.
        """
        unquoted_name = extract_main_table(parsed_exp, dialect=dialect, quoted=False).lower()
        return unquoted_name.lower() if lowercase else unquoted_name
    
    @staticmethod
    def parse_create_database(query: str, dialect: DialectType) -> CreateDatabaseMetadata:
        properties = {}
        
        if "WITH DBPROPERTIES" in query.upper():
            # Extract database properties
            dbprops_match = re.search(r'(?i)WITH\s+DBPROPERTIES\s*\((.*?)\)', query, flags=re.DOTALL)
            if dbprops_match:
                props_str = dbprops_match.group(1)
                # Match strict 'key'='value' or "key"="value" formats 
                kv_pairs = re.findall(r"""(['"])(.*?)\1\s*=\s*(['"])(.*?)\3""", props_str)
                for pair in kv_pairs:
                    properties[pair[1]] = pair[3]
            
            # Remove WITH DBPROPERTIES from query to ensure sqlglot AST parsing doesn't crash
            query = re.sub(r'(?i)WITH\s+DBPROPERTIES\s*\(.*?\)', '', query, flags=re.DOTALL)

        create_exp = parse_one(query, dialect=dialect)
        
        if isinstance(create_exp, exp.Command) and str(create_exp.expression).upper().startswith("DATABASE"):
            parts = str(create_exp.expression).split()
            namespace = parts[1]
            if_not_exists = "IF NOT EXISTS" in query.upper()
        elif isinstance(create_exp, exp.Create) and str(create_exp.args.get("kind", "")).upper() in ("DATABASE", "SCHEMA"):
            namespace = IcebergSQLParser.extract_table_name(create_exp.this, dialect, lowercase=False)
            if_not_exists = bool(create_exp.args.get("exists"))
            
            properties_exp = t.cast(exp.Properties, create_exp.args.get("properties"))
            if properties_exp:
                for property_exp in properties_exp.expressions:
                    if isinstance(property_exp, exp.LocationProperty):
                        properties["location"] = t.cast("exp.Literal", property_exp.this).to_py()
                    elif "value" in property_exp.args:
                        key = t.cast("exp.Literal", property_exp.this).name
                        value = t.cast("exp.Literal", property_exp.args["value"]).name
                        properties[key] = value
        else:
            raise ValueError(f"Expected a CREATE DATABASE statement, got: {query}")

        return CreateDatabaseMetadata(
            namespace=namespace,
            properties=properties,
            if_not_exists=if_not_exists,
        )
    
    @staticmethod
    def convert_data_type(dtype: t.Union[str, exp.DataType], id_counter: t.Iterator[int] = None) -> IcebergType:
        if id_counter is None:
            id_counter = itertools.count(1)
        if isinstance(dtype, str):
            dtype = parse_one(dtype, into=exp.DataType, dialect=Iceberg)

        t_type: exp.DataType.Type = dtype.this
        t_type_str = t_type.value.upper() if hasattr(t_type, "value") else str(t_type).upper()
        
        if t_type_str == "ARRAY":
            type_args = dtype.expressions or []
            return ListType(
                element_id=next(id_counter),
                element=IcebergSQLParser.convert_data_type(type_args[0], id_counter) if len(type_args) > 0 else None,
                element_required=False,
            )
        elif t_type_str == "MAP":
            type_args = dtype.expressions or []
            return MapType(
                key_id=next(id_counter),
                key_type=IcebergSQLParser.convert_data_type(type_args[0], id_counter) if len(type_args) > 0 else None,
                value_id=next(id_counter),
                value_type=IcebergSQLParser.convert_data_type(type_args[1], id_counter) if len(type_args) > 1 else None,
                value_required=False,
            )
        elif t_type_str == "STRUCT":
            fields = []
            col_def: exp.ColumnDef
            for col_def in dtype.expressions:
                fields.append(NestedField(next(id_counter), col_def.name, IcebergSQLParser.convert_data_type(col_def.kind, id_counter)))
            return StructType(*fields)

        return NestedField.convert_field_type(dtype.sql(dialect=Iceberg))
    
    @staticmethod
    def parse_column_def(col_def: exp.ColumnDef, id_counter: t.Optional[t.Iterator[int]] = None) -> ColumnDefMetadata:
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
        
        return ColumnDefMetadata(
            name=col_def.name,
            field_type=IcebergSQLParser.convert_data_type(col_def.kind, id_counter),
            required=not_null_constr,
            doc=comment_col_constr,
            initial_default=default_col_constr,
        )
    
    @staticmethod
    def parse_schema(schema_exp: exp.Schema) -> t.List[NestedField]:
        fields = []
        id_counter = itertools.count(1)
        col_def: exp.ColumnDef
        for col_def in schema_exp.expressions:
            field_id = next(id_counter)
            field = IcebergSQLParser.parse_column_def(col_def, id_counter)
            fields.append(
                NestedField(
                    field_id=field_id,
                    name=field.name,
                    field_type=field.field_type,
                    required=field.required,
                    doc=field.doc,
                    initial_default=field.initial_default,
                )
            )
        return fields

    @staticmethod
    def parse_create_table(create_exp: exp.Create, dialect: DialectType, inferred_schema: t.Optional[Schema] = None) -> CreateTableMetadata:
        """Converts a sqlglot CREATE TABLE AST into PyIceberg primitives."""
        target_table = IcebergSQLParser.extract_table_name(create_exp, dialect, lowercase=False)

        if isinstance(create_exp.this, exp.Schema):
            fields = IcebergSQLParser.parse_schema(create_exp.this)
            schema = Schema(*fields)
        elif isinstance(create_exp.this, exp.Table):
            if inferred_schema is None:
                raise ValueError("CREATE TABLE AS SELECT statements require an inferred schema.")
            schema = inferred_schema
        else:
            raise ValueError("Unsupported CREATE TABLE target.")

        tblproperties = {}
        tbl_location = None
        partition_fields = []
        partition_field_id = PARTITION_FIELD_ID_START
        properties_exp = t.cast(exp.Properties, create_exp.args.get("properties"))
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
                                transform = BucketTransform(num_buckets)
                                partition_field_name = f"{col_name}_bucket_{num_buckets}"
                            elif isinstance(part_exp, exp.PartitionByTruncate):
                                width = int(t.cast("exp.Literal", part_exp.expression).to_py())
                                transform = TruncateTransform(width)
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

        return CreateTableMetadata(
            table_name=target_table,
            schema=schema,
            partition_spec=partition_spec,
            location=tbl_location,
            properties=tblproperties,
            if_not_exists=bool(create_exp.args.get("exists"))
        )