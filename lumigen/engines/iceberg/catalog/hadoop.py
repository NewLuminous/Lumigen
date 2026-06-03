from __future__ import annotations
import typing as t
from typing_extensions import override
import re
import s3fs
import pyarrow as pa
from pyiceberg.catalog import Catalog, PropertiesUpdateSummary, WAREHOUSE_LOCATION
from pyiceberg.io import S3_ANONYMOUS, S3_ENDPOINT, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_PROXY_URI
from pyiceberg.table import Table, StaticTable, CommitTableResponse, CreateTableTransaction
from pyiceberg.table.update import update_table_metadata, TableRequirement, TableUpdate
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT
from pyiceberg.exceptions import (
    NoSuchTableError,
    TableAlreadyExistsError,
    NoSuchNamespaceError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchViewError,
)
from pyiceberg.io import load_file_io

if t.TYPE_CHECKING:
    from pyiceberg.schema import Schema


class HadoopCatalog(Catalog):
    """
    A PyIceberg Catalog that behaves like PySpark's HadoopCatalog.
    Uses .ls() and .touch() to prevent SeaweedFS network timeout issues.
    """
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        
        self.warehouse_location = properties.get(WAREHOUSE_LOCATION, "").rstrip("/")
        if not self.warehouse_location:
            raise ValueError("Warehouse location must be provided in properties.")
            
        self.file_io = load_file_io(properties)
        
        client_kwargs = {}
        if S3_ENDPOINT in properties:
            client_kwargs["endpoint_url"] = properties.get(S3_ENDPOINT)
        
        if S3_PROXY_URI in properties:
            client_kwargs["config_kwargs"] = {"proxies": {"http": properties[S3_PROXY_URI], "https": properties[S3_PROXY_URI]}}
            
        key = properties.get(S3_ACCESS_KEY_ID)
        secret = properties.get(S3_SECRET_ACCESS_KEY)
        
        if properties.get(S3_ANONYMOUS):
            self.fs = s3fs.S3FileSystem(anon=True, client_kwargs=client_kwargs)
        else:
            self.fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs=client_kwargs)

    def _strip_scheme(self, path: str) -> str:
        return re.sub(r'^s3[a-z]?://', '', path)

    def _get_table_dir(self, identifier: t.Tuple[str, ...]) -> str:
        return f"{self.warehouse_location}/{'/'.join(identifier)}"

    def _get_latest_metadata_location(self, metadata_dir: str) -> t.Tuple[str, int]:
        dir_stripped = self._strip_scheme(metadata_dir)
        scheme_match = re.match(r'^(s3[a-z]?://)', metadata_dir)
        scheme = scheme_match.group(1) if scheme_match else "s3a://"
        
        try:
            # Completely avoids .exists() calls to prevent timeouts.
            # Executes a single API request and gets all contents.
            files: t.List[str] = self.fs.ls(dir_stripped, detail=False)
            files = [f.rstrip('/') for f in files]
        except Exception:
            raise NoSuchTableError(f"Directory not found: {metadata_dir}")

        hint_path = f"{dir_stripped}/version-hint.text"
        
        if hint_path in files:
            try:
                with self.fs.open(hint_path, 'r') as f:
                    version = int(f.read().strip())
                expected_file = f"{dir_stripped}/v{version}.metadata.json"
                if expected_file in files:
                    return f"{scheme}{expected_file}", version
            except Exception:
                pass

        max_version, latest_file = -1, None
        pattern = re.compile(r'v(\d+)\.metadata\.json$')

        for path in files:
            match = pattern.search(path.split('/')[-1])
            if match:
                version = int(match.group(1))
                if version > max_version:
                    max_version = version
                    latest_file = f"{scheme}{path}"

        if latest_file is None:
            raise NoSuchTableError(f"No metadata found in {metadata_dir}")
        return latest_file, max_version
    
    @override
    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        ident = self.identifier_to_tuple(identifier)
        if self.table_exists(ident):
            raise TableAlreadyExistsError(f"Table {ident} already exists")
            
        schema = self._convert_schema_if_needed(schema)
        table_dir = location or self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        
        from pyiceberg.table.metadata import new_table_metadata
        metadata = new_table_metadata(schema=schema, partition_spec=partition_spec, sort_order=sort_order, location=table_dir, properties=properties)
        metadata_location = f"{metadata_dir}/v1.metadata.json"
        
        out_file = self.file_io.new_output(metadata_location)
        with out_file.create() as f:
            try: f.write(metadata.model_dump_json().encode("utf-8"))
            except AttributeError: f.write(metadata.json().encode("utf-8"))
                
        with self.fs.open(f"{self._strip_scheme(metadata_dir)}/version-hint.text", "w") as f:
            f.write("1")

        return Table(identifier=ident, metadata=metadata, metadata_location=metadata_location, io=self.file_io, catalog=self)

    @override
    def create_table_transaction(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> CreateTableTransaction:
        return CreateTableTransaction(self, identifier, schema, location, partition_spec, sort_order, properties)

    @override
    def load_table(self, identifier: str | Identifier) -> Table:
        ident = self.identifier_to_tuple(identifier)
        table_dir = self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        latest_file, _ = self._get_latest_metadata_location(metadata_dir)
        
        static_table = StaticTable.from_metadata(latest_file, self.properties)
        return Table(identifier=ident, metadata=static_table.metadata, metadata_location=latest_file, io=self.file_io, catalog=self)

    @override
    def table_exists(self, identifier: str | Identifier) -> bool:
        try:
            self.load_table(identifier)
            return True
        except NoSuchTableError:
            return False

    @override
    def view_exists(self, identifier: str | Identifier) -> bool:
        # Hadoop Catalog historically doesn't support views natively
        return False

    @override
    def namespace_exists(self, namespace: str | Identifier) -> bool:
        namespace_tuple = self.identifier_to_tuple(namespace)
        if not namespace_tuple:
            return True
        path = f"{self.warehouse_location}/{'/'.join(namespace_tuple)}"
        stripped_path = self._strip_scheme(path)
        
        try:
            # Prevents SeaweedFS from hanging on `.exists()`
            # We list the parent directory once and check if our target is in it.
            parent_path = stripped_path.rsplit('/', 1)[0]
            entries: t.List[str] = self.fs.ls(parent_path, detail=False)
            normalized_entries = [e.rstrip('/') for e in entries]
            return stripped_path in normalized_entries
        except Exception:
            return False

    @override
    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        ident = self.identifier_to_tuple(identifier)
        if self.table_exists(ident):
            raise TableAlreadyExistsError(f"Table {ident} already exists")

        static_table = StaticTable.from_metadata(metadata_location, self.properties)
        table_dir = self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        new_metadata_location = f"{metadata_dir}/v1.metadata.json"

        # Copy remote metadata into the catalog's structure
        self.fs.copy(self._strip_scheme(metadata_location), self._strip_scheme(new_metadata_location))
        with self.fs.open(f"{self._strip_scheme(metadata_dir)}/version-hint.text", "w") as f:
            f.write("1")

        return self.load_table(ident)

    @override
    def drop_table(self, identifier: str | Identifier) -> None:
        if not self.table_exists(identifier):
            raise NoSuchTableError(f"Table {identifier} does not exist")
        ident = self.identifier_to_tuple(identifier)
        table_dir = self._get_table_dir(ident)
        self.fs.rm(self._strip_scheme(table_dir), recursive=True)

    @override
    def purge_table(self, identifier: str | Identifier) -> None:
        # For Hadoop, purging is synonymous with recursively dropping the table directory
        self.drop_table(identifier)

    @override
    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        if not self.table_exists(from_identifier):
            raise NoSuchTableError(f"Table {from_identifier} does not exist")
        if self.table_exists(to_identifier):
            raise TableAlreadyExistsError(f"Table {to_identifier} already exists")

        from_ident = self.identifier_to_tuple(from_identifier)
        to_ident = self.identifier_to_tuple(to_identifier)

        target_namespace = self.namespace_from(to_ident)
        if target_namespace and not self.namespace_exists(target_namespace):
            raise NoSuchNamespaceError(f"Namespace {target_namespace} does not exist")

        from_dir = self._get_table_dir(from_ident)
        to_dir = self._get_table_dir(to_ident)
        
        # S3fs performs a deep copy+delete for mv
        self.fs.mv(self._strip_scheme(from_dir), self._strip_scheme(to_dir), recursive=True)
        return self.load_table(to_ident)

    @override
    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        ident = table.name()
        table_dir = self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        latest_file, current_version = self._get_latest_metadata_location(metadata_dir)
        
        current_metadata = table.metadata
        new_metadata = update_table_metadata(current_metadata, updates)
        
        new_version = current_version + 1
        new_location = f"{metadata_dir}/v{new_version}.metadata.json"
        
        out_file = self.file_io.new_output(new_location)
        with out_file.create() as f:
            try: f.write(new_metadata.model_dump_json().encode("utf-8"))
            except AttributeError: f.write(new_metadata.json().encode("utf-8"))
            
        with self.fs.open(f"{self._strip_scheme(metadata_dir)}/version-hint.text", "w") as f:
            f.write(str(new_version))
            
        return CommitTableResponse(metadata=new_metadata, metadata_location=new_location)

    @override
    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        if self.namespace_exists(namespace):
            raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")

        namespace_tuple = self.identifier_to_tuple(namespace)
        path = f"{self.warehouse_location}/{'/'.join(namespace_tuple)}"
        
        # Bypassing fsspec's internal `exists` checks during mkdir by writing a dummy file
        self.fs.touch(f"{self._strip_scheme(path)}/")

    @override
    def drop_namespace(self, namespace: str | Identifier) -> None:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")

        namespace_tuple = self.identifier_to_tuple(namespace)
        path = f"{self.warehouse_location}/{'/'.join(namespace_tuple)}"
        stripped_path = self._strip_scheme(path)
        
        contents: t.List[str] = self.fs.ls(stripped_path, detail=False)
        # s3fs sometimes includes the parent directory itself in the contents list. We filter it out.
        contents = [c for c in contents if c.rstrip('/') != stripped_path.rstrip('/')]
        
        if contents and len(contents) > 0:
            raise NamespaceNotEmptyError(f"Namespace {namespace} is not empty")

        # Avoiding .rmdir() directly to bypass exists checks
        self.fs.rm(f"{stripped_path}/")

    @override
    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")

        namespace_tuple = self.identifier_to_tuple(namespace)
        path = f"{self.warehouse_location}/{'/'.join(namespace_tuple)}"
        
        tables = []
        try:
            entry: str
            for entry in self.fs.ls(self._strip_scheme(path), detail=False):
                # Fetch sub-entries with ls() to warm the s3fs dircache.
                sub_entries: t.List[str] = self.fs.ls(entry, detail=False)
                is_table = any(p.rstrip('/').endswith('/metadata') for p in sub_entries)
                if is_table:
                    table_name = entry.rstrip('/').split("/")[-1]
                    tables.append(namespace_tuple + (table_name,))
        except Exception:
            pass
        return tables

    @override
    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        namespace_tuple = self.identifier_to_tuple(namespace) if namespace else ()
        if namespace_tuple and not self.namespace_exists(namespace_tuple):
            raise NoSuchNamespaceError(f"Namespace {namespace_tuple} does not exist")

        path = f"{self.warehouse_location}/{'/'.join(namespace_tuple)}" if namespace_tuple else self.warehouse_location
        path = self._strip_scheme(path)
        
        namespaces = []
        try:
            entries: t.List[t.Dict] = self.fs.ls(path, detail=True)
            for entry in entries:
                if entry.get("type") == "directory":
                    dir_name = entry["name"].rstrip("/").split("/")[-1]
                    
                    # List the directory contents to warm the s3fs local cache.
                    # This avoids cumulative N+1 HTTP HEAD/LIST requests during subsequent exists() checks.
                    sub_entries: t.List[str] = self.fs.ls(entry["name"], detail=False)
                    is_table = any(p.rstrip('/').endswith('/metadata') for p in sub_entries)
                    
                    if not is_table:
                        namespaces.append(namespace_tuple + (dir_name,))
        except Exception:
            pass
        return namespaces

    @override
    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")
        return []

    @override
    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")
        return {}

    @override
    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")
        # FileSystem-based Hadoop Catalogs generally do not support namespace properties natively
        raise NotImplementedError("HadoopCatalog does not natively support namespace properties.")

    @override
    def drop_view(self, identifier: str | Identifier) -> None:
        raise NoSuchViewError(f"View {identifier} does not exist")