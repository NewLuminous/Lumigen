from __future__ import annotations
import typing as t
from typing_extensions import override
import re
import uuid
import time
import requests
from requests.auth import AuthBase
import pyarrow as pa
import s3fs

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
    CommitFailedException
)
from pyiceberg.io import load_file_io

if t.TYPE_CHECKING:
    from pyiceberg.schema import Schema


class NessieAWSSigV4Auth(AuthBase):
    """
    Custom requests Auth plugin to sign Nessie API calls using AWS SigV4.
    Utilizes botocore (already installed via PyIceberg's S3 dependencies) to avoid extra deps.
    """
    def __init__(self, region: str):
        import botocore.session
        self.region = region
        self.service = "execute-api"  # Standard service name for API Gateway/Custom Endpoints
        self.credentials = botocore.session.get_session().get_credentials()

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        
        req = AWSRequest(method=r.method, url=r.url, data=r.body, headers=dict(r.headers))
        SigV4Auth(self.credentials, self.service, self.region).add_auth(req)
        r.headers.update(dict(req.headers))
        return r


class NessieOAuth2Auth(AuthBase):
    """
    Custom requests Auth plugin to handle rotating OAuth2 credentials (Client Credentials & Password grant).
    Strictly mimics Nessie's Java client configuration options.
    """
    def __init__(
        self,
        token_endpoint: str | None,
        issuer_url: str | None,
        client_id: str | None,
        client_secret: str | None,
        username: str | None = None,
        password: str | None = None,
        scope: str | None = None,
        grant_type: str | None = None,
    ):
        self.token_endpoint = token_endpoint
        self.issuer_url = issuer_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.scope = scope
        self.grant_type = grant_type or ("password" if username else "client_credentials")
        
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def _resolve_token_endpoint(self) -> str:
        if self.token_endpoint:
            return self.token_endpoint
        if self.issuer_url:
            # If standard OpenID issuer is provided, fetch metadata
            try:
                well_known_url = self.issuer_url.rstrip("/") + "/.well-known/openid-configuration"
                r = requests.get(well_known_url, timeout=5)
                r.raise_for_status()
                endpoint = r.json().get("token_endpoint")
                if endpoint:
                    return endpoint
            except Exception:
                pass
            # Fallback format for Keycloak / Standard IAM providers
            return self.issuer_url.rstrip("/") + "/protocol/openid-connect/token"
        raise ValueError("Either 'authentication.oauth2.token-endpoint' or 'authentication.oauth2.issuer-url' must be configured for OAUTH2")

    def _refresh_token(self) -> None:
        endpoint = self._resolve_token_endpoint()
        data = {"grant_type": self.grant_type}
        
        if self.client_id:
            data["client_id"] = self.client_id
        if self.client_secret:
            data["client_secret"] = self.client_secret
        if self.scope:
            data["scope"] = self.scope
            
        if self.grant_type == "password":
            if self.username:
                data["username"] = self.username
            if self.password:
                data["password"] = self.password

        # Authenticate token request using Basic headers or form parameters
        auth = None
        if self.client_id and self.client_secret and self.grant_type == "client_credentials":
            auth = (self.client_id, self.client_secret)
            
        r = requests.post(endpoint, data=data, auth=auth, timeout=10)
        r.raise_for_status()
        res = r.json()
        
        self._access_token = res["access_token"]
        expires_in = res.get("expires_in", 3600)
        # Pad expiration time by 10s to prevent race conditions during request flight
        self._expires_at = time.time() + float(expires_in) - 10.0

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        if not self._access_token or time.time() >= self._expires_at:
            self._refresh_token()
        r.headers["Authorization"] = f"Bearer {self._access_token}"
        return r


class NessieCatalog(Catalog):
    """
    A Custom Native PyIceberg Catalog that behaves like PySpark's NessieCatalog.
    It bypasses the Iceberg REST API and talks directly to Nessie's Core API (/api/v1).
    """
    def __init__(self, name: str, **properties: str):
        # Nessie API Configuration & v2 Downgrade Logic
        raw_uri = properties.pop("uri", "").rstrip("/")
        if not raw_uri:
            raise ValueError("Nessie URI is required (e.g., http://host:port/api/v1)")
            
        # Nessie v1 and v2 have different JSON payloads.
        # Since all Nessie servers support v1 concurrently, gracefully downgrade v2 URIs to v1.
        if raw_uri.endswith("/v2"):
            self.uri = raw_uri[:-3] + "/v1"
        elif raw_uri.endswith("/v1"):
            self.uri = raw_uri
        elif raw_uri.endswith("/api"):
            self.uri = f"{raw_uri}/v1"
        else:
            self.uri = f"{raw_uri}/api/v1"

        self.ref = properties.pop("ref", "main")
        
        # Setup HTTP Session & Authentication
        # Ref: https://projectnessie.org/nessie-latest/client_config/#nessie-client-configuration-options
        self.session = requests.Session()
        auth_type = properties.pop("authentication.type", "NONE").upper()
        if auth_type == "BASIC":
            username = properties.pop("authentication.username", "")
            password = properties.pop("authentication.password", "")
            if username and password:
                self.session.auth = (username, password)
        elif auth_type == "BEARER":
            token = properties.pop("authentication.token", "")
            if token:
                self.session.headers.update({"Authorization": f"Bearer {token}"})
        elif auth_type == "AWS":
            aws_region = properties.pop("authentication.aws.region", properties.get("s3.region", "us-east-1"))
            self.session.auth = NessieAWSSigV4Auth(aws_region)
        elif auth_type == "OAUTH2":
            token_endpoint = properties.pop("authentication.oauth2.token-endpoint", None)
            issuer_url = properties.pop("authentication.oauth2.issuer-url", None)
            client_id = properties.pop("authentication.oauth2.client-id", None)
            client_secret = properties.pop("authentication.oauth2.client-secret", None)
            username = properties.pop("authentication.oauth2.username", None)
            password = properties.pop("authentication.oauth2.password", None)
            scope = properties.pop("authentication.oauth2.scope", None)
            grant_type = properties.pop("authentication.oauth2.grant-type", None)
            
            self.session.auth = NessieOAuth2Auth(
                token_endpoint=token_endpoint,
                issuer_url=issuer_url,
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password,
                scope=scope,
                grant_type=grant_type,
            )

        # Remove unsupported PyIceberg properties
        properties.pop("catalog-impl", None)
        properties.pop("type", None)
        
        super().__init__(name, **properties)
        
        # Setup S3 I/O
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
        
        if properties.get(S3_ANONYMOUS) == "true":
            self.fs = s3fs.S3FileSystem(anon=True, client_kwargs=client_kwargs)
        else:
            self.fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs=client_kwargs)

    def _get_reference(self) -> str:
        """Fetches the current hash of the target branch."""
        resp = self.session.get(f"{self.uri}/trees/tree/{self.ref}")
        resp.raise_for_status()
        return resp.json()["hash"]

    def _get_content(self, elements: list[str]) -> dict | None:
        """Fetches a specific object (Table/Namespace) from Nessie."""
        key_str = ".".join(elements)
        resp = self.session.get(f"{self.uri}/contents/{key_str}?ref={self.ref}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def _commit_operation(self, message: str, operation_type: str, elements: list[str], content: dict = None):
        """Pushes an atomic change to Nessie."""
        current_hash = self._get_reference()
        
        operation = {
            "type": operation_type,
            "key": {"elements": elements}
        }
        if content:
            operation["content"] = content
            
        payload = {
            "commitMeta": {
                "message": message,
                "author": "PyIceberg Native Client"
            },
            "operations": [operation]
        }
        
        # Note: Nessie API v1 expects expectedHash as a query parameter
        resp = self.session.post(
            f"{self.uri}/trees/branch/{self.ref}/commit",
            params={"expectedHash": current_hash},
            json=payload
        )
        
        # Rigorously parse Nessie's HTTP 409 Conflicts to integrate with PyIceberg's IF NOT EXISTS logic
        if resp.status_code == 409:
            try:
                error_msg = resp.json().get("message", resp.text)
            except Exception:
                error_msg = resp.text
                
            error_lower = error_msg.lower()
            
            if "already exists" in error_lower and "namespace" not in error_lower:
                raise TableAlreadyExistsError(f"Nessie Conflict: {error_msg}")
            elif "namespace" in error_lower and ("does not exist" in error_lower or "must exist" in error_lower):
                raise NoSuchNamespaceError(f"Nessie Conflict: {error_msg}")
            elif "already exists" in error_lower and "namespace" in error_lower:
                raise NamespaceAlreadyExistsError(f"Nessie Conflict: {error_msg}")
            else:
                raise CommitFailedException(f"Nessie Commit failed due to conflict: {error_msg}")
                
        elif resp.status_code >= 400:
            raise Exception(f"Nessie commit failed (HTTP {resp.status_code}): {resp.text}")
        
        return resp.json()["hash"]

    def _get_table_dir(self, identifier: t.Tuple[str, ...]) -> str:
        return f"{self.warehouse_location}/{'/'.join(identifier)}"

    def _strip_scheme(self, path: str) -> str:
        return re.sub(r'^s3[a-z]?://', '', path)

    @override
    def load_table(self, identifier: str | Identifier) -> Table:
        ident = self.identifier_to_tuple(identifier)
        content = self._get_content(list(ident))
        
        if not content or content.get("type") != "ICEBERG_TABLE":
            raise NoSuchTableError(f"Table does not exist: {identifier}")
            
        metadata_location = content["metadataLocation"]
        static_table = StaticTable.from_metadata(metadata_location, self.properties)
        
        return Table(
            identifier=ident, 
            metadata=static_table.metadata, 
            metadata_location=metadata_location, 
            io=self.file_io, 
            catalog=self
        )

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
        if self._get_content(list(ident)):
            raise TableAlreadyExistsError(f"Table {ident} already exists")
            
        schema = self._convert_schema_if_needed(schema)
        table_dir = location or self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        
        from pyiceberg.table.metadata import new_table_metadata
        import uuid
        metadata = new_table_metadata(schema=schema, partition_spec=partition_spec, sort_order=sort_order, location=table_dir, properties=properties)
        
        metadata_location = f"{metadata_dir}/00000-{uuid.uuid4()}.metadata.json"
        out_file = self.file_io.new_output(metadata_location)
        with out_file.create() as f:
            try: f.write(metadata.model_dump_json().encode("utf-8"))
            except AttributeError: f.write(metadata.json().encode("utf-8"))
                
        # Nessie complains if you send an ID for a NEW object. Omit the ID here.
        content = {
            "type": "ICEBERG_TABLE",
            "metadataLocation": metadata_location,
            "snapshotId": metadata.current_snapshot_id if metadata.current_snapshot_id is not None else -1,
            "schemaId": metadata.current_schema_id,
            "specId": metadata.default_spec_id,
            "sortOrderId": metadata.default_sort_order_id
        }
        
        self._commit_operation(f"Create table {'.'.join(ident)}", "PUT", list(ident), content)
        return self.load_table(ident)

    @override
    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        ident = table.name()
        existing_content = self._get_content(list(ident))
        if not existing_content:
            raise NoSuchTableError(f"Table does not exist: {ident}")
            
        current_metadata = table.metadata
        new_metadata = update_table_metadata(current_metadata, updates)
        
        table_dir = self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        
        import uuid
        metadata_location = f"{metadata_dir}/00000-{uuid.uuid4()}.metadata.json"
        
        out_file = self.file_io.new_output(metadata_location)
        with out_file.create() as f:
            try: f.write(new_metadata.model_dump_json().encode("utf-8"))
            except AttributeError: f.write(new_metadata.json().encode("utf-8"))
            
        # For an UPDATE, Nessie REQUIRES the existing ID to be passed back.
        new_content = {
            "type": "ICEBERG_TABLE",
            "metadataLocation": metadata_location,
            "id": existing_content["id"], 
            "snapshotId": new_metadata.current_snapshot_id if new_metadata.current_snapshot_id is not None else -1,
            "schemaId": new_metadata.current_schema_id,
            "specId": new_metadata.default_spec_id,
            "sortOrderId": new_metadata.default_sort_order_id
        }
        
        self._commit_operation(f"Update table {'.'.join(ident)}", "PUT", list(ident), new_content)
        return CommitTableResponse(metadata=new_metadata, metadata_location=metadata_location)

    @override
    def drop_table(self, identifier: str | Identifier) -> None:
        ident = self.identifier_to_tuple(identifier)
        if not self._get_content(list(ident)):
            raise NoSuchTableError(f"Table {ident} does not exist")
            
        self._commit_operation(f"Drop table {'.'.join(ident)}", "DELETE", list(ident))

    @override
    def purge_table(self, identifier: str | Identifier) -> None:
        ident = self.identifier_to_tuple(identifier)
        content = self._get_content(list(ident))
        if not content:
            raise NoSuchTableError(f"Table {ident} does not exist")
            
        self._commit_operation(f"Drop table {'.'.join(ident)}", "DELETE", list(ident))
        
        # Purge files locally
        table_dir = self._get_table_dir(ident)
        try:
            self.fs.rm(self._strip_scheme(table_dir), recursive=True)
        except Exception:
            pass

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
    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        ident = self.identifier_to_tuple(identifier)
        if self._get_content(list(ident)):
            raise TableAlreadyExistsError(f"Table {ident} already exists")

        static_table = StaticTable.from_metadata(metadata_location, self.properties)
        metadata = static_table.metadata
        
        table_dir = self._get_table_dir(ident)
        metadata_dir = f"{table_dir}/metadata"
        
        import uuid
        new_metadata_location = f"{metadata_dir}/00000-{uuid.uuid4()}.metadata.json"

        self.fs.copy(self._strip_scheme(metadata_location), self._strip_scheme(new_metadata_location))

        # Nessie complains if you send an ID for a NEW object. Omit the ID here.
        content = {
            "type": "ICEBERG_TABLE",
            "metadataLocation": new_metadata_location,
            "snapshotId": metadata.current_snapshot_id if metadata.current_snapshot_id is not None else -1,
            "schemaId": metadata.current_schema_id,
            "specId": metadata.default_spec_id,
            "sortOrderId": metadata.default_sort_order_id
        }
        
        self._commit_operation(f"Register table {'.'.join(ident)}", "PUT", list(ident), content)
        return self.load_table(ident)

    @override
    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        from_ident = self.identifier_to_tuple(from_identifier)
        to_ident = self.identifier_to_tuple(to_identifier)
        
        content = self._get_content(list(from_ident))
        if not content:
            raise NoSuchTableError(f"Table {from_ident} does not exist")
        if self._get_content(list(to_ident)):
            raise TableAlreadyExistsError(f"Table {to_ident} already exists")

        # In Nessie, renaming is executed as a multi-action transaction: DELETE old, PUT new
        current_hash = self._get_reference()
        payload = {
            "commitMeta": {"message": f"Rename table {'.'.join(from_ident)} to {'.'.join(to_ident)}"},
            "operations": [
                {"type": "DELETE", "key": {"elements": list(from_ident)}},
                {"type": "PUT", "key": {"elements": list(to_ident)}, "content": content} # Reuse source content (with ID)
            ]
        }
        self.session.post(f"{self.uri}/trees/branch/{self.ref}/commit", params={"expectedHash": current_hash}, json=payload).raise_for_status()
        return self.load_table(to_ident)

    @override
    def table_exists(self, identifier: str | Identifier) -> bool:
        ident = self.identifier_to_tuple(identifier)
        content = self._get_content(list(ident))
        return content is not None and content.get("type") == "ICEBERG_TABLE"

    @override
    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        namespace_tuple = self.identifier_to_tuple(namespace)
        if self._get_content(list(namespace_tuple)):
            raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")
            
        # Omit ID for new namespace
        content = {"type": "NAMESPACE"}
        self._commit_operation(f"Create namespace {'.'.join(namespace_tuple)}", "PUT", list(namespace_tuple), content)

    @override
    def drop_namespace(self, namespace: str | Identifier) -> None:
        namespace_tuple = self.identifier_to_tuple(namespace)
        if not self._get_content(list(namespace_tuple)):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")
            
        tables = self.list_tables(namespace_tuple)
        if tables:
            raise NamespaceNotEmptyError(f"Namespace {namespace} is not empty")
            
        self._commit_operation(f"Drop namespace {'.'.join(namespace_tuple)}", "DELETE", list(namespace_tuple))

    @override
    def namespace_exists(self, namespace: str | Identifier) -> bool:
        namespace_tuple = self.identifier_to_tuple(namespace)
        if not namespace_tuple:
            return True
        content = self._get_content(list(namespace_tuple))
        return content is not None and content.get("type") == "NAMESPACE"

    @override
    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        namespace_tuple = self.identifier_to_tuple(namespace) if namespace else ()
        
        resp = self.session.get(f"{self.uri}/trees/tree/{self.ref}/entries")
        resp.raise_for_status()
        
        namespaces = []
        for entry in resp.json().get("entries", []):
            if entry.get("type") == "NAMESPACE":
                elements = tuple(entry["name"]["elements"])
                
                if namespace_tuple:
                    # Return immediate children of the specified namespace
                    if len(elements) == len(namespace_tuple) + 1 and elements[:-1] == namespace_tuple:
                        namespaces.append(elements)
                else:
                    # Return top-level namespaces
                    if len(elements) == 1:
                        namespaces.append(elements)
                        
        return namespaces

    @override
    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        namespace_tuple = self.identifier_to_tuple(namespace)
        if not self.namespace_exists(namespace_tuple):
            raise NoSuchNamespaceError(f"Namespace {namespace} does not exist")
            
        resp = self.session.get(f"{self.uri}/trees/tree/{self.ref}/entries")
        resp.raise_for_status()
        
        tables = []
        for entry in resp.json().get("entries", []):
            if entry.get("type") == "ICEBERG_TABLE":
                elements = tuple(entry["name"]["elements"])
                if elements[:-1] == namespace_tuple:
                    tables.append(elements)
                    
        return tables

    @override
    def view_exists(self, identifier: str | Identifier) -> bool:
        return False
        
    @override
    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        return []
        
    @override
    def drop_view(self, identifier: str | Identifier) -> None:
        raise NoSuchViewError(f"View {identifier} does not exist")

    @override
    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        return {}

    @override
    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError("Nessie Native catalog does not support namespace properties updates natively.")