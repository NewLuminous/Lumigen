from __future__ import annotations
import typing as t
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyiceberg.io import S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, PY_IO_IMPL, FSSPEC_FILE_IO

try:
    from pyiceberg.io import (
        S3_CONNECT_TIMEOUT,
        S3_READ_TIMEOUT,
    )
except ImportError:
    S3_CONNECT_TIMEOUT = "s3.connect-timeout"
    S3_READ_TIMEOUT = "s3.read-timeout"

from pyiceberg.catalog.rest import RestCatalog as BaseRestCatalog
from pyiceberg.catalog.rest import CREDENTIAL

try:
    from pyiceberg.catalog.rest import (
        REST_CLIENT_MAX_RETRIES,
        REST_CLIENT_RETRY_BACKOFF_FACTOR,
    )
except ImportError:
    REST_CLIENT_MAX_RETRIES = "rest.client.max-retries"
    REST_CLIENT_RETRY_BACKOFF_FACTOR = "rest.client.retry-backoff-factor"

from pyiceberg.exceptions import (
    NoSuchTableError,
    TableAlreadyExistsError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError
)
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField

if t.TYPE_CHECKING:
    from pyiceberg.typedef import Identifier, Properties
    from pyiceberg.table import Table


BOOTSTRAP_MARKER_TABLE_NAME = "__sys_catalog_init__"
DUMMY_BOOTSTRAP_SCHEMA = Schema(
    NestedField(field_id=1, name="_sys_init", field_type=IntegerType(), required=False)
)

_CONNECTION_RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)
_CONNECTION_RETRY_ALLOWED_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "POST", "PUT", "DELETE"})


class RestCatalog(BaseRestCatalog):
    def __init__(self, name: str, **properties: str):
        # Default to FsspecFileIO for REST catalog
        properties.setdefault(PY_IO_IMPL, FSSPEC_FILE_IO)

        properties.setdefault(REST_CLIENT_MAX_RETRIES, "5")
        properties.setdefault(REST_CLIENT_RETRY_BACKOFF_FACTOR, "1.5")

        properties.setdefault("s3.max-attempts", "5")
        properties.setdefault("s3.retry-mode", "adaptive")
        properties.setdefault(S3_CONNECT_TIMEOUT, "60")
        properties.setdefault(S3_READ_TIMEOUT, "60")

        s3_access_key = properties.get(S3_ACCESS_KEY_ID)
        s3_secret_key = properties.get(S3_SECRET_ACCESS_KEY)
        if s3_access_key and s3_secret_key and not properties.get(CREDENTIAL):
            properties[CREDENTIAL] = f"{s3_access_key}:{s3_secret_key}"

        super().__init__(name, **properties)

    def _config_headers(self, session: Session) -> None:
        super()._config_headers(session)

        # Disable STS credential vending so SeaweedFS does not throw 501 Not Implemented
        session.headers.pop("X-Iceberg-Access-Delegation", None)

    def _create_session(self) -> Session:
        session = super()._create_session()

        retries = int(self.properties.get(REST_CLIENT_MAX_RETRIES, 5))
        backoff = float(self.properties.get(REST_CLIENT_RETRY_BACKOFF_FACTOR, 1.5))

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=list(_CONNECTION_RETRY_STATUS_FORCELIST),
            allowed_methods=_CONNECTION_RETRY_ALLOWED_METHODS,
            raise_on_status=False,
        )

        http_adapter = session.adapters.get("http://")
        https_adapter = session.adapters.get("https://")

        adapter_patched = False
        for adapter in (http_adapter, https_adapter):
            if adapter and hasattr(adapter, "max_retries") and isinstance(adapter.max_retries, Retry):
                adapter.max_retries.total = retries
                adapter.max_retries.backoff_factor = backoff
                adapter.max_retries.status_forcelist = set(adapter.max_retries.status_forcelist or []) | set(_CONNECTION_RETRY_STATUS_FORCELIST)
                adapter.max_retries.allowed_methods = frozenset((adapter.max_retries.allowed_methods or set()) | {"POST"})
                adapter.max_retries.raise_on_status = False
                adapter_patched = True

        if not adapter_patched:
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=20,
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)

        return session

    def table_exists(self, identifier: str | Identifier) -> bool:
        try:
            return super().table_exists(identifier)
        except (NoSuchTableError, NotImplementedError):
            return False

    def _bootstrap_namespace(self, namespace: str | Identifier) -> None:
        """
        Creates a reserved marker table to force the underlying storage engine
        to write and persist namespace metadata (e.g. 's3tables.metadata').
        """
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        marker_ident = (*namespace_tuple, BOOTSTRAP_MARKER_TABLE_NAME)
        try:
            raw_tables = super().list_tables(namespace)
            table_names = [t[-1] for t in raw_tables]
            # Only create the marker if the namespace is completely empty
            if not table_names:
                self.create_table(
                    identifier=marker_ident,
                    schema=DUMMY_BOOTSTRAP_SCHEMA,
                )
        except (TableAlreadyExistsError, Exception):
            pass

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        super().create_namespace(namespace, properties)
        self._bootstrap_namespace(namespace)

    def create_namespace_if_not_exists(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        try:
            self.create_namespace(namespace, properties)
        except NamespaceAlreadyExistsError:
            self._bootstrap_namespace(namespace)

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        """
        Filter out the internal system bootstrap marker so it remains
        completely invisible across PyIceberg, SHOW TABLES, and custom queries.
        """
        raw_tables = super().list_tables(namespace)
        return [tbl for tbl in raw_tables if tbl[-1] != BOOTSTRAP_MARKER_TABLE_NAME]

    def drop_namespace(self, namespace: str | Identifier) -> None:
        """
        Ensures DROP DATABASE succeeds cleanly if only the bootstrap marker remains.
        """
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        marker_ident = (*namespace_tuple, BOOTSTRAP_MARKER_TABLE_NAME)
        
        raw_tables = super().list_tables(namespace)
        if len(raw_tables) == 1 and raw_tables[0][-1] == BOOTSTRAP_MARKER_TABLE_NAME:
            try:
                self.drop_table(marker_ident, purge_requested=True)
            except Exception:
                pass
        elif len(raw_tables) > 1 or (len(raw_tables) == 1 and raw_tables[0][-1] != BOOTSTRAP_MARKER_TABLE_NAME):
            raise NamespaceNotEmptyError(f"Namespace {namespace} is not empty.")

        super().drop_namespace(namespace)

    def rename_table(
        self, from_identifier: t.Union[str, Identifier], to_identifier: t.Union[str, Identifier]
    ) -> Table:
        try:
            return super().rename_table(from_identifier, to_identifier)
        except NoSuchTableError as e:
            # SeaweedFS returns a raw 404 for the /rename endpoint itself,
            # which PyIceberg mistakenly interprets as the *table* not existing.
            cause_msg = str(e.__cause__) if e.__cause__ else ""
            if "Path not found" in str(e) or "Not Found for url" in cause_msg:
                raise NotImplementedError(
                    "The underlying REST catalog (e.g., SeaweedFS) currently does not natively support the table rename operation. "
                    "Please use 'CREATE TABLE ... AS SELECT' followed by 'DROP TABLE' instead."
                ) from e
            raise