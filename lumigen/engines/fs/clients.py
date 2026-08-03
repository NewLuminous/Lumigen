from __future__ import annotations
import typing as t
import re
from abc import ABC, abstractmethod
from fsspec.implementations.arrow import HadoopFileSystem
from s3fs import S3FileSystem

if t.TYPE_CHECKING:
    from fsspec import AbstractFileSystem

from engines.fs.cursor import FileSystemCursor


class FileSystemConnection(ABC):
    def __init__(self):
        if type(self) is FileSystemConnection:
            raise TypeError("FileSystemConnection is an abstract class and cannot be instantiated directly.")
        self._fs: t.Optional[AbstractFileSystem] = None

    @property
    @abstractmethod
    def fs(self) -> AbstractFileSystem:
        # Reference: https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
        pass

    def get_fs(self) -> AbstractFileSystem:
        return self.fs

    @abstractmethod
    def ping(self) -> None:
        pass

    @abstractmethod
    def exists(self, file_path: str) -> bool:
        pass

    def cursor(self) -> FileSystemCursor:
        return FileSystemCursor(fs=self.fs)
    
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
        """Closes the connection object. Cursors must be closed separately."""
        pass

class HDFSConnection(FileSystemConnection):
    def __init__(self, host: str = "default", port: int = 0,
                 user: t.Optional[str] = None, kerb_ticket: t.Optional[str] = None,
                 replication: int = 3,
                 extra_conf: t.Optional[t.Dict[str, t.Any]] = None):
        self.host = host
        self.port = port
        self.user = user
        self.kerb_ticket = kerb_ticket
        self.replication = replication
        self.extra_conf = extra_conf

        self._fs: t.Optional[HadoopFileSystem] = None

    @property
    def fs(self) -> HadoopFileSystem:
        if self._fs is None:
            extra_conf = dict(self.extra_conf or {})
            if self.replication:
                extra_conf.setdefault("dfs.replication", str(self.replication))
            self._fs = HadoopFileSystem(
                host=self.host,
                port=self.port,
                user=self.user,
                kerb_ticket=self.kerb_ticket,
                replication=self.replication,
                extra_conf=self.extra_conf,
            )

        return self._fs
    
    def ping(self) -> None:
        self.fs.ls("/", detail=False)

    def exists(self, file_path: str) -> bool:
        cleaned_path = re.sub(r'^hdfs://(?:[^/]+)?', '', file_path) or '/'
        return self.fs.exists(cleaned_path)

class S3Connection(FileSystemConnection):
    def __init__(self, endpoint_url: t.Optional[str] = None,
                 s3_access_key_id: t.Optional[str] = None, s3_secret_access_key: t.Optional[str] = None,
                 client_config: t.Optional[t.Dict[str, t.Any]] = None):
        self.endpoint_url = endpoint_url
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key= s3_secret_access_key
        self.client_config = client_config or {}

        self._fs: t.Optional[S3FileSystem] = None

    @property
    def fs(self) -> S3FileSystem:
        if self._fs is None:
            key = self.s3_access_key_id or self.client_config.get("aws_access_key_id")
            secret = self.s3_secret_access_key or self.client_config.get("aws_secret_access_key")
            anon = not key or not secret
            self._fs = S3FileSystem(
                endpoint_url=self.endpoint_url,
                key=self.s3_access_key_id,
                secret=self.s3_secret_access_key,
                anon=anon,
                use_listings_cache=False,
                version_aware=False,
                client_kwargs=self.client_config,
            )

        return self._fs
    
    def ping(self) -> None:
        self.fs.call_s3('list_buckets')

    def exists(self, file_path: str) -> bool:
        stripped_path = re.sub(r'^s3[a-z]?://', '', file_path)
        return self.fs.exists(stripped_path)