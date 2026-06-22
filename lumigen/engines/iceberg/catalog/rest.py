from __future__ import annotations
import typing as t
from pyiceberg.io import S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
from pyiceberg.catalog.rest import RestCatalog as BaseRestCatalog
from pyiceberg.catalog.rest import CREDENTIAL
from pyiceberg.exceptions import NoSuchTableError

if t.TYPE_CHECKING:
    from pyiceberg.typedef import Identifier
    from pyiceberg.table import Table


class RestCatalog(BaseRestCatalog):
    def __init__(self, name: str, **properties: str):
        s3_access_key = properties.get(S3_ACCESS_KEY_ID)
        s3_secret_key = properties.get(S3_SECRET_ACCESS_KEY)
        if s3_access_key and s3_secret_key and not properties.get(CREDENTIAL):
            properties[CREDENTIAL] = f"{s3_access_key}:{s3_secret_key}"

        super().__init__(name, **properties)

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