from __future__ import annotations
from pyiceberg.catalog import URI
from pyiceberg.catalog.glue import GlueCatalog as BaseGlueCatalog
from pyiceberg.catalog.glue import GLUE_ACCESS_KEY_ID, GLUE_CATALOG_ENDPOINT
from pyiceberg.io import AWS_ACCESS_KEY_ID
from pyiceberg.utils.properties import get_first_property_value


class GlueCatalog(BaseGlueCatalog):
    def __init__(self, name: str, **properties: str):
        properties[GLUE_CATALOG_ENDPOINT] = get_first_property_value(properties, GLUE_CATALOG_ENDPOINT, URI)
        aws_access_key_id=get_first_property_value(properties, GLUE_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID)
        if not aws_access_key_id:
            properties[GLUE_ACCESS_KEY_ID] = "any"

        super().__init__(name, **properties)