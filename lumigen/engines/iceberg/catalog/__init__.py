from __future__ import annotations
import typing as t
import os

# SeaweedFS does not parse `aws-chunked` transfer encoding or S3 checksum trailers properly. 
# Newer AWS SDKs (used by pyarrow/botocore) append CRC64NVME checksums as chunked trailers. 
# These variables force the SDK to fall back to standard un-chunked payloads.
os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "WHEN_REQUIRED"

from pyiceberg.catalog import Catalog, CatalogType, TYPE
from pyiceberg.catalog import load_catalog as load_base_catalog
from pyiceberg.utils.config import Config, merge_config
from pyiceberg.io import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
from pyiceberg.io import S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_ANONYMOUS
from pyiceberg.exceptions import NotInstalledError

if t.TYPE_CHECKING:
    from pyiceberg.typedef import Properties, RecursiveDict

from utils.connections import HadoopS3PropertiesSyncer

_ENV_CONFIG = Config()


def load_rest(name: str, conf: Properties) -> Catalog:
    from engines.iceberg.catalog.rest import RestCatalog

    return RestCatalog(name, **conf)

def load_hive(name: str, conf: Properties) -> Catalog:
    try:
        from engines.iceberg.catalog.hive import HiveCatalog

        return HiveCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("Apache Hive support not installed: pip install 'pyiceberg[hive]'") from exc

def load_glue(name: str, conf: Properties) -> Catalog:
    try:
        from engines.iceberg.catalog.glue import GlueCatalog

        return GlueCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("AWS glue support not installed: pip install 'pyiceberg[glue]'") from exc

def load_dynamodb(name: str, conf: Properties) -> Catalog:
    try:
        from engines.iceberg.catalog.dynamodb import DynamoDbCatalog

        return DynamoDbCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("AWS DynamoDB support not installed: pip install 'pyiceberg[dynamodb]'") from exc
    
def load_bigquery(name: str, conf: Properties) -> Catalog:
    try:
        from engines.iceberg.catalog.bigquery_metastore import BigQueryMetastoreCatalog

        return BigQueryMetastoreCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("BigQuery support not installed: pip install 'pyiceberg[bigquery]'") from exc
    
def load_hadoop(name: str, conf: Properties) -> Catalog:
    from engines.iceberg.catalog.hadoop import HadoopCatalog

    return HadoopCatalog(name, **conf)

def load_jdbc(name: str, conf: Properties) -> Catalog:
    from engines.iceberg.catalog.jdbc import JDBCCatalog

    return JDBCCatalog(name, **conf)

def load_nessie(name: str, conf: Properties) -> Catalog:
    from engines.iceberg.catalog.nessie import NessieCatalog

    return NessieCatalog(name, **conf)


EXTENDED_CATALOGS: dict[str, t.Callable[[str, Properties], Catalog]] = {
    CatalogType.REST.value: load_rest,
    CatalogType.HIVE.value: load_hive,
    CatalogType.GLUE.value: load_glue,
    CatalogType.DYNAMODB.value: load_dynamodb,
    CatalogType.SQL.value: load_jdbc,
    CatalogType.BIGQUERY.value: load_bigquery,
    "hadoop": load_hadoop,
    "jdbc": load_jdbc,
    "nessie": load_nessie,
}


def load_catalog(name: t.Optional[str] = None, **properties: t.Optional[str]) -> Catalog:
    if name is None:
        name = _ENV_CONFIG.get_default_catalog_name()

    properties = SparkPropertiesTranslator.translate_to_pyiceberg(name, properties)

    env = _ENV_CONFIG.get_catalog_config(name)
    conf: RecursiveDict = merge_config(env or {}, t.cast("RecursiveDict", properties))

    provided_catalog_type = conf.get(TYPE)
    if isinstance(provided_catalog_type, str):
        catalog_type_str = provided_catalog_type.lower()
        if catalog_type_str in EXTENDED_CATALOGS:
            return EXTENDED_CATALOGS[catalog_type_str](name, t.cast(dict[str, str], conf))

    return load_base_catalog(name, **properties)

    
class SparkPropertiesTranslator:
    """
    Utility class dedicated to cleaning and interpreting properties originally 
    defined in a PySpark/Java environment for native PyIceberg consumption.
    """

    CLIENT_TO_S3_MAP = {
        AWS_REGION: S3_REGION,
        AWS_ACCESS_KEY_ID: S3_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: S3_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN: S3_SESSION_TOKEN,
    }

    @classmethod
    def translate_to_pyiceberg(cls, catalog_name: str, properties: t.Dict[str, str]) -> t.Dict[str, str]:
        """
        Cleans spark/hadoop prefixes from a raw mixed dictionary, segregates properties,
        syncs storage configs, and returns a fully compliant PyIceberg dictionary.
        """
        cleaned: t.Dict[str, str] = {}
        # 1. Strip Prefixes
        for k, v in properties.items():
            clean_k = k
            if clean_k.startswith(f"spark.sql.catalog.{catalog_name}."):
                clean_k = clean_k.replace(f"spark.sql.catalog.{catalog_name}.", "", 1)
            if clean_k.startswith("spark.hadoop."):
                clean_k = clean_k.replace("spark.hadoop.", "hadoop.", 1)
            if clean_k.startswith("hadoop."):
                clean_k = clean_k.replace("hadoop.", "", 1)
            cleaned[clean_k] = v

        hadoop_props: t.Dict[str, str] = {}
        iceberg_props: t.Dict[str, str] = {}

        # 2. Segregate properties and apply 'client.*' overrides 
        for k, v in cleaned.items():
            if k in cls.CLIENT_TO_S3_MAP:
                iceberg_props[cls.CLIENT_TO_S3_MAP[k]] = v

        for k, v in cleaned.items():
            if k.startswith("fs."):
                hadoop_props[k] = v
            else:
                iceberg_props[k] = v
                
        # 3. Synchronize storage configurations
        HadoopS3PropertiesSyncer.sync_inplace(hadoop_props, iceberg_props)

        # 4. Final safety check: if credentials are completely absent, force anonymous
        if not iceberg_props.get(S3_ACCESS_KEY_ID) or not iceberg_props.get(S3_SECRET_ACCESS_KEY):
            iceberg_props[S3_ANONYMOUS] = "true"

        return iceberg_props