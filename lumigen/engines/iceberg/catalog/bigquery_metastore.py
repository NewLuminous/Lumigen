from __future__ import annotations
import typing as t
import os
import json
import logging
from pathlib import Path
from pyiceberg.catalog.bigquery_metastore import BigQueryMetastoreCatalog as BaseBigQueryMetastoreCatalog
from pyiceberg.catalog.bigquery_metastore import GCP_PROJECT_ID, GCP_LOCATION, GCP_CREDENTIALS_FILE, GCP_CREDENTIALS_INFO

logger = logging.getLogger(__name__)


class BigQueryMetastoreCatalog(BaseBigQueryMetastoreCatalog):
    def __init__(self, name: str, **properties: str):
        # Ref: https://github.com/apache/iceberg/blob/main/bigquery/src/main/java/org/apache/iceberg/gcp/bigquery/BigQueryProperties.java
        if properties.get("gcp_project"):
            properties[GCP_PROJECT_ID] = properties.pop("gcp_project")
        if properties.get("gcp_location"):
            properties[GCP_LOCATION] = properties.pop("gcp_location")

        # BigQuery requires current-snapshot-id to be present for tables to be created.
        logger.warning("Setting 'PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID' to 'true' to ensure compatibility with BigQuery Metastore Catalog.")
        os.environ["PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID"] = "true"

        super().__init__(name, **properties)

        target_adc_path = Path("~/.config/gcloud/application_default_credentials.json").expanduser()
        adc_content: t.Optional[str] = None

        # Scenario A: Populate from a source GCP_CREDENTIALS_FILE path
        cred_file_path = properties.get(GCP_CREDENTIALS_FILE)
        if cred_file_path:
            try:
                source_path = Path(cred_file_path).expanduser()
                if source_path.is_file():
                    adc_content = source_path.read_text(encoding="utf-8")
                else:
                    logger.warning(f"Specified GCP credentials source file '{cred_file_path}' does not exist.")
            except Exception as e:
                logger.error(f"Failed to read source GCP credentials file '{cred_file_path}': {e}")

        # Scenario B: Populate from inline GCP_CREDENTIALS_INFO structure (if file was missing/unreadable)
        if not adc_content:
            cred_info = properties.get(GCP_CREDENTIALS_INFO)
            if cred_info:
                try:
                    if isinstance(cred_info, dict):
                        adc_content = json.dumps(cred_info, indent=2)
                    elif isinstance(cred_info, str):
                        # Validate JSON syntax before saving
                        json.loads(cred_info)
                        adc_content = cred_info
                except Exception as e:
                    logger.error(f"Failed to process inline GCP credentials info: {e}")

        # Commit the resolved content to the default gcloud config location
        if adc_content:
            try:
                target_adc_path.parent.mkdir(parents=True, exist_ok=True)
                target_adc_path.write_text(adc_content, encoding="utf-8")
                logger.info(f"Provisioned Application Default Credentials (ADC) at '{target_adc_path}'")
            except Exception as e:
                logger.error(f"Failed to write credentials file to '{target_adc_path}': {e}")