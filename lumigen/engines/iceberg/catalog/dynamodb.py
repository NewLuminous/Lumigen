from __future__ import annotations
import typing as t
from typing_extensions import override
import uuid
from time import time
import boto3
from pyiceberg.catalog import BOTOCORE_SESSION
from pyiceberg.catalog.dynamodb import DynamoDbCatalog as BaseDynamoDbCatalog, _add_property_prefix, _get_create_table_item
from pyiceberg.catalog.dynamodb import DYNAMODB_CLIENT, DYNAMODB_TABLE_NAME, DYNAMODB_PROFILE_NAME, DYNAMODB_REGION, DYNAMODB_ACCESS_KEY_ID, DYNAMODB_SECRET_ACCESS_KEY, DYNAMODB_SESSION_TOKEN, DYNAMODB_COL_IDENTIFIER, DYNAMODB_COL_VERSION, DYNAMODB_COL_UPDATED_AT, PREVIOUS_METADATA_LOCATION, METADATA_LOCATION
from pyiceberg.io import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
from pyiceberg.table import Table, CommitTableResponse, StaticTable
from pyiceberg.table.update import update_table_metadata, TableRequirement, TableUpdate
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.utils.properties import get_first_property_value

if t.TYPE_CHECKING:
    from pyiceberg.typedef import Identifier


class DynamoDbCatalog(BaseDynamoDbCatalog):
    """
    A custom DynamoDbCatalog that overrides client initialization to support
    alternative endpoints (e.g., LocalStack, ScyllaDB Alternator, or DynamoDB Local).
    """
    def __init__(self, name: str, **properties: str):
        properties[DYNAMODB_TABLE_NAME] = get_first_property_value(properties, "dynamodb." + DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME)
        aws_access_key_id=get_first_property_value(properties, DYNAMODB_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID)
        if not aws_access_key_id:
            properties[DYNAMODB_ACCESS_KEY_ID] = "any"
        
        session = boto3.Session(
            profile_name=properties.get(DYNAMODB_PROFILE_NAME),
            region_name=get_first_property_value(properties, DYNAMODB_REGION, AWS_REGION),
            botocore_session=properties.get(BOTOCORE_SESSION),
            aws_access_key_id=get_first_property_value(properties, DYNAMODB_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
            aws_secret_access_key=get_first_property_value(properties, DYNAMODB_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
            aws_session_token=get_first_property_value(properties, DYNAMODB_SESSION_TOKEN, AWS_SESSION_TOKEN),
        )
        endpoint_url = properties.get("dynamodb.endpoint")
        dynamodb_client = session.client(DYNAMODB_CLIENT, endpoint_url=endpoint_url)
        
        super().__init__(name, client=dynamodb_client, **properties)

    @override
    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        """Commit updates to a DynamoDB Iceberg table."""
        identifier = table.name()
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)

        # 1. Fetch current DynamoDB item (contains the concurrency version lock 'v')
        current_item = self._get_iceberg_table_item(database_name=database_name, table_name=table_name)
        current_version = current_item[DYNAMODB_COL_VERSION]["S"]

        # 2. Update Table Metadata
        current_metadata = table.metadata
        new_metadata = update_table_metadata(current_metadata, updates)

        # 3. Write new metadata JSON to S3
        metadata_dir = f"{current_metadata.location}/metadata"
        new_metadata_location = f"{metadata_dir}/00000-{uuid.uuid4()}.metadata.json"

        io = self._load_file_io(new_metadata.properties, new_metadata_location)
        out_file = io.new_output(new_metadata_location)
        with out_file.create() as f:
            try: 
                f.write(new_metadata.model_dump_json().encode("utf-8"))
            except AttributeError: 
                f.write(new_metadata.json().encode("utf-8"))

        # 4. Construct the updated DynamoDB Item
        new_item = current_item.copy()
        new_item[DYNAMODB_COL_VERSION] = {"S": str(uuid.uuid4())}
        new_item[DYNAMODB_COL_UPDATED_AT] = {"N": str(round(time() * 1000))}
        new_item[_add_property_prefix(PREVIOUS_METADATA_LOCATION)] = current_item[_add_property_prefix(METADATA_LOCATION)]
        new_item[_add_property_prefix(METADATA_LOCATION)] = {"S": new_metadata_location}

        # 5. Conditionally put the item (Applying optimistic concurrency lock)
        try:
            self.dynamodb.put_item(
                TableName=self.dynamodb_table_name,
                Item=new_item,
                ConditionExpression=f"{DYNAMODB_COL_VERSION} = :expected_version",
                ExpressionAttributeValues={":expected_version": {"S": current_version}}
            )
        except self.dynamodb.exceptions.ConditionalCheckFailedException as e:
            raise CommitFailedException(
                f"Commit failed for {identifier}: version mismatch (concurrent update detected)"
            ) from e

        return CommitTableResponse(metadata=new_metadata, metadata_location=new_metadata_location)

    @override
    def register_table(self, identifier: str | Identifier, metadata_location: str, overwrite: bool = False) -> Table:
        """Register an existing Iceberg table metadata JSON into DynamoDB."""
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        self._ensure_namespace_exists(database_name=database_name)

        static_table = StaticTable.from_metadata(metadata_location, self.properties)
        
        # Build Item according to PyIceberg's schema map
        item = _get_create_table_item(
            database_name=database_name,
            table_name=table_name,
            properties=static_table.metadata.properties,
            metadata_location=metadata_location
        )

        try:
            kwargs = {
                "TableName": self.dynamodb_table_name,
                "Item": item
            }
            # Add strict lock if we aren't specifically overriding an existing table
            if not overwrite:
                kwargs["ConditionExpression"] = f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})"

            self.dynamodb.put_item(**kwargs)
        except self.dynamodb.exceptions.ConditionalCheckFailedException as e:
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e

        return self.load_table(identifier=identifier)