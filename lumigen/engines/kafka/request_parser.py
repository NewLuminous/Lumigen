from __future__ import annotations
import typing as t
import json
from pydantic import BaseModel, ValidationError, model_validator
from sqlglot import exp
from sqlglot import parse as sqlglot_parse
from utils.sql import extract_main_table
from engines.kafka.dialect import Kafka

# ===================================================================
# Custom Exceptions
# ===================================================================

class RequestParseError(ValueError):
    """
    Raised when an input string is not a valid request.
    This could be due to malformed JSON or a failure to meet the defined Pydantic schema.
    """
    pass

class SQLParseError(ValueError):
    """
    Raised when a SQL-like statement intended for Kafka is malformed,
    unsupported, or cannot be correctly translated into a Kafka operation.
    """
    pass

# ===================================================================
# Pydantic Models for Kafka Operations (The "Instruction Language")
# ===================================================================

class KafkaCreateOperation(BaseModel):
    """Defines the parameters for a Kafka 'create' operation."""
    topic: str
    num_partitions: t.Optional[int] = 1
    replication_factor: t.Optional[int] = 1

class KafkaConsumeOperation(BaseModel):
    """Defines the parameters for a Kafka 'consume' operation."""
    topics: t.List[str]
    stored_offsets: t.Optional[t.Dict[str, t.Dict[str, int]]] = None

class KafkaProduceOperation(BaseModel):
    """Defines the parameters for a Kafka 'produce' operation."""
    topic: str
    messages: t.Optional[t.List[t.Dict[str, t.Any]]] = None

class KafkaDeleteOperation(BaseModel):
    """Defines the parameters for a Kafka 'delete' operation."""
    topics: t.List[str]

class KafkaRequest(BaseModel):
    """
    Represents a single, validated Kafka request.
    This is the root model for the Kafka JSON instruction language.
    """
    create: t.Optional[KafkaCreateOperation] = None
    consume: t.Optional[KafkaConsumeOperation] = None
    produce: t.Optional[KafkaProduceOperation] = None
    delete: t.Optional[KafkaDeleteOperation] = None

    @model_validator(mode='after')
    def check_mutual_exclusion(self) -> "KafkaRequest":
        provided_operations = [
            field_name for field_name, value in self.__dict__.items() if value is not None
        ]

        if len(provided_operations) == 0:
            all_possible_operations = list(self.model_fields.keys())
            raise ValueError(
                f"Operation must contain exactly one of the following keys: {all_possible_operations}"
            )
        
        if len(provided_operations) > 1:
            raise ValueError(
                f"Operation cannot contain multiple keys at once. Provided: {provided_operations}"
            )
            
        return self
    
# ===================================================================
# Parsers
# ===================================================================

class KafkaRequestParser:
    """
    Parses and validates an input string into a structured KafkaRequest.
    It acts as a dispatcher, first attempting to parse the input as direct JSON instructions,
    and falling back to parsing it as a SQL-like statement.
    """
    @staticmethod
    def parse(request_str: str) -> KafkaRequest:
        """
        Parses the JSON request string into a validated KafkaRequest object.

        Args:
            request_str: The raw JSON string defining the request.

        Returns:
            A validated Pydantic KafkaRequest model.
        """
        try:
            try:
                data = json.loads(request_str)
                return KafkaRequest.model_validate(data)
            except Exception as e:
                try:
                    return KafkaSQLParser.parse(request_str)
                except SQLParseError as spe:
                    raise spe from None
                except RequestParseError as rpe:
                    raise rpe from None
                except Exception:
                    raise e
        except json.JSONDecodeError as e:
            raise RequestParseError(f"Invalid request: Not a valid JSON string. Details: {e}") from e
        except ValidationError as e:
            raise RequestParseError(f"Invalid request: Schema validation failed. Details: {e}") from e

class KafkaSQLParser:
    """
    Translates SQL-like statements into structured KafkaRequest objects.
    This provides a familiar, SQL-based interface for interacting with Kafka.
    """
    @staticmethod
    def parse(sql: str) -> KafkaRequest:
        """
        Parses a single SQL-like statement and translates it into a KafkaRequest.
        """
        all_exps = sqlglot_parse(sql, dialect=Kafka)

        parsed_exp = all_exps[0] if all_exps else None
        if not parsed_exp:
            raise SQLParseError("Invalid request: The SQL statement is empty.")

        if len(all_exps) > 1:
            raise RequestParseError("Invalid request: Only a single SQL statement is supported.")

        try:
            if isinstance(parsed_exp, exp.Create):
                return KafkaSQLParser._build_create_request(parsed_exp)
            elif isinstance(parsed_exp, exp.Select):
                return KafkaSQLParser._build_consume_request(parsed_exp)
            elif isinstance(parsed_exp, exp.Insert):
                return KafkaSQLParser._build_produce_request(parsed_exp)
            elif isinstance(parsed_exp, exp.Drop):
                return KafkaSQLParser._build_delete_request(parsed_exp)
        except Exception as e:
            raise SQLParseError(str(e))

        raise SQLParseError(f"Unsupported SQL statement of type: '{type(parsed_exp).__name__.upper()}'")

    @staticmethod
    def _build_create_request(create_exp: exp.Create) -> KafkaRequest:
        data = {"create": {}}

        if create_exp.kind.upper() != "TABLE":
            raise SQLParseError(f"Invalid CREATE statement. Must be 'CREATE TABLE'. Found 'CREATE {create_exp.kind}'.")
        
        topics_str = extract_main_table(create_exp, dialect=Kafka, quoted=False)
        topics = [tp.strip() for tp in topics_str.split(',')]
        if len(topics) == 0:
            raise SQLParseError("Invalid request: Empty topic.")

        if len(topics) > 1:
            raise SQLParseError("Invalid request: Only support creating one topic at a time.")
        data["create"]["topic"] = topics[0]

        properties_exp: exp.Properties = create_exp.args["properties"]
        if properties_exp:
            for property_exp in properties_exp.find_all(exp.Property):
                property_name = str(property_exp.this)
                property_value = property_exp.args.get("value")
                if property_value:
                    if property_name.upper() == "PARTITIONS":
                        data["create"]["num_partitions"] = int(str(property_value))
                    elif property_name.upper() == "REPLICAS":
                        data["create"]["replication_factor"] = int(str(property_value))
        return KafkaRequest.model_validate(data)
    
    @staticmethod
    def _build_consume_request(select_exp: exp.Select) -> KafkaRequest:
        data = {"consume": {}}

        topics_str = extract_main_table(select_exp, dialect=Kafka, quoted=False)
        topics = [tp.strip() for tp in topics_str.split(',')]
        data["consume"]["topics"] = topics
        return KafkaRequest.model_validate(data)
    
    @staticmethod
    def _build_produce_request(insert_exp: exp.Insert) -> KafkaRequest:
        data = {"produce": {}}

        table_exp = t.cast("exp.Expression", insert_exp.this).find(exp.Table)
        if not table_exp:
            raise SQLParseError("INSERT statement is missing a valid topic name (e.g., 'INSERT INTO my_topic ...').")
        data["produce"]["topic"] = table_exp.sql()

        schema_exp: exp.Schema = insert_exp.this
        columns_exp = schema_exp.args.get("expressions", [])
        columns = [str(c) for c in columns_exp]
        if not columns:
            columns = ["key", "value"]

        messages = []
        values_exp = insert_exp.find(exp.Values)
        if values_exp:
            for tuple_exp in values_exp.find_all(exp.Tuple):
                row_values = []
                for lit_exp in tuple_exp.find_all(exp.Literal):
                    lit_exp.set("is_string", False)
                    row_values.append(str(lit_exp))
                msg = {}
                for i in range(min(len(columns), len(row_values))):
                    if columns[i] in {"key", "value"}:
                        msg[columns[i]] = row_values[i]
                messages.append(msg)
        data["produce"]["messages"] = messages
        return KafkaRequest.model_validate(data)
    
    @staticmethod
    def _build_delete_request(drop_exp: exp.Drop) -> KafkaRequest:
        data = {"delete": {}}

        if drop_exp.kind.upper() != "TABLE":
            raise SQLParseError(f"Invalid DROP statement. Must be 'DROP TABLE'. Found 'DROP {drop_exp.kind}'.")

        topics_str = extract_main_table(drop_exp, dialect=Kafka, quoted=False)
        topics = [tp.strip() for tp in topics_str.split(',')]
        data["delete"]["topics"] = topics
        return KafkaRequest.model_validate(data)