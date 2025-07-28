from __future__ import annotations
import typing as t
import json
from pydantic import BaseModel, ValidationError, model_validator

class RequestParseError(ValueError):
    """Raised when an request string is malformed or invalid."""
    pass

class KafkaConsumeOperation(BaseModel):
    """Defines the parameters for a Kafka 'consume' operation."""
    topics: t.List[str]
    storedOffsets: t.Optional[t.Dict[str, t.Dict[str, int]]] = None

class KafkaProduceOperation(BaseModel):
    """Defines the parameters for a Kafka 'produce' operation."""
    topic: str
    message: t.Optional[t.Dict[str, t.Any]] = None

class KafkaDeleteOperation(BaseModel):
    """Defines the parameters for a Kafka 'delete' operation."""
    topics: t.List[str]

class KafkaRequest(BaseModel):
    """
    Represents a single, validated Kafka request.
    This is the root model for the Kafka JSON instruction language.
    """
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

class KafkaRequestParser:
    """
    Parses and validates a JSON string defining a Kafka request.
    """
    @staticmethod
    def parse(request_str: str) -> KafkaRequest:
        """
        Parses the JSON request string into a validated KafkaRequest object.

        Args:
            request_str: The raw JSON string defining the request.

        Returns:
            A validated Pydantic KafkaRequest model.

        Raises:
            RequestParseError: If the string is not valid JSON or fails schema validation.
        """
        try:
            data = json.loads(request_str)
            return KafkaRequest.model_validate(data)
        except json.JSONDecodeError as e:
            raise RequestParseError(f"Invalid request: Not a valid JSON string. Details: {e}") from e
        except ValidationError as e:
            raise RequestParseError(f"Invalid request: Schema validation failed. Details: {e}") from e