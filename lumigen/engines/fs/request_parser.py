from __future__ import annotations
import typing as t
from datetime import datetime
from pydantic import BaseModel, ValidationError, Field, model_validator, field_validator
from enum import Enum


class RequestParseError(ValueError):
    """
    Raised when an input string is not a valid request.
    This could be due to malformed JSON or a failure to meet the defined Pydantic schema.
    """
    pass


class FileOperationType(Enum):
    READ = "read"
    WRITE = "write"
    APPEND = "append"
    REMOVE = "remove"
    DESCRIBE = "describe"
    LIST = "list"

    def __str__(self) -> str:
        return self.value
    
class FileOptions(BaseModel):
    row_delimiter: str = Field(default='\n')
    field_delimiter: str = Field(default=',')
    compression_codec: t.Optional[str] = None
    header: bool = Field(default=True, description="Controls whether the first row is treated as column names.")
    encoding: str = Field(
        default='utf-8', 
        description="Character encoding of the file (e.g., 'utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16')."
    )
    ignore_corrupt_files: bool = Field(
        default=False,
        description="Whether to ignore corrupted/unreadable files and continue processing. If False (default), any error throws an exception."
    )

    csv_quote_char: str = Field(default='"')
    csv_escape_char: str = Field(default='\\')
    csv_comment_char: t.Optional[str] = Field(default=None, description="Single character used to identify and skip comment lines.")
    csv_on_bad_lines: t.Literal['error', 'skip', 'warn'] = Field(
        default='error', 
        description="Handling policy for corrupted lines with too many fields. 'error' throws an exception; 'skip' drops them; 'warn' raises a warning and drops them."
    )
    excel_skip_rows: int = Field(default=0, description="The number of rows from the beginning of the file that should be ignored.")

class FileRequest(BaseModel):
    """
    Represents a single, validated request.
    This is the root model for the JSON instruction language.
    """
    operation: FileOperationType
    file_path: t.Optional[str] = None
    file_pattern: t.Optional[str] = None
    modified_since: t.Optional[datetime] = None
    options: FileOptions = Field(default_factory=FileOptions)
    concurrency: int = 4

    @field_validator('operation', mode='before')
    @classmethod
    def normalize_delete_operation(cls, v: t.Any) -> t.Any:
        if isinstance(v, str) and v.lower() == "delete":
            return "remove"
        return v

    @model_validator(mode='after')
    def check_either_path_or_pattern(self) -> FileRequest:
        if not self.file_path and not self.file_pattern:
            raise ValueError("Either 'file_path' or 'file_pattern' must be provided.")
        return self


class FileRequestParser:
    """
    Parses and validates an input string into a structured FileRequest.
    """
    @staticmethod
    def parse(request_str: str) -> FileRequest:
        """
        Parses the JSON request string into a validated FileRequest object.

        Args:
            request_str: The raw JSON string defining the request.

        Returns:
            A validated Pydantic FileRequest model.
        """
        try:
            return FileRequest.model_validate_json(request_str)
        except ValidationError as e:
            raise RequestParseError(f"Invalid request: {e}") from e