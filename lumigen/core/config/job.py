from __future__ import annotations
import typing as t
import logging
from pydantic import BaseModel, model_validator, field_validator, ValidationError, constr
from enum import Enum
from sqlglot import exp
from sqlmesh.core.config.base import BaseConfig

from utils.sql import dialect_insensitive_tbl_name, extract_main_table
from utils.errors import ConfigError

logger = logging.getLogger(__name__)
StringOrInt = t.Union[str, int]
CliOptionValue = t.Union[StringOrInt, t.List[StringOrInt]]


class JobMode(str, Enum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"

    def __str__(self) -> str:
        return self.value
    
    @classmethod
    def from_string(cls, value: str):
        """Gets JobMode from string, case-insensitively. BATCH by default."""
        _member_map_lower = {m.value.lower(): m for m in cls}
        _member_map_lower["stream"] = cls.STREAMING

        try:
            return _member_map_lower[value.lower()]
        except KeyError:
            return cls.BATCH


class DataLoadMode(str, Enum):
    """Specifies the data loading strategy from the source.

    Options include:
    * `FULL`: Processes the entire dataset from the source in every run.
    * `INCREMENTAL`: Processes only new or changed data since the last run.
    """
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"

    def __str__(self) -> str:
        return self.value


class DataSaveMode(str, Enum):
    """Supported processing schemes for saving data.

    Options include:
    * `APPEND`: Append contents to existing data.
    * `OVERWRITE`: Overwrite existing data.
    * `MERGE`: Merge contents with existing data.
    * `ERROR_IF_EXISTS`: Throw an exception if data already exists.
    * `IGNORE`: Silently ignore this operation if data already exists.
    """
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"
    MERGE = "MERGE"
    ERROR_IF_EXISTS = "ERROR_IF_EXISTS"
    IGNORE = "IGNORE"

    def __str__(self) -> str:
        return self.value


def normalize_topic_list_to_str_validator(v: t.Any) -> t.Any:
    if isinstance(v, list):
        return ",".join(map(str, v))
    return v

class JobSourceRelatedFields(BaseModel):
    source_path: t.Optional[str] = None
    """The identifier for the data source (can be a table, collection, topic, etc.)."""
    source_query: t.Optional[str] = None
    """The query to extract data from the data source."""
    data_load_mode: t.Optional[DataLoadMode] = DataLoadMode.FULL
    """The data loading strategy from the source (FULL or INCREMENTAL)."""
    source_watermark: t.Optional[str] = None
    """The column used to identify new or changed data for INCREMENTAL loads."""
    source_batch_size: t.Optional[int] = None
    """The number of rows to fetch from the data source per round trip."""
    source_limit: t.Optional[int] = None
    """The maximum number of rows to fetch from the data source."""

    SOURCE_ALIASES: t.ClassVar[t.Set[str]] = {
        "source_path",
        "source_table",
        "source_collection",
        "source_topic",
    }

    @model_validator(mode='before')
    @classmethod
    def _normalize_and_validate_source(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data # Let Pydantic handle the basic type error
        
        # Check for mutual exclusivity
        source_definition_fields = cls.SOURCE_ALIASES.copy()
        source_definition_fields.add("source_query")
        provided_source_definitions = source_definition_fields.intersection(data.keys())
        if len(provided_source_definitions) > 1:
            raise ValueError(
                f"Cannot set these source properties at once: {', '.join(sorted(list(provided_source_definitions)))}."
            )
        
        # Check that at least one was provided
        if not provided_source_definitions:
            raise ValueError(
                f"One of '{', '.join(sorted(list(source_definition_fields)))}' must be provided as a data source."
            )
            
        # Get the single alias that was used (e.g., 'source_topic')
        provided_aliases = cls.SOURCE_ALIASES.intersection(data.keys())
        if provided_aliases:
            alias_key = provided_aliases.pop()
            value = data.pop(alias_key)

            # Perform any specific normalization (like for topics)
            if alias_key == "source_topic":
                value = normalize_topic_list_to_str_validator(value)

            # Populate the canonical field
            data['source_path'] = value
        
        return data
    
    def get_source_path(self):
        return self.source_path or extract_main_table(self.source_query)
    
    def build_source_expression(self, dialect: t.Optional[str] = None) -> t.Union[exp.Select, str]:
        """
        Interprets the source fields and returns a SQLGlot expression or a string.
        """
        if self.source_query:
            return self.source_query
        
        if self.source_path:
            return exp.select('*').from_(exp.to_table(
                dialect_insensitive_tbl_name(self.source_path, dialect)
            ))
        
        return None
    
class JobSinkRelatedFields(BaseModel):
    sink_path: t.Optional[str] = None
    """The identifier for the data destination (can be a table, collection, topic, etc.)."""
    data_save_mode: t.Optional[DataSaveMode] = DataSaveMode.APPEND
    """Supported processing schemes for saving data. See `DataSaveMode` enum for options."""
    sink_key: t.Optional[t.List[str]] = None
    """List of columns as unique key for upsert operations."""
    when_matched_filter: t.Optional[str] = None
    """`WHEN MATCHED AND {condition} THEN` in merge operation."""
    replace_query_kwargs: t.Optional[t.Dict[str, t.Any]] = None
    """Optional replace table properties."""

    SINK_ALIASES: t.ClassVar[t.Set[str]] = {
        "sink_path",
        "sink_table",
        "sink_collection",
        "sink_topic",
    }

    @field_validator("sink_key", mode="before")
    @classmethod
    def _normalize_sink_key_to_list(cls, v: t.Any) -> t.Optional[list[str]]:
        """
        Allows sink_key to be provided as a single string (including comma-separated)
        and normalizes it into a list of strings.
        """
        if isinstance(v, str):
            return [key.strip() for key in v.split(',')]
        return v

    @model_validator(mode='before')
    @classmethod
    def _normalize_and_validate_sink(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data  # Let Pydantic handle the basic type error

        # Check for mutual exclusivity
        provided_aliases = cls.SINK_ALIASES.intersection(data.keys())
        if len(provided_aliases) > 1:
            raise ValueError(
                f"Cannot set these sink properties at once: {', '.join(sorted(list(provided_aliases)))}."
            )
        
        # A sink is optional in some cases (like preview), so we don't check for its existence here.
        if provided_aliases:
            alias_key = provided_aliases.pop()
            value = data.pop(alias_key)

            # Perform any specific normalization (like for topics)
            if alias_key == "sink_topic":
                value = normalize_topic_list_to_str_validator(value)

            # Populate the canonical field
            data['sink_path'] = value
        
        return data
    
    @model_validator(mode='after')
    def _validate_merge_requires_key(self) -> "JobSinkRelatedFields":
        if self.data_save_mode == DataSaveMode.MERGE:
            if not self.sink_key:
                raise ValueError(f"`sink_key` must be provided when `data_save_mode` is '{DataSaveMode.MERGE}'.")
        return self
    
    def get_sink_path(self):
        return self.sink_path
    
class SeaTunnelFields(BaseModel):
    seatunnel_transform: t.Optional[t.Union[t.Dict[str, t.Any], t.List[t.Dict[str, t.Any]]]] = None
    """transform parameters for SeaTunnel."""
    seatunnel_env: t.Optional[t.Dict[str, StringOrInt]] = {}
    """engine optional parameters for SeaTunnel."""

class JobPropertiesModel(
    BaseConfig,
    JobSourceRelatedFields,
    JobSinkRelatedFields,
    SeaTunnelFields
):
    job_id: t.Optional[t.Annotated[str, constr(strip_whitespace=True, min_length=1)]] = None
    """A unique, stable, and human-readable identifier for this job."""
    job_mode: t.Optional[JobMode] = JobMode.BATCH
    """Whether the task is in BATCH or STREAMING mode."""
    spark_submit_options: t.Optional[t.Dict[str, CliOptionValue]] = {}
    """A dictionary of parameters when submitting a Spark application using the `spark-submit` command."""

    @model_validator(mode='before')
    @classmethod
    def _default_incremental_for_streaming(cls, data: t.Any) -> t.Any:
        """
        Defaults data_load_mode to INCREMENTAL if job_mode is STREAMING and
        data_load_mode is not explicitly set.
        """
        if isinstance(data, dict):
            job_mode = JobMode.from_string(data.get("job_mode", "BATCH"))
            
            if job_mode == JobMode.STREAMING and "data_load_mode" not in data:
                logger.info("Defaulting data_load_mode to INCREMENTAL for STREAMING job.")
                data["data_load_mode"] = DataLoadMode.INCREMENTAL
        return data

    @model_validator(mode='after')
    def _validate_incremental_requires_job_id(self) -> "JobPropertiesModel":
        if self.data_load_mode == DataLoadMode.INCREMENTAL:
            if not self.job_id:
                raise ValueError(f"`job_id` must be provided when `data_load_mode` is '{DataLoadMode.INCREMENTAL}'.")
        return self
    
def job_properties_validator(value: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"Job properties must be a dictionary, not {type(value)}")

    try:
        job_props_model = JobPropertiesModel(**value)
        return job_props_model.model_dump(exclude_none=True, by_alias=False)

    except ValidationError as e:
        error_messages = []
        # To avoid redundant generic type errors if multiple specific errors exist for a field
        processed_locs_for_generic_type_error = set()

        for error in e.errors():
            loc_tuple = error["loc"]
            # Construct location string, e.g., "field_name" or "parent_field.child_key"
            loc_str = ".".join(map(str, loc_tuple))

            if error["type"] == "extra_forbidden":
                unknown_key = loc_tuple[0] if loc_tuple else "unknown_field_name"
                error_messages.append(f"Unknown property '{unknown_key}'.")
            elif loc_str == "data_save_mode" and error["type"] == "enum_membership":
                invalid_input_value = error['input']
                allowed_enum_members = ", ".join(DataSaveMode._member_names_)
                error_messages.append(
                    f"Unsupported save mode: {invalid_input_value}. Must be either of {allowed_enum_members}."
                )
            elif error["type"] == "value_error": # Catches ValueErrors from custom validators
                error_messages.append(error["msg"])
            # Generic message for type mismatches on a top-level field
            elif len(loc_tuple) == 1 and loc_str not in processed_locs_for_generic_type_error:
                error_messages.append(
                    f"Unsupported value type: {type(error['input']).__name__} for property '{loc_str}'."
                )
                processed_locs_for_generic_type_error.add(loc_str)
            else:
                # Fallback for other Pydantic errors (e.g., nested structures, complex types not caught above)
                error_messages.append(f"Error at '{loc_str}': {error['msg']}")
        
        # Deduplicate and sort messages for consistent error reporting
        final_error_message = "; ".join(sorted(list(set(error_messages))))
        raise ConfigError(final_error_message) from e


class TransformationConfig(BaseConfig):
    name: str
    options: t.Dict[str, t.Any] = {}


def transformation_pipeline_validator(value: t.List[t.Union[str, t.Dict[str, t.Any]]]) -> t.List[TransformationConfig]:
    if not isinstance(value, list):
        raise ConfigError(f"Transformation pipeline must be a list, not {type(value)}")

    result = []
    for transform in value:
        if isinstance(transform, str):
            result.append(TransformationConfig(name=transform))
        elif isinstance(transform, dict):
            try:
                result.append(TransformationConfig.parse_obj(transform))
            except ValidationError:
                for name in transform:
                    options = transform[name] or {}
                    if not isinstance(options, dict):
                        raise ConfigError(f"Transformation options must be a dictionary, not {type(options)}")
                    
                    result.append(TransformationConfig(name=str(name), options=options))
        else:
            raise ConfigError(f"Unrecognized config: {transform}")

    return result