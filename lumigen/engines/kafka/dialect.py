import re
import typing as t
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.errors import ParseError
from sqlglot.parser import Parser
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType

# A regex to validate Kafka topic names as per the rules.
# Allows alphanumeric, '.', '_', '-' and a length of 1-249.
TOPIC_NAME_REGEX = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")

ALLOWED_KAFKA_FIELDS = {
    "key", "value", "topic", "partition", "offset",
    "timestamp", "timestamptype", "headers",
}


class Kafka(Dialect):
    """
    The Kafka custom dialect for SQLGlot.

    This dialect treats Kafka topics as tables and enforces Kafka-specific rules:
    - `TOPIC` and `TABLE` are interchangeable keywords for DDL statements.
    - Unquoted identifiers must follow Kafka topic naming conventions (alphanumeric, '.', '_', '-').
    - Quoted identifiers can represent a comma-separated list of topics.
    - Projections (`SELECT` clauses) are restricted to a fixed set of Kafka message fields.
    - `CREATE TABLE` statements only allow `PARTITIONS` and `REPLICAS` properties with positive integer values.
    """
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    class Tokenizer(Tokenizer):
        IDENTIFIERS = ['"']
        VAR_SINGLE_TOKENS = set(Tokenizer.VAR_SINGLE_TOKENS) | {"-"}

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "TOPIC": TokenType.TABLE,
            "PARTITIONS": TokenType.VAR,
            "REPLICAS": TokenType.VAR,
        }

    class Parser(Parser):
        def _validate_topic_name(self, name: str) -> None:
            """Validates a single topic name against Kafka's naming rules."""
            if not TOPIC_NAME_REGEX.match(name):
                raise ParseError(
                    f"Invalid Kafka topic name: '{name}'. Topic names must contain only ASCII "
                    "alphanumerics, '.', '_', or '-', and be between 1 and 249 characters long."
                )

        def _parse_table(self, **kwargs) -> t.Optional[exp.Expression]:
            """
            Overrides the table parser to validate topic names.
            - For unquoted identifiers, it checks against the topic name regex.
            - For quoted identifiers, it splits by comma and validates each part.
            """
            table_expr = super()._parse_table(**kwargs)

            if table_expr and isinstance(table_expr, exp.Table):
                identifier = table_expr.this
                if isinstance(identifier, exp.Identifier):
                    if identifier.quoted:
                        topic_names = [
                            name.strip() for name in identifier.this.split(",")
                        ]
                        if not topic_names or any(not name for name in topic_names):
                            raise ParseError("Quoted topic identifier cannot be empty or contain empty names after splitting by comma.")
                        for topic_name in topic_names:
                            self._validate_topic_name(topic_name)
                    else:
                        self._validate_topic_name(identifier.this)

            return table_expr

        def _parse_select(self, **kwargs) -> t.Optional[exp.Expression]:
            """
            Overrides the SELECT parser to validate that only allowed Kafka fields are selected
            WHEN querying directly from a topic. This check is skipped for subqueries.
            """
            select_expr = super()._parse_select(**kwargs)

            if not isinstance(select_expr, exp.Select):
                return select_expr

            # Determine if this SELECT statement is querying a derived table (like a subquery).
            # If so, we do not apply the strict Kafka field validation.
            is_querying_derived_table = False
            if select_expr.args.get("from_") or select_expr.args["from"]:
                source = t.cast("exp.From", select_expr.args.get("from_") or select_expr.args["from"]).this
                if not isinstance(source, exp.Table):
                    is_querying_derived_table = True
            
            # Also check joins
            for join in select_expr.args.get("joins", []):
                if not isinstance(join.this, exp.Table):
                    is_querying_derived_table = True
                    break
            
            if is_querying_derived_table:
                return select_expr # Skip validation for queries on derived tables

            # This is a direct query on a topic, so we validate the fields.
            for projection in select_expr.expressions:
                if isinstance(projection, exp.Star):
                    continue

                column = projection.this if isinstance(projection, exp.Alias) else projection
                
                if isinstance(column, exp.Column):
                    field_name = column.this.this.lower()
                    if field_name not in ALLOWED_KAFKA_FIELDS:
                        raise ParseError(
                            f"Invalid field '{column.this.this}'. When querying a topic directly, "
                            f"allowed fields are: {', '.join(sorted(ALLOWED_KAFKA_FIELDS))}"
                        )
            
            return select_expr

        def _parse_create(self) -> t.Union[exp.Create, exp.Command]:
            """
            Overrides the CREATE parser to validate properties for `CREATE TABLE`.
            """
            create_expr = super()._parse_create()

            if not isinstance(create_expr, exp.Create) or create_expr.kind != "TABLE":
                return create_expr

            properties = create_expr.args.get("properties")
            if properties:
                allowed_properties = {"PARTITIONS", "REPLICAS"}
                for prop in properties.expressions:
                    if not isinstance(prop, exp.Property):
                        raise ParseError(f"Unexpected expression in WITH clause: {prop.sql(dialect='kafka')}")
                        
                    prop_name = prop.this.name.upper()
                    if prop_name not in allowed_properties:
                        raise ParseError(
                            f"Invalid property '{prop.this.name}'. "
                            f"Only PARTITIONS and REPLICAS are allowed."
                        )

                    prop_value = prop.args.get("value")
                    if not (
                        isinstance(prop_value, exp.Literal)
                        and prop_value.is_number
                        and prop_value.to_py() > 0
                        and isinstance(prop_value.to_py(), int)
                    ):
                        raise ParseError(
                            f"Property '{prop_name}' must be a positive integer."
                        )
            
            return create_expr
        
    class Generator(Generator):
        RESERVED_KEYWORDS = set()

        def identifier_sql(self, expression: exp.Identifier) -> str:
            """
            Custom identifier generation for the Kafka dialect.
            """
            name = expression.name
            
            # The name MUST be quoted if:
            # 1. The original SQL had it quoted.
            # 2. The generator is configured to quote everything (`identify=True`).
            # 3. It's a reserved keyword for our dialect.
            # 4. It does NOT match our valid unquoted topic name regex.
            
            should_quote = (
                expression.quoted
                or (self.identify is True or self.identify == 'always')
                or (self.identify == 'safe' and self.dialect.case_sensitive(name))
                or name.upper() in self.RESERVED_KEYWORDS
                or not TOPIC_NAME_REGEX.match(name)
            )

            if should_quote:
                escaped_name = name.replace(self._identifier_end, self._escaped_identifier_end)
                return f"{self._identifier_start}{escaped_name}{self._identifier_end}"
            
            return name