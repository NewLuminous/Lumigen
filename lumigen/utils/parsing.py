from __future__ import annotations
import typing as t
import re
from sqlglot import exp, parse_one, ParseError, Dialect

if t.TYPE_CHECKING:
    from core._typing import TableName
    from sqlglot import DialectType


def extract_params(string: str) -> t.List[str]:
    match = re.search(r'([<{(\[])([^>}\])]*)([}>)\]])', string)
    if match:
        content = match.group(2)
        if content:
            return [item.strip() for item in content.split(',')]
    return []

def extract_name_wo_params(string: str) -> t.List[str]:
    match = re.match(r'^(.*?)([<{(\[])', string)
    if match:
        return match.group(1).strip()
    return string.strip()

def parse_into_select_exp(query: t.Union[exp.Expression, str], dialect: t.Optional[str] = None) -> exp.Select:
    """
    Parses or converts a variety of inputs into a SQLGlot SELECT expression.

    Args:
        query: The input to parse, which can be a SQL string or a SQLGlot expression.
        dialect: The preferred SQL dialect to use for parsing. If parsing with this
                 dialect fails, a fallback attempt is made without it.

    Returns:
        A `sqlglot.exp.Select` object.

    Raises:
        TypeError: If the input expression is not a `Select` or `Table`, or if the
                   input is not a string or Expression.
        ValueError: If the input string cannot be parsed into a `Select` or `Table`
                    expression, even after the fallback attempt.
    """
    parsed_expression: t.Optional[exp.Expression] = None
    if isinstance(query, str):
        if dialect:
            try:
                parsed_expression = parse_one(query, dialect=dialect)
            except ParseError:
                pass  # Fall through to the generic parser

        if parsed_expression is None:
            try:
                parsed_expression = parse_one(query)
            except ParseError as e:
                raise ValueError(f"Failed to parse the provided SQL string: '{query}'") from e
        
        query = parsed_expression

    if isinstance(query, exp.Select):
        return query
    
    if isinstance(query, exp.Table):
        return exp.select("*").from_(query)

    if isinstance(query, exp.Expression):
        raise TypeError(
            f"Cannot convert expression of type '{type(query).__name__}' to a SELECT statement. "
            "Only Select and Table expressions are supported."
        )
    else:
        raise TypeError(
            f"Unsupported input type: '{type(query).__name__}'. "
            "Input must be a SQL string or a SQLGlot Expression."
        )

def extract_main_table(query: t.Union[exp.Expression, str],
                       dialect: DialectType = None,
                       quoted: t.Optional[bool] = None,
                       ) -> str:
    """
    Tries to extract the main table from a SQL query.

    This function robustly finds the first table in the FROM clause,
    handling simple joins and ignoring tables within subqueries or CTEs.

    Returns:
        The normalized name of the main table as a string
    """
    if not isinstance(query, exp.Expression):
        if dialect and Dialect.get(dialect):
            select_exp = parse_one(query, dialect=dialect, into=exp.Select)
        else:
            select_exp = parse_one(query, into=exp.Select)
    else:
        select_exp = query

    from_exp = select_exp.find(exp.From) or select_exp
    table_exp = from_exp.find(exp.Table) or from_exp
    if isinstance(table_exp, exp.Table):
        table_name = table_exp
    else:
        table_name = table_exp.find(exp.Identifier)

    if not isinstance(table_name, (exp.Identifier, exp.Table)):
        raise ValueError(f"Could not extract a main table from the query: {str(query)[:100]}...")
    if quoted is not None:
        for identifier_exp in table_name.find_all(exp.Identifier):
            identifier_exp.set("quoted", quoted)

    return table_name.sql()

def dialect_insensitive_tbl_name(table_name: TableName, from_dialect: str) -> str:
    """
    Normalizes a table name by parsing it with a given dialect and then
    rendering it back to a standard SQL string. This removes any dialect-specific
    quoting or casing.
    
    Args:
        table_name: The table name string to normalize.
        from_dialect: The SQL dialect of the input table name.

    Returns:
        A normalized string representation of the table name.
    """
    if not table_name:
        return table_name
        
    if isinstance(table_name, exp.Expression):
        return table_name.sql()

    try:
        table_exp: exp.Table = parse_one(str(table_name), read=from_dialect, into=exp.Table)
        return table_exp.sql()
    except ValueError:
        return str(table_name)