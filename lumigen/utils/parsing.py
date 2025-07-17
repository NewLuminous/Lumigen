from __future__ import annotations
import typing as t
import re
from sqlglot import exp, parse_one

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


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

def extract_main_table(query: t.Union[exp.Expression, str], quoted: t.Optional[bool] = None) -> str:
    """
    Tries to extract the main table from a SQL query.

    This function robustly finds the first table in the FROM clause,
    handling simple joins and ignoring tables within subqueries or CTEs.

    Args:
        query: A SQL query string or a sqlglot Expression.

    Returns:
        The normalized name of the main table as a string
    """
    if not isinstance(query, exp.Expression):
        select_exp = parse_one(query, into=exp.Select)
    else:
        select_exp = query

    from_exp = select_exp.find(exp.From) or select_exp
    table_exp = from_exp.find(exp.Table) or from_exp
    table_name = table_exp.find(exp.Identifier) or table_exp

    if not isinstance(table_name, (exp.Identifier, exp.Table)):
        raise ValueError(f"Could not extract a main table from the query: {str(query)[:100]}...")
    if quoted is not None and isinstance(table_name, exp.Identifier):
        table_name.args["quoted"] = quoted
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