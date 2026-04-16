from __future__ import annotations
import typing as t
import functools
from collections import defaultdict
from sqlglot import exp, parse_one, ParseError, Dialect

if t.TYPE_CHECKING:
    from core._typing import TableName
    from sqlglot import DialectType


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
    
def identifier_requires_quoting(name: str, dialect_name: str, identify_setting: t.Union[str, bool] = "safe") -> bool:
    """
    Determines if a given name string would be quoted for a given dialect.

    Args:
        name: The string name to check.
        dialect_name: The name of the dialect (e.g., "kafka", "postgres", "bigquery").
        identify_setting: The 'identify' setting passed to the Generator.
                          Can be False, True, 'always', or 'safe'.

    Returns:
        True if the name would be quoted, False otherwise.
    """
    # 1. Instantiate the dialect and a generator with the desired settings.
    dialect = Dialect.get_or_raise(dialect_name)
    generator = dialect.generator(identify=identify_setting)

    # 2. Create a dummy Identifier expression. It's crucial to start with `quoted=False`
    #    to test the dialect's default behavior for an unquoted name.
    identifier_node = exp.to_identifier(name, quoted=False)

    # 3. Generate the SQL for this identifier. This invokes the full logic of
    #    the dialect's `generator_class.identifier_sql` method.
    rendered_string = generator.sql(identifier_node)

    # 4. Check if the output is actually quoted.
    is_quoted = rendered_string.startswith(dialect.IDENTIFIER_START) and \
                rendered_string.endswith(dialect.IDENTIFIER_END)

    if not is_quoted:
        return False

    # 5. As a final sanity check (restoring your good point), ensure the content
    #    inside the quotes is the original name. This prevents edge cases where a
    #    name might coincidentally start/end with quote characters.
    unquoted_content = rendered_string[len(dialect.IDENTIFIER_START):-len(dialect.IDENTIFIER_END)]
    
    # The generator might escape the closing identifier char (e.g., "" for "), so we unescape it.
    escaped_end = dialect.IDENTIFIER_END * 2
    original_end = dialect.IDENTIFIER_END
    unquoted_content = unquoted_content.replace(escaped_end, original_end)

    return unquoted_content == name

def get_conjuncts(node):
    """Recursively extract individual conditions connected by AND."""
    if not node:
        return []
    if isinstance(node, exp.And):
        return get_conjuncts(node.left) + get_conjuncts(node.right)
    if isinstance(node, exp.Paren):
        return get_conjuncts(node.this)
    return [node]

class TablePushdownExtractor:
    # Result Key Constants
    TABLE = "table"
    TABLE_NODE = "table_node"
    ALIAS = "alias"
    CONDITION = "condition_sql"
    CONDITION_NODE = "condition_node"
    _CONDITIONS = "_conditions"

    def __init__(self, parsed_ast: exp.Expression):
        """
        Pre-computes and indexes all tables by their local query scope in a single pass.
        """
        self.parsed = parsed_ast
        self.scope_types = (exp.Select, exp.Update, exp.Delete)
        self.tables_by_scope: t.Dict[t.Any, t.List[exp.Table]] = defaultdict(list)
        self.tables_data =[]
        self.node_to_data = {}
        
        # Find all tables and group them by their immediate parent scope
        for table_exp in parsed_ast.find_all(exp.Table):
            scope = table_exp.find_ancestor(*self.scope_types)
            scope_id = id(scope) if scope else 'root'
            self.tables_by_scope[scope_id].append(table_exp)

            data = {
                self.TABLE_NODE: table_exp,
                self.TABLE: table_exp.name,
                self.ALIAS: table_exp.alias_or_name,
                self._CONDITIONS: []
            }
            self.tables_data.append(data)
            self.node_to_data[id(table_exp)] = data

    def get_table_for_column(self, column: exp.Column) -> t.Optional[exp.Table]:
        """Resolves the table for a column using the pre-computed scope index."""
        if not isinstance(column, exp.Column):
            return None

        col_table = column.table 
        current_scope = column.find_ancestor(*self.scope_types)
        
        # Traverse scopes from innermost to outermost
        while True:
            scope_id = id(current_scope) if current_scope else 'root'
            tables_in_scope = self.tables_by_scope.get(scope_id, [])

            if col_table:
                # CASE 1: Qualified Column (e.g., transactions.amount or t.amount)
                for table_exp in tables_in_scope:
                    if col_table in (table_exp.alias_or_name, table_exp.name):
                        return table_exp
            else:
                # CASE 2: Unqualified Column (e.g., amount)
                if len(tables_in_scope) == 1:
                    return tables_in_scope[0]
                elif len(tables_in_scope) > 1:
                    # Ambiguous
                    return None
                    
            # Try Outer Scope (Correlated subqueries)
            if current_scope and current_scope.parent:
                current_scope = current_scope.parent.find_ancestor(*self.scope_types)
            else:
                break
                
        return None
    
    def extract(self) -> t.List[t.Dict[str, t.Any]]:
        for select_node in self.parsed.find_all(exp.Select):
            # Gather candidate conditions from WHERE and ON clauses
            candidates = []

            # WHERE clause: safe to push to any referenced table in this scope
            where_clause = select_node.args.get("where")
            if where_clause:
                for c in get_conjuncts(where_clause.this):
                    candidates.append({"condition": c, "allowed_nodes": None})

            # JOIN ON clauses: extract conditions, but enforce outer-join safety rules
            join: exp.Join
            for join in select_node.args.get("joins") or []:
                on_clause = join.args.get("on")
                if on_clause:
                    side = (join.args.get("side") or "INNER").upper()
                    allowed_nodes = None
                    
                    if side == "LEFT":
                        # In A LEFT JOIN B, you can only safely push down ON conditions to B.
                        # Pushing to A restricts the left side, breaking the OUTER JOIN semantics.
                        if isinstance(join.this, exp.Table):
                            allowed_nodes = {id(join.this)}
                    elif side in ("RIGHT", "FULL"):
                        # Too dangerous/complex to push down ON conditions for RIGHT/FULL blindly.
                        continue
                        
                    for c in get_conjuncts(on_clause):
                        candidates.append({"condition": c, "allowed_nodes": allowed_nodes})

            # Resolve Candidates via the index
            for candidate in candidates:
                condition: exp.Condition = candidate["condition"]
                allowed_nodes = candidate["allowed_nodes"]

                # Subqueries are inherently multi-table operations; skip them
                if condition.find((exp.Select, exp.Subquery)):
                    continue

                columns = list(condition.find_all(exp.Column))
                if not columns:
                    continue

                target_table_node = None
                is_pushable = True
                
                # Check if ALL columns in this condition resolve to the EXACT SAME table
                for col in columns:
                    col_table_node = self.get_table_for_column(col)
                    
                    if not col_table_node:
                        is_pushable = False # Column is ambiguous or untraceable
                        break
                        
                    if target_table_node is None:
                        target_table_node = col_table_node
                    elif target_table_node is not col_table_node:
                        is_pushable = False # Condition references multiple tables (e.g. JOIN)
                        break
                        
                # If the condition exclusively applies to one table, save it
                if is_pushable and target_table_node:
                    if allowed_nodes is not None and id(target_table_node) not in allowed_nodes:
                        continue
                        
                    self.node_to_data[id(target_table_node)][self._CONDITIONS].append(condition)

        # Output Formatting & Column prefix stripping
        for data in self.tables_data:
            conds = data[self._CONDITIONS]
            if conds:
                combined = functools.reduce(lambda x, y: exp.and_(x, y), conds)
                clean_condition = combined.copy()

                for col in clean_condition.find_all(exp.Column):
                    col.set("table", None)
                    col.set("db", None)
                    col.set("catalog", None)
                
                data[self.CONDITION] = clean_condition.sql()
                data[self.CONDITION_NODE] = clean_condition
            else:
                data[self.CONDITION] = None
                data[self.CONDITION_NODE] = None
                
            del data[self._CONDITIONS]
            
        return self.tables_data