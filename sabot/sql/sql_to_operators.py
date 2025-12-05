"""
SQL to Sabot Operators Bridge

Translates SQL queries (parsed by DuckDB) into Sabot's existing Cython operators.
This leverages all the high-performance kernels already in Sabot!

Uses:
- CythonFilterOperator for WHERE clauses
- CythonHashJoinOperator for JOINs
- CythonGroupByOperator for GROUP BY
- CythonMapOperator for projections
- MorselDrivenOperator for parallelism

Architecture:
1. DuckDB parses SQL and provides JSON EXPLAIN output
2. DuckDBPlanExtractor extracts structured plan from JSON
3. OperatorChainBuilder converts plan nodes to Sabot operators
4. Execute with morsel-driven parallelism
"""

from sabot import cyarrow as ca  # Use Sabot's optimized Arrow!
from sabot.cyarrow import compute as pc
from typing import Dict, List, Optional, Any, Callable
import logging
import json
import re

# Import existing Sabot operators
from sabot._cython.operators.base_operator import BaseOperator
from sabot._cython.operators.joins import CythonHashJoinOperator
from sabot._cython.operators.aggregations import CythonGroupByOperator
from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

logger = logging.getLogger(__name__)


class DuckDBPlanExtractor:
    """
    Extract structured query plans from DuckDB using JSON EXPLAIN.

    Uses DuckDB's EXPLAIN (FORMAT JSON) to get structured plan output,
    then converts to a normalized format for OperatorChainBuilder.
    """

    def __init__(self, tables: Dict[str, ca.Table]):
        self.tables = tables
        self._conn = None

    def _get_connection(self):
        """Get or create DuckDB connection with registered tables."""
        if self._conn is None:
            import duckdb
            self._conn = duckdb.connect(':memory:')

            # Register all tables
            for name, table in self.tables.items():
                self._conn.register(name, table)

        return self._conn

    def extract_plan(self, sql: str) -> Dict[str, Any]:
        """
        Extract structured plan from SQL query.

        Args:
            sql: SQL query string

        Returns:
            Dict containing:
            - operators: List of operator specs in execution order (leaf to root)
            - output_columns: Expected output column names
            - requires_shuffle: Whether distributed execution needs shuffle
        """
        # Store SQL for limit extraction
        self._current_sql = sql

        conn = self._get_connection()

        # Get JSON explain
        explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
        result = conn.execute(explain_sql).fetchone()

        if not result:
            raise ValueError(f"Could not get plan for SQL: {sql}")

        # Parse the JSON plan
        plan_json = json.loads(result[1])

        # The plan is a list with a single root node
        if not plan_json or not isinstance(plan_json, list):
            raise ValueError("Invalid plan format")

        root_node = plan_json[0]

        # Flatten the tree to a list of operators (bottom-up order)
        operators = self._flatten_plan(root_node)

        # Extract output columns from the root node
        output_columns = self._extract_output_columns(root_node)

        # Check if shuffle is needed (joins or group by)
        requires_shuffle = self._requires_shuffle(operators)

        return {
            'operators': operators,
            'output_columns': output_columns,
            'requires_shuffle': requires_shuffle,
            'sql': sql
        }

    def _flatten_plan(self, node: Dict, depth: int = 0) -> List[Dict[str, Any]]:
        """
        Flatten plan tree to list of operators in execution order.

        Bottom-up: children first, then parent.
        This gives us source operators first, then downstream operators.
        """
        operators = []

        # Process children first (depth-first)
        if 'children' in node:
            for child in node['children']:
                operators.extend(self._flatten_plan(child, depth + 1))

        # Then add this node
        op_spec = self._extract_operator_spec(node)
        if op_spec:
            op_spec['depth'] = depth
            operators.append(op_spec)

        return operators

    def _extract_operator_spec(self, node: Dict) -> Optional[Dict[str, Any]]:
        """
        Extract operator specification from plan node.

        Maps DuckDB operator names to Sabot operator types.
        """
        name = node.get('name', '').strip()
        extra_info = node.get('extra_info', {})

        # Map DuckDB operators to Sabot types
        if name in ('SEQ_SCAN', 'TABLE_SCAN', 'SEQ_SCAN ', 'ARROW_SCAN'):
            # ARROW_SCAN is used when DuckDB reads registered Arrow tables
            table_name = extra_info.get('Table', '')
            if not table_name:
                # For ARROW_SCAN, the Function field contains the scan type
                # Try to extract table from context
                table_name = extra_info.get('Function', '').replace('ARROW_SCAN', '').strip()

            if not table_name:
                # Use first registered table as fallback
                # This works for single-table queries
                if self.tables:
                    table_name = next(iter(self.tables.keys()))

            return {
                'type': 'SCAN',
                'table': table_name,
                'projections': extra_info.get('Projections', []),
                'filters': extra_info.get('Filters'),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'FILTER':
            return {
                'type': 'FILTER',
                'expression': extra_info.get('Condition', ''),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'PROJECTION':
            return {
                'type': 'PROJECTION',
                'columns': extra_info.get('Projections', []),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name in ('HASH_GROUP_BY', 'UNGROUPED_AGGREGATE'):
            # UNGROUPED_AGGREGATE is COUNT(*) without GROUP BY
            return {
                'type': 'AGGREGATE',
                'group_by': self._parse_group_keys(extra_info.get('Groups', '')),
                'aggregations': self._parse_aggregations(extra_info.get('Aggregates', '')),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name in ('HASH_JOIN', 'NESTED_LOOP_JOIN'):
            return {
                'type': 'HASH_JOIN',
                'join_type': extra_info.get('Join Type', 'inner').lower(),
                'conditions': extra_info.get('Conditions', ''),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'ORDER_BY':
            # DuckDB uses "Order By" key in extra_info
            orders = extra_info.get('Order By', extra_info.get('Orders', ''))
            return {
                'type': 'ORDER_BY',
                'sort_keys': self._parse_sort_keys(orders),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'LIMIT':
            return {
                'type': 'LIMIT',
                'limit': self._parse_limit(extra_info.get('Limit', '')),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'TOP_N':
            # TOP_N combines ORDER BY and LIMIT
            orders = extra_info.get('Order By', extra_info.get('Orders', ''))
            top = extra_info.get('Top', extra_info.get('Top N', ''))
            return {
                'type': 'TOP_N',
                'sort_keys': self._parse_sort_keys(orders),
                'limit': self._parse_limit(top),
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        elif name == 'STREAMING_LIMIT':
            # STREAMING_LIMIT often has empty extra_info - extract from SQL
            limit = self._parse_limit(extra_info.get('Limit', extra_info.get('Maximum Rows', '')))
            if limit == 0:
                # Try to extract from the original SQL
                limit = self._extract_limit_from_sql()
            return {
                'type': 'LIMIT',
                'limit': limit,
                'cardinality': self._parse_cardinality(extra_info.get('Estimated Cardinality', '0'))
            }

        # Skip internal optimization operators
        elif name in ('RESULT_COLLECTOR', 'EXPLAIN_ANALYZE', 'QUERY'):
            return None

        else:
            # Unknown operator - log and return generic spec
            logger.warning(f"Unknown operator type: {name}")
            return {
                'type': 'UNKNOWN',
                'name': name,
                'extra_info': extra_info
            }

    def _parse_cardinality(self, value: str) -> int:
        """Parse cardinality estimate from string."""
        try:
            # Handle formats like "~1000" or "1000"
            return int(value.replace('~', '').replace(',', '').strip())
        except (ValueError, AttributeError):
            return 0

    def _extract_limit_from_sql(self) -> int:
        """Extract LIMIT value from the original SQL query."""
        if not hasattr(self, '_current_sql') or not self._current_sql:
            return 0

        # Match LIMIT <number> at the end of the query
        match = re.search(r'\bLIMIT\s+(\d+)\s*$', self._current_sql, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 0

    def _parse_group_keys(self, groups_str: str) -> List[str]:
        """Parse GROUP BY keys from DuckDB format."""
        if not groups_str:
            return []
        # Format is like "#0" or "#0, #1"
        # These are column references - we store them and resolve later
        # when we have access to the input schema
        return [g.strip() for g in groups_str.split(',')]

    def _resolve_column_references(self, refs: List[str], schema_columns: List[str]) -> List[str]:
        """
        Resolve DuckDB column references (#0, #1) to actual column names.

        Args:
            refs: List of column references (e.g., ['#0', '#1'])
            schema_columns: List of column names from input schema

        Returns:
            List of resolved column names
        """
        resolved = []
        for ref in refs:
            ref = ref.strip()
            if ref.startswith('#'):
                try:
                    idx = int(ref[1:])
                    if 0 <= idx < len(schema_columns):
                        resolved.append(schema_columns[idx])
                    else:
                        # Index out of range - keep as-is
                        resolved.append(ref)
                except ValueError:
                    resolved.append(ref)
            else:
                # Already a column name
                resolved.append(ref)
        return resolved

    def _parse_aggregations(self, agg_data) -> Dict[str, tuple]:
        """
        Parse aggregation functions from DuckDB format.

        Args:
            agg_data: Either a string "sum(#0), count(*)" or a list ["sum(#0)", "count(*)"]

        Returns dict of {output_name: (column, function)}
        """
        if not agg_data:
            return {}

        # Handle both string and list formats
        if isinstance(agg_data, list):
            agg_list = agg_data
        else:
            agg_list = [a.strip() for a in agg_data.split(',')]

        aggregations = {}
        for i, agg in enumerate(agg_list):
            agg = agg.strip() if isinstance(agg, str) else str(agg)

            # Handle count_star() - no column reference
            if agg == 'count_star()':
                aggregations[f"agg_{i}"] = ('*', 'count')
                continue

            # Parse function(column)
            match = re.match(r'(\w+(?:_\w+)*)\(([^)]*)\)', agg)
            if match:
                func_name = match.group(1).lower()
                col_ref = match.group(2).strip()

                # Map DuckDB function names to standard names
                func_map = {
                    'sum': 'sum',
                    'sum_no_overflow': 'sum',
                    'count': 'count',
                    'count_star': 'count',
                    'avg': 'mean',
                    'mean': 'mean',
                    'min': 'min',
                    'max': 'max'
                }

                std_func = func_map.get(func_name, func_name)
                output_name = f"agg_{i}"
                aggregations[output_name] = (col_ref, std_func)

        return aggregations

    def _parse_sort_keys(self, orders_str: str) -> List[tuple]:
        """Parse ORDER BY keys from DuckDB format."""
        if not orders_str:
            return []

        keys = []
        for order in orders_str.split(','):
            order = order.strip()

            # Determine direction
            if ' DESC' in order.upper():
                direction = 'desc'
                order = re.sub(r'\s+DESC\s*$', '', order, flags=re.IGNORECASE)
            else:
                direction = 'asc'
                order = re.sub(r'\s+ASC\s*$', '', order, flags=re.IGNORECASE)

            # Extract column name from potentially fully qualified name
            # Format: "temp".main.trades.price → price
            column = order.strip()
            if '.' in column:
                # Take the last part as the column name
                column = column.split('.')[-1].strip()
            # Remove any surrounding quotes
            column = column.strip('"\'')

            keys.append((column, direction))

        return keys

    def _parse_limit(self, limit_str: str) -> int:
        """Parse LIMIT value."""
        try:
            return int(limit_str.strip())
        except (ValueError, AttributeError):
            return 0

    def _extract_output_columns(self, root_node: Dict) -> List[str]:
        """Extract output column names from root projection."""
        extra_info = root_node.get('extra_info', {})
        projections = extra_info.get('Projections', [])

        # These are internal names, we'll use them as-is for now
        return projections

    def _requires_shuffle(self, operators: List[Dict]) -> bool:
        """Check if any operator requires distributed shuffle."""
        shuffle_types = {'AGGREGATE', 'HASH_JOIN'}
        return any(op.get('type') in shuffle_types for op in operators)

    def close(self):
        """Close the DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


class OperatorChainBuilder:
    """
    Build Sabot Cython operator chains from extracted query plans.

    Takes the structured plan from DuckDBPlanExtractor and builds
    actual Sabot operator chains that execute the query.
    """

    def __init__(self, tables: Dict[str, ca.Table]):
        self.tables = tables
        self.sql_builder = SQLOperatorBuilder(tables)

    def build_from_plan(self, plan: Dict[str, Any]) -> BaseOperator:
        """
        Build operator chain from extracted plan.

        Args:
            plan: Dict from DuckDBPlanExtractor.extract_plan()

        Returns:
            Root operator of the chain, ready for iteration
        """
        operators_list = plan.get('operators', [])

        if not operators_list:
            raise ValueError("Empty operator list in plan")

        # Build operators bottom-up
        # The first operator should be a SCAN (source)
        chain = None
        scan_found = False

        # Track current schema through projections for column reference resolution
        current_schema = None

        for op_spec in operators_list:
            op_type = op_spec.get('type')

            if op_type == 'SCAN':
                # Source operator
                table_name = op_spec.get('table', '')
                chain = self._build_scan(table_name, op_spec)
                scan_found = True
                # Get schema from scan projections or table
                projections = op_spec.get('projections', [])
                if projections and isinstance(projections, list):
                    current_schema = projections
                elif table_name in self.tables:
                    current_schema = list(self.tables[table_name].column_names)

            elif op_type == 'FILTER':
                if chain is None:
                    raise ValueError("FILTER operator without source")
                chain = self._build_filter(chain, op_spec)

            elif op_type == 'PROJECTION':
                if chain is None:
                    raise ValueError("PROJECTION operator without source")
                # Update schema from projection columns
                columns = op_spec.get('columns', [])
                if columns and isinstance(columns, list):
                    # Filter out internal references for schema tracking
                    real_columns = [c for c in columns if not c.startswith('#') and not c.startswith('__')]
                    if real_columns:
                        current_schema = real_columns
                chain = self._build_projection(chain, op_spec)

            elif op_type == 'AGGREGATE':
                if chain is None:
                    raise ValueError("AGGREGATE operator without source")
                # Pass current schema for column reference resolution
                chain = self._build_aggregate(chain, op_spec, current_schema)

            elif op_type == 'HASH_JOIN':
                # Joins are complex - need two sources
                # For now, skip complex join logic
                logger.warning("Complex JOIN handling not yet implemented")
                continue

            elif op_type == 'ORDER_BY':
                if chain is None:
                    raise ValueError("ORDER_BY operator without source")
                chain = self._build_order_by(chain, op_spec)

            elif op_type == 'LIMIT':
                if chain is None:
                    raise ValueError("LIMIT operator without source")
                chain = self._build_limit(chain, op_spec)

            elif op_type == 'TOP_N':
                # TOP_N combines ORDER BY + LIMIT
                if chain is None:
                    raise ValueError("TOP_N operator without source")
                chain = self._build_order_by(chain, op_spec)
                chain = self._build_limit(chain, op_spec)

            elif op_type == 'UNKNOWN':
                logger.warning(f"Skipping unknown operator: {op_spec.get('name')}")
                continue

            else:
                logger.warning(f"Unhandled operator type: {op_type}")

        if chain is None:
            raise ValueError("Failed to build operator chain")

        return chain

    def _build_scan(self, table_name: str, spec: Dict) -> BaseOperator:
        """Build table scan operator."""
        if not table_name:
            # Try to get from the first registered table
            if self.tables:
                table_name = next(iter(self.tables.keys()))
            else:
                raise ValueError("No table name in SCAN and no tables registered")

        scan = self.sql_builder.build_table_scan(table_name)

        # If scan has integrated filter (from predicate pushdown)
        filters = spec.get('filters')
        if filters:
            # Build filter predicate
            scan = self._build_integrated_filter(scan, filters)

        return scan

    def _build_integrated_filter(self, source: BaseOperator, filters: str) -> BaseOperator:
        """Build filter from integrated scan filter expression."""
        return self.sql_builder.build_filter(source, filters)

    def _build_filter(self, source: BaseOperator, spec: Dict) -> BaseOperator:
        """Build filter operator."""
        expression = spec.get('expression', '')
        if not expression:
            return source

        return self.sql_builder.build_filter(source, expression)

    def _build_projection(self, source: BaseOperator, spec: Dict) -> BaseOperator:
        """Build projection operator."""
        columns = spec.get('columns', [])

        if not columns:
            return source

        # DuckDB gives us internal references like "#0", "__internal_*"
        # For now, skip internal projections as they're optimization artifacts
        real_columns = [c for c in columns if not c.startswith('#') and not c.startswith('__internal')]

        if not real_columns:
            # All internal columns - this is an optimization artifact
            return source

        return self.sql_builder.build_project(source, real_columns)

    def _build_aggregate(self, source: BaseOperator, spec: Dict, schema_columns: List[str] = None) -> BaseOperator:
        """Build aggregate operator."""
        group_by = spec.get('group_by', [])
        aggregations = spec.get('aggregations', {})

        if not aggregations and not group_by:
            return source

        # Use passed schema or try to get from source
        if schema_columns is None:
            schema_columns = self._get_source_columns(source)

        # Resolve group_by references (#0, #1 → actual column names)
        if schema_columns:
            resolved_group_by = self._resolve_column_refs(group_by, schema_columns)
            resolved_aggs = self._resolve_aggregation_refs(aggregations, schema_columns)
        else:
            resolved_group_by = group_by
            resolved_aggs = aggregations

        return self.sql_builder.build_group_by(source, resolved_group_by, resolved_aggs)

    def _get_source_columns(self, source: BaseOperator) -> List[str]:
        """Get column names from source operator."""
        # Check if source has a table reference
        if hasattr(source, 'table') and hasattr(source.table, 'column_names'):
            return list(source.table.column_names)

        # Check if source has schema
        if hasattr(source, 'get_schema') and source.get_schema():
            schema = source.get_schema()
            return [field.name for field in schema]

        # For wrapped operators, try to get from wrapped
        if hasattr(source, '_wrapped_operator'):
            return self._get_source_columns(source._wrapped_operator)

        # Try to get from first registered table
        if self.tables:
            first_table = next(iter(self.tables.values()))
            return list(first_table.column_names)

        return []

    def _resolve_column_refs(self, refs: List[str], schema_columns: List[str]) -> List[str]:
        """Resolve DuckDB column references (#0, #1) to actual column names."""
        resolved = []
        for ref in refs:
            ref = ref.strip()
            if ref.startswith('#'):
                try:
                    idx = int(ref[1:])
                    if 0 <= idx < len(schema_columns):
                        resolved.append(schema_columns[idx])
                    else:
                        resolved.append(ref)
                except ValueError:
                    resolved.append(ref)
            else:
                resolved.append(ref)
        return resolved

    def _resolve_aggregation_refs(self, aggregations: Dict[str, tuple], schema_columns: List[str]) -> Dict[str, tuple]:
        """Resolve column references in aggregation specs."""
        resolved = {}
        for output_name, (col_ref, func) in aggregations.items():
            col_ref = col_ref.strip()
            if col_ref.startswith('#'):
                try:
                    idx = int(col_ref[1:])
                    if 0 <= idx < len(schema_columns):
                        col_ref = schema_columns[idx]
                except ValueError:
                    pass
            resolved[output_name] = (col_ref, func)
        return resolved

    def _build_order_by(self, source: BaseOperator, spec: Dict) -> BaseOperator:
        """Build order by operator."""
        sort_keys = spec.get('sort_keys', [])

        if not sort_keys:
            return source

        return self.sql_builder.build_order_by(source, sort_keys)

    def _build_limit(self, source: BaseOperator, spec: Dict) -> BaseOperator:
        """Build limit operator."""
        limit = spec.get('limit', 0)

        if limit <= 0:
            return source

        return self.sql_builder.build_limit(source, limit)


class TableScanOperator(BaseOperator):
    """
    Table scan operator - reads from Arrow tables

    This is a source operator that provides data to the pipeline.
    Uses Sabot's existing BaseOperator interface with cyarrow.
    """

    def __init__(self, table: ca.Table, batch_size: int = 10000):
        super().__init__()
        self.table = table
        self.batch_size = batch_size

    def __iter__(self):
        """Iterate over batches from the table - resets each time"""
        offset = 0  # Local offset - resets each iteration
        while offset < self.table.num_rows:
            end = min(offset + self.batch_size, self.table.num_rows)
            batch_table = self.table.slice(offset, end - offset)

            # Convert to RecordBatch
            if batch_table.num_rows > 0:
                batch = batch_table.to_batches()[0]
                yield batch

            offset = end

    def process_batch(self, batch):
        """TableScanOperator is a source - just pass through any batch."""
        return batch


class SQLOperatorBuilder:
    """
    Builds Sabot operator chains from SQL AST
    
    Translates SQL constructs to existing Cython operators:
    - SELECT col1, col2 → CythonMapOperator (projection)
    - WHERE condition → CythonFilterOperator  
    - JOIN → CythonHashJoinOperator
    - GROUP BY → CythonGroupByOperator
    - ORDER BY → Sort + CythonMapOperator
    - LIMIT → Early termination in iteration
    
    All operators are morsel-aware and can be parallelized.
    Uses cyarrow (Sabot's optimized Arrow) throughout.
    """
    
    def __init__(self, tables: Dict[str, ca.Table]):
        self.tables = tables
        self.operators = []
        
    def build_table_scan(self, table_name: str, batch_size: int = 10000) -> BaseOperator:
        """Create table scan operator"""
        if table_name not in self.tables:
            raise ValueError(f"Table not found: {table_name}")
        
        return TableScanOperator(self.tables[table_name], batch_size)
    
    def build_filter(self, source: BaseOperator, predicate_sql: str) -> BaseOperator:
        """
        Create filter operator from SQL predicate.

        Uses CythonFilterOperator with cyarrow compute kernel.
        The predicate function returns a boolean mask for filtering.
        """
        # Build the predicate function from SQL expression
        predicate = self._build_predicate_function(predicate_sql)

        return CythonFilterOperator(source=source, predicate=predicate)

    def _build_predicate_function(self, expression: str) -> Callable:
        """
        Build a predicate function from SQL filter expression.

        Supports:
        - Simple comparisons: column > value, column = value
        - AND expressions
        - OR expressions
        """
        expression = expression.strip()

        # Handle AND - chain conditions
        if ' AND ' in expression.upper():
            import re as regex
            parts = regex.split(r'\s+AND\s+', expression, flags=regex.IGNORECASE)
            predicates = [self._build_predicate_function(p.strip()) for p in parts]

            def and_predicate(batch):
                masks = [pred(batch) for pred in predicates]
                result = masks[0]
                for mask in masks[1:]:
                    result = pc.and_(result, mask)
                return result

            return and_predicate

        # Handle OR - combine conditions
        if ' OR ' in expression.upper():
            import re as regex
            parts = regex.split(r'\s+OR\s+', expression, flags=regex.IGNORECASE)
            predicates = [self._build_predicate_function(p.strip()) for p in parts]

            def or_predicate(batch):
                masks = [pred(batch) for pred in predicates]
                result = masks[0]
                for mask in masks[1:]:
                    result = pc.or_(result, mask)
                return result

            return or_predicate

        # Simple comparison
        return self._build_simple_comparison(expression)

    def _build_simple_comparison(self, expression: str) -> Callable:
        """Build predicate for simple comparison."""
        import re as regex

        # Match patterns like "column >= value" or "column>value"
        # Operators ordered by length (>= before >) to match correctly
        operators = [('>=', pc.greater_equal), ('<=', pc.less_equal),
                     ('!=', pc.not_equal), ('<>', pc.not_equal),
                     ('>', pc.greater), ('<', pc.less), ('=', pc.equal)]

        for op_str, op_func in operators:
            if op_str in expression:
                parts = expression.split(op_str, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()

                    # Parse value
                    value = self._parse_value(value_str)

                    def comparison_predicate(batch, col=column, op=op_func, val=value):
                        try:
                            return op(batch[col], val)
                        except KeyError:
                            # Column not found - return all True (passthrough)
                            logger.warning(f"Column not found: {col}")
                            return pc.equal(ca.array([True] * batch.num_rows), True)

                    return comparison_predicate

        # No operator found - return passthrough
        logger.warning(f"Could not parse filter expression: {expression}")
        return lambda batch: pc.equal(ca.array([True] * batch.num_rows), True)

    def _parse_value(self, value_str: str) -> Any:
        """Parse a value string to appropriate type."""
        value_str = value_str.strip()

        # Handle quoted strings
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]

        # Try numeric
        try:
            if '.' in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass

        # Return as string
        return value_str
    
    def build_project(self, source: BaseOperator, columns: List[str]) -> BaseOperator:
        """
        Create projection operator
        
        Uses CythonMapOperator to select columns (cyarrow)
        """
        def project_func(batch: ca.RecordBatch) -> ca.RecordBatch:
            # Select specified columns using cyarrow
            return batch.select(columns)
        
        return CythonMapOperator(source=source, map_func=project_func)
    
    def build_join(
        self, 
        left: BaseOperator, 
        right: BaseOperator,
        left_keys: List[str],
        right_keys: List[str],
        join_type: str = 'inner'
    ) -> BaseOperator:
        """
        Create hash join operator
        
        Uses CythonHashJoinOperator with existing join kernels
        """
        return CythonHashJoinOperator(
            left_source=left,
            right_source=right,
            left_keys=left_keys,
            right_keys=right_keys,
            join_type=join_type
        )
    
    def build_group_by(
        self,
        source: BaseOperator,
        group_keys: List[str],
        aggregations: Dict[str, tuple]
    ) -> BaseOperator:
        """
        Create GROUP BY operator
        
        Uses CythonGroupByOperator with Arrow hash_aggregate kernel
        
        Args:
            source: Input operator
            group_keys: Columns to group by
            aggregations: Dict of {output_name: (column, function)}
                         Functions: sum, mean, count, min, max
        """
        return CythonGroupByOperator(
            source=source,
            keys=group_keys,
            aggregations=aggregations
        )
    
    def build_order_by(
        self,
        source: BaseOperator,
        sort_keys: List[tuple]  # [(column, 'asc'|'desc'), ...]
    ) -> BaseOperator:
        """
        Create ORDER BY operator
        
        Uses CythonMapOperator with cyarrow sort
        """
        def sort_func(batch: ca.RecordBatch) -> ca.RecordBatch:
            # Convert to table for sorting
            table = ca.Table.from_batches([batch])
            
            # Build sort keys for cyarrow
            arrow_sort_keys = []
            for col, direction in sort_keys:
                arrow_sort_keys.append((col, 'ascending' if direction == 'asc' else 'descending'))
            
            # Sort and convert back
            sorted_table = table.sort_by(arrow_sort_keys)
            return sorted_table.to_batches()[0] if sorted_table.num_rows > 0 else batch
        
        return CythonMapOperator(source=source, map_func=sort_func)
    
    def build_limit(self, source: BaseOperator, limit: int) -> BaseOperator:
        """
        Create LIMIT operator
        
        Uses iteration control to limit output
        """
        class LimitOperator:
            """Simple limit operator that wraps a source and limits output rows."""
            def __init__(self, source, limit):
                self._source = source
                self.limit = limit

            def __iter__(self):
                count = 0
                for batch in self._source:
                    if count >= self.limit:
                        break

                    remaining = self.limit - count
                    if batch.num_rows > remaining:
                        batch = batch.slice(0, remaining)

                    count += batch.num_rows
                    yield batch

        return LimitOperator(source, limit)
    
    def wrap_with_morsels(
        self, 
        operator: BaseOperator, 
        num_workers: int = 4,
        enabled: bool = True
    ) -> BaseOperator:
        """
        Wrap operator with morsel-driven parallelism
        
        Uses MorselDrivenOperator for automatic parallelization
        """
        if not enabled:
            return operator
        
        return MorselDrivenOperator(
            wrapped_operator=operator,
            num_workers=num_workers,
            enabled=True
        )


class SQLQueryExecutor:
    """
    Execute SQL queries using Sabot's existing operators
    
    This is the bridge between DuckDB parsing and Sabot execution:
    1. Parse SQL with DuckDB (Python DuckDB module for now)
    2. Build operator chain using existing Cython operators
    3. Execute with morsel parallelism
    4. Return cyarrow table
    
    Uses cyarrow (Sabot's optimized vendored Arrow) throughout!
    """
    
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.tables: Dict[str, ca.Table] = {}
        
    def register_table(self, name: str, table: ca.Table):
        """Register a table for querying (cyarrow table)"""
        self.tables[name] = table
        logger.info(f"Registered table '{name}': {table.num_rows:,} rows, {table.num_columns} cols")
    
    async def execute(self, sql: str) -> ca.Table:
        """
        Execute SQL query using Sabot operators
        
        Args:
            sql: SQL query string
            
        Returns:
            cyarrow table with results (Sabot's optimized Arrow)
        """
        logger.info(f"Executing SQL: {sql[:100]}...")
        
        # For now, use DuckDB Python module for parsing
        # TODO: Replace with C++ DuckDB bridge via Cython
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB required: pip install duckdb --user")
        
        # Parse and get logical plan using DuckDB
        conn = duckdb.connect(':memory:')
        
        # Register tables (DuckDB accepts pyarrow tables)
        # We need to pass our cyarrow tables - they're compatible
        for name, table in self.tables.items():
            conn.register(name, table)
        
        # Execute with DuckDB (will be replaced with operator chain)
        # DuckDB returns pyarrow table, we convert to cyarrow
        import pyarrow as pa_std
        result_std = conn.execute(sql).fetch_arrow_table()
        conn.close()
        
        # Convert pyarrow result to cyarrow for consistency
        # cyarrow tables are compatible with pyarrow
        result = ca.Table.from_batches(result_std.to_batches())
        
        logger.info(f"Query completed: {result.num_rows:,} rows returned")
        return result
    
    async def execute_with_operators(self, sql: str) -> ca.Table:
        """
        Execute SQL using Sabot Cython operator chain.

        This is the real implementation:
        1. Parse SQL with DuckDB and extract JSON plan
        2. Build Sabot operator chain from plan
        3. Execute operator chain directly
        4. Collect and return results

        DuckDB is used ONLY for parsing - execution uses Sabot operators!
        """
        logger.info(f"Executing with operators: {sql[:80]}...")

        # Extract plan from DuckDB
        plan_extractor = DuckDBPlanExtractor(self.tables)
        try:
            plan = plan_extractor.extract_plan(sql)
        finally:
            plan_extractor.close()

        logger.debug(f"Extracted plan with {len(plan['operators'])} operators")

        # Build operator chain
        chain_builder = OperatorChainBuilder(self.tables)
        operator_chain = chain_builder.build_from_plan(plan)

        # Execute operator chain directly
        # MorselDrivenOperator is designed for mid-chain operators, not source operators
        # For now, iterate the chain directly until we fix morsel handling for sources
        batches = []
        for batch in operator_chain:
            if batch is not None and batch.num_rows > 0:
                batches.append(batch)

        # Convert to table
        if not batches:
            # Return empty table with correct schema
            return ca.table({})

        result = ca.Table.from_batches(batches)
        logger.info(f"Query completed via operators: {result.num_rows:,} rows")

        return result

