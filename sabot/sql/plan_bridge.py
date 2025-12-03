"""
Plan Bridge - DuckDB SQL to Distributed Execution Plan

Uses DuckDB's Python API to parse SQL and extract logical plan info,
then creates DistributedQueryPlan dicts for StageScheduler execution.
"""

import duckdb
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from sabot import cyarrow as ca


@dataclass
class TableInfo:
    """Information about a registered table."""
    name: str
    schema: ca.Schema
    num_rows: int


class PlanBridge:
    """
    Bridge between DuckDB SQL parsing and Sabot distributed execution.

    Uses DuckDB to:
    1. Parse and validate SQL
    2. Extract logical plan structure
    3. Identify operators and their dependencies

    Then converts to DistributedQueryPlan format for StageScheduler.
    """

    def __init__(self, parallelism: int = 4):
        """
        Initialize the plan bridge.

        Args:
            parallelism: Default parallelism for distributed execution
        """
        self._conn = duckdb.connect(":memory:")
        self._tables: Dict[str, TableInfo] = {}
        self._parallelism = parallelism

    def register_table(self, table_name: str, table: ca.Table) -> None:
        """
        Register an Arrow table for SQL queries.

        Args:
            table_name: Name to use in SQL queries
            table: Arrow table to register
        """
        # Register with DuckDB
        self._conn.register(table_name, table)

        # Store metadata
        self._tables[table_name] = TableInfo(
            name=table_name,
            schema=table.schema,
            num_rows=table.num_rows
        )

    def parse_and_partition(self, sql: str) -> Dict[str, Any]:
        """
        Parse SQL and create a distributed execution plan.

        Args:
            sql: SQL query string

        Returns:
            Dictionary suitable for DistributedQueryPlan.from_dict()
        """
        # Use DuckDB to explain and extract plan info
        plan_info = self._extract_plan_info(sql)

        # Partition into stages based on operator types
        stages, shuffles = self._partition_into_stages(sql, plan_info)

        # Build execution waves (dependency order)
        waves = self._build_execution_waves(stages, shuffles)

        # Get output schema
        output_cols, output_types = self._get_output_schema(sql)

        return {
            'sql': sql,
            'stages': stages,
            'shuffles': shuffles,
            'execution_waves': waves,
            'output_column_names': output_cols,
            'output_column_types': output_types,
            'requires_shuffle': len(shuffles) > 0,
            'total_parallelism': self._parallelism,
            'estimated_total_rows': plan_info.get('estimated_rows', 1000),
        }

    def _extract_plan_info(self, sql: str) -> Dict[str, Any]:
        """Extract plan information from DuckDB EXPLAIN."""
        # Get explain output
        explain_result = self._conn.execute(f"EXPLAIN {sql}").fetchall()
        explain_text = "\n".join(row[1] for row in explain_result)

        # Parse the explain output to extract operators
        info = {
            'has_filter': 'FILTER' in explain_text.upper(),
            'has_projection': 'PROJECTION' in explain_text.upper(),
            'has_join': 'JOIN' in explain_text.upper() or 'HASH_JOIN' in explain_text.upper(),
            'has_aggregate': 'AGGREGATE' in explain_text.upper() or 'GROUP' in explain_text.upper(),
            'has_sort': 'ORDER' in explain_text.upper() or 'SORT' in explain_text.upper(),
            'has_limit': 'LIMIT' in explain_text.upper(),
            'explain_text': explain_text,
            'estimated_rows': 1000,  # Default estimate
        }

        # Extract table scans - check SQL directly since DuckDB shows ARROW_SCAN not table name
        info['tables_scanned'] = []
        sql_upper = sql.upper()
        for table_name in self._tables:
            if table_name.upper() in sql_upper:
                info['tables_scanned'].append(table_name)

        return info

    def _partition_into_stages(
        self,
        sql: str,
        plan_info: Dict[str, Any]
    ) -> Tuple[List[Dict], Dict[str, Dict]]:
        """
        Partition the query into execution stages.

        Creates shuffle boundaries at:
        - GROUP BY (requires hash partition on group keys)
        - JOIN (requires hash partition on join keys)
        """
        stages = []
        shuffles = {}
        stage_counter = 0
        shuffle_counter = 0

        # Parse SQL to extract more details
        sql_upper = sql.upper()

        # Stage 0: Source scan with filter/projection
        source_ops = []

        # Add table scans
        for table_name in plan_info['tables_scanned']:
            table_info = self._tables.get(table_name)
            if table_info:
                source_ops.append({
                    'type': 'TableScan',
                    'name': f'Scan {table_name}',
                    'table_name': table_name,
                    'projected_columns': [f.name for f in table_info.schema],
                    'estimated_cardinality': table_info.num_rows,
                })

        # Add filter if present
        if plan_info['has_filter']:
            filter_expr = self._extract_filter_expression(sql)
            if filter_expr:
                source_ops.append({
                    'type': 'Filter',
                    'name': f'Filter {filter_expr[:30]}...' if len(filter_expr) > 30 else f'Filter {filter_expr}',
                    'filter_expression': filter_expr,
                    'estimated_cardinality': plan_info['estimated_rows'] // 2,
                })

        # Check if we need a shuffle for aggregate
        if plan_info['has_aggregate']:
            group_keys = self._extract_group_by_keys(sql)
            agg_funcs, agg_cols = self._extract_aggregations(sql)

            if group_keys:
                # Stage 0: Scan + partial aggregate
                stages.append({
                    'stage_id': f'stage_{stage_counter}',
                    'operators': source_ops,
                    'input_shuffle_ids': [],
                    'output_shuffle_id': f'shuffle_{shuffle_counter}',
                    'parallelism': self._parallelism,
                    'is_source': True,
                    'is_sink': False,
                    'estimated_rows': plan_info['estimated_rows'],
                })

                # Create shuffle spec
                shuffles[f'shuffle_{shuffle_counter}'] = {
                    'shuffle_id': f'shuffle_{shuffle_counter}',
                    'type': 'HASH',
                    'partition_keys': group_keys,
                    'num_partitions': self._parallelism,
                    'producer_stage_id': f'stage_{stage_counter}',
                    'consumer_stage_id': f'stage_{stage_counter + 1}',
                    'column_names': group_keys + agg_cols,
                    'column_types': ['int64'] * len(group_keys) + ['double'] * len(agg_cols),
                }

                stage_counter += 1
                shuffle_counter += 1

                # Stage 1: Final aggregate
                stages.append({
                    'stage_id': f'stage_{stage_counter}',
                    'operators': [{
                        'type': 'Aggregate',
                        'name': f"GroupBy {', '.join(group_keys)}",
                        'group_by_keys': group_keys,
                        'aggregate_functions': agg_funcs,
                        'aggregate_columns': agg_cols,
                        'estimated_cardinality': len(group_keys) * 10,
                    }],
                    'input_shuffle_ids': [f'shuffle_{shuffle_counter - 1}'],
                    'parallelism': self._parallelism,
                    'is_source': False,
                    'is_sink': True,
                    'estimated_rows': len(group_keys) * 10,
                    'dependency_stage_ids': [f'stage_{stage_counter - 1}'],
                })
            else:
                # Global aggregate - single stage
                source_ops.append({
                    'type': 'Aggregate',
                    'name': 'Global Aggregate',
                    'group_by_keys': [],
                    'aggregate_functions': agg_funcs,
                    'aggregate_columns': agg_cols,
                    'estimated_cardinality': 1,
                })
                stages.append({
                    'stage_id': f'stage_{stage_counter}',
                    'operators': source_ops,
                    'input_shuffle_ids': [],
                    'parallelism': self._parallelism,
                    'is_source': True,
                    'is_sink': True,
                    'estimated_rows': 1,
                })

        elif plan_info['has_join']:
            # Join requires shuffle on join keys
            join_info = self._extract_join_info(sql)

            # This is a simplified two-table join
            # Real implementation would handle multiple tables
            stages.append({
                'stage_id': f'stage_{stage_counter}',
                'operators': source_ops + [{
                    'type': 'HashJoin',
                    'name': 'Hash Join',
                    'left_keys': join_info.get('left_keys', []),
                    'right_keys': join_info.get('right_keys', []),
                    'join_type': join_info.get('join_type', 'inner'),
                    'estimated_cardinality': plan_info['estimated_rows'],
                }],
                'input_shuffle_ids': [],
                'parallelism': self._parallelism,
                'is_source': True,
                'is_sink': True,
                'estimated_rows': plan_info['estimated_rows'],
            })

        else:
            # Simple query - single stage
            # Add projection if we have SELECT columns
            select_cols = self._extract_select_columns(sql)
            if select_cols and select_cols != ['*']:
                source_ops.append({
                    'type': 'Projection',
                    'name': f"Project {', '.join(select_cols[:3])}...",
                    'projected_columns': select_cols,
                    'estimated_cardinality': plan_info['estimated_rows'],
                })

            # Add limit if present
            if plan_info['has_limit']:
                limit_val = self._extract_limit(sql)
                source_ops.append({
                    'type': 'Limit',
                    'name': f'Limit {limit_val}',
                    'int_params': {'limit': limit_val, 'offset': 0},
                    'estimated_cardinality': limit_val,
                })

            stages.append({
                'stage_id': f'stage_{stage_counter}',
                'operators': source_ops,
                'input_shuffle_ids': [],
                'parallelism': self._parallelism,
                'is_source': True,
                'is_sink': True,
                'estimated_rows': plan_info['estimated_rows'],
            })

        return stages, shuffles

    def _build_execution_waves(
        self,
        stages: List[Dict],
        shuffles: Dict[str, Dict]
    ) -> List[List[str]]:
        """Build execution waves based on stage dependencies."""
        waves = []
        executed = set()

        while len(executed) < len(stages):
            wave = []
            for stage in stages:
                stage_id = stage['stage_id']
                if stage_id in executed:
                    continue

                # Check if all dependencies are satisfied
                deps = stage.get('dependency_stage_ids', [])
                if all(dep in executed for dep in deps):
                    wave.append(stage_id)

            if not wave:
                # Prevent infinite loop
                remaining = [s['stage_id'] for s in stages if s['stage_id'] not in executed]
                wave = remaining

            waves.append(wave)
            executed.update(wave)

        return waves

    def _get_output_schema(self, sql: str) -> Tuple[List[str], List[str]]:
        """Get output column names and types by executing DESCRIBE."""
        try:
            # Create a view to get the schema
            self._conn.execute(f"CREATE OR REPLACE VIEW _temp_view AS {sql}")
            result = self._conn.execute("DESCRIBE _temp_view").fetchall()
            self._conn.execute("DROP VIEW IF EXISTS _temp_view")

            col_names = [row[0] for row in result]
            col_types = [self._duckdb_type_to_arrow(row[1]) for row in result]

            return col_names, col_types
        except Exception:
            # Fallback
            return ['result'], ['string']

    def _duckdb_type_to_arrow(self, duckdb_type: str) -> str:
        """Convert DuckDB type string to Arrow type string."""
        duckdb_type = duckdb_type.upper()
        type_map = {
            'BIGINT': 'int64',
            'INTEGER': 'int32',
            'SMALLINT': 'int16',
            'TINYINT': 'int8',
            'DOUBLE': 'double',
            'FLOAT': 'float',
            'REAL': 'float',
            'VARCHAR': 'string',
            'BOOLEAN': 'bool',
            'DATE': 'date32',
            'TIMESTAMP': 'timestamp[us]',
        }

        for duck, arrow in type_map.items():
            if duck in duckdb_type:
                return arrow

        return 'string'

    def _extract_filter_expression(self, sql: str) -> Optional[str]:
        """Extract WHERE clause from SQL."""
        sql_upper = sql.upper()
        where_idx = sql_upper.find('WHERE')
        if where_idx == -1:
            return None

        # Find end of WHERE clause
        end_keywords = ['GROUP BY', 'ORDER BY', 'LIMIT', 'HAVING', ';']
        end_idx = len(sql)
        for kw in end_keywords:
            idx = sql_upper.find(kw, where_idx)
            if idx != -1 and idx < end_idx:
                end_idx = idx

        return sql[where_idx + 5:end_idx].strip()

    def _extract_group_by_keys(self, sql: str) -> List[str]:
        """Extract GROUP BY columns from SQL."""
        sql_upper = sql.upper()
        group_idx = sql_upper.find('GROUP BY')
        if group_idx == -1:
            return []

        # Find end of GROUP BY
        end_keywords = ['ORDER BY', 'LIMIT', 'HAVING', ';']
        end_idx = len(sql)
        for kw in end_keywords:
            idx = sql_upper.find(kw, group_idx)
            if idx != -1 and idx < end_idx:
                end_idx = idx

        group_clause = sql[group_idx + 8:end_idx].strip()
        return [col.strip() for col in group_clause.split(',')]

    def _extract_aggregations(self, sql: str) -> Tuple[List[str], List[str]]:
        """Extract aggregate functions and columns from SQL."""
        import re

        # Find aggregate functions
        agg_pattern = r'\b(SUM|COUNT|AVG|MIN|MAX|FIRST|LAST)\s*\(\s*(\w+)\s*\)'
        matches = re.findall(agg_pattern, sql, re.IGNORECASE)

        funcs = [m[0].upper() for m in matches]
        cols = [m[1] for m in matches]

        return funcs, cols

    def _extract_join_info(self, sql: str) -> Dict[str, Any]:
        """Extract JOIN information from SQL."""
        import re

        sql_upper = sql.upper()

        # Determine join type
        join_type = 'inner'
        if 'LEFT JOIN' in sql_upper or 'LEFT OUTER' in sql_upper:
            join_type = 'left'
        elif 'RIGHT JOIN' in sql_upper or 'RIGHT OUTER' in sql_upper:
            join_type = 'right'
        elif 'FULL JOIN' in sql_upper or 'FULL OUTER' in sql_upper:
            join_type = 'outer'

        # Extract ON clause
        on_pattern = r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.search(on_pattern, sql, re.IGNORECASE)

        if match:
            return {
                'join_type': join_type,
                'left_keys': [match.group(2)],
                'right_keys': [match.group(4)],
            }

        return {'join_type': join_type, 'left_keys': [], 'right_keys': []}

    def _extract_select_columns(self, sql: str) -> List[str]:
        """Extract SELECT columns from SQL."""
        sql_upper = sql.upper()

        select_idx = sql_upper.find('SELECT')
        from_idx = sql_upper.find('FROM')

        if select_idx == -1 or from_idx == -1:
            return ['*']

        select_clause = sql[select_idx + 6:from_idx].strip()

        if select_clause == '*':
            return ['*']

        # Simple column extraction (doesn't handle complex expressions)
        cols = []
        for part in select_clause.split(','):
            part = part.strip()
            # Handle aliases
            if ' AS ' in part.upper():
                cols.append(part.split()[-1])
            elif '(' in part:
                # Aggregate function - use the whole thing as column name
                cols.append(part)
            else:
                cols.append(part.split('.')[-1] if '.' in part else part)

        return cols

    def _extract_limit(self, sql: str) -> int:
        """Extract LIMIT value from SQL."""
        import re

        match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 100

    def execute_with_duckdb(self, sql: str) -> ca.Table:
        """
        Execute SQL directly with DuckDB (for testing/comparison).

        Args:
            sql: SQL query string

        Returns:
            Arrow table with results
        """
        result = self._conn.execute(sql).fetch_arrow_table()
        return result

    @property
    def parallelism(self) -> int:
        """Get the current parallelism level."""
        return self._parallelism

    @parallelism.setter
    def parallelism(self, value: int) -> None:
        """Set the parallelism level."""
        self._parallelism = value


def create_plan_bridge(parallelism: int = 4) -> PlanBridge:
    """
    Create a new PlanBridge instance.

    Args:
        parallelism: Default parallelism for distributed execution

    Returns:
        PlanBridge instance
    """
    return PlanBridge(parallelism)
