"""
SQL Controller for Agent-Based SQL Execution

Provisions agents for distributed SQL query execution using Sabot's morsel operators.

Architecture:
1. Parse SQL with DuckDB (via sabot_sql C++ library)
2. Partition into stages at shuffle boundaries (StagePartitioner)
3. Execute stages in waves using StageScheduler
4. Use Sabot Cython operators for actual computation
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from sabot import cyarrow as ca  # Use Sabot's optimized Arrow
import logging

from sabot.agent_manager import DurableAgentManager
from sabot.agents.runtime import AgentSpec
from sabot.sql.agents import SQLScanAgent, SQLJoinAgent, SQLAggregateAgent
from sabot.sql.sql_to_operators import SQLQueryExecutor
from sabot.sql.distributed_plan import (
    DistributedQueryPlan,
    ExecutionStage,
    OperatorSpec,
)
from sabot.sql.stage_scheduler import StageScheduler, ShuffleCoordinator

logger = logging.getLogger(__name__)


def _apply_filter(table, expr: str, pc):
    """
    Apply a filter expression to a table.
    Handles AND/OR expressions and simple comparisons.
    """
    import re

    expr = expr.strip()

    # Handle AND
    if ' AND ' in expr.upper():
        parts = re.split(r'\s+AND\s+', expr, flags=re.IGNORECASE)
        result = table
        for part in parts:
            result = _apply_filter(result, part.strip(), pc)
        return result

    # Handle OR
    if ' OR ' in expr.upper():
        parts = re.split(r'\s+OR\s+', expr, flags=re.IGNORECASE)
        masks = []
        for part in parts:
            mask = _get_filter_mask(table, part.strip(), pc)
            if mask is not None:
                masks.append(mask)
        if masks:
            combined = masks[0]
            for m in masks[1:]:
                combined = pc.or_(combined, m)
            return table.filter(combined)
        return table

    # Simple comparison
    mask = _get_filter_mask(table, expr, pc)
    if mask is not None:
        return table.filter(mask)
    return table


def _get_filter_mask(table, expr: str, pc):
    """Get a boolean mask for a simple comparison expression."""
    import re

    # Parse: "column op value"
    match = re.match(r'(\w+)\s*(>=|<=|!=|<>|>|<|=)\s*(\d+(?:\.\d+)?)', expr.strip())
    if not match:
        return None

    col_name, op, value = match.groups()
    value = float(value) if '.' in value else int(value)

    if col_name not in table.column_names:
        return None

    col = table.column(col_name)

    if op == '>':
        return pc.greater(col, value)
    elif op == '<':
        return pc.less(col, value)
    elif op == '>=':
        return pc.greater_equal(col, value)
    elif op == '<=':
        return pc.less_equal(col, value)
    elif op == '=' or op == '==':
        return pc.equal(col, value)
    elif op == '!=' or op == '<>':
        return pc.not_equal(col, value)

    return None


@dataclass
class SQLExecutionPlan:
    """Represents an execution plan for SQL query (legacy structure)"""
    query: str
    stages: List[Dict[str, Any]]
    estimated_rows: int
    requires_shuffle: bool
    

class SQLController:
    """
    SQL Controller for Agent-Based Execution
    
    Provisions and coordinates agents for distributed SQL execution.
    Uses DuckDB for parsing/optimization and Sabot morsel operators for execution.
    
    Architecture:
    1. Parse SQL with DuckDB (C++ layer)
    2. Create execution plan with operator stages
    3. Provision agents for each stage
    4. Execute with morsel-driven parallelism
    5. Collect and return results
    
    Example:
        controller = SQLController(agent_manager)
        await controller.register_table("orders", orders_table)
        result = await controller.execute("SELECT * FROM orders WHERE amount > 1000")
    """
    
    def __init__(self, agent_manager: Optional[DurableAgentManager] = None):
        """
        Initialize SQL controller
        
        Args:
            agent_manager: Agent manager for provisioning agents
        """
        self.agent_manager = agent_manager
        self.tables: Dict[str, ca.Table] = {}
        self.running_agents: Dict[str, Any] = {}
        
        # Initialize SQL executor using existing Sabot operators
        self.sql_executor = SQLQueryExecutor(num_workers=4)
        
    async def register_table(self, table_name: str, table: ca.Table) -> None:
        """
        Register an Arrow table for querying
        
        Args:
            table_name: Name to register table as
            table: Arrow table data
        """
        self.tables[table_name] = table
        self.sql_executor.register_table(table_name, table)
        logger.info(f"Registered table '{table_name}' with {table.num_rows} rows, {table.num_columns} columns")
        
    async def register_table_from_file(self, table_name: str, file_path: str) -> None:
        """
        Register a table from file (CSV, Parquet, Arrow IPC)
        
        Args:
            table_name: Name to register table as
            file_path: Path to data file
        """
        # Auto-detect format and load using cyarrow
        if file_path.endswith('.parquet'):
            from sabot.cyarrow import parquet as pq
            table = pq.read_table(file_path)
        elif file_path.endswith('.csv'):
            from sabot.cyarrow import csv
            table = csv.read_csv(file_path)
        elif file_path.endswith('.arrow') or file_path.endswith('.ipc'):
            from sabot.cyarrow import ipc
            with ca.memory_map(file_path, 'r') as source:
                table = ipc.open_file(source).read_all()
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        await self.register_table(table_name, table)
        
    async def execute(
        self, 
        sql: str, 
        num_agents: int = 4,
        execution_mode: str = "local_parallel"
    ) -> ca.Table:
        """
        Execute SQL query with distributed agents
        
        Args:
            sql: SQL query string
            num_agents: Number of agents to provision
            execution_mode: "local", "local_parallel", or "distributed"
            
        Returns:
            Arrow table with query results
        """
        logger.info(f"Executing SQL: {sql}")
        logger.info(f"Execution mode: {execution_mode}, agents: {num_agents}")
        
        # Step 1: Create execution plan
        plan = await self._create_execution_plan(sql)
        logger.info(f"Created execution plan with {len(plan.stages)} stages")
        
        # Step 2: Choose execution strategy
        if execution_mode == "local":
            result = await self._execute_local(sql)
        elif execution_mode == "local_parallel":
            result = await self._execute_local_parallel(sql, num_agents)
        elif execution_mode == "distributed":
            result = await self._execute_distributed(plan, num_agents)
        else:
            raise ValueError(f"Unknown execution mode: {execution_mode}")
        
        logger.info(f"Query completed: {result.num_rows} rows returned")
        return result
    
    async def explain(self, sql: str) -> str:
        """
        Get query execution plan (EXPLAIN)
        
        Args:
            sql: SQL query string
            
        Returns:
            String representation of execution plan
        """
        # TODO: Call C++ SQL engine EXPLAIN via Cython
        plan = await self._create_execution_plan(sql)
        
        output = ["Query Execution Plan:", ""]
        for i, stage in enumerate(plan.stages):
            output.append(f"Stage {i}: {stage['operator']}")
            output.append(f"  Estimated rows: {stage.get('estimated_rows', 'unknown')}")
            if stage.get('requires_shuffle'):
                output.append(f"  Requires shuffle: yes")
            output.append("")
        
        return "\n".join(output)
    
    async def _create_execution_plan(self, sql: str) -> SQLExecutionPlan:
        """
        Create execution plan for SQL query
        
        This will eventually call the C++ SQL engine via Cython to:
        1. Parse SQL with DuckDB
        2. Optimize with DuckDB
        3. Translate to Sabot operators
        4. Identify stage boundaries
        """
        # For now, create a simple placeholder plan
        # TODO: Replace with actual C++ SQL engine call
        
        stages = [
            {
                "operator": "TableScan",
                "table": "orders",
                "estimated_rows": 1000000,
                "requires_shuffle": False
            },
            {
                "operator": "Filter",
                "predicate": "amount > 1000",
                "estimated_rows": 100000,
                "requires_shuffle": False
            },
            {
                "operator": "Project",
                "columns": ["id", "amount", "customer_id"],
                "estimated_rows": 100000,
                "requires_shuffle": False
            }
        ]
        
        return SQLExecutionPlan(
            query=sql,
            stages=stages,
            estimated_rows=100000,
            requires_shuffle=False
        )
    
    async def _execute_local(self, sql: str) -> ca.Table:
        """
        Execute query locally without parallelism
        
        This is the simplest execution mode - single-threaded.
        """
        logger.info("Executing locally (single-threaded)")
        
        # Use SQL executor with existing Sabot operators
        return await self.sql_executor.execute(sql)
    
    async def _execute_local_parallel(
        self, 
        sql: str, 
        num_workers: int
    ) -> ca.Table:
        """
        Execute query locally with morsel parallelism
        
        Uses multiple threads for parallel execution within a single process.
        """
        logger.info(f"Executing locally with {num_workers} workers (morsel parallelism)")
        
        # Update executor worker count
        self.sql_executor.num_workers = num_workers
        
        # Execute with Sabot operators (morsel-driven)
        return await self.sql_executor.execute(sql)
    
    async def _execute_distributed(
        self, 
        plan: SQLExecutionPlan,
        num_agents: int
    ) -> ca.Table:
        """
        Execute query across distributed agents
        
        This is the full distributed execution mode:
        1. Provision agents for each stage
        2. Distribute work across agents
        3. Handle shuffle operations
        4. Collect and combine results
        """
        logger.info(f"Executing distributedly with {num_agents} agents")
        
        if not self.agent_manager:
            raise ValueError("Agent manager required for distributed execution")
        
        # Provision agents for each stage
        agents = await self._provision_agents(plan, num_agents)
        
        # Execute stages
        result = await self._execute_stages(agents, plan)
        
        # Clean up agents
        await self._cleanup_agents(agents)
        
        return result
    
    async def _provision_agents(
        self, 
        plan: SQLExecutionPlan,
        num_agents: int
    ) -> List[str]:
        """
        Provision agents for query execution
        
        Creates agents for each stage of the query plan.
        """
        agent_ids = []
        
        for i, stage in enumerate(plan.stages):
            operator_type = stage["operator"]
            
            # Choose agent type based on operator
            if operator_type == "TableScan":
                agent_class = SQLScanAgent
            elif operator_type in ["Join", "HashJoin"]:
                agent_class = SQLJoinAgent
            elif operator_type in ["Aggregate", "GroupBy"]:
                agent_class = SQLAggregateAgent
            else:
                # Generic agent for other operators
                agent_class = SQLScanAgent
            
            # Create agent spec
            spec = AgentSpec(
                name=f"sql_stage_{i}_{operator_type.lower()}",
                func=agent_class.process,  # type: ignore
                concurrency=num_agents,
                max_restarts=3
            )
            
            # Deploy agent
            agent_id = await self.agent_manager.deploy_agent(spec)
            agent_ids.append(agent_id)
            logger.info(f"Provisioned agent {agent_id} for stage {i} ({operator_type})")
        
        return agent_ids
    
    async def _execute_stages(
        self,
        agent_ids: List[str],
        plan: SQLExecutionPlan
    ) -> ca.Table:
        """
        Execute query stages with provisioned agents
        """
        # TODO: Implement actual stage execution
        # For now, return empty result
        schema = ca.schema([
            ca.field("id", ca.int64()),
            ca.field("amount", ca.float64()),
        ])
        return ca.table({}, schema=schema)
    
    async def _cleanup_agents(self, agent_ids: List[str]) -> None:
        """
        Clean up provisioned agents after query execution
        """
        for agent_id in agent_ids:
            try:
                await self.agent_manager.stop_agent(agent_id)
                logger.info(f"Stopped agent {agent_id}")
            except Exception as e:
                logger.warning(f"Failed to stop agent {agent_id}: {e}")

    async def execute_distributed(
        self,
        sql: str,
        parallelism: int = 4
    ) -> ca.Table:
        """
        Execute SQL with distributed stage-based execution.

        This is the main entry point for distributed SQL execution:
        1. Parse SQL with DuckDB (via PlanBridge)
        2. Partition into stages with shuffle boundaries
        3. Execute stages in waves using StageScheduler
        4. Return final result

        Args:
            sql: SQL query string
            parallelism: Number of parallel workers per stage

        Returns:
            Arrow table with query results
        """
        from sabot.sql.plan_bridge import PlanBridge

        logger.info(f"Executing distributed SQL: {sql}")

        # Create plan bridge and register tables
        bridge = PlanBridge(parallelism=parallelism)
        for table_name, table in self.tables.items():
            bridge.register_table(table_name, table)

        # Parse and partition
        plan_dict = bridge.parse_and_partition(sql)
        plan = DistributedQueryPlan.from_dict(plan_dict)

        logger.info(f"Created plan with {len(plan.stages)} stages, "
                    f"{len(plan.execution_waves)} waves")

        # Execute with stage scheduler
        return await self.execute_with_stage_scheduler(plan)

    async def execute_with_stage_scheduler(
        self,
        plan: DistributedQueryPlan,
        operator_builder: Optional[Callable[[OperatorSpec, ca.Table], Any]] = None
    ) -> ca.Table:
        """
        Execute a distributed query plan using the StageScheduler.

        This is the new wave-based distributed execution that:
        1. Executes stages in dependency order (waves)
        2. Handles shuffle between stages
        3. Uses Cython operators for computation

        Args:
            plan: DistributedQueryPlan from C++ StagePartitioner
            operator_builder: Function to build operators from specs

        Returns:
            Final result as Arrow table
        """
        logger.info(f"Executing with StageScheduler: {len(plan.stages)} stages, "
                    f"{len(plan.execution_waves)} waves")

        # Create shuffle coordinator if plan requires shuffles
        shuffle_coord = None
        if plan.requires_shuffle:
            shuffle_coord = ShuffleCoordinator(plan.shuffles)

        # Use default operator builder if not provided
        if operator_builder is None:
            operator_builder = self._default_operator_builder

        # Create and run stage scheduler
        scheduler = StageScheduler(
            table_registry=self.tables,
            operator_builder=operator_builder,
            shuffle_coordinator=shuffle_coord,
        )

        result = await scheduler.execute(plan)
        return result

    def _default_operator_builder(
        self,
        spec: OperatorSpec,
        input_data: ca.Table
    ) -> Any:
        """
        Build a Sabot operator from an OperatorSpec.

        This is the bridge between the plan representation and
        actual operator instances using Arrow compute.

        Returns a callable that takes a table and returns a table.
        """
        import pyarrow.compute as pc

        if spec.type == "Filter":
            # Use Cython filter operator
            filter_expr = spec.filter_expression
            try:
                from sabot._cython.operators.filter_operator import CythonFilterOperator
                filter_op = CythonFilterOperator(filter_expr)
                # Return a lambda that calls apply - don't return the bound method directly
                return lambda table, op=filter_op: op.apply(table)
            except ImportError:
                # Fallback to Python implementation
                def filter_func(table, expr=filter_expr):
                    return _apply_filter(table, expr, pc)
                return filter_func

        elif spec.type == "Aggregate":
            # Use Cython GroupBy operator for GROUP BY
            group_keys = spec.group_by_keys or []
            agg_funcs = spec.aggregate_functions or []
            agg_cols = spec.aggregate_columns or []

            if not group_keys:
                # Global aggregation - use Arrow compute directly
                def global_aggregate_func(table):
                    results = {}
                    for func, col in zip(agg_funcs, agg_cols):
                        col_name = col if col != '*' else table.column_names[0]
                        if col_name in table.column_names:
                            column = table.column(col_name)
                            func_upper = func.upper()
                            if func_upper == 'SUM':
                                results[f'{func}_{col}'] = [pc.sum(column).as_py()]
                            elif func_upper == 'COUNT':
                                results[f'{func}_{col}'] = [table.num_rows]
                            elif func_upper in ('AVG', 'MEAN'):
                                results[f'{func}_{col}'] = [pc.mean(column).as_py()]
                            elif func_upper == 'MIN':
                                results[f'{func}_{col}'] = [pc.min(column).as_py()]
                            elif func_upper == 'MAX':
                                results[f'{func}_{col}'] = [pc.max(column).as_py()]
                    return ca.table(results) if results else table
                return global_aggregate_func
            else:
                # GROUP BY - try Cython operator first
                try:
                    from sabot._cython.operators.aggregations import CythonGroupByOperator

                    # Build aggregations dict: {output_name: (column, function)}
                    aggregations = {}
                    for func, col in zip(agg_funcs, agg_cols):
                        output_name = f'{func}_{col}'
                        aggregations[output_name] = (col, func.lower())

                    def cython_groupby_func(table, keys=group_keys, aggs=aggregations):
                        # Create operator and process
                        op = CythonGroupByOperator(
                            source=None,
                            keys=keys,
                            aggregations=aggs
                        )
                        # Process the table as batches
                        for batch in table.to_batches():
                            op.process_batch(batch)
                        # Get result
                        result_batch = op.get_result()
                        if result_batch is not None:
                            return ca.Table.from_batches([result_batch])
                        return table

                    return cython_groupby_func
                except ImportError:
                    # Fallback to Arrow group_by directly
                    def arrow_groupby_func(table, keys=group_keys):
                        grouped = table.group_by(keys)
                        agg_list = []
                        for func, col in zip(agg_funcs, agg_cols):
                            col_name = col if col != '*' else keys[0]
                            func_lower = func.lower()
                            if func_lower in ('avg', 'average'):
                                func_lower = 'mean'
                            agg_list.append((col_name, func_lower))
                        return grouped.aggregate(agg_list)
                    return arrow_groupby_func

        elif spec.type == "HashJoin":
            # Build a join operator that handles two inputs
            # The join requires special handling in the scheduler for multi-input
            left_keys = spec.left_keys or []
            right_keys = spec.right_keys or []
            join_type = spec.join_type or 'inner'

            try:
                from sabot._cython.operators.joins import CythonHashJoinOperator

                # Create a wrapper that performs the join given left and right tables
                def join_func(left_table, right_table=None,
                             l_keys=left_keys, r_keys=right_keys, jtype=join_type):
                    if right_table is None:
                        # Single input - can't join, return as-is
                        return left_table

                    # Create iterator wrappers for the join operator
                    def left_iter():
                        for batch in left_table.to_batches():
                            yield batch

                    def right_iter():
                        for batch in right_table.to_batches():
                            yield batch

                    # Create and execute join
                    join_op = CythonHashJoinOperator(
                        left_source=left_iter(),
                        right_source=right_iter(),
                        left_keys=l_keys,
                        right_keys=r_keys,
                        join_type=jtype
                    )

                    # Collect results
                    result_batches = list(join_op)
                    if result_batches:
                        return ca.Table.from_batches(result_batches)
                    return ca.table({})

                return join_func
            except ImportError:
                # Fallback - return a placeholder that indicates join needed
                return lambda table: table

        elif spec.type == "Projection":
            # Projection is handled as column selection
            projected_cols = spec.projected_columns or []

            def project(table, cols=projected_cols):
                existing = [c for c in cols if c in table.column_names]
                if existing:
                    return table.select(existing)
                return table
            return project

        elif spec.type == "Limit":
            limit = spec.int_params.get('limit', 100) if spec.int_params else 100
            offset = spec.int_params.get('offset', 0) if spec.int_params else 0

            def limit_func(table, lim=limit, off=offset):
                return table.slice(off, lim)
            return limit_func

        elif spec.type == "Distinct":
            # Use Cython distinct operator
            try:
                from sabot._cython.operators.aggregations import CythonDistinctOperator

                def distinct_func(table):
                    # For now use Arrow's unique function on all columns
                    # CythonDistinctOperator is for streaming
                    return table.group_by(table.column_names).aggregate([])
                return distinct_func
            except ImportError:
                def distinct_fallback(table):
                    return table.group_by(table.column_names).aggregate([])
                return distinct_fallback

        elif spec.type == "Sort":
            # Sort using Arrow compute
            sort_keys = spec.string_list_params.get('sort_keys', []) if spec.string_list_params else []
            sort_orders = spec.string_list_params.get('sort_orders', []) if spec.string_list_params else []

            def sort_func(table, keys=sort_keys, orders=sort_orders):
                if not keys:
                    return table
                # Build sort keys for Arrow
                sort_keys_arrow = []
                for i, key in enumerate(keys):
                    if key in table.column_names:
                        order = 'ascending'
                        if i < len(orders):
                            order = 'descending' if orders[i].lower() in ('desc', 'descending') else 'ascending'
                        sort_keys_arrow.append((key, order))
                if sort_keys_arrow:
                    indices = pc.sort_indices(table, sort_keys=sort_keys_arrow)
                    return pc.take(table, indices)
                return table
            return sort_func

        elif spec.type == "TopN":
            # Optimized LIMIT + ORDER BY using Arrow's select_k_unstable
            limit = spec.int_params.get('limit', 10) if spec.int_params else 10
            sort_keys = spec.string_list_params.get('sort_keys', []) if spec.string_list_params else []
            sort_orders = spec.string_list_params.get('sort_orders', []) if spec.string_list_params else []

            def topn_func(table, n=limit, keys=sort_keys, orders=sort_orders):
                if not keys or table.num_rows == 0:
                    return table.slice(0, min(n, table.num_rows))

                # Build sort keys
                sort_keys_arrow = []
                for i, key in enumerate(keys):
                    if key in table.column_names:
                        order = 'ascending'
                        if i < len(orders):
                            order = 'descending' if orders[i].lower() in ('desc', 'descending') else 'ascending'
                        sort_keys_arrow.append((key, order))

                if sort_keys_arrow:
                    # Use select_k_unstable for efficient top-N
                    try:
                        indices = pc.select_k_unstable(
                            table,
                            k=min(n, table.num_rows),
                            sort_keys=sort_keys_arrow
                        )
                        return pc.take(table, indices)
                    except Exception:
                        # Fallback to full sort + slice
                        indices = pc.sort_indices(table, sort_keys=sort_keys_arrow)
                        return pc.take(table, indices).slice(0, min(n, table.num_rows))
                return table.slice(0, min(n, table.num_rows))
            return topn_func

        elif spec.type == "Window":
            # Window functions using Arrow compute
            partition_by = spec.window_partition_by or []
            order_by = spec.window_order_by or []
            order_dirs = spec.window_order_directions or []
            window_func = spec.window_function or 'row_number'
            window_col = spec.window_column or ''

            def window_func_impl(table, part_by=partition_by, ord_by=order_by,
                                 ord_dirs=order_dirs, func=window_func, col=window_col):
                if table.num_rows == 0:
                    return table

                func_upper = func.upper()

                # Convert to pandas for window operations (Arrow doesn't have native window support)
                df = table.to_pandas()

                # Build sort columns for ordering within partitions
                sort_cols = [c for c in ord_by if c in df.columns]
                ascending = [d.lower() not in ('desc', 'descending') for d in ord_dirs[:len(sort_cols)]]

                # Sort by partition keys + order keys
                if part_by or sort_cols:
                    sort_all = [c for c in part_by if c in df.columns] + sort_cols
                    asc_all = [True] * len([c for c in part_by if c in df.columns]) + ascending
                    if sort_all:
                        df = df.sort_values(sort_all, ascending=asc_all)

                # Apply window function
                if part_by:
                    grouper = df.groupby(part_by, sort=False)
                else:
                    grouper = df

                if func_upper == 'ROW_NUMBER':
                    if part_by:
                        df['row_number'] = grouper.cumcount() + 1
                    else:
                        df['row_number'] = range(1, len(df) + 1)
                elif func_upper == 'RANK':
                    if sort_cols and part_by:
                        df['rank'] = grouper[sort_cols[0]].rank(method='min').astype(int)
                    elif sort_cols:
                        df['rank'] = df[sort_cols[0]].rank(method='min').astype(int)
                    else:
                        df['rank'] = 1
                elif func_upper == 'DENSE_RANK':
                    if sort_cols and part_by:
                        df['dense_rank'] = grouper[sort_cols[0]].rank(method='dense').astype(int)
                    elif sort_cols:
                        df['dense_rank'] = df[sort_cols[0]].rank(method='dense').astype(int)
                    else:
                        df['dense_rank'] = 1
                elif func_upper in ('SUM', 'CUMSUM') and col and col in df.columns:
                    if part_by:
                        df[f'sum_{col}'] = grouper[col].cumsum()
                    else:
                        df[f'sum_{col}'] = df[col].cumsum()
                elif func_upper in ('AVG', 'MEAN', 'CUMAVG') and col and col in df.columns:
                    if part_by:
                        df[f'avg_{col}'] = grouper[col].expanding().mean().reset_index(level=0, drop=True)
                    else:
                        df[f'avg_{col}'] = df[col].expanding().mean()
                elif func_upper in ('MIN', 'CUMMIN') and col and col in df.columns:
                    if part_by:
                        df[f'min_{col}'] = grouper[col].cummin()
                    else:
                        df[f'min_{col}'] = df[col].cummin()
                elif func_upper in ('MAX', 'CUMMAX') and col and col in df.columns:
                    if part_by:
                        df[f'max_{col}'] = grouper[col].cummax()
                    else:
                        df[f'max_{col}'] = df[col].cummax()
                elif func_upper == 'LAG' and col and col in df.columns:
                    offset = spec.int_params.get('offset', 1) if spec.int_params else 1
                    if part_by:
                        df[f'lag_{col}'] = grouper[col].shift(offset)
                    else:
                        df[f'lag_{col}'] = df[col].shift(offset)
                elif func_upper == 'LEAD' and col and col in df.columns:
                    offset = spec.int_params.get('offset', 1) if spec.int_params else 1
                    if part_by:
                        df[f'lead_{col}'] = grouper[col].shift(-offset)
                    else:
                        df[f'lead_{col}'] = df[col].shift(-offset)

                return ca.Table.from_pandas(df)
            return window_func_impl

        elif spec.type == "Union":
            # UNION / UNION ALL operator
            # This is a multi-input operator, handled similarly to joins
            union_all = spec.set_all

            def union_func(left_table, right_table=None, is_all=union_all):
                if right_table is None:
                    return left_table

                # Concatenate tables
                combined = ca.concat_tables([left_table, right_table])

                if not is_all:
                    # UNION (distinct) - remove duplicates
                    return combined.group_by(combined.column_names).aggregate([])
                return combined
            return union_func

        elif spec.type == "Intersect":
            # INTERSECT operator
            def intersect_func(left_table, right_table=None):
                if right_table is None or right_table.num_rows == 0:
                    return ca.table({}, schema=left_table.schema)

                # Convert to sets of tuples for intersection
                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Find common rows
                merged = left_df.merge(right_df, how='inner')
                merged = merged.drop_duplicates()

                return ca.Table.from_pandas(merged)
            return intersect_func

        elif spec.type == "Except":
            # EXCEPT operator (left minus right)
            def except_func(left_table, right_table=None):
                if right_table is None or right_table.num_rows == 0:
                    return left_table.group_by(left_table.column_names).aggregate([])

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Find rows in left but not in right
                merged = left_df.merge(right_df, how='left', indicator=True)
                result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                result = result.drop_duplicates()

                return ca.Table.from_pandas(result)
            return except_func

        elif spec.type == "SemiJoin":
            # Semi join (EXISTS) - return left rows where match exists in right
            left_keys = spec.left_keys or []
            right_keys = spec.right_keys or []

            def semi_join_func(left_table, right_table=None,
                              l_keys=left_keys, r_keys=right_keys):
                if right_table is None or right_table.num_rows == 0:
                    return ca.table({}, schema=left_table.schema)

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Get unique keys from right
                right_keys_only = right_df[r_keys].drop_duplicates()
                right_keys_only.columns = l_keys

                # Inner join but only keep left columns
                result = left_df.merge(right_keys_only, on=l_keys, how='inner')
                result = result[left_df.columns]

                return ca.Table.from_pandas(result)
            return semi_join_func

        elif spec.type == "AntiJoin":
            # Anti join (NOT EXISTS) - return left rows where no match in right
            left_keys = spec.left_keys or []
            right_keys = spec.right_keys or []

            def anti_join_func(left_table, right_table=None,
                              l_keys=left_keys, r_keys=right_keys):
                if right_table is None or right_table.num_rows == 0:
                    return left_table

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Get unique keys from right
                right_keys_only = right_df[r_keys].drop_duplicates()
                right_keys_only.columns = l_keys

                # Left join with indicator
                merged = left_df.merge(right_keys_only, on=l_keys, how='left', indicator=True)
                result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                result = result[left_df.columns]

                return ca.Table.from_pandas(result)
            return anti_join_func

        elif spec.type == "CrossJoin":
            # Cross join (Cartesian product)
            def cross_join_func(left_table, right_table=None):
                if right_table is None:
                    return left_table

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Add dummy key for cross join
                left_df['_cross_key'] = 1
                right_df['_cross_key'] = 1

                result = left_df.merge(right_df, on='_cross_key').drop('_cross_key', axis=1)
                return ca.Table.from_pandas(result)
            return cross_join_func

        # ===============================
        # Time Series Operators
        # ===============================

        elif spec.type == "AsofJoin":
            # ASOF Join - join with most recent match by timestamp
            # Uses CythonAsofJoinOperator if available, falls back to pandas
            left_keys = spec.left_keys or []
            right_keys = spec.right_keys or []
            time_col = spec.time_column or (left_keys[0] if left_keys else 'timestamp')
            direction = spec.time_direction or 'backward'

            def asof_join_func(left_table, right_table=None,
                              l_keys=left_keys, r_keys=right_keys,
                              time_column=time_col, dir=direction):
                if right_table is None or right_table.num_rows == 0:
                    return left_table

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Sort both by time column
                if time_column in left_df.columns:
                    left_df = left_df.sort_values(time_column)
                if time_column in right_df.columns:
                    right_df = right_df.sort_values(time_column)

                # Use pandas merge_asof
                import pandas as pd
                if l_keys and r_keys and len(l_keys) > 1:
                    # Multiple keys - use by parameter
                    by_keys = l_keys[1:] if len(l_keys) > 1 else None
                    result = pd.merge_asof(
                        left_df, right_df,
                        on=time_column,
                        by=by_keys,
                        direction=dir,
                        suffixes=('', '_right')
                    )
                else:
                    result = pd.merge_asof(
                        left_df, right_df,
                        on=time_column,
                        direction=dir,
                        suffixes=('', '_right')
                    )

                return ca.Table.from_pandas(result)
            return asof_join_func

        elif spec.type == "IntervalJoin":
            # Interval Join - join within time interval
            left_keys = spec.left_keys or []
            right_keys = spec.right_keys or []
            time_col = spec.time_column or 'timestamp'
            lower = spec.interval_lower or 0
            upper = spec.interval_upper or 0

            def interval_join_func(left_table, right_table=None,
                                  l_keys=left_keys, r_keys=right_keys,
                                  time_column=time_col,
                                  interval_lower=lower, interval_upper=upper):
                if right_table is None or right_table.num_rows == 0:
                    return left_table

                left_df = left_table.to_pandas()
                right_df = right_table.to_pandas()

                # Cross join then filter by time interval
                left_df['_join_key'] = 1
                right_df['_join_key'] = 1

                cross = left_df.merge(right_df, on='_join_key', suffixes=('', '_right'))
                cross = cross.drop('_join_key', axis=1)

                # Filter by time interval
                if time_column in left_df.columns and f'{time_column}_right' in cross.columns:
                    time_diff = (cross[f'{time_column}_right'] - cross[time_column])
                    # Convert to milliseconds if datetime
                    if hasattr(time_diff.iloc[0] if len(time_diff) > 0 else 0, 'total_seconds'):
                        time_diff = time_diff.dt.total_seconds() * 1000
                    mask = (time_diff >= interval_lower) & (time_diff <= interval_upper)
                    result = cross[mask]
                else:
                    result = cross

                return ca.Table.from_pandas(result)
            return interval_join_func

        elif spec.type == "TimeBucket":
            # Group timestamps into buckets (tumbling windows)
            time_col = spec.time_column or 'timestamp'
            bucket_size = spec.bucket_size or '1h'
            origin = spec.bucket_origin
            group_keys = spec.group_by_keys or []
            agg_funcs = spec.aggregate_functions or []
            agg_cols = spec.aggregate_columns or []

            def time_bucket_func(table, time_column=time_col, bucket=bucket_size,
                                bucket_origin=origin, group_by=group_keys,
                                aggs=agg_funcs, cols=agg_cols):
                import pandas as pd
                df = table.to_pandas()

                if time_column not in df.columns:
                    return table

                # Parse bucket size (e.g., "1h", "5m", "1d")
                import re
                match = re.match(r'(\d+)([smhdwMy])', bucket)
                if match:
                    num = int(match.group(1))
                    unit = match.group(2)
                    freq_map = {'s': 'S', 'm': 'T', 'h': 'H', 'd': 'D', 'w': 'W', 'M': 'M', 'y': 'Y'}
                    freq = f'{num}{freq_map.get(unit, "H")}'
                else:
                    freq = bucket

                # Create bucket column
                df['_time_bucket'] = pd.to_datetime(df[time_column]).dt.floor(freq)

                # Group by bucket and any additional keys
                all_group_keys = ['_time_bucket'] + list(group_by)

                if aggs and cols:
                    agg_dict = {}
                    for func, col in zip(aggs, cols):
                        if col in df.columns:
                            func_lower = func.lower()
                            if func_lower in ('sum', 'mean', 'min', 'max', 'count', 'std', 'var'):
                                agg_dict[col] = func_lower
                            elif func_lower == 'avg':
                                agg_dict[col] = 'mean'
                            elif func_lower == 'first':
                                agg_dict[col] = 'first'
                            elif func_lower == 'last':
                                agg_dict[col] = 'last'

                    if agg_dict:
                        result = df.groupby(all_group_keys, as_index=False).agg(agg_dict)
                    else:
                        result = df.groupby(all_group_keys, as_index=False).first()
                else:
                    result = df.groupby(all_group_keys, as_index=False).first()

                # Rename bucket column back to original time column
                result = result.rename(columns={'_time_bucket': time_column})

                return ca.Table.from_pandas(result)
            return time_bucket_func

        elif spec.type == "SessionWindow":
            # Group by session gaps
            time_col = spec.time_column or 'timestamp'
            gap = spec.session_gap or 30000  # 30 seconds default
            group_keys = spec.group_by_keys or []
            agg_funcs = spec.aggregate_functions or []
            agg_cols = spec.aggregate_columns or []

            def session_window_func(table, time_column=time_col, session_gap=gap,
                                   group_by=group_keys, aggs=agg_funcs, cols=agg_cols):
                import pandas as pd
                df = table.to_pandas()

                if time_column not in df.columns:
                    return table

                # Sort by time
                df = df.sort_values(time_column)

                # Convert time to numeric for gap detection
                times = pd.to_datetime(df[time_column])
                time_diffs = times.diff()

                # Convert gap to timedelta
                gap_td = pd.Timedelta(milliseconds=session_gap)

                # Create session IDs based on gaps
                session_starts = time_diffs > gap_td
                df['_session_id'] = session_starts.cumsum()

                # If grouping by other keys, combine with session
                if group_by:
                    all_keys = list(group_by) + ['_session_id']
                else:
                    all_keys = ['_session_id']

                # Aggregate
                if aggs and cols:
                    agg_dict = {}
                    for func, col in zip(aggs, cols):
                        if col in df.columns:
                            func_lower = func.lower()
                            if func_lower in ('sum', 'mean', 'min', 'max', 'count', 'std', 'var'):
                                agg_dict[col] = func_lower
                            elif func_lower == 'avg':
                                agg_dict[col] = 'mean'
                    if agg_dict:
                        # Also get session start/end times
                        agg_dict[time_column] = ['min', 'max']
                        result = df.groupby(all_keys, as_index=False).agg(agg_dict)
                        # Flatten column names
                        result.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                                         for col in result.columns]
                    else:
                        result = df.groupby(all_keys, as_index=False).agg({
                            time_column: ['min', 'max', 'count']
                        })
                        result.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                                         for col in result.columns]
                else:
                    result = df.groupby(all_keys, as_index=False).agg({
                        time_column: ['min', 'max', 'count']
                    })
                    result.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                                     for col in result.columns]

                return ca.Table.from_pandas(result)
            return session_window_func

        elif spec.type == "RollingWindow":
            # Moving/rolling aggregates
            time_col = spec.time_column or 'timestamp'
            window_size = spec.rolling_window_size or 10
            min_periods = spec.rolling_min_periods or 1
            group_keys = spec.group_by_keys or []
            agg_funcs = spec.aggregate_functions or ['mean']
            agg_cols = spec.aggregate_columns or []

            def rolling_window_func(table, time_column=time_col, window=window_size,
                                   min_per=min_periods, group_by=group_keys,
                                   aggs=agg_funcs, cols=agg_cols):
                import pandas as pd
                df = table.to_pandas()

                # Sort by time
                if time_column in df.columns:
                    df = df.sort_values(time_column)

                # Apply rolling to each aggregation column
                for func, col in zip(aggs, cols):
                    if col in df.columns:
                        func_lower = func.lower()
                        new_col = f'{col}_rolling_{func_lower}'

                        if group_by:
                            # Rolling within groups
                            def apply_rolling(group, column=col, f=func_lower, w=window, mp=min_per):
                                roller = group[column].rolling(window=w, min_periods=mp)
                                if f == 'sum':
                                    return roller.sum()
                                elif f in ('mean', 'avg'):
                                    return roller.mean()
                                elif f == 'min':
                                    return roller.min()
                                elif f == 'max':
                                    return roller.max()
                                elif f == 'std':
                                    return roller.std()
                                elif f == 'var':
                                    return roller.var()
                                elif f == 'count':
                                    return roller.count()
                                else:
                                    return roller.mean()

                            df[new_col] = df.groupby(group_by, group_keys=False).apply(
                                lambda g: apply_rolling(g)
                            ).reset_index(drop=True)
                        else:
                            roller = df[col].rolling(window=window, min_periods=min_per)
                            if func_lower == 'sum':
                                df[new_col] = roller.sum()
                            elif func_lower in ('mean', 'avg'):
                                df[new_col] = roller.mean()
                            elif func_lower == 'min':
                                df[new_col] = roller.min()
                            elif func_lower == 'max':
                                df[new_col] = roller.max()
                            elif func_lower == 'std':
                                df[new_col] = roller.std()
                            elif func_lower == 'var':
                                df[new_col] = roller.var()
                            elif func_lower == 'count':
                                df[new_col] = roller.count()
                            else:
                                df[new_col] = roller.mean()

                return ca.Table.from_pandas(df)
            return rolling_window_func

        elif spec.type == "Resample":
            # Upsample/downsample time series
            time_col = spec.time_column or 'timestamp'
            rule = spec.resample_rule or '1h'
            agg = spec.resample_agg or 'mean'
            group_keys = spec.group_by_keys or []

            def resample_func(table, time_column=time_col, resample_rule=rule,
                             agg_method=agg, group_by=group_keys):
                import pandas as pd
                df = table.to_pandas()

                if time_column not in df.columns:
                    return table

                # Convert to datetime index
                df[time_column] = pd.to_datetime(df[time_column])

                # Parse resample rule
                import re
                match = re.match(r'(\d+)([smhdwMy])', resample_rule)
                if match:
                    num = int(match.group(1))
                    unit = match.group(2)
                    freq_map = {'s': 'S', 'm': 'T', 'h': 'H', 'd': 'D', 'w': 'W', 'M': 'M', 'y': 'Y'}
                    freq = f'{num}{freq_map.get(unit, "H")}'
                else:
                    freq = resample_rule

                if group_by:
                    # Resample within groups
                    results = []
                    for keys, group in df.groupby(group_by):
                        group = group.set_index(time_column)
                        resampled = group.resample(freq)
                        if agg_method == 'sum':
                            r = resampled.sum(numeric_only=True)
                        elif agg_method in ('mean', 'avg'):
                            r = resampled.mean(numeric_only=True)
                        elif agg_method == 'min':
                            r = resampled.min(numeric_only=True)
                        elif agg_method == 'max':
                            r = resampled.max(numeric_only=True)
                        elif agg_method == 'first':
                            r = resampled.first()
                        elif agg_method == 'last':
                            r = resampled.last()
                        elif agg_method == 'count':
                            r = resampled.count()
                        elif agg_method == 'ohlc':
                            # For OHLC (Open-High-Low-Close)
                            r = resampled.ohlc()
                        else:
                            r = resampled.mean(numeric_only=True)

                        r = r.reset_index()
                        # Add group keys back
                        if isinstance(keys, tuple):
                            for k, v in zip(group_by, keys):
                                r[k] = v
                        else:
                            r[group_by[0]] = keys
                        results.append(r)

                    result = pd.concat(results, ignore_index=True)
                else:
                    df = df.set_index(time_column)
                    resampled = df.resample(freq)
                    if agg_method == 'sum':
                        result = resampled.sum(numeric_only=True)
                    elif agg_method in ('mean', 'avg'):
                        result = resampled.mean(numeric_only=True)
                    elif agg_method == 'min':
                        result = resampled.min(numeric_only=True)
                    elif agg_method == 'max':
                        result = resampled.max(numeric_only=True)
                    elif agg_method == 'first':
                        result = resampled.first()
                    elif agg_method == 'last':
                        result = resampled.last()
                    elif agg_method == 'count':
                        result = resampled.count()
                    else:
                        result = resampled.mean(numeric_only=True)
                    result = result.reset_index()

                return ca.Table.from_pandas(result)
            return resample_func

        elif spec.type == "Fill":
            # Forward fill, backward fill, interpolate
            fill_method = spec.fill_method or 'forward'
            fill_limit = spec.fill_limit or None
            fill_cols = spec.aggregate_columns or []  # Columns to fill

            def fill_func(table, method=fill_method, limit=fill_limit, columns=fill_cols):
                import pandas as pd
                df = table.to_pandas()

                cols_to_fill = columns if columns else df.columns.tolist()

                for col in cols_to_fill:
                    if col not in df.columns:
                        continue

                    if method == 'forward':
                        df[col] = df[col].ffill(limit=limit if limit else None)
                    elif method == 'backward':
                        df[col] = df[col].bfill(limit=limit if limit else None)
                    elif method == 'linear':
                        df[col] = df[col].interpolate(method='linear', limit=limit if limit else None)
                    elif method == 'time':
                        df[col] = df[col].interpolate(method='time', limit=limit if limit else None)
                    elif method == 'nearest':
                        df[col] = df[col].interpolate(method='nearest', limit=limit if limit else None)
                    elif method == 'zero':
                        df[col] = df[col].fillna(0)
                    elif method == 'null':
                        pass  # Keep nulls
                    else:
                        df[col] = df[col].ffill(limit=limit if limit else None)

                return ca.Table.from_pandas(df)
            return fill_func

        elif spec.type == "TimeDiff":
            # Calculate time differences
            time_col = spec.time_column or 'timestamp'
            group_keys = spec.group_by_keys or []

            def time_diff_func(table, time_column=time_col, group_by=group_keys):
                import pandas as pd
                df = table.to_pandas()

                if time_column not in df.columns:
                    return table

                # Sort by time
                df = df.sort_values(time_column)
                df[time_column] = pd.to_datetime(df[time_column])

                if group_by:
                    df['time_diff'] = df.groupby(group_by)[time_column].diff()
                else:
                    df['time_diff'] = df[time_column].diff()

                # Convert to seconds
                if df['time_diff'].dtype == 'timedelta64[ns]':
                    df['time_diff_seconds'] = df['time_diff'].dt.total_seconds()
                else:
                    df['time_diff_seconds'] = df['time_diff']

                return ca.Table.from_pandas(df)
            return time_diff_func

        elif spec.type == "EWMA":
            # Exponentially Weighted Moving Average
            alpha = spec.ewma_alpha or 0.5
            adjust = spec.ewma_adjust
            value_col = spec.aggregate_columns[0] if spec.aggregate_columns else 'value'
            group_keys = spec.group_by_keys or []

            def ewma_func(table, ewma_alpha=alpha, ewma_adjust=adjust,
                         column=value_col, group_by=group_keys):
                import pandas as pd
                df = table.to_pandas()

                if column not in df.columns:
                    return table

                new_col = f'{column}_ewma'

                if group_by:
                    df[new_col] = df.groupby(group_by)[column].transform(
                        lambda x: x.ewm(alpha=ewma_alpha, adjust=ewma_adjust).mean()
                    )
                else:
                    df[new_col] = df[column].ewm(alpha=ewma_alpha, adjust=ewma_adjust).mean()

                return ca.Table.from_pandas(df)
            return ewma_func

        elif spec.type == "LogReturns":
            # Calculate log returns: log(p_t / p_{t-1})
            price_col = spec.aggregate_columns[0] if spec.aggregate_columns else 'price'
            group_keys = spec.group_by_keys or []

            def log_returns_func(table, column=price_col, group_by=group_keys):
                import pandas as pd
                import numpy as np
                df = table.to_pandas()

                if column not in df.columns:
                    return table

                new_col = f'{column}_log_returns'

                if group_by:
                    df[new_col] = df.groupby(group_by)[column].transform(
                        lambda x: np.log(x / x.shift(1))
                    )
                else:
                    df[new_col] = np.log(df[column] / df[column].shift(1))

                return ca.Table.from_pandas(df)
            return log_returns_func

        else:
            # Unknown operator - passthrough
            return lambda table: table

    # ========================================================================
    # Time Series Helper Methods (for TimeSeriesStreamOps integration)
    # ========================================================================

    async def _apply_rolling_window(
        self,
        table: ca.Table,
        column: str,
        window_size: int,
        agg: str = 'mean',
        min_periods: int = 1,
        output_column: Optional[str] = None
    ) -> ca.Table:
        """Apply rolling window aggregation to a table."""
        import pandas as pd
        df = table.to_pandas()

        if column not in df.columns:
            return table

        new_col = output_column or f'{column}_{agg}_{window_size}'
        roller = df[column].rolling(window=window_size, min_periods=min_periods)

        agg_lower = agg.lower()
        if agg_lower == 'sum':
            df[new_col] = roller.sum()
        elif agg_lower in ('mean', 'avg'):
            df[new_col] = roller.mean()
        elif agg_lower == 'min':
            df[new_col] = roller.min()
        elif agg_lower == 'max':
            df[new_col] = roller.max()
        elif agg_lower == 'std':
            df[new_col] = roller.std()
        elif agg_lower == 'var':
            df[new_col] = roller.var()
        elif agg_lower == 'count':
            df[new_col] = roller.count()
        else:
            df[new_col] = roller.mean()

        return ca.Table.from_pandas(df)

    async def _apply_ewma(
        self,
        table: ca.Table,
        column: str,
        alpha: float = 0.5,
        output_column: Optional[str] = None
    ) -> ca.Table:
        """Apply Exponential Weighted Moving Average to a table."""
        import pandas as pd
        df = table.to_pandas()

        if column not in df.columns:
            return table

        new_col = output_column or f'{column}_ewma'
        df[new_col] = df[column].ewm(alpha=alpha, adjust=True).mean()

        return ca.Table.from_pandas(df)

    async def _apply_log_returns(
        self,
        table: ca.Table,
        column: str = 'price',
        output_column: Optional[str] = None
    ) -> ca.Table:
        """Calculate logarithmic returns."""
        import pandas as pd
        import numpy as np
        df = table.to_pandas()

        if column not in df.columns:
            return table

        new_col = output_column or 'log_returns'
        df[new_col] = np.log(df[column] / df[column].shift(1))

        return ca.Table.from_pandas(df)

    async def _apply_time_bucket(
        self,
        table: ca.Table,
        time_column: str = 'timestamp',
        bucket_size: str = '1h',
        output_column: str = 'bucket'
    ) -> ca.Table:
        """Assign rows to time buckets."""
        import pandas as pd
        import re
        df = table.to_pandas()

        if time_column not in df.columns:
            return table

        df[time_column] = pd.to_datetime(df[time_column])

        # Parse bucket size
        match = re.match(r'(\d+)([smhdwMy])', bucket_size)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            freq_map = {'s': 'S', 'm': 'T', 'h': 'H', 'd': 'D', 'w': 'W', 'M': 'M', 'y': 'Y'}
            freq = f'{num}{freq_map.get(unit, "H")}'
        else:
            freq = bucket_size

        df[output_column] = df[time_column].dt.floor(freq)

        return ca.Table.from_pandas(df)

    async def _apply_time_diff(
        self,
        table: ca.Table,
        time_column: str = 'timestamp',
        output_column: str = 'time_diff_ms'
    ) -> ca.Table:
        """Calculate time difference between consecutive rows."""
        import pandas as pd
        df = table.to_pandas()

        if time_column not in df.columns:
            return table

        df[time_column] = pd.to_datetime(df[time_column])
        diff = df[time_column].diff()
        df[output_column] = diff.dt.total_seconds() * 1000  # Convert to milliseconds

        return ca.Table.from_pandas(df)

    async def _apply_resample(
        self,
        table: ca.Table,
        time_column: str = 'timestamp',
        rule: str = '1h',
        agg: Dict[str, str] = None
    ) -> ca.Table:
        """Resample time series data to a different frequency."""
        import pandas as pd
        import re
        df = table.to_pandas()

        if time_column not in df.columns:
            return table

        df[time_column] = pd.to_datetime(df[time_column])

        # Parse resample rule
        match = re.match(r'(\d+)([smhdwMy])', rule)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            freq_map = {'s': 'S', 'm': 'T', 'h': 'H', 'd': 'D', 'w': 'W', 'M': 'M', 'y': 'Y'}
            freq = f'{num}{freq_map.get(unit, "H")}'
        else:
            freq = rule

        df = df.set_index(time_column)
        resampled = df.resample(freq)

        if agg:
            # Apply custom aggregations per column
            agg_funcs = {}
            for col, func in agg.items():
                if col in df.columns:
                    agg_funcs[col] = func
            if agg_funcs:
                result = resampled.agg(agg_funcs)
            else:
                result = resampled.mean(numeric_only=True)
        else:
            result = resampled.mean(numeric_only=True)

        result = result.reset_index()
        return ca.Table.from_pandas(result)

    async def _apply_fill(
        self,
        table: ca.Table,
        method: str = 'forward',
        columns: Optional[List[str]] = None
    ) -> ca.Table:
        """Fill missing values in time series."""
        import pandas as pd
        df = table.to_pandas()

        # Select columns to fill
        if columns:
            cols = [c for c in columns if c in df.columns]
        else:
            cols = df.select_dtypes(include=['number']).columns.tolist()

        for col in cols:
            if method == 'forward':
                df[col] = df[col].ffill()
            elif method == 'backward':
                df[col] = df[col].bfill()
            elif method == 'linear':
                df[col] = df[col].interpolate(method='linear')
            elif method == 'zero':
                df[col] = df[col].fillna(0)
            elif method == 'mean':
                df[col] = df[col].fillna(df[col].mean())

        return ca.Table.from_pandas(df)

    async def _apply_session_window(
        self,
        table: ca.Table,
        time_column: str = 'timestamp',
        gap_ms: int = 30000,
        output_column: str = 'session_id'
    ) -> ca.Table:
        """Assign session IDs based on activity gaps."""
        import pandas as pd
        df = table.to_pandas()

        if time_column not in df.columns:
            return table

        df = df.sort_values(time_column)
        df[time_column] = pd.to_datetime(df[time_column])

        # Calculate time differences
        time_diff = df[time_column].diff()
        gap_threshold = pd.Timedelta(milliseconds=gap_ms)

        # New session starts when gap exceeds threshold
        new_session = (time_diff > gap_threshold) | time_diff.isna()
        df[output_column] = new_session.cumsum()

        return ca.Table.from_pandas(df)

    async def _apply_asof_join(
        self,
        left_table: ca.Table,
        right_table: ca.Table,
        time_column: str = 'timestamp',
        direction: str = 'backward'
    ) -> ca.Table:
        """Apply as-of join between two tables."""
        import pandas as pd
        left_df = left_table.to_pandas()
        right_df = right_table.to_pandas()

        if time_column not in left_df.columns or time_column not in right_df.columns:
            return left_table

        left_df[time_column] = pd.to_datetime(left_df[time_column])
        right_df[time_column] = pd.to_datetime(right_df[time_column])

        # Sort both by time column
        left_df = left_df.sort_values(time_column)
        right_df = right_df.sort_values(time_column)

        result = pd.merge_asof(
            left_df,
            right_df,
            on=time_column,
            direction=direction,
            suffixes=('', '_right')
        )

        return ca.Table.from_pandas(result)

    async def _apply_interval_join(
        self,
        left_table: ca.Table,
        right_table: ca.Table,
        time_column: str = 'timestamp',
        lower_bound: int = -60000,
        upper_bound: int = 60000
    ) -> ca.Table:
        """Apply interval join between two tables."""
        import pandas as pd
        left_df = left_table.to_pandas()
        right_df = right_table.to_pandas()

        if time_column not in left_df.columns or time_column not in right_df.columns:
            return left_table

        left_df[time_column] = pd.to_datetime(left_df[time_column])
        right_df[time_column] = pd.to_datetime(right_df[time_column])

        # Cross join and filter by time interval
        left_df['_key'] = 1
        right_df['_key'] = 1
        merged = left_df.merge(right_df, on='_key', suffixes=('', '_right'))
        merged = merged.drop('_key', axis=1)

        # Filter by interval
        time_diff = (merged[f'{time_column}_right'] - merged[time_column]).dt.total_seconds() * 1000
        mask = (time_diff >= lower_bound) & (time_diff <= upper_bound)
        result = merged[mask]

        return ca.Table.from_pandas(result)


def _apply_filter(table: ca.Table, expr: str, pc) -> ca.Table:
    """Apply a filter expression to a table using Arrow compute."""
    # Parse simple expressions: column op value
    import re

    # Handle AND/OR
    if ' AND ' in expr.upper():
        parts = re.split(r'\s+AND\s+', expr, flags=re.IGNORECASE)
        result = table
        for part in parts:
            result = _apply_filter(result, part.strip(), pc)
        return result

    if ' OR ' in expr.upper():
        parts = re.split(r'\s+OR\s+', expr, flags=re.IGNORECASE)
        masks = []
        for part in parts:
            # Get mask for each part
            filtered = _apply_filter(table, part.strip(), pc)
            # Can't easily combine OR masks, use pandas fallback
            pass
        # Fallback to pandas for OR
        df = table.to_pandas()
        result = df.query(expr.replace('=', '==').replace('<>', '!='))
        return ca.Table.from_pandas(result)

    # Parse comparison
    patterns = [
        (r'(\w+)\s*>=\s*(.+)', 'ge'),
        (r'(\w+)\s*<=\s*(.+)', 'le'),
        (r'(\w+)\s*<>\s*(.+)', 'ne'),
        (r'(\w+)\s*!=\s*(.+)', 'ne'),
        (r'(\w+)\s*>\s*(.+)', 'gt'),
        (r'(\w+)\s*<\s*(.+)', 'lt'),
        (r'(\w+)\s*=\s*(.+)', 'eq'),
        (r"(\w+)\s+LIKE\s+'(.+)'", 'like'),
        (r"(\w+)\s+like\s+'(.+)'", 'like'),
        (r'(\w+)\s+IS\s+NULL', 'is_null'),
        (r'(\w+)\s+is\s+null', 'is_null'),
        (r'(\w+)\s+IS\s+NOT\s+NULL', 'is_not_null'),
        (r'(\w+)\s+is\s+not\s+null', 'is_not_null'),
        (r"(\w+)\s+IN\s*\((.+)\)", 'in'),
        (r"(\w+)\s+in\s*\((.+)\)", 'in'),
        (r"(\w+)\s+BETWEEN\s+(.+)\s+AND\s+(.+)", 'between'),
        (r"(\w+)\s+between\s+(.+)\s+and\s+(.+)", 'between'),
    ]

    for pattern, op in patterns:
        match = re.match(pattern, expr.strip(), re.IGNORECASE)
        if match:
            groups = match.groups()
            col_name = groups[0]

            if col_name not in table.column_names:
                return table

            column = table.column(col_name)

            if op == 'is_null':
                mask = pc.is_null(column)
            elif op == 'is_not_null':
                mask = pc.invert(pc.is_null(column))
            elif op == 'like':
                # Convert SQL LIKE to regex
                pattern_str = groups[1].replace('%', '.*').replace('_', '.')
                mask = pc.match_like(column, groups[1])
            elif op == 'in':
                # Parse IN list
                values_str = groups[1]
                values = [v.strip().strip("'\"") for v in values_str.split(',')]
                # Try to convert to appropriate type
                try:
                    values = [float(v) for v in values]
                except ValueError:
                    pass
                mask = pc.is_in(column, value_set=ca.array(values))
            elif op == 'between':
                low = groups[1].strip()
                high = groups[2].strip()
                try:
                    low_val = float(low)
                    high_val = float(high)
                except ValueError:
                    low_val = low.strip("'\"")
                    high_val = high.strip("'\"")
                mask_low = pc.greater_equal(column, low_val)
                mask_high = pc.less_equal(column, high_val)
                mask = pc.and_(mask_low, mask_high)
            else:
                # Comparison operators
                value_str = groups[1].strip()
                # Try to parse as number
                try:
                    value = float(value_str)
                    if value == int(value):
                        value = int(value)
                except ValueError:
                    value = value_str.strip("'\"")

                if op == 'gt':
                    mask = pc.greater(column, value)
                elif op == 'ge':
                    mask = pc.greater_equal(column, value)
                elif op == 'lt':
                    mask = pc.less(column, value)
                elif op == 'le':
                    mask = pc.less_equal(column, value)
                elif op == 'eq':
                    mask = pc.equal(column, value)
                elif op == 'ne':
                    mask = pc.not_equal(column, value)
                else:
                    return table

            return table.filter(mask)

    return table

