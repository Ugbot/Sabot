"""
SQL Controller for Agent-Based SQL Execution

Provisions agents for distributed SQL query execution using Sabot's morsel operators.
"""

import asyncio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from sabot import cyarrow as ca  # Use Sabot's optimized Arrow
import logging

from sabot.agent_manager import DurableAgentManager
from sabot.agents.runtime import AgentSpec
from sabot.sql.agents import SQLScanAgent, SQLJoinAgent, SQLAggregateAgent
from sabot.sql.sql_to_operators import SQLQueryExecutor

logger = logging.getLogger(__name__)


@dataclass
class SQLExecutionPlan:
    """Represents an execution plan for SQL query"""
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
    ) -> pa.Table:
        """
        Execute query stages with provisioned agents
        """
        # TODO: Implement actual stage execution
        # For now, return empty result
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("amount", pa.float64()),
        ])
        return pa.table({}, schema=schema)
    
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

