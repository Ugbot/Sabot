"""
High-Level Python API for SQL Engine

Provides a simple interface for SQL query execution with Sabot.
Uses cyarrow (Sabot's optimized Arrow) throughout.
"""

from typing import Optional, Dict, Any
from sabot import cyarrow as ca  # Use Sabot's optimized Arrow!
import logging

from sabot.sql.controller import SQLController
from sabot.agent_manager import DurableAgentManager

logger = logging.getLogger(__name__)


class SQLEngine:
    """
    High-Level SQL Engine Interface
    
    Simple API for SQL query execution with automatic agent provisioning
    and distributed execution.
    
    Example:
        engine = SQLEngine(num_agents=4)
        engine.register_table("customers", customers_table)
        engine.register_table("orders", orders_table)
        
        result = await engine.execute('''
            SELECT c.name, COUNT(*) as order_count
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            GROUP BY c.name
            HAVING order_count > 5
            ORDER BY order_count DESC
        ''')
        
        print(result.to_pandas())
    """
    
    def __init__(
        self,
        num_agents: int = 4,
        execution_mode: str = "local_parallel",
        storage_backend: str = "memory",
        database_url: Optional[str] = None
    ):
        """
        Initialize SQL engine
        
        Args:
            num_agents: Number of agents for distributed execution
            execution_mode: "local", "local_parallel", or "distributed"
            storage_backend: Storage backend ("memory", "rocksdb")
            database_url: Database URL for agent manager (if using distributed mode)
        """
        self.num_agents = num_agents
        self.execution_mode = execution_mode
        self.storage_backend = storage_backend
        
        # Initialize agent manager if using distributed mode
        self.agent_manager: Optional[DurableAgentManager] = None
        if execution_mode == "distributed":
            if not database_url:
                database_url = "postgresql://localhost/sabot"
            # TODO: Initialize agent manager
            # self.agent_manager = DurableAgentManager(app=None, database_url=database_url)
            logger.warning("Distributed mode requires agent manager - using local_parallel for now")
            self.execution_mode = "local_parallel"
        
        # Initialize SQL controller
        self.controller = SQLController(agent_manager=self.agent_manager)
        
        logger.info(
            f"SQL Engine initialized: mode={self.execution_mode}, "
            f"agents={self.num_agents}, backend={self.storage_backend}"
        )
    
    def register_table(self, table_name: str, table: ca.Table) -> None:
        """
        Register a cyarrow table for querying
        
        Args:
            table_name: Name to register table as
            table: cyarrow table data (Sabot's optimized Arrow)
        """
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.controller.register_table(table_name, table))
    
    def register_table_from_file(self, table_name: str, file_path: str) -> None:
        """
        Register a table from file (CSV, Parquet, Arrow IPC)
        
        Args:
            table_name: Name to register table as
            file_path: Path to data file
        """
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self.controller.register_table_from_file(table_name, file_path)
        )
    
    def register_kafka_source(self, table_name: str, topic: str, **kwargs: Any) -> None:
        """
        Register a Kafka stream as a table source
        
        Args:
            table_name: Name to register table as
            topic: Kafka topic name
            **kwargs: Additional Kafka configuration
        """
        # TODO: Implement Kafka source registration
        logger.warning("Kafka source registration not yet implemented")
    
    async def execute(self, sql: str) -> ca.Table:
        """
        Execute SQL query and return results
        
        Args:
            sql: SQL query string
            
        Returns:
            cyarrow table with results (Sabot's optimized Arrow)
        """
        return await self.controller.execute(
            sql,
            num_agents=self.num_agents,
            execution_mode=self.execution_mode
        )
    
    async def execute_streaming(self, sql: str):
        """
        Execute SQL query and stream results
        
        Args:
            sql: SQL query string
            
        Yields:
            Record batches from query execution
        """
        # TODO: Implement streaming execution
        result = await self.execute(sql)
        
        # Convert table to batches
        batch_size = 10000
        for i in range(0, result.num_rows, batch_size):
            end = min(i + batch_size, result.num_rows)
            sliced = result.slice(i, end - i)
            yield sliced.to_batches()[0] if sliced.num_rows > 0 else None
    
    async def explain(self, sql: str) -> str:
        """
        Get query execution plan
        
        Args:
            sql: SQL query string
            
        Returns:
            String representation of execution plan
        """
        return await self.controller.explain(sql)
    
    def list_tables(self) -> list[str]:
        """
        Get list of registered tables
        
        Returns:
            List of table names
        """
        return list(self.controller.tables.keys())
    
    def get_table_schema(self, table_name: str) -> ca.Schema:
        """
        Get schema for a registered table
        
        Args:
            table_name: Name of the table
            
        Returns:
            cyarrow schema
        """
        if table_name not in self.controller.tables:
            raise ValueError(f"Table not found: {table_name}")
        return self.controller.tables[table_name].schema
    
    async def close(self) -> None:
        """
        Close the SQL engine and clean up resources
        """
        if self.agent_manager:
            await self.agent_manager.stop()
        logger.info("SQL Engine closed")


# Convenience function for quick SQL execution
async def execute_sql(sql: str, **tables: ca.Table) -> ca.Table:
    """
    Quick SQL execution without explicit engine creation
    
    Args:
        sql: SQL query string
        **tables: Named cyarrow tables to register
        
    Returns:
        cyarrow table with results (Sabot's optimized Arrow)
        
    Example:
        result = await execute_sql(
            "SELECT * FROM orders WHERE amount > 1000",
            orders=orders_table
        )
    """
    engine = SQLEngine()
    
    for table_name, table in tables.items():
        engine.register_table(table_name, table)
    
    result = await engine.execute(sql)
    await engine.close()
    
    return result

