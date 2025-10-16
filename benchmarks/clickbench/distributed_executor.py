#!/usr/bin/env python3
"""
Distributed SQL Executor for ClickBench

Handles distributed execution of SQL queries across multiple Sabot agents.
Integrates with Sabot's orchestrator and agent management system.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import pyarrow as pa
from sabot import cyarrow as ca
from sabot_sql import SabotSQLBridge, create_sabot_sql_bridge

logger = logging.getLogger(__name__)

class DistributedSQLExecutor:
    """Distributed SQL executor for ClickBench queries."""
    
    def __init__(self, num_agents: int = 4, morsel_size_kb: int = 64):
        """
        Initialize distributed executor.
        
        Args:
            num_agents: Number of agents to distribute work across
            morsel_size_kb: Morsel size for parallel execution
        """
        self.num_agents = num_agents
        self.morsel_size_kb = morsel_size_kb
        
        # Agent bridges
        self.agent_bridges: List[SabotSQLBridge] = []
        self.agent_tables: Dict[int, ca.Table] = {}
        
        # Performance metrics
        self.execution_stats: Dict[str, Any] = {
            'total_queries': 0,
            'total_time': 0.0,
            'agent_times': {},
            'shuffle_times': {},
            'aggregation_times': {}
        }
    
    async def initialize(self, hits_table: ca.Table):
        """
        Initialize distributed executor with data.
        
        Args:
            hits_table: The hits table to distribute across agents
        """
        logger.info(f"Initializing distributed executor with {self.num_agents} agents")
        
        # Distribute table across agents
        await self._distribute_table(hits_table)
        
        logger.info("Distributed executor initialized")
    
    async def _distribute_table(self, hits_table: ca.Table):
        """Distribute the hits table across agents using round-robin partitioning."""
        logger.info(f"Distributing {hits_table.num_rows} rows across {self.num_agents} agents")
        
        # Calculate rows per agent
        rows_per_agent = hits_table.num_rows // self.num_agents
        remainder = hits_table.num_rows % self.num_agents
        
        start_row = 0
        for agent_id in range(self.num_agents):
            # Calculate end row for this agent
            end_row = start_row + rows_per_agent
            if agent_id < remainder:
                end_row += 1
            
            # Slice table for this agent
            agent_table = self._slice_table(hits_table, start_row, end_row)
            self.agent_tables[agent_id] = agent_table
            
            # Create bridge for this agent
            bridge = create_sabot_sql_bridge()
            bridge.register_table("hits", agent_table)
            self.agent_bridges.append(bridge)
            
            logger.info(f"Agent {agent_id}: {agent_table.num_rows} rows")
            start_row = end_row
    
    def _slice_table(self, table: ca.Table, start_row: int, end_row: int) -> ca.Table:
        """Slice a table to get rows from start_row to end_row."""
        # Convert to Arrow for slicing
        arrow_table = table.to_pyarrow()
        
        # Slice the table
        sliced_table = arrow_table.slice(start_row, end_row - start_row)
        
        # Convert back to cyarrow
        return ca.Table.from_pyarrow(sliced_table)
    
    async def execute_query(self, query: str) -> ca.Table:
        """
        Execute a query across distributed agents.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Combined result from all agents
        """
        start_time = time.time()
        
        # Analyze query to determine execution strategy
        execution_strategy = self._analyze_query(query)
        
        logger.info(f"Executing query with strategy: {execution_strategy}")
        
        if execution_strategy == "local_only":
            # Query can be executed locally on each agent
            result = await self._execute_local_distributed(query)
        elif execution_strategy == "requires_shuffle":
            # Query requires data shuffling between agents
            result = await self._execute_with_shuffle(query)
        elif execution_strategy == "requires_aggregation":
            # Query requires aggregation across agents
            result = await self._execute_with_aggregation(query)
        else:
            # Default to local distributed execution
            result = await self._execute_local_distributed(query)
        
        execution_time = time.time() - start_time
        self.execution_stats['total_queries'] += 1
        self.execution_stats['total_time'] += execution_time
        
        logger.info(f"Query executed in {execution_time:.3f}s, result: {result.num_rows} rows")
        return result
    
    def _analyze_query(self, query: str) -> str:
        """Analyze query to determine execution strategy."""
        query_upper = query.upper()
        
        # Check for aggregation functions
        aggregation_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP BY']
        if any(func in query_upper for func in aggregation_functions):
            return "requires_aggregation"
        
        # Check for joins (would require shuffle)
        if 'JOIN' in query_upper:
            return "requires_shuffle"
        
        # Check for ORDER BY with LIMIT (might benefit from shuffle)
        if 'ORDER BY' in query_upper and 'LIMIT' in query_upper:
            return "requires_shuffle"
        
        # Default to local execution
        return "local_only"
    
    async def _execute_local_distributed(self, query: str) -> ca.Table:
        """Execute query locally on each agent and combine results."""
        logger.info("Executing query locally on each agent")
        
        # Execute on each agent in parallel
        tasks = []
        for agent_id, bridge in enumerate(self.agent_bridges):
            task = asyncio.create_task(self._execute_on_agent(bridge, query, agent_id))
            tasks.append(task)
        
        # Wait for all agents to complete
        agent_results = await asyncio.gather(*tasks)
        
        # Combine results
        return self._combine_results(agent_results)
    
    async def _execute_on_agent(self, bridge: SabotSQLBridge, query: str, agent_id: int) -> ca.Table:
        """Execute query on a single agent."""
        try:
            start_time = time.time()
            result = bridge.execute_sql(query)
            execution_time = time.time() - start_time
            
            # Track agent performance
            if agent_id not in self.execution_stats['agent_times']:
                self.execution_stats['agent_times'][agent_id] = []
            self.execution_stats['agent_times'][agent_id].append(execution_time)
            
            logger.debug(f"Agent {agent_id} executed query in {execution_time:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"Agent {agent_id} failed to execute query: {e}")
            # Return empty result
            return ca.Table.from_pydict({'count': [0]})
    
    def _combine_results(self, agent_results: List[ca.Table]) -> ca.Table:
        """Combine results from multiple agents."""
        if not agent_results:
            return ca.Table.from_pydict({'count': [0]})
        
        # If all results are empty, return empty
        if all(result.num_rows == 0 for result in agent_results):
            return agent_results[0]
        
        # Filter out empty results
        non_empty_results = [result for result in agent_results if result.num_rows > 0]
        
        if not non_empty_results:
            return ca.Table.from_pydict({'count': [0]})
        
        if len(non_empty_results) == 1:
            return non_empty_results[0]
        
        # Combine multiple results
        try:
            # Convert to Arrow tables for concatenation
            arrow_tables = [result.to_pyarrow() for result in non_empty_results]
            
            # Concatenate tables
            combined_arrow = pa.concat_tables(arrow_tables)
            
            # Convert back to cyarrow
            return ca.Table.from_pyarrow(combined_arrow)
            
        except Exception as e:
            logger.error(f"Failed to combine results: {e}")
            # Return the first non-empty result
            return non_empty_results[0]
    
    async def _execute_with_shuffle(self, query: str) -> ca.Table:
        """Execute query that requires data shuffling between agents."""
        logger.info("Executing query with data shuffle")
        
        # For now, fall back to local distributed execution
        # In a full implementation, this would:
        # 1. Determine shuffle keys
        # 2. Redistribute data based on keys
        # 3. Execute query on redistributed data
        # 4. Combine results
        
        return await self._execute_local_distributed(query)
    
    async def _execute_with_aggregation(self, query: str) -> ca.Table:
        """Execute query that requires aggregation across agents."""
        logger.info("Executing query with cross-agent aggregation")
        
        # Execute locally on each agent first
        local_results = await self._execute_local_distributed(query)
        
        # For aggregation queries, we need to combine results properly
        # This is a simplified implementation - in reality would need
        # proper aggregation logic based on the specific functions used
        
        return local_results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        stats = self.execution_stats.copy()
        
        if stats['total_queries'] > 0:
            stats['avg_time_per_query'] = stats['total_time'] / stats['total_queries']
        
        # Calculate agent performance
        for agent_id, times in stats['agent_times'].items():
            if times:
                stats['agent_times'][agent_id] = {
                    'total_time': sum(times),
                    'avg_time': sum(times) / len(times),
                    'min_time': min(times),
                    'max_time': max(times),
                    'query_count': len(times)
                }
        
        return stats
    
    def print_performance_stats(self):
        """Print performance statistics."""
        stats = self.get_performance_stats()
        
        print("\n" + "="*60)
        print("DISTRIBUTED EXECUTOR PERFORMANCE STATS")
        print("="*60)
        
        print(f"Total queries executed: {stats['total_queries']}")
        print(f"Total execution time: {stats['total_time']:.3f}s")
        
        if stats['total_queries'] > 0:
            print(f"Average time per query: {stats['avg_time_per_query']:.3f}s")
        
        print(f"\nAgent Performance:")
        for agent_id, agent_stats in stats['agent_times'].items():
            print(f"  Agent {agent_id}:")
            print(f"    Queries: {agent_stats['query_count']}")
            print(f"    Total time: {agent_stats['total_time']:.3f}s")
            print(f"    Avg time: {agent_stats['avg_time']:.3f}s")
            print(f"    Min time: {agent_stats['min_time']:.3f}s")
            print(f"    Max time: {agent_stats['max_time']:.3f}s")
        
        print("="*60)
    
    async def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up distributed executor")
        
        # Clear agent bridges
        self.agent_bridges.clear()
        self.agent_tables.clear()
        
        logger.info("Distributed executor cleanup complete")

