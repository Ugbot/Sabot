#!/usr/bin/env python3
"""
Sabot ClickBench Benchmark Runner

Runs ClickBench queries on Sabot SQL with distributed agents across multiple nodes.
Supports parquet file loading and distributed execution with performance metrics.
"""

import asyncio
import time
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Sabot imports
from sabot import cyarrow as ca
from sabot_sql import SabotSQLOrchestrator, create_sabot_sql_bridge
from sabot.distributed_agents import DistributedAgentManager, AgentSpec
from sabot.distributed_coordinator import DistributedCoordinator
from sabot.channel_manager import ChannelManager
from sabot.dbos_parallel_controller import DBOSParallelController

# Local imports
from distributed_executor import DistributedSQLExecutor

logger = logging.getLogger(__name__)

class SabotClickBenchRunner:
    """ClickBench benchmark runner for Sabot SQL with distributed execution."""
    
    def __init__(self, 
                 parquet_file: str,
                 num_agents: int = 4,
                 execution_mode: str = "distributed",
                 morsel_size_kb: int = 64):
        """
        Initialize ClickBench runner.
        
        Args:
            parquet_file: Path to hits.parquet file
            num_agents: Number of distributed agents to use
            execution_mode: "local", "local_parallel", or "distributed"
            morsel_size_kb: Morsel size for parallel execution
        """
        self.parquet_file = Path(parquet_file)
        self.num_agents = num_agents
        self.execution_mode = execution_mode
        self.morsel_size_kb = morsel_size_kb
        
        # Components
        self.orchestrator: Optional[SabotSQLOrchestrator] = None
        self.distributed_executor: Optional[DistributedSQLExecutor] = None
        self.agent_manager: Optional[DistributedAgentManager] = None
        self.coordinator: Optional[DistributedCoordinator] = None
        self.channel_manager: Optional[ChannelManager] = None
        self.dbos_controller: Optional[DBOSParallelController] = None
        
        # Data
        self.hits_table: Optional[ca.Table] = None
        self.queries: List[str] = []
        
        # Results
        self.results: List[Dict[str, Any]] = []
        
    async def initialize(self):
        """Initialize all components and load data."""
        logger.info("Initializing Sabot ClickBench runner...")
        
        # Load queries
        await self._load_queries()
        
        # Load parquet data
        await self._load_parquet_data()
        
        # Initialize distributed components
        if self.execution_mode == "distributed":
            await self._initialize_distributed_components()
        else:
            await self._initialize_local_components()
            
        # Initialize distributed executor
        self.distributed_executor = DistributedSQLExecutor(
            num_agents=self.num_agents,
            morsel_size_kb=self.morsel_size_kb
        )
        await self.distributed_executor.initialize(self.hits_table)
            
        logger.info(f"Initialization complete: {self.execution_mode} mode with {self.num_agents} agents")
    
    async def _load_queries(self):
        """Load ClickBench queries from SQL file."""
        queries_file = Path(__file__).parent / "queries.sql"
        if not queries_file.exists():
            raise FileNotFoundError(f"Queries file not found: {queries_file}")
            
        with open(queries_file) as f:
            self.queries = [line.strip() for line in f if line.strip()]
            
        logger.info(f"Loaded {len(self.queries)} queries")
    
    async def _load_parquet_data(self):
        """Load parquet data and convert to Sabot cyarrow table."""
        if not self.parquet_file.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.parquet_file}")
            
        logger.info(f"Loading parquet data from {self.parquet_file}")
        start_time = time.time()
        
        # Load with pandas first to handle type conversions
        df = pd.read_parquet(self.parquet_file)
        
        # Fix datetime columns
        if "EventTime" in df.columns:
            df["EventTime"] = pd.to_datetime(df["EventTime"], unit="s")
        if "EventDate" in df.columns:
            df["EventDate"] = pd.to_datetime(df["EventDate"], unit="D")
            
        # Convert object columns to string
        for col in df.columns:
            if df[col].dtype == "O":
                df[col] = df[col].astype(str)
        
        # Convert to Arrow then Sabot cyarrow
        arrow_table = pa.Table.from_pandas(df)
        self.hits_table = ca.Table.from_pyarrow(arrow_table)
        
        load_time = time.time() - start_time
        logger.info(f"Loaded {self.hits_table.num_rows} rows, {self.hits_table.num_columns} columns in {load_time:.3f}s")
    
    async def _initialize_distributed_components(self):
        """Initialize distributed execution components."""
        logger.info("Initializing distributed components...")
        
        # Create coordinator
        self.coordinator = DistributedCoordinator()
        await self.coordinator.start()
        
        # Create channel manager
        self.channel_manager = ChannelManager()
        await self.channel_manager.start()
        
        # Create DBOS controller
        self.dbos_controller = DBOSParallelController()
        await self.dbos_controller.start()
        
        # Create agent manager
        self.agent_manager = DistributedAgentManager(
            self.coordinator,
            self.channel_manager,
            self.dbos_controller
        )
        await self.agent_manager.start()
        
        # Create orchestrator
        self.orchestrator = SabotSQLOrchestrator()
        
        # Register table with orchestrator
        self.orchestrator.register_table("hits", self.hits_table)
        
        # Create distributed agents
        await self._create_distributed_agents()
        
        logger.info("Distributed components initialized")
    
    async def _initialize_local_components(self):
        """Initialize local execution components."""
        logger.info("Initializing local components...")
        
        # Create orchestrator for local execution
        self.orchestrator = SabotSQLOrchestrator()
        
        # Register table
        self.orchestrator.register_table("hits", self.hits_table)
        
        logger.info("Local components initialized")
    
    async def _create_distributed_agents(self):
        """Create distributed agents for SQL execution."""
        logger.info(f"Creating {self.num_agents} distributed agents...")
        
        async def sql_execution_agent(event):
            """Agent function for SQL execution."""
            # This would contain the actual SQL execution logic
            # For now, just return the event
            return event
        
        # Create agents
        for i in range(self.num_agents):
            agent_name = f"sql_agent_{i}"
            await self.agent_manager.create_agent(
                name=agent_name,
                func=sql_execution_agent,
                concurrency=1,
                isolated_partitions=True
            )
        
        logger.info(f"Created {self.num_agents} distributed agents")
    
    async def run_benchmark(self, warmup_runs: int = 1, benchmark_runs: int = 3):
        """Run the complete ClickBench benchmark."""
        logger.info(f"Starting ClickBench benchmark: {len(self.queries)} queries, {benchmark_runs} runs each")
        
        for query_idx, query in enumerate(self.queries):
            logger.info(f"Running query {query_idx + 1}/{len(self.queries)}: {query[:50]}...")
            
            # Warmup runs
            for _ in range(warmup_runs):
                await self._execute_query(query, warmup=True)
            
            # Benchmark runs
            times = []
            for run in range(benchmark_runs):
                start_time = time.time()
                result = await self._execute_query(query)
                end_time = time.time()
                
                execution_time = end_time - start_time
                times.append(execution_time)
                
                logger.info(f"  Run {run + 1}: {execution_time:.3f}s")
            
            # Store results
            self.results.append({
                'query_id': query_idx + 1,
                'query': query,
                'times': times,
                'avg_time': sum(times) / len(times),
                'min_time': min(times),
                'max_time': max(times),
                'num_rows': result.num_rows if hasattr(result, 'num_rows') else 0
            })
            
            logger.info(f"  Average: {sum(times) / len(times):.3f}s")
    
    async def _execute_query(self, query: str, warmup: bool = False) -> ca.Table:
        """Execute a single query."""
        if self.execution_mode == "distributed" and self.distributed_executor:
            return await self.distributed_executor.execute_query(query)
        else:
            return await self._execute_local(query)
    
    async def _execute_distributed(self, query: str) -> ca.Table:
        """Execute query using distributed agents."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")
        
        # Get agent IDs
        agent_ids = list(self.orchestrator.agents.keys()) if hasattr(self.orchestrator, 'agents') else []
        
        if not agent_ids:
            # Fallback to local execution
            logger.warning("No agents available, falling back to local execution")
            return await self._execute_local(query)
        
        # Execute distributed query
        results = self.orchestrator.execute_distributed_query(query, agent_ids)
        
        # Combine results (simplified - in reality would need proper aggregation)
        if results and len(results) > 0:
            # For now, just return the first result
            first_result = results[0]
            if 'result' in first_result and hasattr(first_result['result'], 'to_pyarrow'):
                return ca.Table.from_pyarrow(first_result['result'].to_pyarrow())
            elif 'result' in first_result:
                return first_result['result']
        
        # Fallback to empty result
        return ca.Table.from_pydict({'count': [0]})
    
    async def _execute_local(self, query: str) -> ca.Table:
        """Execute query locally."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")
        
        # Create a bridge for local execution
        bridge = create_sabot_sql_bridge()
        bridge.register_table("hits", self.hits_table)
        
        # Execute query
        result = bridge.execute_sql(query)
        return result
    
    def print_results(self):
        """Print benchmark results."""
        print("\n" + "="*80)
        print("SABOT CLICKBENCH RESULTS")
        print("="*80)
        
        total_time = 0
        total_queries = len(self.results)
        
        for result in self.results:
            print(f"\nQuery {result['query_id']}: {result['avg_time']:.3f}s avg")
            print(f"  Times: {[f'{t:.3f}' for t in result['times']]}")
            print(f"  SQL: {result['query'][:60]}...")
            print(f"  Rows: {result['num_rows']}")
            
            total_time += result['avg_time']
        
        print(f"\n" + "="*80)
        print(f"SUMMARY:")
        print(f"  Total queries: {total_queries}")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Average per query: {total_time/total_queries:.3f}s")
        print(f"  Execution mode: {self.execution_mode}")
        print(f"  Agents: {self.num_agents}")
        print("="*80)
    
    def save_results(self, output_file: str):
        """Save results to JSON file."""
        import json
        
        # Convert results to JSON-serializable format
        json_results = []
        for result in self.results:
            json_results.append({
                'query_id': result['query_id'],
                'query': result['query'],
                'times': result['times'],
                'avg_time': result['avg_time'],
                'min_time': result['min_time'],
                'max_time': result['max_time'],
                'num_rows': result['num_rows']
            })
        
        with open(output_file, 'w') as f:
            json.dump({
                'execution_mode': self.execution_mode,
                'num_agents': self.num_agents,
                'morsel_size_kb': self.morsel_size_kb,
                'results': json_results
            }, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")
    
    async def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        
        if self.agent_manager:
            await self.agent_manager.stop()
        
        if self.coordinator:
            await self.coordinator.stop()
        
        if self.channel_manager:
            await self.channel_manager.stop()
        
        if self.dbos_controller:
            await self.dbos_controller.stop()
        
        if self.distributed_executor:
            await self.distributed_executor.cleanup()
        
        logger.info("Cleanup complete")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Sabot ClickBench Benchmark")
    parser.add_argument("--parquet-file", required=True, help="Path to hits.parquet file")
    parser.add_argument("--num-agents", type=int, default=4, help="Number of distributed agents")
    parser.add_argument("--execution-mode", choices=["local", "local_parallel", "distributed"], 
                       default="distributed", help="Execution mode")
    parser.add_argument("--morsel-size-kb", type=int, default=64, help="Morsel size in KB")
    parser.add_argument("--warmup-runs", type=int, default=1, help="Number of warmup runs per query")
    parser.add_argument("--benchmark-runs", type=int, default=3, help="Number of benchmark runs per query")
    parser.add_argument("--output-file", help="Output file for results (JSON)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run benchmark
    runner = SabotClickBenchRunner(
        parquet_file=args.parquet_file,
        num_agents=args.num_agents,
        execution_mode=args.execution_mode,
        morsel_size_kb=args.morsel_size_kb
    )
    
    try:
        await runner.initialize()
        await runner.run_benchmark(
            warmup_runs=args.warmup_runs,
            benchmark_runs=args.benchmark_runs
        )
        
        runner.print_results()
        
        # Print distributed executor stats
        if runner.distributed_executor:
            runner.distributed_executor.print_performance_stats()
        
        if args.output_file:
            runner.save_results(args.output_file)
    
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
