#!/usr/bin/env python3
"""
Optimized Distributed Enrichment Demo

Demonstrates Sabot's Phase 6 + Phase 7 integration:
1. Build JobGraph with logical operators
2. Apply PlanOptimizer (filter pushdown, projection pushdown, join reordering)
3. Distribute optimized tasks to agents
4. Show optimization impact on execution

This bridges simple_distributed_demo.py with full Sabot infrastructure.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict, Optional
import pyarrow as pa
import pyarrow.compute as pc
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer


# ============================================================================
# Task and Agent (from simple demo)
# ============================================================================

@dataclass
class Task:
    """A task to be executed by an agent"""
    task_id: str
    operator_type: str
    operator_name: str
    parameters: dict
    data: Optional[pa.Table] = None


class Agent:
    """Agent that executes tasks"""

    def __init__(self, agent_id: str, port: int):
        self.agent_id = agent_id
        self.port = port
        self.running = False
        self.completed_tasks: List[Task] = []

    async def start(self):
        """Start agent"""
        self.running = True
        print(f"✅ {self.agent_id} started on port {self.port}")

    async def stop(self):
        """Stop agent"""
        self.running = False
        print(f"🛑 {self.agent_id} stopped")

    async def execute_task(self, task: Task) -> pa.Table:
        """Execute a task and return result"""
        print(f"  [{self.agent_id}] Executing {task.operator_type}: {task.operator_name}")

        # Simulate processing time
        await asyncio.sleep(0.05)

        if task.operator_type == 'source':
            # Load data
            result = task.data
            rows = result.num_rows if result else 0
            print(f"    → Loaded {rows:,} rows")

        elif task.operator_type == 'filter':
            # Apply filter
            column = task.parameters.get('column')
            value = task.parameters.get('value')
            operator = task.parameters.get('operator', '>')

            if operator == '>':
                mask = pc.greater(task.data[column], value)
            elif operator == '<':
                mask = pc.less(task.data[column], value)
            elif operator == '==':
                mask = pc.equal(task.data[column], value)
            else:
                raise ValueError(f"Unknown operator: {operator}")

            result = task.data.filter(mask)
            print(f"    → Filtered: {task.data.num_rows:,} → {result.num_rows:,} rows ({result.num_rows/task.data.num_rows*100:.1f}% retained)")

        elif task.operator_type == 'select':
            # Column projection
            columns = task.parameters.get('columns', [])
            result = task.data.select(columns)
            print(f"    → Selected {len(columns)} columns: {', '.join(columns)}")

        elif task.operator_type == 'hash_join':
            # Hash join
            left_data = task.parameters.get('left_data')
            right_data = task.parameters.get('right_data')
            left_key = task.parameters.get('left_key')
            right_key = task.parameters.get('right_key')

            result = left_data.join(
                right_data,
                keys=left_key,
                right_keys=right_key,
                join_type='inner'
            )
            print(f"    → Joined: {left_data.num_rows:,} × {right_data.num_rows:,} → {result.num_rows:,} rows")

        else:
            result = task.data

        self.completed_tasks.append(task)
        return result


class JobManager:
    """Job manager with optimizer integration"""

    def __init__(self):
        self.agents: Dict[str, Agent] = {}
        self.jobs: Dict[str, Dict] = {}
        self.optimizer = PlanOptimizer(
            enable_filter_pushdown=True,
            enable_projection_pushdown=True,
            enable_join_reordering=False,  # Not needed for this simple demo
            enable_operator_fusion=False,   # Keep operators separate for visibility
            enable_dead_code_elimination=True
        )

    def register_agent(self, agent: Agent):
        """Register an agent"""
        self.agents[agent.agent_id] = agent
        print(f"📝 Registered {agent.agent_id}")

    def build_job_graph(self, job_name: str) -> JobGraph:
        """
        Build a logical JobGraph for fintech enrichment.

        Pipeline:
        1. Load quotes (source)
        2. Load securities (source)
        3. Filter quotes (price > 100)
        4. Select columns from securities (ID, CUSIP only)
        5. Join quotes + securities
        """
        graph = JobGraph(job_name=job_name)

        # Source 1: Load quotes
        source_quotes = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            name="load_quotes",
            parameters={'table': 'quotes'}
        )
        graph.add_operator(source_quotes)

        # Source 2: Load securities
        source_securities = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            name="load_securities",
            parameters={'table': 'securities'}
        )
        graph.add_operator(source_securities)

        # Filter quotes (BEFORE join - will be pushed down by optimizer)
        filter_quotes = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            name="filter_quotes_price",
            parameters={'column': 'price', 'value': 100.0, 'operator': '>'}
        )
        graph.add_operator(filter_quotes)

        # Select columns from securities (BEFORE join - will be pushed down)
        select_securities = StreamOperatorNode(
            operator_type=OperatorType.SELECT,
            name="select_securities_columns",
            parameters={'columns': ['ID', 'CUSIP', 'NAME']}
        )
        graph.add_operator(select_securities)

        # Join quotes + securities
        join_op = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            name="join_quotes_securities",
            parameters={'left_key': 'instrumentId', 'right_key': 'ID'},
            stateful=True
        )
        graph.add_operator(join_op)

        # Sink (just for completeness)
        sink_op = StreamOperatorNode(
            operator_type=OperatorType.SINK,
            name="output_enriched",
            parameters={}
        )
        graph.add_operator(sink_op)

        # Connect operators
        graph.connect(source_quotes.operator_id, filter_quotes.operator_id)
        graph.connect(filter_quotes.operator_id, join_op.operator_id)
        graph.connect(source_securities.operator_id, select_securities.operator_id)
        graph.connect(select_securities.operator_id, join_op.operator_id)
        graph.connect(join_op.operator_id, sink_op.operator_id)

        return graph

    async def submit_job(self, graph: JobGraph, data: Dict[str, pa.Table]) -> Dict:
        """
        Submit job with optimization.

        Args:
            graph: JobGraph (will be optimized)
            data: Input data tables

        Returns:
            Job results with optimization stats
        """
        job_id = f"job-{len(self.jobs) + 1}"

        print(f"\n{'='*70}")
        print(f"Job Submission: {graph.job_name}")
        print(f"{'='*70}")

        # PHASE 7: Apply optimizer
        print(f"\n📊 Optimization Phase")
        print(f"{'-'*70}")
        print(f"Original operators: {len(graph.operators)}")

        start_opt = time.perf_counter()
        optimized_graph = self.optimizer.optimize(graph)
        opt_time = (time.perf_counter() - start_opt) * 1000

        stats = self.optimizer.get_stats()
        print(f"Optimized operators: {len(optimized_graph.operators)}")
        print(f"Optimization time: {opt_time:.2f}ms")
        print(f"")
        print(f"Optimizations applied:")
        print(f"  • Filters pushed: {stats.filters_pushed}")
        print(f"  • Projections pushed: {stats.projections_pushed}")
        print(f"  • Joins reordered: {stats.joins_reordered}")
        print(f"  • Operators fused: {stats.operators_fused}")
        print(f"  • Dead code eliminated: {stats.dead_code_eliminated}")

        # Convert optimized graph to tasks
        print(f"\n🚀 Task Generation")
        print(f"{'-'*70}")
        tasks = self._graph_to_tasks(optimized_graph, data)
        print(f"Generated {len(tasks)} tasks from optimized graph")

        # Execute tasks
        print(f"\n⚡ Execution Phase")
        print(f"{'-'*70}")

        start_exec = time.perf_counter()
        results = await self._execute_tasks(tasks)
        exec_time = (time.perf_counter() - start_exec) * 1000

        print(f"\n✅ Execution complete in {exec_time:.1f}ms")

        return {
            'job_id': job_id,
            'status': 'completed',
            'optimization_stats': stats,
            'optimization_time_ms': opt_time,
            'execution_time_ms': exec_time,
            'results': results
        }

    def _graph_to_tasks(self, graph: JobGraph, data: Dict[str, pa.Table]) -> List[Task]:
        """Convert optimized JobGraph to executable tasks"""
        tasks = []

        # Topological sort to get execution order
        sorted_ops = graph.topological_sort()

        for op in sorted_ops:
            # Skip sink for now
            if op.operator_type == OperatorType.SINK:
                continue

            task = Task(
                task_id=op.operator_id,
                operator_type=op.operator_type.value,
                operator_name=op.name,
                parameters=op.parameters.copy()
            )

            # Attach data for sources
            if op.operator_type == OperatorType.SOURCE:
                table_name = op.parameters.get('table')
                task.data = data.get(table_name)

            tasks.append(task)

        return tasks

    async def _execute_tasks(self, tasks: List[Task]) -> List[pa.Table]:
        """Execute tasks across agents"""
        agent_list = list(self.agents.values())
        results = {}  # task_id -> result

        # Execute in order (simplified - real system would handle dependencies)
        for i, task in enumerate(tasks):
            agent = agent_list[i % len(agent_list)]

            # If task needs input from previous tasks, wire it up by matching names
            if task.operator_type == 'filter':
                # Filter operates on quotes
                for prev_task in tasks[:i]:
                    if prev_task.operator_type == 'source' and 'quote' in prev_task.operator_name.lower():
                        task.data = results.get(prev_task.task_id)
                        break

            elif task.operator_type == 'select':
                # Select operates on securities
                for prev_task in tasks[:i]:
                    if prev_task.operator_type == 'source' and 'securities' in prev_task.operator_name.lower():
                        task.data = results.get(prev_task.task_id)
                        break

            elif task.operator_type == 'hash_join':
                # Wire up join inputs
                left_data = None
                right_data = None

                for prev_task in tasks[:i]:
                    if 'quote' in prev_task.operator_name.lower() and prev_task.task_id in results:
                        left_data = results[prev_task.task_id]
                    elif 'securities' in prev_task.operator_name.lower() and prev_task.task_id in results:
                        right_data = results[prev_task.task_id]

                task.parameters['left_data'] = left_data
                task.parameters['right_data'] = right_data

            result = await agent.execute_task(task)
            results[task.task_id] = result

        return list(results.values())


# ============================================================================
# Demo Main
# ============================================================================

async def main():
    print("="*70)
    print("Optimized Distributed Enrichment Demo")
    print("Phase 6 (DBOS Control) + Phase 7 (Plan Optimization)")
    print("="*70)
    print()

    # Step 1: Start agents
    print("Step 1: Starting Agents")
    print("-"*70)

    agent1 = Agent("agent-1", 8816)
    agent2 = Agent("agent-2", 8817)

    await agent1.start()
    await agent2.start()
    print()

    # Step 2: Create JobManager
    print("Step 2: Create JobManager with Optimizer")
    print("-"*70)

    job_manager = JobManager()
    job_manager.register_agent(agent1)
    job_manager.register_agent(agent2)
    print()

    # Step 3: Create test data
    print("Step 3: Create Test Data")
    print("-"*70)

    # Securities (large table)
    securities = pa.table({
        'ID': range(10000),
        'CUSIP': [f'CUSIP{i:05d}' for i in range(10000)],
        'NAME': [f'Security {i}' for i in range(10000)],
        'SECTOR': [f'Sector {i % 10}' for i in range(10000)]  # Extra column for projection demo
    })
    print(f"✅ Securities: {securities.num_rows:,} rows, {securities.num_columns} columns")

    # Quotes (smaller, with varying prices for filter demo)
    import random
    random.seed(42)
    quote_prices = [random.uniform(50, 200) for _ in range(1000)]

    quotes = pa.table({
        'instrumentId': [random.randint(0, 9999) for _ in range(1000)],
        'price': quote_prices,
        'size': [random.randint(100, 1000) for _ in range(1000)]
    })
    print(f"✅ Quotes: {quotes.num_rows:,} rows, {quotes.num_columns} columns")
    print(f"   Price range: ${min(quote_prices):.2f} - ${max(quote_prices):.2f}")
    print(f"   Quotes with price > 100: {sum(1 for p in quote_prices if p > 100)}")
    print()

    # Step 4: Build JobGraph
    print("Step 4: Build Logical JobGraph")
    print("-"*70)

    graph = job_manager.build_job_graph("fintech_enrichment")
    print(f"✅ JobGraph created with {len(graph.operators)} operators")
    print()

    # Step 5: Submit job (with optimization)
    result = await job_manager.submit_job(
        graph,
        data={'quotes': quotes, 'securities': securities}
    )

    # Step 6: Show results
    print()
    print("="*70)
    print("Results Summary")
    print("="*70)
    print()
    print(f"Job ID: {result['job_id']}")
    print(f"Status: {result['status']}")
    print(f"Optimization time: {result['optimization_time_ms']:.2f}ms")
    print(f"Execution time: {result['execution_time_ms']:.1f}ms")
    print()

    stats = result['optimization_stats']
    print(f"Optimizations:")
    print(f"  • Total optimizations: {stats.total_optimizations()}")
    print(f"  • Filters pushed: {stats.filters_pushed}")
    print(f"  • Projections pushed: {stats.projections_pushed}")
    print()

    # Show final enriched data
    enriched = result['results'][-1]  # Last result should be join output
    print(f"Enriched data: {enriched.num_rows:,} rows, {enriched.num_columns} columns")
    print()
    print("Sample results:")
    print(enriched.to_pandas().head(5))
    print()

    # Step 7: Agent stats
    print("="*70)
    print("Agent Statistics")
    print("="*70)
    print(f"Agent 1: {len(agent1.completed_tasks)} tasks completed")
    print(f"Agent 2: {len(agent2.completed_tasks)} tasks completed")
    print()

    # Cleanup
    await agent1.stop()
    await agent2.stop()

    print("="*70)
    print("Demo Complete!")
    print("="*70)
    print()
    print("Key Achievements:")
    print("  ✅ JobGraph built with logical operators")
    print("  ✅ PlanOptimizer applied optimizations")
    print("  ✅ Tasks distributed across agents")
    print("  ✅ Optimized execution completed successfully")
    print("  ✅ Ready for full Phase 6 + Phase 7 integration")


if __name__ == '__main__':
    asyncio.run(main())
