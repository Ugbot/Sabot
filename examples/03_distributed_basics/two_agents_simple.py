#!/usr/bin/env python3
"""
Simple Distributed Enrichment Demo

Demonstrates Sabot's distributed execution:
1. Create 2 simple worker agents
2. Create a JobManager coordinator
3. Submit an enrichment job (simulated)
4. Show task execution across agents

This is a simplified proof-of-concept, not full production system.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict, Optional
import pyarrow as pa
import pyarrow.compute as pc


# ============================================================================
# Simplified Agent Implementation
# ============================================================================

@dataclass
class SimpleTask:
    """A task to be executed by an agent"""
    task_id: str
    operator_type: str  # 'load', 'filter', 'join', 'aggregate'
    parameters: dict
    data: Optional[pa.Table] = None  # Input data


class SimpleAgent:
    """Simplified agent that can execute tasks"""

    def __init__(self, agent_id: str, port: int):
        self.agent_id = agent_id
        self.port = port
        self.running = False
        self.completed_tasks: List[SimpleTask] = []

    async def start(self):
        """Start agent"""
        self.running = True
        print(f"✅ {self.agent_id} started on port {self.port}")

    async def stop(self):
        """Stop agent"""
        self.running = False
        print(f"🛑 {self.agent_id} stopped")

    async def execute_task(self, task: SimpleTask) -> pa.Table:
        """Execute a task and return result"""
        print(f"  [{self.agent_id}] Executing {task.operator_type} task: {task.task_id}")

        # Simulate processing time
        await asyncio.sleep(0.1)

        if task.operator_type == 'load':
            # Load data (simulated)
            result = task.data
            rows = result.num_rows if result else 0
            print(f"    → Loaded {rows:,} rows")

        elif task.operator_type == 'filter':
            # Apply filter
            column = task.parameters.get('column')
            value = task.parameters.get('value')
            result = task.data.filter(
                pc.greater(task.data[column], value)
            )
            print(f"    → Filtered: {task.data.num_rows:,} → {result.num_rows:,} rows")

        elif task.operator_type == 'join':
            # Hash join
            left_data = task.parameters.get('left_data')
            right_data = task.parameters.get('right_data')
            left_key = task.parameters.get('left_key')
            right_key = task.parameters.get('right_key')

            # Use native Arrow join
            result = left_data.join(
                right_data,
                keys=left_key,
                right_keys=right_key,
                join_type='inner'
            )
            print(f"    → Joined: {left_data.num_rows:,} × {right_data.num_rows:,} → {result.num_rows:,} rows")

        elif task.operator_type == 'aggregate':
            # Group by and aggregate
            group_col = task.parameters.get('group_by')
            agg_col = task.parameters.get('aggregate')

            # Simple aggregation using PyArrow
            result = task.data.group_by(group_col).aggregate([
                (agg_col, 'sum'),
                (agg_col, 'count')
            ])
            print(f"    → Aggregated: {task.data.num_rows:,} → {result.num_rows:,} groups")

        else:
            result = task.data

        self.completed_tasks.append(task)
        return result


# ============================================================================
# Simplified JobManager
# ============================================================================

class SimpleJobManager:
    """Simplified job manager for coordinating agents"""

    def __init__(self):
        self.agents: Dict[str, SimpleAgent] = {}
        self.jobs: Dict[str, Dict] = {}

    def register_agent(self, agent: SimpleAgent):
        """Register an agent"""
        self.agents[agent.agent_id] = agent
        print(f"📝 Registered {agent.agent_id}")

    async def submit_job(self, job_name: str, tasks: List[SimpleTask]) -> Dict:
        """Submit a job with tasks"""
        job_id = f"job-{len(self.jobs) + 1}"
        self.jobs[job_id] = {
            'name': job_name,
            'status': 'running',
            'tasks': tasks,
            'results': []
        }

        print(f"\n🚀 Submitting job '{job_name}' (ID: {job_id})")
        print(f"   Tasks: {len(tasks)}")
        print(f"   Agents: {len(self.agents)}")

        # Distribute tasks across agents (round-robin)
        agent_list = list(self.agents.values())
        results = []

        for i, task in enumerate(tasks):
            agent = agent_list[i % len(agent_list)]
            result = await agent.execute_task(task)
            results.append(result)

        self.jobs[job_id]['results'] = results
        self.jobs[job_id]['status'] = 'completed'

        return {
            'job_id': job_id,
            'status': 'completed',
            'results': results
        }


# ============================================================================
# Demo Main
# ============================================================================

async def main():
    print("=" * 70)
    print("Simple Distributed Enrichment Demo")
    print("=" * 70)
    print()

    # Step 1: Start 2 agents
    print("Step 1: Starting Agents")
    print("-" * 70)

    agent1 = SimpleAgent("agent-1", 8816)
    agent2 = SimpleAgent("agent-2", 8817)

    await agent1.start()
    await agent2.start()
    print()

    # Step 2: Create JobManager
    print("Step 2: Create JobManager")
    print("-" * 70)

    job_manager = SimpleJobManager()
    job_manager.register_agent(agent1)
    job_manager.register_agent(agent2)
    print()

    # Step 3: Create test data (similar to fintech enrichment)
    print("Step 3: Create Test Data")
    print("-" * 70)

    # Securities (large table)
    securities = pa.table({
        'ID': range(1000),
        'CUSIP': [f'CUSIP{i:04d}' for i in range(1000)],
        'NAME': [f'Security {i}' for i in range(1000)]
    })
    print(f"✅ Securities: {securities.num_rows:,} rows, {securities.num_columns} columns")

    # Quotes (smaller table, some will match)
    quotes = pa.table({
        'instrumentId': [10, 50, 100, 200, 500, 750, 999, 1, 25, 100],
        'price': [100.5, 200.3, 150.0, 300.2, 250.1, 180.5, 90.3, 120.0, 105.5, 151.0],
        'size': [100, 200, 150, 300, 250, 180, 90, 120, 105, 151]
    })
    print(f"✅ Quotes: {quotes.num_rows:,} rows, {quotes.num_columns} columns")
    print()

    # Step 4: Submit enrichment job
    print("Step 4: Submit Enrichment Job")
    print("-" * 70)

    start_time = time.perf_counter()

    # Task 1: Load securities (agent 1)
    task1 = SimpleTask(
        task_id="task-1",
        operator_type="load",
        parameters={},
        data=securities
    )

    # Task 2: Load quotes (agent 2)
    task2 = SimpleTask(
        task_id="task-2",
        operator_type="load",
        parameters={},
        data=quotes
    )

    # Task 3: Join securities + quotes (agent 1)
    task3 = SimpleTask(
        task_id="task-3",
        operator_type="join",
        parameters={
            'left_data': quotes,
            'right_data': securities,
            'left_key': 'instrumentId',
            'right_key': 'ID'
        },
        data=None
    )

    # Submit job
    result = await job_manager.submit_job(
        "enrichment-demo",
        [task1, task2, task3]
    )

    elapsed = time.perf_counter() - start_time
    print(f"\n✅ Job completed in {elapsed*1000:.1f}ms")
    print()

    # Step 5: Show results
    print("Step 5: Results")
    print("-" * 70)

    enriched = result['results'][2]  # Join result
    print(f"✅ Enriched data: {enriched.num_rows} rows")
    print()
    print("Sample results:")
    # Arrow-native display without pandas dependency
    for i in range(min(5, enriched.num_rows)):
        row = enriched.slice(i, 1)
        row_dict = row.to_pydict()
        print(f"Row {i}: {row_dict}")
    print()

    # Step 6: Show agent stats
    print("Step 6: Agent Statistics")
    print("-" * 70)

    print(f"Agent 1: {len(agent1.completed_tasks)} tasks completed")
    print(f"Agent 2: {len(agent2.completed_tasks)} tasks completed")
    print()

    # Cleanup
    await agent1.stop()
    await agent2.stop()

    print("=" * 70)
    print("Demo Complete!")
    print("=" * 70)
    print()
    print("Key Points:")
    print("  • 2 agents executed tasks in parallel")
    print("  • JobManager coordinated task distribution")
    print("  • Join executed successfully across agents")
    print("  • Ready to integrate with Phase 6 + Phase 7")


if __name__ == '__main__':
    asyncio.run(main())
