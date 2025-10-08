#!/usr/bin/env python3
"""
Round Robin Scheduling - Task Distribution
===========================================

**What this demonstrates:**
- Task scheduling strategies (round-robin)
- How JobManager distributes tasks to agents
- Load balancing across multiple agents
- Task assignment visualization

**Prerequisites:** Completed two_agents_simple.py

**Runtime:** ~10 seconds

**Next steps:**
- See agent_failure_recovery.py for fault tolerance
- See 04_production_patterns/ for production distributed patterns

**Pattern:**
JobManager → Tasks → Agents (round-robin distribution)

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import pyarrow as pa
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class Task:
    """Simplified task representation."""
    task_id: str
    operator_type: str
    data: pa.Table = None
    result: pa.Table = None


class SimpleAgent:
    """Simplified agent that executes tasks."""

    def __init__(self, agent_id: str, host: str, port: int):
        self.agent_id = agent_id
        self.host = host
        self.port = port
        self.tasks_executed = 0
        self.total_rows_processed = 0

    async def execute_task(self, task: Task) -> pa.Table:
        """Execute a single task."""

        print(f"    Agent {self.agent_id}: Executing {task.task_id} ({task.operator_type})")

        # Simulate work
        await asyncio.sleep(0.1)

        # Simple processing
        if task.operator_type == "filter":
            # Filter: keep rows where value > 50
            import pyarrow.compute as pc
            mask = pc.greater(task.data['value'], 50)
            result = task.data.filter(mask)

        elif task.operator_type == "map":
            # Map: add doubled_value column
            import pyarrow.compute as pc
            doubled = pc.multiply(task.data['value'], 2.0)
            result = task.data.append_column('doubled_value', doubled)

        else:
            # Pass through
            result = task.data

        self.tasks_executed += 1
        self.total_rows_processed += result.num_rows

        print(f"    Agent {self.agent_id}: Completed {task.task_id} ({result.num_rows} rows)")

        return result


class RoundRobinScheduler:
    """
    Simple round-robin task scheduler.

    Distributes tasks evenly across agents in circular order.
    """

    def __init__(self, agents: Dict[str, SimpleAgent]):
        self.agents = agents
        self.agent_list = list(agents.values())
        self.current_index = 0

    def schedule_task(self, task: Task) -> SimpleAgent:
        """
        Schedule task to next agent in round-robin order.

        Returns:
            Agent assigned to execute the task
        """

        # Get next agent
        agent = self.agent_list[self.current_index]

        # Advance to next agent (circular)
        self.current_index = (self.current_index + 1) % len(self.agent_list)

        print(f"  Scheduler: Assigned {task.task_id} → Agent {agent.agent_id}")

        return agent


class SimpleJobManager:
    """Simplified job manager with round-robin scheduling."""

    def __init__(self):
        self.agents: Dict[str, SimpleAgent] = {}
        self.scheduler = None

    def register_agent(self, agent: SimpleAgent):
        """Register an agent."""
        self.agents[agent.agent_id] = agent
        print(f"✅ Registered agent: {agent.agent_id} ({agent.host}:{agent.port})")

        # Update scheduler
        self.scheduler = RoundRobinScheduler(self.agents)

    async def submit_job(self, tasks: List[Task]) -> List[pa.Table]:
        """
        Submit job (list of tasks) for execution.

        Uses round-robin scheduling to distribute tasks.
        """

        print(f"\n📋 Submitting job with {len(tasks)} tasks")
        print(f"   Agents available: {len(self.agents)}")
        print("=" * 70)

        results = []

        for task in tasks:
            # Schedule task
            agent = self.scheduler.schedule_task(task)

            # Execute on assigned agent
            result = await agent.execute_task(task)
            results.append(result)

        return results


def create_sample_tasks(num_tasks: int = 10) -> List[Task]:
    """Create sample tasks for demonstration."""

    tasks = []

    for i in range(num_tasks):
        # Create sample data
        data = pa.table({
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [j + i * 100 for j in range(100)]
        })

        # Alternate between filter and map tasks
        operator_type = "filter" if i % 2 == 0 else "map"

        task = Task(
            task_id=f"task_{i}",
            operator_type=operator_type,
            data=data
        )

        tasks.append(task)

    return tasks


async def main():
    print("🔄 Round Robin Scheduling Demo")
    print("=" * 70)
    print("\nDemonstrates how JobManager distributes tasks across agents")

    # Create agents
    print("\n\n🤖 Creating Agents")
    print("=" * 70)

    agent1 = SimpleAgent("agent_1", "localhost", 5001)
    agent2 = SimpleAgent("agent_2", "localhost", 5002)
    agent3 = SimpleAgent("agent_3", "localhost", 5003)

    # Create job manager
    job_manager = SimpleJobManager()

    # Register agents
    print("\n📝 Registering Agents")
    print("=" * 70)

    job_manager.register_agent(agent1)
    job_manager.register_agent(agent2)
    job_manager.register_agent(agent3)

    # Create tasks
    print("\n\n📦 Creating Tasks")
    print("=" * 70)

    num_tasks = 12
    tasks = create_sample_tasks(num_tasks)

    print(f"Created {len(tasks)} tasks:")
    for task in tasks:
        print(f"  {task.task_id}: {task.operator_type}")

    # Submit job
    print("\n\n⚙️  Executing Job (Round Robin Scheduling)")
    print("=" * 70)

    results = await job_manager.submit_job(tasks)

    # Show results
    print("\n\n📊 Execution Summary")
    print("=" * 70)

    print(f"\nTask Distribution:")
    print(f"  Agent 1: {agent1.tasks_executed} tasks, {agent1.total_rows_processed:,} rows")
    print(f"  Agent 2: {agent2.tasks_executed} tasks, {agent2.total_rows_processed:,} rows")
    print(f"  Agent 3: {agent3.tasks_executed} tasks, {agent3.total_rows_processed:,} rows")

    total_tasks = agent1.tasks_executed + agent2.tasks_executed + agent3.tasks_executed
    print(f"\nTotal: {total_tasks} tasks executed")

    # Verify round-robin
    print("\n\n✅ Round Robin Verification")
    print("=" * 70)

    expected_per_agent = num_tasks // 3
    remainder = num_tasks % 3

    print(f"Expected distribution (12 tasks ÷ 3 agents):")
    print(f"  Each agent: {expected_per_agent} tasks")
    print(f"  Remainder: {remainder} task(s) → assigned to first {remainder} agent(s)")

    print(f"\nActual distribution:")
    print(f"  Agent 1: {agent1.tasks_executed} tasks {'✅' if agent1.tasks_executed == expected_per_agent + (1 if remainder > 0 else 0) else '❌'}")
    print(f"  Agent 2: {agent2.tasks_executed} tasks {'✅' if agent2.tasks_executed == expected_per_agent + (1 if remainder > 1 else 0) else '❌'}")
    print(f"  Agent 3: {agent3.tasks_executed} tasks {'✅' if agent3.tasks_executed == expected_per_agent else '❌'}")

    # Visualization
    print("\n\n📈 Task Assignment Visualization")
    print("=" * 70)

    print("\nTask Assignment Pattern (12 tasks → 3 agents):")
    print("  Task 0  → Agent 1")
    print("  Task 1  → Agent 2")
    print("  Task 2  → Agent 3")
    print("  Task 3  → Agent 1  (cycle repeats)")
    print("  Task 4  → Agent 2")
    print("  Task 5  → Agent 3")
    print("  Task 6  → Agent 1")
    print("  Task 7  → Agent 2")
    print("  Task 8  → Agent 3")
    print("  Task 9  → Agent 1")
    print("  Task 10 → Agent 2")
    print("  Task 11 → Agent 3")

    print("\n\n💡 Key Takeaways")
    print("=" * 70)
    print("1. Round-robin distributes tasks evenly across agents")
    print("2. Simple and fair scheduling strategy")
    print("3. Each agent gets (total_tasks ÷ num_agents) tasks")
    print("4. Remainder tasks assigned to first agents")
    print("5. Good for homogeneous agents (same performance)")

    print("\n\n🔗 Other Scheduling Strategies")
    print("=" * 70)
    print("Round-robin:     Distribute evenly (current demo)")
    print("Least-loaded:    Assign to agent with fewest tasks")
    print("Resource-based:  Assign based on CPU/memory availability")
    print("Locality-aware:  Assign based on data locality")
    print("Custom:          Application-specific logic")

    print("\n\n🔗 When to Use Round-Robin")
    print("=" * 70)
    print("✅ Agents have similar performance")
    print("✅ Tasks have similar cost")
    print("✅ Simple, predictable distribution needed")
    print("✅ No data locality constraints")

    print("\n❌ Avoid when:")
    print("   Agents have different performance (use resource-based)")
    print("   Tasks have variable cost (use least-loaded)")
    print("   Data locality matters (use locality-aware)")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See agent_failure_recovery.py for fault tolerance")
    print("- See state_partitioning.py for stateful distributed processing")
    print("- See 04_production_patterns/ for production patterns")


if __name__ == "__main__":
    asyncio.run(main())
