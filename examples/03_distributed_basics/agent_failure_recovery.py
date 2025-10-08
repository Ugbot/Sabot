#!/usr/bin/env python3
"""
Agent Failure Recovery - Fault Tolerance
=========================================

**What this demonstrates:**
- Agent failure detection
- Automatic task retry
- Fault tolerance in distributed execution
- Recovery strategies

**Prerequisites:** Completed round_robin_scheduling.py

**Runtime:** ~15 seconds

**Next steps:**
- See state_partitioning.py for stateful distributed processing
- See 04_production_patterns/ for production fault tolerance

**Pattern:**
JobManager → Task → Agent fails → Detect → Retry on different agent

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import pyarrow as pa
from typing import Dict, List, Optional
from dataclasses import dataclass
import random


@dataclass
class Task:
    """Task representation."""
    task_id: str
    operator_type: str
    data: pa.Table
    attempt: int = 0
    max_retries: int = 3


class AgentFailureError(Exception):
    """Raised when agent fails to execute task."""
    pass


class SimpleAgent:
    """Agent that can fail randomly."""

    def __init__(self, agent_id: str, failure_probability: float = 0.0):
        self.agent_id = agent_id
        self.failure_probability = failure_probability
        self.tasks_executed = 0
        self.tasks_failed = 0
        self.is_alive = True

    async def execute_task(self, task: Task) -> pa.Table:
        """Execute task (may fail randomly)."""

        # Check if agent is alive
        if not self.is_alive:
            raise AgentFailureError(f"Agent {self.agent_id} is dead")

        # Simulate random failure
        if random.random() < self.failure_probability:
            self.tasks_failed += 1
            raise AgentFailureError(f"Agent {self.agent_id} failed to execute {task.task_id}")

        # Simulate work
        await asyncio.sleep(0.1)

        # Process task
        import pyarrow.compute as pc

        if task.operator_type == "filter":
            mask = pc.greater(task.data['value'], 50)
            result = task.data.filter(mask)
        elif task.operator_type == "map":
            doubled = pc.multiply(task.data['value'], 2.0)
            result = task.data.append_column('doubled_value', doubled)
        else:
            result = task.data

        self.tasks_executed += 1
        return result

    def kill(self):
        """Kill the agent (simulate crash)."""
        self.is_alive = False
        print(f"💀 Agent {self.agent_id} crashed!")


class FaultTolerantJobManager:
    """Job manager with fault tolerance."""

    def __init__(self, max_retries: int = 3):
        self.agents: Dict[str, SimpleAgent] = {}
        self.max_retries = max_retries
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.retried_tasks = 0

    def register_agent(self, agent: SimpleAgent):
        """Register an agent."""
        self.agents[agent.agent_id] = agent
        print(f"✅ Registered agent: {agent.agent_id}")

    def get_available_agents(self) -> List[SimpleAgent]:
        """Get list of alive agents."""
        return [agent for agent in self.agents.values() if agent.is_alive]

    async def execute_task_with_retry(self, task: Task) -> Optional[pa.Table]:
        """
        Execute task with automatic retry on failure.

        Returns:
            Task result or None if all retries exhausted
        """

        self.total_tasks += 1

        for attempt in range(self.max_retries):
            task.attempt = attempt + 1

            # Get available agents
            available_agents = self.get_available_agents()

            if not available_agents:
                print(f"  ❌ {task.task_id}: No agents available!")
                self.failed_tasks += 1
                return None

            # Pick agent (simple round-robin)
            agent = available_agents[attempt % len(available_agents)]

            try:
                print(f"  🔄 {task.task_id}: Attempt {attempt + 1}/{self.max_retries} on {agent.agent_id}")

                # Execute task
                result = await agent.execute_task(task)

                print(f"  ✅ {task.task_id}: Succeeded on {agent.agent_id}")

                if attempt > 0:
                    self.retried_tasks += 1

                self.successful_tasks += 1
                return result

            except AgentFailureError as e:
                print(f"  ⚠️  {task.task_id}: Failed on {agent.agent_id} - {e}")

                # Last attempt?
                if attempt == self.max_retries - 1:
                    print(f"  ❌ {task.task_id}: All retries exhausted!")
                    self.failed_tasks += 1
                    return None

                # Retry
                print(f"  🔁 {task.task_id}: Retrying...")
                await asyncio.sleep(0.1)

        return None

    async def submit_job(self, tasks: List[Task]) -> List[Optional[pa.Table]]:
        """Submit job with fault tolerance."""

        print(f"\n📋 Submitting job with {len(tasks)} tasks")
        print(f"   Agents available: {len(self.get_available_agents())}")
        print(f"   Max retries: {self.max_retries}")
        print("=" * 70)

        results = []

        for task in tasks:
            result = await self.execute_task_with_retry(task)
            results.append(result)

        return results


def create_sample_tasks(num_tasks: int = 10) -> List[Task]:
    """Create sample tasks."""

    tasks = []

    for i in range(num_tasks):
        data = pa.table({
            'id': list(range(i * 10, (i + 1) * 10)),
            'value': [j + i * 10 for j in range(10)]
        })

        operator_type = "filter" if i % 2 == 0 else "map"

        task = Task(
            task_id=f"task_{i}",
            operator_type=operator_type,
            data=data
        )

        tasks.append(task)

    return tasks


async def main():
    print("🛡️  Agent Failure Recovery Demo")
    print("=" * 70)
    print("\nDemonstrates fault tolerance with automatic retry")

    # Scenario 1: Agents with random failures
    print("\n\n🎲 Scenario 1: Random Agent Failures")
    print("=" * 70)

    agent1 = SimpleAgent("agent_1", failure_probability=0.2)  # 20% failure rate
    agent2 = SimpleAgent("agent_2", failure_probability=0.1)  # 10% failure rate
    agent3 = SimpleAgent("agent_3", failure_probability=0.3)  # 30% failure rate

    job_manager = FaultTolerantJobManager(max_retries=3)

    job_manager.register_agent(agent1)
    job_manager.register_agent(agent2)
    job_manager.register_agent(agent3)

    tasks = create_sample_tasks(10)

    results = await job_manager.submit_job(tasks)

    print("\n\n📊 Scenario 1 Results")
    print("=" * 70)
    print(f"Total tasks: {job_manager.total_tasks}")
    print(f"Successful: {job_manager.successful_tasks} ({job_manager.successful_tasks / job_manager.total_tasks * 100:.1f}%)")
    print(f"Failed: {job_manager.failed_tasks}")
    print(f"Retried: {job_manager.retried_tasks}")

    print(f"\nAgent statistics:")
    print(f"  Agent 1: {agent1.tasks_executed} executed, {agent1.tasks_failed} failed")
    print(f"  Agent 2: {agent2.tasks_executed} executed, {agent2.tasks_failed} failed")
    print(f"  Agent 3: {agent3.tasks_executed} executed, {agent3.tasks_failed} failed")

    # Scenario 2: Agent crash during execution
    print("\n\n💀 Scenario 2: Agent Crash")
    print("=" * 70)

    agent4 = SimpleAgent("agent_4", failure_probability=0.0)
    agent5 = SimpleAgent("agent_5", failure_probability=0.0)
    agent6 = SimpleAgent("agent_6", failure_probability=0.0)

    job_manager2 = FaultTolerantJobManager(max_retries=3)

    job_manager2.register_agent(agent4)
    job_manager2.register_agent(agent5)
    job_manager2.register_agent(agent6)

    tasks2 = create_sample_tasks(10)

    # Execute first 3 tasks normally
    print("\nExecuting first 3 tasks...")
    results2 = []
    for i in range(3):
        result = await job_manager2.execute_task_with_retry(tasks2[i])
        results2.append(result)

    # Crash agent4
    print("\n💥 Simulating agent4 crash...")
    agent4.kill()

    # Continue with remaining tasks
    print("\nContinuing with remaining tasks...")
    for i in range(3, len(tasks2)):
        result = await job_manager2.execute_task_with_retry(tasks2[i])
        results2.append(result)

    print("\n\n📊 Scenario 2 Results")
    print("=" * 70)
    print(f"Total tasks: {job_manager2.total_tasks}")
    print(f"Successful: {job_manager2.successful_tasks} ({job_manager2.successful_tasks / job_manager2.total_tasks * 100:.1f}%)")
    print(f"Failed: {job_manager2.failed_tasks}")
    print(f"Retried: {job_manager2.retried_tasks}")

    print(f"\nAgent statistics:")
    print(f"  Agent 4: {agent4.tasks_executed} executed, alive={agent4.is_alive}")
    print(f"  Agent 5: {agent5.tasks_executed} executed, alive={agent5.is_alive}")
    print(f"  Agent 6: {agent6.tasks_executed} executed, alive={agent6.is_alive}")

    print("\n\n💡 Key Takeaways")
    print("=" * 70)
    print("1. Automatic retry recovers from transient failures")
    print("2. Tasks reassigned to different agents on failure")
    print("3. Dead agents detected and avoided")
    print("4. Configurable max retries (tradeoff: reliability vs latency)")
    print("5. Production systems use checkpoints for long-running tasks")

    print("\n\n🔗 Recovery Strategies")
    print("=" * 70)
    print("Retry:        Re-execute failed task (current demo)")
    print("Checkpoint:   Resume from last checkpoint")
    print("Replication:  Execute on multiple agents, use first result")
    print("Failover:     Standby agents take over")
    print("Circuit breaker: Temporarily skip failing agents")

    print("\n\n🔗 Production Considerations")
    print("=" * 70)
    print("✅ Set appropriate max_retries (balance reliability vs latency)")
    print("✅ Use exponential backoff between retries")
    print("✅ Monitor agent health proactively")
    print("✅ Checkpoint long-running tasks")
    print("✅ Log failures for debugging")
    print("✅ Alert on high failure rates")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See state_partitioning.py for stateful recovery")
    print("- See 04_production_patterns/ for production fault tolerance")
    print("- See docs/ARCHITECTURE_OVERVIEW.md for Chandy-Lamport checkpoints")


if __name__ == "__main__":
    asyncio.run(main())
