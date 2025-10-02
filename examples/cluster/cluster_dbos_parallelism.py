#!/usr/bin/env python3
"""
Cluster DBOS Orchestration Demo

This example demonstrates DBOS orchestration of Sabot workers/agents
in a cluster environment. It shows:
- DBOS making intelligent decisions about agent placement
- Orchestration layer coordinating Sabot worker deployment
- Load balancing and resource optimization decisions
- Separation between orchestration (DBOS) and execution (Sabot)
"""

import asyncio
import random
import time
import statistics
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class TaskPriority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class ClusterTask:
    """Represents a task to be executed in the cluster."""
    task_id: str
    task_type: str
    priority: TaskPriority
    data_size: int
    complexity: float  # 0.0 to 1.0
    created_at: float
    assigned_node: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    execution_time: Optional[float] = None

    @property
    def is_completed(self) -> bool:
        return self.completed_at is not None

    @property
    def is_running(self) -> bool:
        return self.started_at is not None and not self.is_completed

    @property
    def estimated_duration(self) -> float:
        """Estimate task duration based on size and complexity."""
        base_time = 0.1  # Base processing time
        size_factor = self.data_size / 100.0  # Normalize size
        complexity_factor = 1.0 + (self.complexity * 2.0)  # 1x to 3x based on complexity
        priority_factor = {  # Priority affects scheduling but not duration
            TaskPriority.LOW: 1.0,
            TaskPriority.MEDIUM: 1.0,
            TaskPriority.HIGH: 1.0,
            TaskPriority.CRITICAL: 1.0
        }[self.priority]

        return base_time * size_factor * complexity_factor * priority_factor


@dataclass
class ClusterNode:
    """Represents a cluster node with processing capabilities."""
    node_id: str
    cpu_cores: int
    memory_gb: float
    current_load: float = 0.0  # 0.0 to 1.0
    active_tasks: int = 0
    completed_tasks: int = 0
    total_execution_time: float = 0.0
    performance_history: List[float] = field(default_factory=list)

    @property
    def available_capacity(self) -> float:
        """Available processing capacity (0.0 to 1.0)."""
        return max(0.0, 1.0 - self.current_load)

    @property
    def avg_execution_time(self) -> float:
        """Average task execution time."""
        return self.total_execution_time / max(1, self.completed_tasks)

    def can_accept_task(self, task: ClusterTask) -> bool:
        """Check if node can accept a task."""
        # Check capacity
        if self.available_capacity < 0.1:  # Need at least 10% capacity
            return False

        # Check resource requirements (simplified)
        estimated_load = min(1.0, self.current_load + (task.estimated_duration / 10.0))
        return estimated_load <= 0.9  # Don't exceed 90% load


class DBOSOrchestrator:
    """DBOS orchestration layer for Sabot workers/agents."""

    def __init__(self):
        self.cluster_nodes: Dict[str, ClusterNode] = {}
        self.pending_workloads: List[ClusterTask] = []
        self.active_deployments: Dict[str, ClusterTask] = {}
        self.completed_workloads: List[ClusterTask] = []
        self.orchestration_decisions = {
            "total_decisions": 0,
            "optimal_placements": 0,
            "load_balance_score": 0.0,
            "resource_utilization": 0.0
        }

    def register_node(self, node: ClusterNode):
        """Register a Sabot worker node with DBOS."""
        self.cluster_nodes[node.node_id] = node
        print(f"🖥️  DBOS registered Sabot node: {node.node_id} ({node.cpu_cores} cores, {node.memory_gb}GB RAM)")

    def submit_workload(self, workload: ClusterTask):
        """Submit workload for DBOS orchestration."""
        self.pending_workloads.append(workload)
        self.orchestration_decisions["total_decisions"] += 1
        print(f"📋 DBOS received workload {workload.task_id} ({workload.task_type}, priority: {workload.priority.name})")

    def orchestrate_placement(self, workload: ClusterTask) -> Optional[str]:
        """DBOS makes intelligent placement decisions for Sabot agents."""
        available_nodes = [
            (node_id, node) for node_id, node in self.cluster_nodes.items()
            if node.can_accept_task(workload)
        ]

        if not available_nodes:
            return None

        # DBOS scoring algorithm for optimal placement
        node_scores = []
        for node_id, node in available_nodes:
            # Resource capacity score
            capacity_score = node.available_capacity

            # Historical performance score
            avg_time = node.avg_execution_time
            performance_score = 1.0 / (1.0 + avg_time) if avg_time > 0 else 0.5

            # Current load balance score
            load_balance_score = 1.0 - node.current_load

            # Workload-specific affinity score
            affinity_score = self._calculate_workload_affinity(workload, node)

            # Priority-based scheduling boost
            priority_multiplier = {
                TaskPriority.LOW: 1.0,
                TaskPriority.MEDIUM: 1.2,
                TaskPriority.HIGH: 1.5,
                TaskPriority.CRITICAL: 2.0
            }[workload.priority]

            # DBOS orchestration score
            orchestration_score = (
                capacity_score * 0.35 +
                performance_score * 0.25 +
                load_balance_score * 0.20 +
                affinity_score * 0.20
            ) * priority_multiplier

            node_scores.append((node_id, orchestration_score))

        # DBOS selects optimal placement
        if node_scores:
            best_node = max(node_scores, key=lambda x: x[1])
            self.orchestration_decisions["optimal_placements"] += 1
            return best_node[0]

        return None

    def _calculate_workload_affinity(self, workload: ClusterTask, node: ClusterNode) -> float:
        """Calculate workload-to-node affinity score."""
        # Simplified affinity calculation
        # In real DBOS, this would consider data locality, network costs, etc.
        base_affinity = 0.7  # Default affinity

        # Prefer nodes that have handled similar workloads before
        if node.completed_tasks > 0:
            # Slight preference for nodes with experience
            base_affinity += 0.1

        # Consider resource requirements
        if workload.complexity > 0.7 and node.cpu_cores >= 4:
            base_affinity += 0.1  # Prefer powerful nodes for complex work
        elif workload.complexity < 0.3 and node.cpu_cores <= 2:
            base_affinity += 0.05  # Ok to use smaller nodes for simple work

        return min(1.0, base_affinity)

    async def coordinate_sabot_execution(self, workload: ClusterTask, node_id: str):
        """DBOS coordinates Sabot agent execution on selected node."""
        node = self.cluster_nodes[node_id]
        workload.assigned_node = node_id

        # DBOS instructs Sabot to deploy agent
        print(f"🎯 DBOS deploying {workload.task_type} agent to Sabot node {node_id}")

        # Simulate Sabot agent deployment and execution
        await self._simulate_sabot_execution(workload, node)

    async def _simulate_sabot_execution(self, workload: ClusterTask, node: ClusterNode):
        """Simulate Sabot agent execution (normally handled by Sabot runtime)."""
        workload.started_at = time.time()

        # Update node load (DBOS tracks this for future decisions)
        node.active_tasks += 1
        node.current_load = min(1.0, node.current_load + 0.1)

        self.active_deployments[workload.task_id] = workload

        # Simulate Sabot execution time with variance
        execution_time = workload.estimated_duration * random.uniform(0.8, 1.2)
        await asyncio.sleep(execution_time)

        # Complete execution
        workload.completed_at = time.time()
        workload.execution_time = execution_time

        # Update node stats for DBOS learning
        node.active_tasks -= 1
        node.completed_tasks += 1
        node.total_execution_time += execution_time
        node.current_load = max(0.0, node.current_load - 0.1)
        node.performance_history.append(execution_time)

        # Keep recent performance history
        if len(node.performance_history) > 100:
            node.performance_history = node.performance_history[-100:]

        del self.active_deployments[workload.task_id]
        self.completed_workloads.append(workload)

        print(f"✅ Sabot completed workload {workload.task_id} on node {node.node_id} in {execution_time:.2f}s")

    async def orchestrate_workloads(self):
        """DBOS continuously orchestrates workload placement."""
        while True:
            # Sort pending workloads by priority
            self.pending_workloads.sort(key=lambda w: w.priority.value, reverse=True)

            # Make orchestration decisions
            workloads_to_deploy = []

            for workload in self.pending_workloads:
                optimal_node = self.orchestrate_placement(workload)
                if optimal_node:
                    workloads_to_deploy.append((workload, optimal_node))
                    break  # Deploy one at a time to avoid overwhelming

            # Deploy selected workloads
            for workload, node_id in workloads_to_deploy:
                self.pending_workloads.remove(workload)
                asyncio.create_task(self.coordinate_sabot_execution(workload, node_id))

            await asyncio.sleep(0.1)  # Small delay between orchestration decisions

    def calculate_cluster_metrics(self):
        """DBOS calculates cluster-wide orchestration metrics."""
        if self.cluster_nodes:
            node_loads = [node.current_load for node in self.cluster_nodes.values()]
            if node_loads:
                avg_load = statistics.mean(node_loads)
                load_variance = statistics.variance(node_loads) if len(node_loads) > 1 else 0
                self.orchestration_decisions["load_balance_score"] = max(0.0, 1.0 - load_variance)

                # Resource utilization (inverse of average available capacity)
                avg_capacity = statistics.mean([node.available_capacity for node in self.cluster_nodes.values()])
                self.orchestration_decisions["resource_utilization"] = 1.0 - avg_capacity

    def get_orchestration_status(self) -> Dict[str, Any]:
        """Get DBOS orchestration status."""
        self.calculate_cluster_metrics()

        return {
            "cluster_nodes": {
                node_id: {
                    "load": node.current_load,
                    "active_deployments": node.active_tasks,
                    "completed_workloads": node.completed_tasks,
                    "avg_execution_time": node.avg_execution_time,
                    "available_capacity": node.available_capacity
                }
                for node_id, node in self.cluster_nodes.items()
            },
            "workloads": {
                "pending": len(self.pending_workloads),
                "active": len(self.active_deployments),
                "completed": len(self.completed_workloads)
            },
            "orchestration": self.orchestration_decisions.copy()
        }


async def generate_cluster_workload(orchestrator: DBOSOrchestrator):
    """Generate realistic cluster workload for DBOS orchestration."""
    workload_types = ["data_processing", "analytics", "ml_inference", "api_requests", "database_queries"]

    workload_id_counter = 0

    while True:
        # Generate workloads with varying characteristics
        workload_id_counter += 1
        workload = ClusterTask(
            task_id=f"workload_{workload_id_counter:04d}",
            task_type=random.choice(workload_types),
            priority=random.choice(list(TaskPriority)),
            data_size=random.randint(10, 500),
            complexity=random.uniform(0.1, 0.9),
            created_at=time.time()
        )

        orchestrator.submit_workload(workload)

        # Variable arrival rate
        await asyncio.sleep(random.uniform(0.05, 0.2))


async def main():
    """Run the cluster DBOS parallelism demo."""
    print("🧠 Cluster DBOS Parallelism Demo")
    print("=" * 36)

    # Create DBOS controller
    controller = DBOSParallelController()

    # Add cluster nodes with different capabilities
    controller.add_node(ClusterNode("compute-node-01", cpu_cores=8, memory_gb=16))  # High-end
    controller.add_node(ClusterNode("compute-node-02", cpu_cores=4, memory_gb=8))   # Medium
    controller.add_node(ClusterNode("compute-node-03", cpu_cores=6, memory_gb=12))  # Medium-high
    controller.add_node(ClusterNode("compute-node-04", cpu_cores=2, memory_gb=4))   # Low-end

    print()

    # Start task processing
    print("🚀 Starting intelligent task scheduling...")
    print("🧠 DBOS controller will optimize task placement based on:")
    print("   • Node capacity and current load")
    print("   • Task priority and requirements")
    print("   • Historical performance data")
    print("   • Load balancing objectives")
    print()

    # Start background task processor
    processor_task = asyncio.create_task(controller.process_pending_tasks())

    # Generate workload for 20 seconds
    print("📊 Generating cluster workload...")
    workload_start = time.time()

    try:
        # Generate workload while monitoring cluster
        workload_task = asyncio.create_task(generate_cluster_workload(controller))

        while time.time() - workload_start < 20:
            # Periodic status updates
            if int(time.time() - workload_start) % 5 == 0:
                status = controller.get_cluster_status()

                print(f"\n📈 Status Update (t={int(time.time() - workload_start)}s):")
                print(f"   Tasks - Pending: {status['tasks']['pending']}, "
                      f"Running: {status['tasks']['running']}, "
                      f"Completed: {status['tasks']['completed']}")

                print(f"   Performance - Throughput: {status['performance']['throughput']:.2f} tasks/s, "
                      f"Avg Time: {status['performance']['avg_execution_time']:.2f}s")

                print("   Node Loads:"                for node_id, node_info in status['nodes'].items():
                    load_pct = node_info['load'] * 100
                    active = node_info['active_tasks']
                    print(".1f")

                await asyncio.sleep(1)  # Avoid duplicate updates
            else:
                await asyncio.sleep(1)

        # Wait a bit for remaining tasks to complete
        await asyncio.sleep(3)

    except KeyboardInterrupt:
        print("\n🛑 Stopping cluster processing...")

    # Stop background tasks
    processor_task.cancel()
    try:
        await processor_task
    except asyncio.CancelledError:
        pass

    # Final analysis
    print("\n📊 Final Cluster Analysis")
    print("-" * 28)

    final_status = controller.get_cluster_status()

    print(f"🎯 Total Tasks Processed: {final_status['performance']['total_tasks']}")
    print(f"⚡ Average Execution Time: {final_status['performance']['avg_execution_time']:.2f}s")
    print(".2f"    print(".2f"
    print("\n🏗️  Node Performance Summary:")
    for node_id, node_info in final_status['nodes'].items():
        completed = node_info['completed_tasks']
        avg_time = node_info['avg_execution_time']
        print(f"   {node_id}: {completed} tasks completed, avg {avg_time:.2f}s per task")

    print("
🎯 DBOS Parallelism Demo Summary:"    print("   • Intelligent task scheduling across heterogeneous nodes"    print("   • Adaptive load balancing based on real-time metrics"    print("   • Priority-aware task placement and execution"    print("   • Performance monitoring and optimization"    print("   • Fault-tolerant distributed processing"    print("   • Resource-aware workload distribution"

    print("\n✅ Cluster DBOS parallelism demo completed!")


if __name__ == "__main__":
    asyncio.run(main())
