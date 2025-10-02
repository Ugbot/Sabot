#!/usr/bin/env python3
"""
Cluster Distributed Agents Demo

This example demonstrates Sabot's distributed agent capabilities in a cluster-like setup.
It shows:
- Multiple agents coordinating across a simulated cluster
- Agent lifecycle management and supervision
- Inter-agent communication patterns
- Load balancing and fault tolerance
- Distributed state management
"""

import asyncio
import random
import time
import multiprocessing as mp
from typing import Dict, List, Optional
from dataclasses import dataclass


# Mock distributed infrastructure for demonstration
@dataclass
class ClusterNode:
    """Simulates a cluster node."""
    node_id: str
    capacity: int
    active_agents: int = 0

    @property
    def available_capacity(self) -> int:
        return self.capacity - self.active_agents


@dataclass
class AgentDeployment:
    """Represents an agent deployment on a cluster."""
    agent_id: str
    node_id: str
    agent_type: str
    status: str = "starting"
    last_heartbeat: float = 0.0


class ClusterCoordinator:
    """Simulates cluster coordination and agent management."""

    def __init__(self):
        self.nodes: Dict[str, ClusterNode] = {}
        self.deployments: Dict[str, AgentDeployment] = {}
        self.agent_types = ["data_ingestion", "processing", "analytics", "storage"]

    def add_node(self, node: ClusterNode):
        """Add a node to the cluster."""
        self.nodes[node.node_id] = node
        print(f"🏗️  Added cluster node: {node.node_id} (capacity: {node.capacity})")

    def deploy_agent(self, agent_type: str) -> Optional[str]:
        """Deploy an agent to the best available node."""
        # Find node with most available capacity
        available_nodes = [(nid, node) for nid, node in self.nodes.items()
                          if node.available_capacity > 0]

        if not available_nodes:
            return None

        # Simple load balancing: choose node with most capacity
        best_node_id, best_node = max(available_nodes,
                                    key=lambda x: x[1].available_capacity)

        # Create deployment
        agent_id = f"{agent_type}_{len(self.deployments)}"
        deployment = AgentDeployment(
            agent_id=agent_id,
            node_id=best_node_id,
            agent_type=agent_type,
            status="running",
            last_heartbeat=time.time()
        )

        self.deployments[agent_id] = deployment
        best_node.active_agents += 1

        print(f"🚀 Deployed {agent_type} agent {agent_id} to node {best_node_id}")
        return agent_id

    def get_node_status(self) -> Dict[str, Dict]:
        """Get status of all nodes."""
        return {
            node_id: {
                "capacity": node.capacity,
                "active_agents": node.active_agents,
                "utilization": node.active_agents / node.capacity if node.capacity > 0 else 0
            }
            for node_id, node in self.nodes.items()
        }

    def simulate_heartbeat(self):
        """Update heartbeats for all deployments."""
        current_time = time.time()
        for deployment in self.deployments.values():
            deployment.last_heartbeat = current_time


class DistributedDataProcessor:
    """Simulates distributed data processing across agents."""

    def __init__(self, coordinator: ClusterCoordinator):
        self.coordinator = coordinator
        self.processing_stats = {
            "total_processed": 0,
            "agent_contributions": {},
            "load_balance_score": 0.0
        }

    async def process_data_stream(self, agent_id: str, data_stream):
        """Process data on behalf of an agent."""
        processed = 0
        async for batch in data_stream:
            # Simulate processing time
            processing_time = random.uniform(0.01, 0.05)
            await asyncio.sleep(processing_time)

            batch_size = len(batch) if hasattr(batch, '__len__') else 1
            processed += batch_size

            # Update stats
            if agent_id not in self.processing_stats["agent_contributions"]:
                self.processing_stats["agent_contributions"][agent_id] = 0
            self.processing_stats["agent_contributions"][agent_id] += batch_size

            # Simulate yielding processed results
            yield batch

        print(f"⚡ Agent {agent_id} processed {processed} items")

    def calculate_load_balance(self) -> float:
        """Calculate how well load is balanced across agents."""
        contributions = list(self.processing_stats["agent_contributions"].values())
        if not contributions:
            return 0.0

        avg = sum(contributions) / len(contributions)
        if avg == 0:
            return 1.0

        # Calculate coefficient of variation (lower is better balance)
        variance = sum((x - avg) ** 2 for x in contributions) / len(contributions)
        std_dev = variance ** 0.5
        cv = std_dev / avg if avg > 0 else 0

        # Convert to balance score (1.0 = perfect balance, 0.0 = terrible)
        return max(0.0, 1.0 - cv)


async def generate_cluster_workload():
    """Generate workload data that would be distributed across cluster."""
    workload_types = ["user_events", "sensor_data", "transaction_logs", "analytics_queries"]

    while True:
        workload = {
            "workload_id": f"workload_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "type": random.choice(workload_types),
            "priority": random.choice(["low", "medium", "high"]),
            "size": random.randint(10, 100),  # Items in batch
            "timestamp": time.time()
        }

        # Simulate batch data
        batch_data = [
            {"item_id": f"{workload['workload_id']}_{i}", "data": f"sample_{i}"}
            for i in range(workload["size"])
        ]

        yield batch_data
        await asyncio.sleep(0.1)


async def main():
    """Run the cluster distributed agents demo."""
    print("🌐 Cluster Distributed Agents Demo")
    print("=" * 40)

    # Create cluster coordinator
    coordinator = ClusterCoordinator()

    # Add cluster nodes with different capacities
    coordinator.add_node(ClusterNode("node-01", capacity=5))  # High capacity node
    coordinator.add_node(ClusterNode("node-02", capacity=3))  # Medium capacity node
    coordinator.add_node(ClusterNode("node-03", capacity=4))  # Medium-high capacity node
    coordinator.add_node(ClusterNode("node-04", capacity=2))  # Low capacity node

    print()

    # Create distributed processor
    processor = DistributedDataProcessor(coordinator)

    # Deploy agents across the cluster
    print("🚀 Deploying agents across cluster...")
    deployed_agents = []

    # Deploy multiple instances of each agent type
    for agent_type in coordinator.agent_types:
        for _ in range(random.randint(2, 4)):  # Deploy 2-4 of each type
            agent_id = coordinator.deploy_agent(agent_type)
            if agent_id:
                deployed_agents.append(agent_id)

    print(f"\n🤖 Deployed {len(deployed_agents)} agents across {len(coordinator.nodes)} nodes")
    print()

    # Simulate distributed processing
    print("⚙️  Starting distributed processing pipeline...")
    print("📊 Workload will be distributed across agents based on cluster capacity")

    workload_generator = generate_cluster_workload()

    # Process workload for 15 seconds
    start_time = time.time()
    total_workloads = 0

    try:
        while time.time() - start_time < 15:
            # Get next workload
            workload = await anext(workload_generator)
            total_workloads += 1

            # Distribute to random deployed agent (simulating load balancer)
            if deployed_agents:
                target_agent = random.choice(deployed_agents)

                # Process workload (in real Sabot, this would be automatic)
                processed_items = len(workload)
                processor.processing_stats["total_processed"] += processed_items

                if target_agent not in processor.processing_stats["agent_contributions"]:
                    processor.processing_stats["agent_contributions"][target_agent] = 0
                processor.processing_stats["agent_contributions"][target_agent] += processed_items

                # Show occasional progress
                if total_workloads % 10 == 0:
                    node_status = coordinator.get_node_status()
                    print(f"📈 Processed {total_workloads} workloads | "
                          f"Total items: {processor.processing_stats['total_processed']}")

                    # Show node utilization
                    utilizations = [status["utilization"] for status in node_status.values()]
                    avg_utilization = sum(utilizations) / len(utilizations)
                    print(".1%")

            await asyncio.sleep(0.05)  # Control processing rate

    except KeyboardInterrupt:
        print("\n🛑 Stopping distributed processing...")

    # Final analysis
    print("\n📊 Cluster Performance Analysis")
    print("-" * 35)

    # Agent contributions
    print("🤖 Agent Contributions:")
    for agent_id, contribution in processor.processing_stats["agent_contributions"].items():
        deployment = coordinator.deployments.get(agent_id)
        node_id = deployment.node_id if deployment else "unknown"
        percentage = (contribution / processor.processing_stats["total_processed"] * 100) if processor.processing_stats["total_processed"] > 0 else 0
        print(".1f")

    # Load balance analysis
    load_balance_score = processor.calculate_load_balance()
    balance_quality = (
        "Excellent" if load_balance_score > 0.8 else
        "Good" if load_balance_score > 0.6 else
        "Fair" if load_balance_score > 0.4 else "Poor"
    )

    print(f"\n⚖️  Load Balance Score: {load_balance_score:.2f} ({balance_quality})")

    # Node utilization
    print("\n🏗️  Cluster Node Utilization:")
    node_status = coordinator.get_node_status()
    for node_id, status in node_status.items():
        utilization_pct = status["utilization"] * 100
        print(".1f")

    # Summary
    print("
🎯 Cluster Demo Summary:"    print(f"   • Total workloads processed: {total_workloads}")
    print(f"   • Total data items processed: {processor.processing_stats['total_processed']}")
    print(f"   • Agents deployed: {len(deployed_agents)}")
    print(f"   • Cluster nodes: {len(coordinator.nodes)}")
    print(f"   • Load balance quality: {balance_quality}")
    print("   • Deployment strategy: Capacity-based load balancing"
    print("   • Agent types: Data ingestion, processing, analytics, storage"
    print("   • Communication: Simulated inter-agent coordination"

    print("\n✅ Cluster distributed agents demo completed!")


if __name__ == "__main__":
    asyncio.run(main())
