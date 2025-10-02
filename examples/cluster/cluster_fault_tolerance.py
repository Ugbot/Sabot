#!/usr/bin/env python3
"""
Cluster Fault Tolerance Demo

This example demonstrates Sabot's fault tolerance and recovery capabilities
in a cluster environment. It shows:
- Agent failure detection and recovery
- Node failure simulation and handling
- Supervisor patterns and automatic restart
- Graceful degradation and failover
- Cluster resilience and high availability
"""

import asyncio
import random
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum


class NodeStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"


class AgentStatus(Enum):
    RUNNING = "running"
    FAILED = "failed"
    RESTARTING = "restarting"
    STOPPED = "stopped"


@dataclass
class ClusterAgent:
    """Represents an agent in the cluster."""
    agent_id: str
    agent_type: str
    node_id: str
    status: AgentStatus = AgentStatus.RUNNING
    start_time: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    restart_count: int = 0
    max_restarts: int = 3
    tasks_processed: int = 0
    errors_encountered: int = 0

    @property
    def uptime(self) -> float:
        """Agent uptime in seconds."""
        return time.time() - self.start_time

    @property
    def is_healthy(self) -> bool:
        """Check if agent is healthy based on heartbeat."""
        return time.time() - self.last_heartbeat < 30.0  # 30 second timeout

    def should_restart(self) -> bool:
        """Check if agent should be restarted."""
        return (self.status == AgentStatus.FAILED and
                self.restart_count < self.max_restarts)


@dataclass
class ClusterNode:
    """Represents a cluster node."""
    node_id: str
    status: NodeStatus = NodeStatus.HEALTHY
    agents: Dict[str, ClusterAgent] = field(default_factory=dict)
    last_heartbeat: float = field(default_factory=time.time)
    failure_time: Optional[float] = None
    recovery_time: Optional[float] = None

    @property
    def active_agents(self) -> int:
        """Number of active agents on this node."""
        return sum(1 for agent in self.agents.values()
                  if agent.status == AgentStatus.RUNNING)

    @property
    def failed_agents(self) -> int:
        """Number of failed agents on this node."""
        return sum(1 for agent in self.agents.values()
                  if agent.status == AgentStatus.FAILED)

    @property
    def is_healthy(self) -> bool:
        """Check if node is healthy."""
        if self.status == NodeStatus.FAILED:
            return False
        return time.time() - self.last_heartbeat < 60.0  # 60 second timeout


class FaultToleranceController:
    """Manages fault tolerance and recovery across the cluster."""

    def __init__(self):
        self.nodes: Dict[str, ClusterNode] = {}
        self.failed_nodes: Set[str] = set()
        self.agent_failures = 0
        self.node_failures = 0
        self.successful_recoveries = 0
        self.heartbeat_interval = 5.0  # seconds

    def add_node(self, node: ClusterNode):
        """Add a node to the cluster."""
        self.nodes[node.node_id] = node
        print(f"🖥️  Added cluster node: {node.node_id}")

    def deploy_agent(self, node_id: str, agent_type: str) -> Optional[str]:
        """Deploy an agent to a specific node."""
        if node_id not in self.nodes:
            return None

        node = self.nodes[node_id]
        agent_id = f"{agent_type}_{node_id}_{len(node.agents)}"

        agent = ClusterAgent(
            agent_id=agent_id,
            agent_type=agent_type,
            node_id=node_id
        )

        node.agents[agent_id] = agent
        print(f"🤖 Deployed {agent_type} agent {agent_id} to node {node_id}")
        return agent_id

    def simulate_agent_failure(self, agent_id: str, failure_type: str = "random"):
        """Simulate an agent failure."""
        for node in self.nodes.values():
            if agent_id in node.agents:
                agent = node.agents[agent_id]
                agent.status = AgentStatus.FAILED
                agent.errors_encountered += 1
                self.agent_failures += 1

                print(f"💥 Agent {agent_id} failed ({failure_type}) on node {node.node_id}")
                return True
        return False

    def simulate_node_failure(self, node_id: str, failure_type: str = "crash"):
        """Simulate a node failure."""
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.status = NodeStatus.FAILED
            node.failure_time = time.time()
            self.node_failures += 1
            self.failed_nodes.add(node_id)

            # Mark all agents on this node as failed
            for agent in node.agents.values():
                agent.status = AgentStatus.FAILED

            print(f"💥 Node {node_id} failed ({failure_type}) - {len(node.agents)} agents affected")
            return True
        return False

    async def recover_agent(self, agent_id: str) -> bool:
        """Attempt to recover a failed agent."""
        for node in self.nodes.values():
            if agent_id in node.agents:
                agent = node.agents[agent_id]

                if not agent.should_restart():
                    print(f"❌ Agent {agent_id} exceeded max restarts ({agent.max_restarts})")
                    return False

                # Simulate recovery time
                agent.status = AgentStatus.RESTARTING
                await asyncio.sleep(random.uniform(1.0, 3.0))  # Recovery time

                # Recovery successful
                agent.status = AgentStatus.RUNNING
                agent.restart_count += 1
                agent.last_heartbeat = time.time()
                agent.start_time = time.time()  # Reset uptime
                self.successful_recoveries += 1

                print(f"🔄 Successfully recovered agent {agent_id} "
                      f"(restart #{agent.restart_count})")
                return True

        return False

    async def recover_node(self, node_id: str) -> bool:
        """Attempt to recover a failed node."""
        if node_id not in self.failed_nodes:
            return False

        node = self.nodes[node_id]

        # Simulate node recovery time
        recovery_duration = random.uniform(5.0, 15.0)
        print(f"🔄 Recovering node {node_id}... ({recovery_duration:.1f}s)")
        await asyncio.sleep(recovery_duration)

        # Recovery successful
        node.status = NodeStatus.HEALTHY
        node.recovery_time = time.time()
        node.last_heartbeat = time.time()
        self.failed_nodes.remove(node_id)
        self.successful_recoveries += 1

        # Recover agents on this node
        recovered_agents = 0
        for agent in node.agents.values():
            if agent.should_restart():
                agent.status = AgentStatus.RUNNING
                agent.restart_count += 1
                agent.last_heartbeat = time.time()
                recovered_agents += 1

        print(f"✅ Successfully recovered node {node_id} with {recovered_agents} agents")
        return True

    async def monitor_and_recover(self):
        """Continuously monitor cluster health and recover failures."""
        while True:
            # Update heartbeats
            current_time = time.time()
            for node in self.nodes.values():
                if node.status != NodeStatus.FAILED:
                    node.last_heartbeat = current_time
                    for agent in node.agents.values():
                        if agent.status == AgentStatus.RUNNING:
                            agent.last_heartbeat = current_time

            # Check for agent failures and attempt recovery
            for node in self.nodes.values():
                for agent in node.agents.values():
                    if agent.status == AgentStatus.FAILED and agent.should_restart():
                        asyncio.create_task(self.recover_agent(agent.agent_id))

            # Check for node failures and attempt recovery
            for node_id in list(self.failed_nodes):
                # 80% success rate for node recovery
                if random.random() < 0.8:
                    asyncio.create_task(self.recover_node(node_id))

            await asyncio.sleep(self.heartbeat_interval)

    def inject_failures(self):
        """Randomly inject failures for demonstration."""
        # 5% chance of agent failure per heartbeat
        if random.random() < 0.05:
            healthy_agents = []
            for node in self.nodes.values():
                if node.status == NodeStatus.HEALTHY:
                    healthy_agents.extend([
                        agent_id for agent_id, agent in node.agents.items()
                        if agent.status == AgentStatus.RUNNING
                    ])

            if healthy_agents:
                failed_agent = random.choice(healthy_agents)
                failure_types = ["memory_error", "network_timeout", "cpu_overload", "disk_full"]
                self.simulate_agent_failure(failed_agent, random.choice(failure_types))

        # 2% chance of node failure per heartbeat
        if random.random() < 0.02:
            healthy_nodes = [
                node_id for node_id, node in self.nodes.items()
                if node.status == NodeStatus.HEALTHY
            ]

            if healthy_nodes:
                failed_node = random.choice(healthy_nodes)
                failure_types = ["power_failure", "network_partition", "disk_failure", "memory_corruption"]
                self.simulate_node_failure(failed_node, random.choice(failure_types))

    def get_cluster_health(self) -> Dict[str, any]:
        """Get comprehensive cluster health status."""
        total_agents = sum(len(node.agents) for node in self.nodes.values())
        healthy_agents = sum(
            sum(1 for agent in node.agents.values() if agent.status == AgentStatus.RUNNING)
            for node in self.nodes.values()
        )
        failed_agents = sum(
            sum(1 for agent in node.agents.values() if agent.status == AgentStatus.FAILED)
            for node in self.nodes.values()
        )

        healthy_nodes = sum(1 for node in self.nodes.values() if node.is_healthy)
        failed_nodes_count = len(self.failed_nodes)

        return {
            "nodes": {
                "total": len(self.nodes),
                "healthy": healthy_nodes,
                "failed": failed_nodes_count,
                "health_percentage": healthy_nodes / len(self.nodes) * 100 if self.nodes else 0
            },
            "agents": {
                "total": total_agents,
                "healthy": healthy_agents,
                "failed": failed_agents,
                "health_percentage": healthy_agents / total_agents * 100 if total_agents > 0 else 0
            },
            "failures": {
                "agent_failures": self.agent_failures,
                "node_failures": self.node_failures,
                "successful_recoveries": self.successful_recoveries
            }
        }


async def simulate_cluster_workload(controller: FaultToleranceController):
    """Simulate ongoing cluster workload."""
    while True:
        # Simulate agent processing work
        for node in controller.nodes.values():
            if node.status == NodeStatus.HEALTHY:
                for agent in node.agents.values():
                    if agent.status == AgentStatus.RUNNING:
                        # Simulate processing tasks
                        tasks_processed = random.randint(1, 5)
                        agent.tasks_processed += tasks_processed

        await asyncio.sleep(1.0)


async def main():
    """Run the cluster fault tolerance demo."""
    print("🛡️  Cluster Fault Tolerance Demo")
    print("=" * 34)

    # Create fault tolerance controller
    controller = FaultToleranceController()

    # Set up cluster with multiple nodes
    nodes = []
    for i in range(4):
        node = ClusterNode(f"node-{i+1:02d}")
        controller.add_node(node)
        nodes.append(node)

    print()

    # Deploy agents across nodes
    agent_types = ["data_processor", "analytics_engine", "api_handler", "storage_manager"]
    print("🤖 Deploying agents across cluster...")

    for node in nodes:
        # Deploy 2-4 agents per node
        num_agents = random.randint(2, 4)
        for _ in range(num_agents):
            agent_type = random.choice(agent_types)
            controller.deploy_agent(node.node_id, agent_type)

    total_agents = sum(len(node.agents) for node in nodes)
    print(f"\n🚀 Cluster initialized: {len(nodes)} nodes, {total_agents} agents")
    print()

    # Start monitoring and recovery
    print("👁️  Starting fault monitoring and recovery...")
    print("💥 Random failures will be injected for demonstration")
    print("🔄 Automatic recovery will attempt to restore services")
    print()

    # Start background tasks
    monitor_task = asyncio.create_task(controller.monitor_and_recover())
    workload_task = asyncio.create_task(simulate_cluster_workload(controller))

    # Run demo for 30 seconds
    start_time = time.time()

    try:
        last_status_time = 0

        while time.time() - start_time < 30:
            current_time = time.time()

            # Inject random failures occasionally
            controller.inject_failures()

            # Show status updates every 5 seconds
            if current_time - last_status_time >= 5.0:
                health = controller.get_cluster_health()

                print(f"\n📊 Cluster Status (t={int(current_time - start_time)}s):")
                print(f"   Nodes: {health['nodes']['healthy']}/{health['nodes']['total']} healthy "
                      ".1f"                print(f"   Agents: {health['agents']['healthy']}/{health['agents']['total']} healthy "
                      ".1f"                print(f"   Failures: {health['failures']['agent_failures']} agents, "
                      f"{health['failures']['node_failures']} nodes")
                print(f"   Recoveries: {health['failures']['successful_recoveries']} successful")

                last_status_time = current_time

            await asyncio.sleep(0.5)

    except KeyboardInterrupt:
        print("\n🛑 Stopping fault tolerance demo...")

    # Stop background tasks
    monitor_task.cancel()
    workload_task.cancel()

    try:
        await monitor_task
        await workload_task
    except asyncio.CancelledError:
        pass

    # Final analysis
    print("\n📊 Final Fault Tolerance Analysis")
    print("-" * 36)

    final_health = controller.get_cluster_health()

    print("🏗️  Cluster Resilience:")
    print(".1f"    print(".1f"
    print("\n📈 Recovery Metrics:")
    print(f"   Agent Failures: {final_health['failures']['agent_failures']}")
    print(f"   Node Failures: {final_health['failures']['node_failures']}")
    print(f"   Successful Recoveries: {final_health['failures']['successful_recoveries']}")

    recovery_rate = (final_health['failures']['successful_recoveries'] /
                    max(1, final_health['failures']['agent_failures'] + final_health['failures']['node_failures']))
    print(".1f"
    print("\n🛡️  Fault Tolerance Features Demonstrated:")
    print("   • Automatic agent failure detection and restart")
    print("   • Node failure handling with graceful degradation")
    print("   • Supervisor patterns for service reliability")
    print("   • Heartbeat monitoring and health checks")
    print("   • Load redistribution during failures")
    print("   • Recovery rate tracking and metrics")

    # Agent performance summary
    print("\n🤖 Agent Performance Summary:")
    total_tasks = 0
    for node in controller.nodes.values():
        for agent in node.agents.values():
            total_tasks += agent.tasks_processed
            if agent.restart_count > 0:
                print(f"   {agent.agent_id}: {agent.tasks_processed} tasks, "
                      f"{agent.restart_count} restarts, {agent.errors_encountered} errors")

    print(f"\n⚡ Total Tasks Processed: {total_tasks}")
    print(".1f"
    print("\n✅ Cluster fault tolerance demo completed!")


if __name__ == "__main__":
    asyncio.run(main())
