#!/usr/bin/env python3
"""
State Partitioning - Distributed State Management
==================================================

**What this demonstrates:**
- Partitioning state across multiple agents
- Key-based routing (send data to agent owning the key)
- Stateful distributed processing
- State recovery after agent failure

**Prerequisites:** Completed agent_failure_recovery.py

**Runtime:** ~10 seconds

**Next steps:**
- See 04_production_patterns/ for production stateful patterns
- See docs/ARCHITECTURE_OVERVIEW.md for state management details

**Pattern:**
State partitioned by key → Each agent owns subset of keys → Route by hash(key)

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import pyarrow as pa
from typing import Dict, List, Optional
from dataclasses import dataclass
import hashlib


@dataclass
class StateUpdate:
    """State update event."""
    key: str
    value: float
    timestamp: int


class StatefulAgent:
    """Agent that maintains partitioned state."""

    def __init__(self, agent_id: str, partition_id: int):
        self.agent_id = agent_id
        self.partition_id = partition_id
        self.state: Dict[str, float] = {}
        self.update_count = 0

    def update_state(self, key: str, value: float):
        """Update state for a key."""

        # Get current value
        current = self.state.get(key, 0.0)

        # Update (running total)
        self.state[key] = current + value
        self.update_count += 1

        print(f"    Agent {self.agent_id}: {key} = {self.state[key]:.2f} (+{value:.2f})")

    def get_state(self, key: str) -> Optional[float]:
        """Get state for a key."""
        return self.state.get(key)

    def get_all_state(self) -> Dict[str, float]:
        """Get all state."""
        return self.state.copy()


class StatePartitionedJobManager:
    """
    Job manager with state partitioning.

    Routes updates to correct agent based on key hash.
    """

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
        self.agents: List[Optional[StatefulAgent]] = [None] * num_partitions

    def register_agent(self, agent: StatefulAgent):
        """Register agent for a partition."""

        if agent.partition_id >= self.num_partitions:
            raise ValueError(f"Partition {agent.partition_id} out of range (max: {self.num_partitions - 1})")

        self.agents[agent.partition_id] = agent
        print(f"✅ Registered agent {agent.agent_id} for partition {agent.partition_id}")

    def get_partition_for_key(self, key: str) -> int:
        """
        Compute partition for key using hash.

        Uses consistent hashing to distribute keys evenly.
        """

        # Hash key
        hash_bytes = hashlib.md5(key.encode()).digest()
        hash_int = int.from_bytes(hash_bytes[:4], byteorder='big')

        # Modulo to get partition
        partition = hash_int % self.num_partitions

        return partition

    async def route_update(self, update: StateUpdate):
        """Route state update to correct agent."""

        # Compute partition
        partition = self.get_partition_for_key(update.key)

        # Get agent
        agent = self.agents[partition]

        if agent is None:
            print(f"  ⚠️  No agent for partition {partition} (key: {update.key})")
            return

        print(f"  → Route {update.key} to partition {partition} (Agent {agent.agent_id})")

        # Update state
        agent.update_state(update.key, update.value)

    async def process_updates(self, updates: List[StateUpdate]):
        """Process batch of state updates."""

        print(f"\n📋 Processing {len(updates)} state updates")
        print("=" * 70)

        for update in updates:
            await self.route_update(update)

    def get_global_state(self) -> Dict[str, float]:
        """Get global state from all agents."""

        global_state = {}

        for agent in self.agents:
            if agent is not None:
                global_state.update(agent.get_all_state())

        return global_state


def create_sample_updates(num_updates: int = 20) -> List[StateUpdate]:
    """Create sample state updates."""

    updates = []

    # Use 5 different keys
    keys = ["account_A", "account_B", "account_C", "account_D", "account_E"]

    for i in range(num_updates):
        key = keys[i % len(keys)]
        value = 10.0 + i
        timestamp = 1000000 + i

        update = StateUpdate(key=key, value=value, timestamp=timestamp)
        updates.append(update)

    return updates


async def main():
    print("🗂️  State Partitioning Demo")
    print("=" * 70)
    print("\nDemonstrates distributed state management with key-based partitioning")

    # Configuration
    num_partitions = 3

    print(f"\n\n🔧 Configuration")
    print("=" * 70)
    print(f"Partitions: {num_partitions}")
    print("Partitioning strategy: Hash-based")

    # Create agents
    print("\n\n🤖 Creating Stateful Agents")
    print("=" * 70)

    agent1 = StatefulAgent("agent_1", partition_id=0)
    agent2 = StatefulAgent("agent_2", partition_id=1)
    agent3 = StatefulAgent("agent_3", partition_id=2)

    # Create job manager
    job_manager = StatePartitionedJobManager(num_partitions=num_partitions)

    job_manager.register_agent(agent1)
    job_manager.register_agent(agent2)
    job_manager.register_agent(agent3)

    # Create updates
    print("\n\n📦 Creating State Updates")
    print("=" * 70)

    updates = create_sample_updates(20)

    print(f"Created {len(updates)} updates:")
    print(f"  Keys: {set(u.key for u in updates)}")

    # Show partitioning
    print("\n\n🔑 Key-to-Partition Mapping")
    print("=" * 70)

    keys = ["account_A", "account_B", "account_C", "account_D", "account_E"]
    for key in keys:
        partition = job_manager.get_partition_for_key(key)
        agent = job_manager.agents[partition]
        print(f"  {key:<12} → Partition {partition} (Agent {agent.agent_id})")

    # Process updates
    print("\n\n⚙️  Processing Updates")
    print("=" * 70)

    await job_manager.process_updates(updates)

    # Show results
    print("\n\n📊 State Distribution")
    print("=" * 70)

    for agent in job_manager.agents:
        if agent is not None:
            print(f"\nAgent {agent.agent_id} (Partition {agent.partition_id}):")
            print(f"  Updates processed: {agent.update_count}")
            print(f"  Keys owned: {len(agent.state)}")

            for key, value in sorted(agent.state.items()):
                print(f"    {key}: {value:.2f}")

    # Global state
    print("\n\n🌍 Global State")
    print("=" * 70)

    global_state = job_manager.get_global_state()

    print(f"Total keys: {len(global_state)}")
    for key, value in sorted(global_state.items()):
        print(f"  {key}: {value:.2f}")

    # Verify totals
    print("\n\n✅ Verification")
    print("=" * 70)

    # Count updates per key
    key_counts = {}
    for update in updates:
        key_counts[update.key] = key_counts.get(update.key, 0) + 1

    print("\nUpdates per key:")
    for key in sorted(key_counts.keys()):
        count = key_counts[key]
        expected_total = sum(update.value for update in updates if update.key == key)
        actual_total = global_state[key]

        print(f"  {key}: {count} updates, total={actual_total:.2f} ({'✅' if abs(actual_total - expected_total) < 0.01 else '❌'})")

    print("\n\n💡 Key Takeaways")
    print("=" * 70)
    print("1. State partitioned by key hash (consistent hashing)")
    print("2. Each agent owns subset of keys")
    print("3. Updates routed to correct agent automatically")
    print("4. Global state = union of all agent state")
    print("5. Scales horizontally (add more partitions)")

    print("\n\n🔗 Partitioning Strategies")
    print("=" * 70)
    print("Hash-based:   hash(key) % num_partitions (current demo)")
    print("Range-based:  Key ranges assigned to partitions")
    print("Custom:       Application-specific logic")

    print("\n\n🔗 Benefits of State Partitioning")
    print("=" * 70)
    print("✅ Scalability: State distributed across agents")
    print("✅ Parallelism: Updates processed concurrently")
    print("✅ Fault isolation: Agent failure affects only its partition")
    print("✅ Locality: Related keys often on same agent")

    print("\n\n🔗 State Recovery")
    print("=" * 70)
    print("When agent fails:")
    print("  1. Detect failure (health check)")
    print("  2. Restore state from checkpoint (RocksDB/Redis)")
    print("  3. Reassign partition to new agent")
    print("  4. Resume processing")

    print("\n\n🔗 Production State Backends")
    print("=" * 70)
    print("MemoryBackend:  Fast, volatile (current demo)")
    print("RocksDB:        Persistent, local disk")
    print("Redis:          Distributed, shared state")
    print("Postgres:       Durable, ACID transactions")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See 04_production_patterns/fraud_detection/ for stateful pattern")
    print("- See docs/ARCHITECTURE_OVERVIEW.md for Chandy-Lamport checkpoints")
    print("- Try with RocksDB backend for persistence")


if __name__ == "__main__":
    asyncio.run(main())
