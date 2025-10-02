#!/usr/bin/env python3
"""
Cluster Benchmarks for Sabot

Benchmarks distributed cluster operations including:
- Node discovery and registration
- Work distribution across nodes
- Cluster failover and recovery
- Network communication overhead
- Scalability with cluster size
"""

import asyncio
import time
import random
from typing import Dict, Any, List
from dataclasses import dataclass

from .runner import BenchmarkRunner, BenchmarkConfig, benchmark


@dataclass
class ClusterBenchmarkConfig:
    """Configuration for cluster benchmarks."""
    node_count: int = 5
    work_items: int = 1000
    work_complexity: str = "simple"  # simple, medium, complex
    network_latency_ms: int = 10
    failure_rate: float = 0.0  # Percentage of nodes that fail
    recovery_time_seconds: float = 30


class ClusterBenchmark:
    """Benchmark suite for cluster operations."""

    def __init__(self):
        self.runner = BenchmarkRunner()

    @benchmark(BenchmarkConfig(
        name="cluster_work_distribution",
        description="Benchmark work distribution across cluster nodes",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_work_distribution(self) -> Dict[str, Any]:
        """Benchmark how efficiently work is distributed across cluster."""
        config = ClusterBenchmarkConfig(
            node_count=5,
            work_items=2000,
            work_complexity="simple"
        )

        return await self._run_cluster_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="cluster_failover_recovery",
        description="Benchmark cluster failover and recovery",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_failover_recovery(self) -> Dict[str, Any]:
        """Benchmark cluster behavior during node failures and recovery."""
        config = ClusterBenchmarkConfig(
            node_count=5,
            work_items=1000,
            failure_rate=0.2,  # 20% nodes fail
            recovery_time_seconds=15
        )

        return await self._run_cluster_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="cluster_scaling_performance",
        description="Benchmark performance scaling with cluster size",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_scaling_performance(self) -> Dict[str, Any]:
        """Benchmark how performance scales with cluster size."""
        config = ClusterBenchmarkConfig(
            node_count=10,
            work_items=5000,
            work_complexity="medium"
        )

        return await self._run_cluster_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="cluster_network_overhead",
        description="Benchmark network communication overhead",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_network_overhead(self) -> Dict[str, Any]:
        """Benchmark network communication overhead in cluster."""
        config = ClusterBenchmarkConfig(
            node_count=5,
            work_items=1000,
            network_latency_ms=50  # High latency
        )

        return await self._run_cluster_benchmark(config)

    async def _run_cluster_benchmark(self, config: ClusterBenchmarkConfig) -> Dict[str, Any]:
        """Run a cluster benchmark with given configuration."""
        # Create mock cluster
        cluster = MockCluster(config)

        # Initialize cluster
        await cluster.initialize()

        start_time = time.time()

        # Submit work to cluster
        work_ids = []
        for i in range(config.work_items):
            work_id = await cluster.submit_work({
                'id': f"work_{i}",
                'complexity': config.work_complexity,
                'data': 'x' * random.randint(100, 1000)
            })
            work_ids.append(work_id)

        # Wait for work completion
        completed_work = 0
        failed_work = 0
        max_wait_time = 300  # 5 minutes max wait

        wait_start = time.time()
        while (completed_work + failed_work) < len(work_ids) and (time.time() - wait_start) < max_wait_time:
            await asyncio.sleep(1)

            # Check work status
            completed_work = sum(1 for wid in work_ids if cluster.is_work_completed(wid))
            failed_work = sum(1 for wid in work_ids if cluster.is_work_failed(wid))

        end_time = time.time()
        total_time = end_time - start_time

        # Gather cluster statistics
        cluster_stats = cluster.get_statistics()

        return {
            'total_work_items': config.work_items,
            'completed_work': completed_work,
            'failed_work': failed_work,
            'success_rate': completed_work / config.work_items,
            'total_time_seconds': total_time,
            'work_throughput_per_sec': completed_work / total_time if total_time > 0 else 0,
            'node_count': config.node_count,
            'failure_rate': config.failure_rate,
            'network_latency_ms': config.network_latency_ms,
            'cluster_stats': cluster_stats
        }

    async def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all cluster benchmarks."""
        benchmarks = [
            self.benchmark_work_distribution,
            self.benchmark_failover_recovery,
            self.benchmark_scaling_performance,
            self.benchmark_network_overhead
        ]

        return await self.runner.run_comprehensive_suite(benchmarks)


class MockClusterNode:
    """Mock cluster node for benchmarking."""

    def __init__(self, node_id: str, capacity: int = 10):
        self.node_id = node_id
        self.capacity = capacity
        self.active_work = 0
        self.completed_work = 0
        self.failed_work = 0
        self.is_alive = True

    async def execute_work(self, work_item: Dict[str, Any]) -> Dict[str, Any]:
        """Execute work item on this node."""
        if not self.is_alive or self.active_work >= self.capacity:
            return {'status': 'rejected', 'reason': 'node_unavailable'}

        self.active_work += 1

        try:
            # Simulate work execution based on complexity
            complexity = work_item.get('complexity', 'simple')

            if complexity == 'simple':
                execution_time = random.uniform(0.01, 0.05)  # 10-50ms
            elif complexity == 'medium':
                execution_time = random.uniform(0.05, 0.2)  # 50-200ms
            else:  # complex
                execution_time = random.uniform(0.2, 1.0)  # 200ms-1s

            await asyncio.sleep(execution_time)

            # Randomly fail some work (simulate real-world conditions)
            if random.random() < 0.02:  # 2% failure rate
                raise Exception("Simulated work failure")

            result = {
                'status': 'completed',
                'work_id': work_item['id'],
                'execution_time': execution_time,
                'node_id': self.node_id,
                'result': f"processed_{work_item['id']}"
            }

        except Exception as e:
            self.failed_work += 1
            result = {
                'status': 'failed',
                'work_id': work_item['id'],
                'error': str(e),
                'node_id': self.node_id
            }
        finally:
            self.active_work -= 1

        if result['status'] == 'completed':
            self.completed_work += 1

        return result


class MockCluster:
    """Mock cluster for benchmarking distributed operations."""

    def __init__(self, config: ClusterBenchmarkConfig):
        self.config = config
        self.nodes: Dict[str, MockClusterNode] = {}
        self.work_queue = asyncio.Queue()
        self.completed_work: Dict[str, Dict[str, Any]] = {}
        self.failed_work: Dict[str, Dict[str, Any]] = {}
        self.work_assignments: Dict[str, str] = {}  # work_id -> node_id

        # Background tasks
        self.distribution_task: asyncio.Task = None
        self.monitoring_task: asyncio.Task = None

    async def initialize(self) -> None:
        """Initialize the mock cluster."""
        # Create nodes
        for i in range(self.config.node_count):
            node_id = f"node_{i}"
            capacity = random.randint(5, 15)  # Variable capacity
            self.nodes[node_id] = MockClusterNode(node_id, capacity)

        # Simulate node failures
        failed_nodes = int(self.config.node_count * self.config.failure_rate)
        failed_node_ids = random.sample(list(self.nodes.keys()), failed_nodes)
        for node_id in failed_node_ids:
            self.nodes[node_id].is_alive = False

        # Start background tasks
        self.distribution_task = asyncio.create_task(self._distribute_work())
        self.monitoring_task = asyncio.create_task(self._monitor_failures())

    async def submit_work(self, work_item: Dict[str, Any]) -> str:
        """Submit work to the cluster."""
        work_id = work_item['id']
        await self.work_queue.put(work_item)
        return work_id

    async def _distribute_work(self) -> None:
        """Distribute work to available nodes."""
        while True:
            try:
                # Get work from queue
                work_item = await self.work_queue.get()

                # Find available node
                available_nodes = [
                    node for node in self.nodes.values()
                    if node.is_alive and node.active_work < node.capacity
                ]

                if available_nodes:
                    # Select node (simple round-robin for demo)
                    selected_node = available_nodes[0]

                    # Assign work
                    self.work_assignments[work_item['id']] = selected_node.node_id

                    # Execute work asynchronously
                    asyncio.create_task(self._execute_work_on_node(selected_node, work_item))
                else:
                    # No available nodes, re-queue
                    await asyncio.sleep(0.1)  # Brief delay
                    await self.work_queue.put(work_item)

            except Exception as e:
                print(f"Error in work distribution: {e}")
                await asyncio.sleep(1)

    async def _execute_work_on_node(self, node: MockClusterNode, work_item: Dict[str, Any]) -> None:
        """Execute work on a specific node."""
        try:
            # Simulate network latency
            await asyncio.sleep(self.config.network_latency_ms / 1000.0)

            result = await node.execute_work(work_item)

            if result['status'] == 'completed':
                self.completed_work[work_item['id']] = result
            else:
                self.failed_work[work_item['id']] = result

        except Exception as e:
            self.failed_work[work_item['id']] = {
                'status': 'failed',
                'work_id': work_item['id'],
                'error': str(e)
            }

    async def _monitor_failures(self) -> None:
        """Monitor for node failures and recovery."""
        while True:
            await asyncio.sleep(5)  # Check every 5 seconds

            # Simulate random node failures
            if random.random() < 0.05:  # 5% chance per check
                alive_nodes = [node for node in self.nodes.values() if node.is_alive]
                if alive_nodes:
                    failed_node = random.choice(alive_nodes)
                    failed_node.is_alive = False
                    print(f"Simulated failure of node {failed_node.node_id}")

            # Simulate node recovery
            failed_nodes = [node for node in self.nodes.values() if not node.is_alive]
            if failed_nodes and random.random() < 0.1:  # 10% chance per check
                recovered_node = random.choice(failed_nodes)
                recovered_node.is_alive = True
                print(f"Simulated recovery of node {recovered_node.node_id}")

    def is_work_completed(self, work_id: str) -> bool:
        """Check if work is completed."""
        return work_id in self.completed_work

    def is_work_failed(self, work_id: str) -> bool:
        """Check if work has failed."""
        return work_id in self.failed_work

    def get_statistics(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        total_capacity = sum(node.capacity for node in self.nodes.values())
        active_capacity = sum(node.active_work for node in self.nodes.values())
        alive_nodes = sum(1 for node in self.nodes.values() if node.is_alive)

        return {
            'total_nodes': len(self.nodes),
            'alive_nodes': alive_nodes,
            'total_capacity': total_capacity,
            'active_capacity': active_capacity,
            'utilization_rate': active_capacity / total_capacity if total_capacity > 0 else 0,
            'completed_work': len(self.completed_work),
            'failed_work': len(self.failed_work),
            'queued_work': self.work_queue.qsize()
        }
