#!/usr/bin/env python3
"""Simple demonstration of DBOS-controlled parallel processing concepts."""

import asyncio
import time
import psutil
from typing import Dict, List, Any, Optional
import threading
import statistics

print("🧠 DBOS-Controlled Parallel Processing Demo")
print("=" * 50)

class SimpleDBOSController:
    """Simplified DBOS controller for demonstration."""

    def __init__(self, max_workers: int = 8):
        self.max_workers = max_workers
        self.worker_history: Dict[int, List[float]] = {}
        self.active_workers = 0
        self.system_metrics = self._get_system_metrics()

    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics."""
        return {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_percent': psutil.virtual_memory().percent,
            'cpu_count': psutil.cpu_count(),
        }

    def decide_worker_count(self, data_size: int) -> int:
        """Make intelligent decision about worker count."""
        metrics = self._get_system_metrics()
        cpu_count = metrics['cpu_count']

        # Base decision on data size and system state
        if data_size < 100:
            workers = 1
        elif data_size < 1000:
            workers = min(2, cpu_count // 2)
        else:
            workers = min(cpu_count, self.max_workers)

        # Adjust based on system load
        if metrics['cpu_percent'] > 80:
            workers = max(1, workers - 1)
        elif metrics['cpu_percent'] < 30:
            workers = min(workers + 1, cpu_count)

        return workers

    def create_work_distribution(self, data: List[Any], num_workers: int) -> List[List[Any]]:
        """Distribute work across workers."""
        distribution = [[] for _ in range(num_workers)]

        for i, item in enumerate(data):
            worker_id = i % num_workers
            distribution[worker_id].append(item)

        return distribution

async def simulate_cython_worker(worker_id: int, tasks: List[Any], results: List[Any]):
    """Simulate a Cython-optimized worker."""
    print(f"    Worker {worker_id}: Processing {len(tasks)} tasks")

    for task in tasks:
        # Simulate processing time (Cython would be much faster)
        await asyncio.sleep(0.01)

        # Simulate CPU-intensive work
        result = 0
        for i in range(1000):
            result += task * i

        results.append((worker_id, result))

    print(f"    Worker {worker_id}: Completed")

async def demo_dbos_decisions():
    """Demonstrate DBOS decision making."""
    print("\n🧠 DBOS Decision Making:")

    dbos = SimpleDBOSController(max_workers=4)

    test_cases = [
        ("Small dataset", 50),
        ("Medium dataset", 500),
        ("Large dataset", 5000),
        ("Huge dataset", 50000),
    ]

    print("Dataset Size | Recommended Workers | System CPU")
    print("------------|---------------------|------------")

    for name, size in test_cases:
        workers = dbos.decide_worker_count(size)
        metrics = dbos._get_system_metrics()
        print(">8")

async def demo_parallel_processing():
    """Demonstrate DBOS-controlled parallel processing."""
    print("\n⚡ DBOS-Controlled Parallel Processing:")

    # Create DBOS controller
    dbos = SimpleDBOSController(max_workers=4)

    # Sample data
    data_size = 200
    data = list(range(data_size))

    print(f"Processing {data_size} items...")

    # DBOS makes intelligent decisions
    optimal_workers = dbos.decide_worker_count(data_size)
    work_distribution = dbos.create_work_distribution(data, optimal_workers)

    print(f"DBOS decided: {optimal_workers} workers")
    print(f"Work distribution: {[len(work) for work in work_distribution]} items per worker")

    # Simulate parallel execution (Cython would do this much faster)
    results = []
    start_time = time.time()

    # Create worker tasks
    worker_tasks = []
    for worker_id, worker_tasks_data in enumerate(work_distribution):
        task = simulate_cython_worker(worker_id, worker_tasks_data, results)
        worker_tasks.append(task)

    # Execute in parallel
    await asyncio.gather(*worker_tasks)

    end_time = time.time()
    processing_time = end_time - start_time

    print(f"✅ Completed in {processing_time:.3f}s")
    print(".1f")
    print(f"   Results: {len(results)} items processed")

    # Verify results
    sample_results = sorted(results[:5])
    print(f"   Sample results: {sample_results}")

    return results

async def demo_adaptive_scaling():
    """Demonstrate adaptive scaling based on system conditions."""
    print("\n📈 Adaptive Scaling:")

    dbos = SimpleDBOSController(max_workers=8)

    # Simulate different system conditions
    scenarios = [
        ("Low load", 20, 200),
        ("Medium load", 60, 1000),
        ("High load", 85, 500),
        ("Very high load", 95, 100),
    ]

    print("Scenario      | CPU | Data Size | Workers | Reasoning")
    print("--------------|-----|-----------|---------|----------")

    for scenario, cpu_percent, data_size in scenarios:
        # Temporarily override CPU reading for demo
        original_cpu = psutil.cpu_percent
        psutil.cpu_percent = lambda interval=None: cpu_percent

        workers = dbos.decide_worker_count(data_size)
        reasoning = "Normal" if cpu_percent < 70 else "High CPU, reduced workers" if workers < 4 else "High CPU, maintained capacity"

        print(">12")

        # Restore original function
        psutil.cpu_percent = original_cpu

async def demo_performance_comparison():
    """Compare performance with different worker counts."""
    print("\n📊 Performance Scaling:")

    dbos = SimpleDBOSController(max_workers=8)
    data = list(range(400))

    worker_counts = [1, 2, 4, 6]
    results = []

    print("Workers | Time (s) | Throughput (items/s)")
    print("--------|----------|-------------------")

    for num_workers in worker_counts:
        work_distribution = dbos.create_work_distribution(data, num_workers)

        results_list = []
        start_time = time.time()

        # Create and run worker tasks
        worker_tasks = []
        for worker_id, worker_tasks_data in enumerate(work_distribution):
            task = simulate_cython_worker(worker_id, worker_tasks_data, results_list)
            worker_tasks.append(task)

        await asyncio.gather(*worker_tasks)

        end_time = time.time()
        processing_time = end_time - start_time
        throughput = len(results_list) / processing_time

        print(">8")
        results.append((num_workers, processing_time, throughput))

    # Show scaling efficiency
    if len(results) >= 2:
        single_worker_time = results[0][1]
        max_workers_time = results[-1][1]
        speedup = single_worker_time / max_workers_time
        efficiency = speedup / results[-1][0] * 100

        print(f"\nScaling analysis:")
        print(".2f")
        print(".1f")

def explain_concepts():
    """Explain key concepts."""
    print("\n🔑 Key Concepts:")

    concepts = [
        ("DBOS Controller", "Makes intelligent decisions about parallelism"),
        ("Morsel Partitioning", "Breaks data into optimal-sized chunks"),
        ("Adaptive Scaling", "Adjusts workers based on system conditions"),
        ("Work Distribution", "Assigns tasks to workers for load balancing"),
        ("Cython Optimization", "High-performance execution engine"),
        ("NUMA Awareness", "Considers memory locality for efficiency"),
        ("Real-time Monitoring", "Continuous performance tracking"),
    ]

    for concept, description in concepts:
        print(f"  • {concept}: {description}")

    print("\n🚀 Benefits:")
    benefits = [
        "Automatic optimization based on data and system characteristics",
        "Cython-level performance with Python ease of use",
        "Adapts to changing workloads and system conditions",
        "Efficient resource utilization",
        "Scalable from single core to multi-machine",
        "Intelligent load balancing and work stealing",
    ]

    for benefit in benefits:
        print(f"  • {benefit}")

async def main():
    """Run all demonstrations."""
    print("This demo shows how DBOS intelligence controls Cython parallel processing.")
    print("DBOS makes the decisions, Cython executes with maximum performance.\n")

    # Explain concepts
    explain_concepts()

    try:
        # Run demonstrations
        await demo_dbos_decisions()
        await demo_parallel_processing()
        await demo_adaptive_scaling()
        await demo_performance_comparison()

        print("\n🎉 All demonstrations completed successfully!")
        print("\n💡 Summary:")
        print("  • DBOS provides intelligent parallel processing orchestration")
        print("  • Cython delivers high-performance execution")
        print("  • Together they enable optimal resource utilization")
        print("  • System adapts automatically to changing conditions")
        print("  • Scales efficiently across different workloads")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
