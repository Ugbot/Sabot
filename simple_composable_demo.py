#!/usr/bin/env python3
"""Simple demonstration of Sabot's composable architecture concepts."""

import asyncio
import os
import psutil
from typing import Dict, Any
from enum import Enum

print("🔧 Sabot Composable Architecture Demo")
print("=" * 50)

class DeploymentMode(Enum):
    """Possible deployment modes."""
    SINGLE_NODE = "single-node"
    COORDINATOR = "coordinator"
    WORKER = "worker"
    AUTO = "auto"

def detect_deployment_mode() -> DeploymentMode:
    """Auto-detect deployment mode (simplified version)."""
    mode_env = os.getenv("SABOT_MODE", "").lower()

    if mode_env == "coordinator":
        return DeploymentMode.COORDINATOR
    elif mode_env == "worker":
        return DeploymentMode.WORKER
    elif mode_env == "single-node":
        return DeploymentMode.SINGLE_NODE

    # Check for Kubernetes
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        pod_name = os.getenv("HOSTNAME", "")
        if "coordinator" in pod_name:
            return DeploymentMode.COORDINATOR
        else:
            return DeploymentMode.WORKER

    # Check for distributed coordinator
    if os.getenv("COORDINATOR_HOST"):
        return DeploymentMode.WORKER

    return DeploymentMode.SINGLE_NODE

def load_configuration() -> Dict[str, Any]:
    """Load configuration from environment."""
    return {
        "host": os.getenv("SABOT_HOST", "0.0.0.0"),
        "port": int(os.getenv("SABOT_PORT", "8080")),
        "coordinator_host": os.getenv("COORDINATOR_HOST", "localhost"),
        "coordinator_port": int(os.getenv("COORDINATOR_PORT", "8080")),
        "cluster_name": os.getenv("CLUSTER_NAME", "sabot-cluster"),
        "max_workers": int(os.getenv("MAX_WORKERS", "0")),
        "morsel_size_kb": int(os.getenv("MORSEL_SIZE_KB", "64")),
        "target_utilization": float(os.getenv("TARGET_UTILIZATION", "0.8")),
    }

class MockProcessor:
    """Mock processor for demonstration."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False

    async def start(self):
        self.is_running = True
        print("   • Started mock processor")

    async def stop(self):
        self.is_running = False
        print("   • Stopped mock processor")

    async def process_data(self, data, processor_func):
        """Mock data processing."""
        print(f"   • Processing {len(data)} items...")

        # Simulate parallel processing
        results = []
        for item in data:
            result = await processor_func(item)
            results.append(result)

        print(f"   • Processed {len(results)} items")
        return results

    def get_stats(self):
        return {
            "mode": "mock_processor",
            "morsel_size_kb": self.config["morsel_size_kb"],
            "target_utilization": self.config["target_utilization"],
            "system_cpu": psutil.cpu_percent(),
            "system_memory": psutil.virtual_memory().percent,
        }

class MockCoordinator:
    """Mock distributed coordinator."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.nodes = {}
        self.jobs = {}
        self.is_running = False

    async def start(self):
        self.is_running = True
        print(f"   • Started coordinator on {self.config['host']}:{self.config['port']}")

    async def stop(self):
        self.is_running = False
        print("   • Stopped coordinator")

    async def submit_job(self, data, processor_func):
        job_id = f"job-{len(self.jobs)}"
        self.jobs[job_id] = {"data": data, "func": processor_func, "status": "running"}
        print(f"   • Submitted distributed job {job_id}")
        return job_id

    async def get_job_result(self, job_id):
        # Simulate job completion
        await asyncio.sleep(0.1)
        job = self.jobs.get(job_id, {})
        data = job.get("data", [])
        # Simulate processing
        return [item * 2 for item in data]  # Simple transformation

    def get_cluster_stats(self):
        return {
            "nodes": {"total": len(self.nodes), "alive": len(self.nodes)},
            "jobs": {"total": len(self.jobs), "running": len(self.jobs)},
            "system_resources": {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
            }
        }

class MockWorker:
    """Mock worker node."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False

    async def start(self):
        self.is_running = True
        print(f"   • Started worker connecting to {self.config['coordinator_host']}:{self.config['coordinator_port']}")

    async def stop(self):
        self.is_running = False
        print("   • Stopped worker")

async def demo_composable_processing():
    """Demonstrate composable processing."""
    print("\n🎭 Composable Processing Demo")
    print("-" * 30)

    # Detect deployment mode
    mode = detect_deployment_mode()
    config = load_configuration()

    print(f"✅ Detected mode: {mode.value}")
    print(f"   Configuration: {config}")

    # Create appropriate components based on mode
    processor = None
    coordinator = None
    worker = None

    try:
        if mode == DeploymentMode.SINGLE_NODE:
            print("🏠 Single-Node Mode:")
            processor = MockProcessor(config)
            await processor.start()

        elif mode == DeploymentMode.COORDINATOR:
            print("🎛️  Coordinator Mode:")
            coordinator = MockCoordinator(config)
            await coordinator.start()

        elif mode == DeploymentMode.WORKER:
            print("👷 Worker Mode:")
            worker = MockWorker(config)
            await worker.start()

        else:
            print("🤖 Auto Mode (defaulting to single-node):")
            processor = MockProcessor(config)
            await processor.start()

        # Sample processing function
        async def sample_processor(item):
            """Sample processing."""
            await asyncio.sleep(0.001)  # Simulate work
            return item * item

        # Sample data
        data = list(range(20))
        print(f"\n📊 Processing {len(data)} items...")

        # Process data (different logic based on mode)
        start_time = asyncio.get_event_loop().time()

        if processor:
            # Single-node processing
            results = await processor.process_data(data, sample_processor)
        elif coordinator:
            # Distributed processing
            job_id = await coordinator.submit_job(data, sample_processor)
            results = await coordinator.get_job_result(job_id)
        elif worker:
            # Worker mode - would normally receive jobs from coordinator
            print("   • Worker mode: would process jobs from coordinator")
            results = []
        else:
            results = []

        end_time = asyncio.get_event_loop().time()
        processing_time = end_time - start_time

        print("✅ Processing completed!")
        print(".1f")
        print(f"   Results: {len(results)} items")

        if results:
            sample_results = results[:5] if len(results) >= 5 else results
            print(f"   Sample: {sample_results}")

        # Show stats
        print("\n📈 System Stats:")
        if processor:
            stats = processor.get_stats()
            print(f"   Mode: {stats['mode']}")
            print(f"   CPU: {stats['system_cpu']:.1f}%")
            print(f"   Memory: {stats['system_memory']:.1f}%")
        elif coordinator:
            stats = coordinator.get_cluster_stats()
            print(f"   Nodes: {stats['nodes']['total']}")
            print(f"   Jobs: {stats['jobs']['total']}")
            print(f"   CPU: {stats['system_resources']['cpu_percent']:.1f}%")

        # Cleanup
        if processor:
            await processor.stop()
        if coordinator:
            await coordinator.stop()
        if worker:
            await worker.stop()

        return results

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return []

def show_deployment_scenarios():
    """Show different deployment scenarios."""
    print("\n🏗️  Deployment Scenarios")
    print("-" * 25)

    scenarios = [
        ("Local Development", "python simple_composable_demo.py", "Single-node with local processing"),
        ("Distributed Cluster", "COORDINATOR + Multiple WORKERS", "Multi-machine processing"),
        ("Kubernetes", "kubectl apply -f k8s-deployment.yaml", "Production K8s deployment"),
        ("Docker", "docker run sabot", "Containerized deployment"),
        ("Auto-Detection", "Just run the script", "Detects environment automatically")
    ]

    print("Scenario".ljust(20) + "Command".ljust(35) + "Description")
    print("-" * 80)
    for scenario, command, desc in scenarios:
        print(f"{scenario:<20}{command:<35}{desc}")

def show_key_benefits():
    """Show key benefits of composable architecture."""
    print("\n🎯 Key Benefits")
    print("-" * 15)

    benefits = [
        "✨ Write once, deploy anywhere",
        "📈 Seamless scaling from 1 to N nodes",
        "🐳 Cloud-native with Kubernetes support",
        "🧠 Intelligent resource management",
        "⚡ High-performance Cython execution",
        "🔄 Zero code changes between environments",
        "🛡️ Fault tolerance and auto-recovery",
        "📊 Unified monitoring across all modes"
    ]

    for benefit in benefits:
        print(f"  {benefit}")

    print("\n💡 The same Sabot code runs everywhere!")

async def main():
    """Run the composable demo."""
    print("This demo shows how Sabot adapts to different deployment")
    print("environments while using the same codebase.\n")

    # Show deployment options
    show_deployment_scenarios()
    show_key_benefits()

    # Run the actual demo
    results = await demo_composable_processing()

    print("\n🎉 Demo completed successfully!")
    print(f"   Demonstrated composable processing with {len(results)} results")

    print("\n🚀 Try different modes:")
    print("   • Single-node: python simple_composable_demo.py")
    print("   • Coordinator: SABOT_MODE=coordinator python simple_composable_demo.py")
    print("   • Worker: SABOT_MODE=worker COORDINATOR_HOST=localhost python simple_composable_demo.py")
    print("   • Kubernetes: kubectl apply -f k8s-deployment.yaml")

if __name__ == "__main__":
    asyncio.run(main())
