#!/usr/bin/env python3
"""Demonstration of Sabot's composable architecture - same code runs everywhere."""

import asyncio
import sys
import os

# Add sabot to path for demo
sys.path.insert(0, os.path.dirname(__file__))

print("🔧 Sabot Composable Architecture Demo")
print("=" * 50)

async def demo_composable_processing():
    """Demonstrate that the same code works in all deployment modes."""
    print("\n🎭 Same Code, Different Deployments")
    print("-" * 40)

    # This code works in single-node, multi-node, or Kubernetes!

    try:
        from sabot import create_composable_launcher

        # Create launcher (auto-detects deployment mode from environment)
        launcher = create_composable_launcher()

        print("✅ Created launcher - mode auto-detected from environment")
        print(f"   Mode: {launcher.mode.value}")
        print(f"   Config: {launcher.config}")

        # Start the launcher (works in all modes)
        await launcher.start()

        print("✅ Launcher started successfully")

        # Create sample data
        data = list(range(100))
        print(f"📊 Created sample data: {len(data)} items")

        # Define processing function
        async def sample_processor(item):
            """Sample processing function."""
            # Simulate some work
            result = item * item
            await asyncio.sleep(0.001)  # Small async delay
            return result

        # Process data (works in all modes!)
        print("⚡ Processing data...")
        start_time = asyncio.get_event_loop().time()

        results = await launcher.process_data(data, sample_processor)

        end_time = asyncio.get_event_loop().time()
        processing_time = end_time - start_time

        print("✅ Processing completed!")
        print(".1f")
        print(f"   Items processed: {len(results)}")

        # Show some results
        sample_results = results[:5] if len(results) >= 5 else results
        print(f"   Sample results: {sample_results}")

        # Get stats (different info based on mode)
        stats = launcher.get_stats()
        print("📈 System Stats:")
        print(f"   Mode: {stats.get('mode', 'unknown')}")

        if 'dbos_controller' in stats:
            # Distributed mode stats
            dbos_stats = stats['dbos_controller']
            print(f"   Active workers: {dbos_stats.get('active_workers', 0)}")
            print(f"   Total processed: {dbos_stats.get('total_processed', 0)}")
        elif 'cython_processor' in stats:
            # Single-node stats
            proc_stats = stats['cython_processor']
            print(f"   Morsel size: {proc_stats.get('morsel_size_kb', 0)}KB")
        else:
            print("   Single-node mode with basic processor")

        # Show system resources
        if 'system_resources' in stats:
            sys_res = stats['system_resources']
            print(f"   CPU usage: {sys_res.get('cpu_percent', 0):.1f}%")
            print(f"   Memory usage: {sys_res.get('memory_percent', 0):.1f}%")

        await launcher.stop()
        print("✅ Launcher stopped")

        return results

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return []

def show_deployment_modes():
    """Show different ways to deploy Sabot."""
    print("\n🏗️  Deployment Modes")
    print("-" * 20)

    modes = [
        ("Single-Node Development", "SABOT_MODE=single-node python composable_demo.py"),
        ("Distributed Coordinator", "SABOT_MODE=coordinator python composable_demo.py"),
        ("Distributed Worker", "SABOT_MODE=worker COORDINATOR_HOST=localhost python composable_demo.py"),
        ("Kubernetes Cluster", "kubectl apply -f k8s-deployment.yaml"),
        ("Docker Container", "docker run -e SABOT_MODE=single-node sabot"),
        ("Auto-Detection", "python composable_demo.py  # detects from environment")
    ]

    for mode, command in modes:
        print(">25")

def show_environment_variables():
    """Show important environment variables."""
    print("\n⚙️  Environment Variables")
    print("-" * 25)

    vars = [
        ("SABOT_MODE", "single-node, coordinator, worker, auto", "Deployment mode"),
        ("SABOT_HOST", "0.0.0.0", "Host to bind to"),
        ("SABOT_PORT", "8080", "Port to listen on"),
        ("COORDINATOR_HOST", "localhost", "Coordinator hostname"),
        ("COORDINATOR_PORT", "8080", "Coordinator port"),
        ("MORSEL_SIZE_KB", "64", "Data morsel size"),
        ("TARGET_UTILIZATION", "0.8", "Target CPU utilization"),
        ("MAX_WORKERS", "auto", "Maximum workers per node"),
        ("LOG_LEVEL", "INFO", "Logging level")
    ]

    print("Variable".ljust(20) + "Default".ljust(15) + "Description")
    print("-" * 70)
    for var, default, desc in vars:
        print(f"{var:<20}{default:<15}{desc}")

def show_architecture_benefits():
    """Show benefits of composable architecture."""
    print("\n🎯 Architecture Benefits")
    print("-" * 25)

    benefits = [
        "🔄 Same codebase for all deployment scenarios",
        "📈 Seamless scaling from 1 to N nodes",
        "🐳 Container-ready for cloud deployments",
        "☸️ Kubernetes-native with HPA and PDB",
        "🧠 Intelligent resource management",
        "⚡ Cython performance optimizations",
        "🔍 Unified monitoring and observability",
        "🛡️ Fault tolerance and auto-recovery"
    ]

    for benefit in benefits:
        print(f"  {benefit}")

    print("\n💡 Key Insight: Write once, deploy anywhere!")

async def main():
    """Run all demonstrations."""
    print("This demo shows how Sabot's composable architecture")
    print("lets you write code once and run it everywhere.\n")

    # Show deployment options
    show_deployment_modes()
    show_environment_variables()
    show_architecture_benefits()

    # Run the actual demo
    results = await demo_composable_processing()

    print("\n🎉 Demo completed successfully!")
    print(f"   Processed {len(results)} items using composable architecture")

    print("\n🚀 Try different deployment modes:")
    print("   • Single-node: SABOT_MODE=single-node python composable_demo.py")
    print("   • Distributed: Start coordinator, then add workers")
    print("   • Kubernetes: kubectl apply -f k8s-deployment.yaml")

if __name__ == "__main__":
    asyncio.run(main())
