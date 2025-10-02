#!/usr/bin/env python3
"""Demonstration of DBOS-controlled Cython Morsel-Driven Parallelism."""

import asyncio
import time
import sys
import os

# Add sabot to path for demo
sys.path.insert(0, os.path.dirname(__file__))

# Mock dependencies
from unittest.mock import MagicMock
sys.modules['typer'] = MagicMock()
sys.modules['rich.console'] = MagicMock()
sys.modules['rich.table'] = MagicMock()
sys.modules['rich.panel'] = MagicMock()
sys.modules['rich.text'] = MagicMock()
sys.modules['rich.live'] = MagicMock()
sys.modules['rich.spinner'] = MagicMock()
sys.modules['rich.progress'] = MagicMock()
sys.modules['rich.prompt'] = MagicMock()
sys.modules['rich'] = MagicMock()

# Mock typer
mock_typer = sys.modules['typer']
mock_app = MagicMock()
mock_typer.Typer.return_value = mock_app
mock_typer.Argument = lambda x, **kwargs: x
mock_typer.Option = lambda *args, **kwargs: args[0] if args else None

# Mock rich
mock_console = MagicMock()
sys.modules['rich.console'].Console.return_value = mock_console

print("🧠 DBOS-Controlled Cython Morsel-Driven Parallelism Demo")
print("=" * 60)

async def demo_dbos_cython_parallel():
    """Demonstrate DBOS-controlled Cython parallel processing."""
    print("\n🚀 DBOS + Cython Parallel Processing:")

    try:
        from sabot.dbos_cython_parallel import DBOSCythonParallelProcessor

        # Create DBOS-controlled processor
        processor = DBOSCythonParallelProcessor(
            max_workers=4,
            morsel_size_kb=32,
            target_utilization=0.8
        )

        print("  • Created DBOS-controlled Cython processor")
        print("  • Target utilization: 80%")
        print("  • Morsel size: 32KB")
        print("  • Max workers: 4")

        # Sample data
        data = list(range(200))
        print(f"  • Processing {len(data)} items...")

        # Processing function
        async def process_item(item):
            # Simulate CPU-intensive work
            result = 0
            for i in range(1000):
                result += item * i
            await asyncio.sleep(0.001)  # Small async delay
            return result

        # Start processing
        start_time = time.time()
        await processor.start()

        results = await processor.process_data(data, process_item)

        await processor.stop()
        end_time = time.time()

        processing_time = end_time - start_time

        print(f"  ✅ Processed {len(results)} items in {processing_time:.3f}s")
        print(f"     Throughput: {len(results)/processing_time:.1f} items/second")

        # Get performance stats
        stats = processor.get_stats()
        print("  📊 Performance Stats:")
        print(f"     DBOS controller active: {stats['dbos_controller']['active_workers'] > 0}")
        print(f"     System CPU: {stats['system_resources']['cpu_percent']:.1f}%")
        print(f"     System memory: {stats['system_resources']['memory_percent']:.1f}%")

        return results

    except ImportError as e:
        print(f"  ⚠️  Cython not available: {e}")
        print("     Falling back to Python implementation...")

        # Fallback to Python implementation
        from sabot.morsel_parallelism import ParallelProcessor

        processor = ParallelProcessor(num_workers=4, morsel_size_kb=32)
        await processor.start()

        data = list(range(50))  # Smaller dataset for demo

        async def process_item(item):
            result = 0
            for i in range(500):  # Less work for demo
                result += item * i
            await asyncio.sleep(0.001)
            return result

        start_time = time.time()
        results = await processor.process_with_function(data, process_item)
        await processor.stop()
        end_time = time.time()

        processing_time = end_time - start_time
        print(f"  ✅ Python fallback processed {len(results)} items in {processing_time:.3f}s")

        return results

async def demo_fan_out_fan_in():
    """Demonstrate fan-out/fan-in with DBOS control."""
    print("\n🌬️  Fan-Out/Fan-In with DBOS Control:")

    try:
        from sabot.dbos_cython_parallel import FanOutFanInCythonPipeline

        # Create fan-out/fan-in pipeline
        async def transform_data(chunk):
            """Transform data chunk."""
            await asyncio.sleep(0.005)  # Simulate processing
            return [item * 2 for item in chunk] if isinstance(chunk, list) else [chunk * 2]

        pipeline = FanOutFanInCythonPipeline(
            processor_func=transform_data,
            num_branches=3,
            morsel_size_kb=16
        )

        print("  • Created fan-out/fan-in pipeline")
        print("  • 3 parallel branches")
        print("  • 16KB morsels")

        # Create input stream
        input_stream = asyncio.Queue()
        test_data = list(range(60))

        # Feed data
        for item in test_data:
            await input_stream.put(item)

        print(f"  • Feeding {len(test_data)} items to pipeline...")

        # Process through pipeline
        start_time = time.time()
        output_stream = await pipeline.process_stream(input_stream)
        await pipeline.start()  # Start the pipeline

        # Collect results
        results = []
        collected = 0
        while collected < len(test_data):
            try:
                result = await asyncio.wait_for(output_stream.get(), timeout=2.0)
                results.extend(result)
                collected += 1
                output_stream.task_done()
            except asyncio.TimeoutError:
                break

        await pipeline.stop()
        end_time = time.time()

        processing_time = end_time - start_time

        print(f"  ✅ Pipeline processed {len(results)} results in {processing_time:.3f}s")
        print(f"     Throughput: {len(results)/processing_time:.1f} items/second")

        # Verify results (should be doubled)
        sample_results = results[:5] if len(results) >= 5 else results
        print(f"     Sample results: {sample_results}")

        return results

    except Exception as e:
        print(f"  ⚠️  Fan-out/fan-in demo failed: {e}")
        return []

async def demo_app_integration():
    """Demonstrate app-level integration."""
    print("\n🏗️  App-Level Integration:")

    try:
        # This would normally be done through the app, but we'll simulate
        print("  • App.create_dbos_parallel_processor() - Creates intelligent processor")
        print("  • App.parallel_process_data() - High-level parallel processing")
        print("  • App.create_fan_out_fan_in_pipeline() - Pipeline creation")
        print("  • Automatic DBOS decision making")
        print("  • Cython optimization when available")
        print("  • Graceful Python fallback")

        # Simulate app usage
        print("\n  💡 Example App Usage:")
        print("""
    app = sabot.create_app("parallel-processing-app")

    # Intelligent parallel processing
    results = await app.parallel_process_data(
        large_dataset,
        async def process_item(item):
            return expensive_computation(item)
    )

    # Fan-out/fan-in pipeline
    pipeline = app.create_fan_out_fan_in_pipeline(
        processor_func=process_chunk,
        num_branches=8
    )

    result_stream = await pipeline.process_stream(input_stream)
        """)

        return True

    except Exception as e:
        print(f"  ⚠️  App integration demo failed: {e}")
        return False

def explain_architecture():
    """Explain the DBOS + Cython architecture."""
    print("\n🏛️  Architecture Overview:")

    arch = """
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Application   │    │   DBOS Controller│    │  Cython Runtime │
│                 │    │                  │    │                 │
│ • User Code     │───▶│ • Intelligent     │───▶│ • Fast Execution│
│ • Data Input    │    │   Decisions       │    │ • Morsel Proc. │
│ • Result Output │    │ • Load Balancing  │    │ • Zero Copy     │
└─────────────────┘    │ • Scaling         │    └─────────────────┘
                       │ • NUMA Aware      │
                       └───────────────────┘
                                ▲
                                │
                       ┌──────────────────┐
                       │  System Metrics  │
                       │ • CPU Usage      │
                       │ • Memory Usage   │
                       │ • Worker Health  │
                       │ • Queue Depths   │
                       └──────────────────┘
    """

    print(arch)

    print("🔄 Control Flow:")
    print("  1. Application submits data + processing function")
    print("  2. DBOS analyzes system state and requirements")
    print("  3. DBOS decides: worker count, morsel size, distribution")
    print("  4. Cython runtime executes with optimal performance")
    print("  5. DBOS monitors and adapts in real-time")

    print("\n🎯 Key Innovations:")
    innovations = [
        "DBOS-guided intelligent decisions",
        "Cython-optimized execution engine",
        "Morsel-driven data partitioning",
        "NUMA-aware work distribution",
        "Adaptive load balancing",
        "Real-time performance monitoring",
        "Graceful fallback to Python",
    ]

    for i, innovation in enumerate(innovations, 1):
        print(f"  {i}. {innovation}")

async def main():
    """Run all demonstrations."""
    print("This demo showcases DBOS-controlled Cython parallel processing")
    print("where intelligent decisions meet high-performance execution.\n")

    # Explain architecture
    explain_architecture()

    try:
        # Run demonstrations
        await demo_dbos_cython_parallel()
        await demo_fan_out_fan_in()
        await demo_app_integration()

        print("\n🎉 All demonstrations completed!")
        print("\n💡 Key Takeaways:")
        print("  • DBOS provides intelligent parallel processing decisions")
        print("  • Cython delivers high-performance execution")
        print("  • Morsel-driven approach optimizes cache usage")
        print("  • Fan-out/fan-in enables complex parallel workflows")
        print("  • System adapts to changing conditions automatically")
        print("  • Graceful fallbacks ensure reliability")

        print("\n🚀 Perfect for:")
        print("  • Large-scale data processing")
        print("  • Real-time analytics pipelines")
        print("  • CPU-intensive streaming applications")
        print("  • High-throughput event processing")
        print("  • Memory-constrained distributed systems")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
