#!/usr/bin/env python3
"""Demonstration of Morsel-Driven Parallelism in Sabot."""

import asyncio
import time
import random
from typing import List, Any
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

print("🍽️  Morsel-Driven Parallelism Demo")
print("=" * 50)

async def demo_basic_parallelism():
    """Demonstrate basic morsel-driven parallelism."""
    print("\n🔄 Basic Parallel Processing:")

    # Import the parallel processor
    from sabot.morsel_parallelism import ParallelProcessor, Morsel

    # Create processor with 4 workers
    processor = ParallelProcessor(num_workers=4, morsel_size_kb=16)
    await processor.start()

    # Sample data to process
    data = list(range(100))  # 0 to 99

    print(f"Processing {len(data)} items using {processor.num_workers} workers...")

    # Define processing function
    async def square_processor(morsel: Morsel) -> List[int]:
        """Square each number in the morsel."""
        results = []
        items = morsel.data if isinstance(morsel.data, list) else [morsel.data]

        # Simulate some processing time
        await asyncio.sleep(0.01)

        for item in items:
            results.append(item * item)

        return results

    # Process data in parallel
    start_time = time.time()
    results = await processor.process_with_function(data, square_processor)
    end_time = time.time()

    # Flatten results
    flattened = []
    for result_list in results:
        flattened.extend(result_list)

    processing_time = end_time - start_time

    print(f"✅ Processed {len(flattened)} items in {processing_time:.3f}s")
    print(f"   Throughput: {len(flattened)/processing_time:.1f} items/second")

    # Show stats
    stats = processor.get_stats()
    print(f"   Morsels created: {stats['total_morsels_created']}")
    print(f"   Morsels processed: {stats['total_morsels_processed']}")
    print(f"   Morsels/second: {stats['morsels_per_second']:.1f}")

    await processor.stop()
    return flattened

async def demo_fan_out_fan_in():
    """Demonstrate fan-out/fan-in processing."""
    print("\n🌬️  Fan-Out/Fan-In Processing:")

    from sabot.morsel_parallelism import create_fan_out_fan_in_pipeline

    # Create input stream
    input_stream = asyncio.Queue()

    # Create fan-out/fan-in pipeline
    async def process_item(morsel):
        """Process a morsel by doubling values."""
        await asyncio.sleep(0.005)  # Simulate processing time

        items = morsel.data if isinstance(morsel.data, list) else [morsel.data]
        return [item * 2 for item in items]

    pipeline = create_fan_out_fan_in_pipeline(
        input_stream=input_stream,
        processor_func=process_item,
        num_branches=4,
        num_workers=4
    )

    # Feed data into input stream
    input_data = list(range(50))
    for item in input_data:
        await input_stream.put(item)

    # Collect results
    results = []
    result_count = 0

    start_time = time.time()
    while result_count < len(input_data):
        try:
            result = await asyncio.wait_for(pipeline.get(), timeout=1.0)
            results.extend(result)
            result_count += 1
        except asyncio.TimeoutError:
            break

    end_time = time.time()

    processing_time = end_time - start_time
    print(f"✅ Fan-out/fan-in processed {len(results)} items in {processing_time:.3f}s")
    print(f"   Throughput: {len(results)/processing_time:.1f} items/second")

    return results

async def demo_stream_parallelism():
    """Demonstrate parallel stream processing."""
    print("\n🌊 Parallel Stream Processing:")

    from sabot.stream_parallel import parallel_map_stream

    # Create input stream
    input_stream = asyncio.Queue()

    # Create parallel mapping function
    async def expensive_computation(item: int) -> int:
        """Expensive computation on each item."""
        # Simulate CPU-intensive work
        result = 0
        for i in range(1000):
            result += item * i
        await asyncio.sleep(0.001)  # Small async delay
        return result

    # Process stream in parallel
    output_stream = await parallel_map_stream(
        input_stream=input_stream,
        map_func=expensive_computation,
        num_workers=4
    )

    # Feed data
    input_data = list(range(20))
    for item in input_data:
        await input_stream.put(item)

    # Collect results
    results = []
    for _ in range(len(input_data)):
        result = await output_stream.get()
        results.extend(result)

    print(f"✅ Parallel stream processing completed: {len(results)} results")

    # Verify results are computed
    sample_results = results[:5] if len(results) >= 5 else results
    print(f"   Sample results: {sample_results}")

    return results

async def demo_scaling_comparison():
    """Compare performance with different worker counts."""
    print("\n📊 Scaling Comparison:")

    from sabot.morsel_parallelism import create_parallel_processor

    data_sizes = [100, 500, 1000]
    worker_counts = [1, 2, 4, 8]

    print("Data Size | Workers | Time (s) | Throughput (items/s)")
    print("----------|---------|----------|-------------------")

    for data_size in data_sizes:
        for workers in worker_counts:
            # Skip unrealistic combinations
            if workers > data_size:
                continue

            processor = create_parallel_processor(workers, morsel_size_kb=8)
            await processor.start()

            data = list(range(data_size))

            async def simple_processor(morsel):
                items = morsel.data if isinstance(morsel.data, list) else [morsel.data]
                await asyncio.sleep(0.001)  # Fixed small delay
                return [item + 1 for item in items]

            start_time = time.time()
            results = await processor.process_with_function(data, simple_processor)
            end_time = time.time()

            processing_time = end_time - start_time
            total_items = sum(len(r) for r in results)
            throughput = total_items / processing_time if processing_time > 0 else 0

            print(">8")

            await processor.stop()

def demo_morsel_concepts():
    """Explain morsel concepts."""
    print("\n🧩 Morsel-Driven Parallelism Concepts:")
    print("  • Morsel: Small chunk of data (fits in L2/L3 cache)")
    print("  • Fan-out: Split processing across multiple workers")
    print("  • Fan-in: Merge results from parallel workers")
    print("  • Work-stealing: Dynamic load balancing")
    print("  • NUMA-aware: Memory locality optimization")

    print("\n🎯 Key Benefits:")
    print("  • Cache-efficient processing")
    print("  • Dynamic load balancing")
    print("  • Memory bandwidth optimization")
    print("  • Scalable to many cores/machines")
    print("  • Fault tolerance through isolation")

    print("\n🏗️ Architecture:")
    print("  Input Stream → Fan-out → Parallel Workers → Fan-in → Output Stream")
    print("                    ↓")
    print("              Morsel Creation")
    print("                    ↓")
    print("            Work Distribution")

async def main():
    """Run all demonstrations."""
    print("This demo shows how Morsel-Driven Parallelism enables")
    print("efficient fan-out/fan-in processing across multiple threads/machines.\n")

    # Show concepts
    demo_morsel_concepts()

    try:
        # Run demonstrations
        await demo_basic_parallelism()
        await demo_fan_out_fan_in()
        await demo_stream_parallelism()
        await demo_scaling_comparison()

        print("\n🎉 All demonstrations completed successfully!")
        print("\n💡 Key Takeaways:")
        print("  • Morsel-driven parallelism enables efficient parallel processing")
        print("  • Fan-out/fan-in patterns scale across threads and machines")
        print("  • Small morsels optimize cache usage and load balancing")
        print("  • Dynamic work distribution adapts to varying workloads")
        print("  • Stream processing integrates seamlessly with parallelism")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
