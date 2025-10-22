#!/usr/bin/env python3
"""
Memory Benchmarks for Sabot

Benchmarks memory usage and efficiency including:
- Memory consumption during processing
- Memory leak detection
- Garbage collection efficiency
- Object pooling effectiveness
- Large dataset handling
"""

import asyncio
import time
import gc
import psutil
from typing import Dict, Any, List
from dataclasses import dataclass

from .runner import BenchmarkRunner, BenchmarkConfig, benchmark


@dataclass
class MemoryBenchmarkConfig:
    """Configuration for memory benchmarks."""
    dataset_size: int = 100000
    object_size_bytes: int = 1024
    processing_iterations: int = 10
    enable_gc: bool = True
    track_allocations: bool = True


class MemoryBenchmark:
    """Benchmark suite for memory usage and efficiency."""

    def __init__(self):
        self.runner = BenchmarkRunner()
        self.process = psutil.Process()

    @benchmark(BenchmarkConfig(
        name="memory_processing_efficiency",
        description="Benchmark memory usage during data processing",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_processing_memory(self) -> Dict[str, Any]:
        """Benchmark memory usage during data processing."""
        config = MemoryBenchmarkConfig(
            dataset_size=50000,
            object_size_bytes=512,
            processing_iterations=5
        )

        return await self._run_memory_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="memory_large_dataset",
        description="Benchmark memory handling of large datasets",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_large_dataset_memory(self) -> Dict[str, Any]:
        """Benchmark memory usage with large datasets."""
        config = MemoryBenchmarkConfig(
            dataset_size=500000,  # 500K records
            object_size_bytes=256,
            processing_iterations=3
        )

        return await self._run_memory_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="memory_gc_efficiency",
        description="Benchmark garbage collection efficiency",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_gc_efficiency(self) -> Dict[str, Any]:
        """Benchmark garbage collection efficiency."""
        config = MemoryBenchmarkConfig(
            dataset_size=100000,
            processing_iterations=10,
            enable_gc=True
        )

        # Run with GC enabled
        result_gc = await self._run_memory_benchmark(config)

        # Run with GC disabled
        config.enable_gc = False
        result_no_gc = await self._run_memory_benchmark(config)

        return {
            'with_gc': result_gc,
            'without_gc': result_no_gc,
            'memory_improvement_percent': (
                (result_no_gc['peak_memory_mb'] - result_gc['peak_memory_mb']) /
                result_no_gc['peak_memory_mb'] * 100
            ) if result_no_gc['peak_memory_mb'] > 0 else 0,
            'performance_impact_percent': (
                (result_no_gc['total_time_seconds'] - result_gc['total_time_seconds']) /
                result_gc['total_time_seconds'] * 100
            ) if result_gc['total_time_seconds'] > 0 else 0
        }

    async def _run_memory_benchmark(self, config: MemoryBenchmarkConfig) -> Dict[str, Any]:
        """Run a memory benchmark with given configuration."""
        # Initial memory reading
        initial_memory = self.process.memory_info().rss

        # Create test dataset
        dataset = self._create_test_dataset(config.dataset_size, config.object_size_bytes)

        # Track memory during processing
        memory_readings = []
        processing_times = []

        start_time = time.time()

        for iteration in range(config.processing_iterations):
            iteration_start = time.time()

            # Process dataset
            processed_data = await self._process_dataset(dataset, config.enable_gc)

            iteration_time = time.time() - iteration_start
            processing_times.append(iteration_time)

            # Record memory usage
            current_memory = self.process.memory_info().rss
            memory_readings.append(current_memory)

            # Optional GC between iterations
            if config.enable_gc:
                gc.collect()

        end_time = time.time()
        total_time = end_time - start_time

        # Final memory reading
        final_memory = self.process.memory_info().rss

        # Calculate metrics
        memory_delta = final_memory - initial_memory
        peak_memory = max(memory_readings) if memory_readings else initial_memory
        avg_memory = sum(memory_readings) / len(memory_readings) if memory_readings else initial_memory

        return {
            'dataset_size': config.dataset_size,
            'object_size_bytes': config.object_size_bytes,
            'processing_iterations': config.processing_iterations,
            'enable_gc': config.enable_gc,
            'total_time_seconds': total_time,
            'avg_processing_time_per_iteration': sum(processing_times) / len(processing_times),
            'initial_memory_mb': initial_memory / (1024 * 1024),
            'final_memory_mb': final_memory / (1024 * 1024),
            'peak_memory_mb': peak_memory / (1024 * 1024),
            'avg_memory_mb': avg_memory / (1024 * 1024),
            'memory_delta_mb': memory_delta / (1024 * 1024),
            'memory_efficiency_mb_per_item': memory_delta / config.dataset_size / config.processing_iterations,
            'throughput_items_per_sec': (config.dataset_size * config.processing_iterations) / total_time
        }

    def _create_test_dataset(self, size: int, object_size: int) -> List[Dict[str, Any]]:
        """Create a test dataset for memory benchmarking."""
        dataset = []

        for i in range(size):
            # Create object with specified size
            obj = {
                'id': f"object_{i}",
                'timestamp': time.time(),
                'data': 'x' * object_size,  # Variable-sized payload
                'metadata': {
                    'created_by': 'benchmark',
                    'version': '1.0',
                    'tags': ['test', 'memory', f'batch_{i % 10}']
                },
                'nested_object': {
                    'property1': i,
                    'property2': f"value_{i}",
                    'property3': [j for j in range(min(10, i % 20))]  # Variable-length array
                }
            }
            dataset.append(obj)

        return dataset

    async def _process_dataset(self, dataset: List[Dict[str, Any]], enable_gc: bool) -> List[Dict[str, Any]]:
        """Process the dataset with memory tracking."""
        processed = []

        for item in dataset:
            # Simulate processing work
            processed_item = item.copy()

            # Add processing results
            processed_item['processed_at'] = time.time()
            processed_item['processing_result'] = {
                'hash_value': hash(str(item['data'])),
                'data_length': len(item['data']),
                'transformed_data': item['data'][:100] + "..." if len(item['data']) > 100 else item['data']
            }

            # Simulate some computational work
            await asyncio.sleep(0.00001)  # 10 microseconds per item

            # Perform some transformations that create new objects
            processed_item['derived_fields'] = {
                'data_category': 'large' if len(item['data']) > 500 else 'small',
                'tag_count': len(item['metadata']['tags']),
                'has_nested': 'nested_object' in item,
                'array_sum': sum(item['nested_object']['property3']) if item['nested_object']['property3'] else 0
            }

            processed.append(processed_item)

            # Periodic GC if enabled (simulate cleanup)
            if enable_gc and len(processed) % 1000 == 0:
                gc.collect()

        return processed

    async def run_memory_analysis(self) -> Dict[str, Any]:
        """Run comprehensive memory analysis."""
        print("Running memory analysis...")

        # Test different dataset sizes
        sizes = [10000, 50000, 100000]
        results = {}

        for size in sizes:
            print(f"Testing dataset size: {size}")

            config = MemoryBenchmarkConfig(
                dataset_size=size,
                object_size_bytes=512,
                processing_iterations=3
            )

            result = await self._run_memory_benchmark(config)
            results[f"size_{size}"] = result

        # Analyze scaling
        scaling_analysis = self._analyze_memory_scaling(results)

        return {
            'individual_results': results,
            'scaling_analysis': scaling_analysis
        }

    def _analyze_memory_scaling(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze how memory usage scales with dataset size."""
        sizes = []
        memory_usage = []
        processing_times = []

        for key, result in results.items():
            size = result['dataset_size']
            memory_mb = result['peak_memory_mb']
            time_sec = result['total_time_seconds']

            sizes.append(size)
            memory_usage.append(memory_mb)
            processing_times.append(time_sec)

        # Calculate scaling metrics
        if len(sizes) >= 2:
            # Memory scaling factor
            memory_ratio = memory_usage[-1] / memory_usage[0]
            size_ratio = sizes[-1] / sizes[0]
            memory_scaling_factor = memory_ratio / size_ratio

            # Time scaling factor
            time_ratio = processing_times[-1] / processing_times[0]
            time_scaling_factor = time_ratio / size_ratio

            return {
                'size_range': f"{sizes[0]} to {sizes[-1]}",
                'memory_scaling_factor': memory_scaling_factor,
                'time_scaling_factor': time_scaling_factor,
                'memory_efficiency': "good" if memory_scaling_factor < 1.5 else "concerning",
                'time_efficiency': "good" if time_scaling_factor < 1.5 else "concerning"
            }

        return {'error': 'Insufficient data for scaling analysis'}

    async def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all memory benchmarks."""
        benchmarks = [
            self.benchmark_processing_memory,
            self.benchmark_large_dataset_memory,
            self.benchmark_gc_efficiency
        ]

        results = await self.runner.run_comprehensive_suite(benchmarks)

        # Add memory analysis
        try:
            memory_analysis = await self.run_memory_analysis()
            analysis_result = {
                'benchmark_name': 'memory_analysis',
                'timestamp': time.time(),
                'config': BenchmarkConfig(name="memory_analysis", description="Memory scaling analysis"),
                'metrics': memory_analysis,
                'system_info': self.runner.system_info,
                'raw_measurements': [],
                'statistics': {}
            }
            results.append(analysis_result)
        except Exception as e:
            print(f"Memory analysis failed: {e}")

        return results
