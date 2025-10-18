#!/usr/bin/env python3
"""
Simple PySpark vs Sabot Benchmark

A simplified benchmark that tests basic operations to compare PySpark and Sabot
performance without complex pipeline dependencies.
"""

import os
import sys
import time
import psutil
import tempfile
import json
import statistics
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, '/Users/bengamble/Sabot')

# Optional imports with fallbacks
try:
    import pyarrow as pa
    import pyarrow.compute as pc
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    print("Warning: PyArrow not available, some features disabled")

try:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, count, max as spark_max, min as spark_min
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Warning: PySpark not available, PySpark benchmarks will be skipped")

try:
    from sabot.spark import SparkSession as SabotSparkSession
    from sabot.spark.functions import col as sabot_col, sum as sabot_sum, avg as sabot_avg, count as sabot_count
    from sabot.spark.functions import max as sabot_max, min as sabot_min
    SABOT_AVAILABLE = True
except ImportError:
    SABOT_AVAILABLE = False
    print("Warning: Sabot not available, Sabot benchmarks will be skipped")


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    system: str
    operation: str
    dataset_size: int
    execution_time: float
    memory_peak: float
    memory_average: float
    throughput_rows_per_sec: float
    cpu_percent: float
    success: bool
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class DataGenerator:
    """Generate test datasets for benchmarking."""
    
    def __init__(self, base_dir: str = None):
        self.base_dir = Path(base_dir) if base_dir else Path(tempfile.mkdtemp())
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_simple_data(self, num_rows: int = 1_000_000) -> str:
        """Generate simple test dataset."""
        print(f"Generating {num_rows:,} simple records...")
        
        np.random.seed(42)  # Reproducible results
        
        data = {
            'id': list(range(1, num_rows + 1)),
            'value': np.random.uniform(0, 1000, num_rows).round(2),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows),
            'score': np.random.randint(1, 100, num_rows)
        }
        
        df = pd.DataFrame(data)
        output_path = self.base_dir / f"simple_{num_rows}.csv"
        df.to_csv(output_path, index=False)
        
        print(f"✅ Generated: {output_path}")
        return str(output_path)


class PerformanceMonitor:
    """Monitor system performance during benchmarks."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        self.peak_memory = 0
        self.memory_samples = []
        self.cpu_samples = []
    
    def start(self):
        """Start monitoring."""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory
        self.memory_samples = [self.start_memory]
        self.cpu_samples = []
    
    def sample(self):
        """Take a performance sample."""
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        current_cpu = self.process.cpu_percent()
        
        self.memory_samples.append(current_memory)
        self.cpu_samples.append(current_cpu)
        
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
    
    def stop(self) -> Tuple[float, float, float, float]:
        """Stop monitoring and return metrics."""
        if self.start_time is None:
            return 0, 0, 0, 0
        
        execution_time = time.time() - self.start_time
        peak_memory = self.peak_memory
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        
        return execution_time, peak_memory, avg_memory, avg_cpu


class PySparkBenchmark:
    """PySpark benchmark implementation."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def benchmark_data_loading(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark data loading performance."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Load data
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Force evaluation
            count = df.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="PySpark",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="PySpark",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_filtering(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark filtering operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Simple filtering
            filtered_df = df.filter(col("value") > 500)
            
            # Force evaluation
            count = filtered_df.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="PySpark",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="PySpark",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_groupby(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark groupBy aggregations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Simple groupBy
            result = df.groupBy("category").agg(
                count("*").alias("count"),
                spark_sum("value").alias("total_value"),
                spark_avg("value").alias("avg_value")
            )
            
            # Force evaluation
            count = result.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="PySpark",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="PySpark",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )


class SabotBenchmark:
    """Sabot benchmark implementation using Spark-compatible API."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def benchmark_data_loading(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark data loading performance."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Load data
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Force evaluation
            count = df.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="Sabot",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="Sabot",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_filtering(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark filtering operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Simple filtering
            filtered_df = df.filter(sabot_col("value") > 500)
            
            # Force evaluation
            count = filtered_df.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="Sabot",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="Sabot",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_groupby(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark groupBy aggregations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Simple groupBy
            result = df.groupBy("category").agg(
                sabot_count("*").alias("count"),
                sabot_sum("value").alias("total_value"),
                sabot_avg("value").alias("avg_value")
            )
            
            # Force evaluation
            count = result.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="Sabot",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            return BenchmarkResult(
                system="Sabot",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )


class BenchmarkRunner:
    """Main benchmark runner."""
    
    def __init__(self, output_dir: str = "benchmarks/results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results: List[BenchmarkResult] = []
        
        # Dataset sizes to test
        self.dataset_sizes = [10_000, 100_000]
        
        # Initialize data generator
        self.data_generator = DataGenerator()
    
    def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark suite."""
        print("🚀 Starting Simple PySpark vs Sabot Benchmark")
        print("=" * 60)
        
        # Generate test datasets
        print("\n📊 Generating test datasets...")
        datasets = {}
        
        for size in self.dataset_sizes:
            datasets[size] = self.data_generator.generate_simple_data(size)
        
        # Run PySpark benchmarks
        if PYSPARK_AVAILABLE:
            print("\n🔥 Running PySpark benchmarks...")
            self._run_pyspark_benchmarks(datasets)
        else:
            print("⚠️  PySpark not available, skipping PySpark benchmarks")
        
        # Run Sabot benchmarks
        if SABOT_AVAILABLE:
            print("\n⚡ Running Sabot benchmarks...")
            self._run_sabot_benchmarks(datasets)
        else:
            print("⚠️  Sabot not available, skipping Sabot benchmarks")
        
        # Analyze results
        print("\n📈 Analyzing results...")
        self._analyze_results()
        
        # Save results
        self._save_results()
        
        print("\n✅ Benchmark completed!")
        print(f"Results saved to: {self.output_dir}")
    
    def _run_pyspark_benchmarks(self, datasets: Dict[int, str]):
        """Run PySpark benchmarks."""
        try:
            # Create Spark session
            spark = PySparkSession.builder \
                .appName("PySparkSimpleBenchmark") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            benchmark = PySparkBenchmark(spark)
            
            for size, path in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # Data loading
                result = benchmark.benchmark_data_loading(path, size)
                self.results.append(result)
                print(f"    Data loading: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Filtering
                result = benchmark.benchmark_filtering(path, size)
                self.results.append(result)
                print(f"    Filtering: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # GroupBy
                result = benchmark.benchmark_groupby(path, size)
                self.results.append(result)
                print(f"    GroupBy: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            spark.stop()
            
        except Exception as e:
            print(f"❌ PySpark benchmark failed: {e}")
    
    def _run_sabot_benchmarks(self, datasets: Dict[int, str]):
        """Run Sabot benchmarks."""
        try:
            # Create Sabot Spark session
            spark = SabotSparkSession.builder \
                .appName("SabotSimpleBenchmark") \
                .master("local[*]") \
                .getOrCreate()
            
            benchmark = SabotBenchmark(spark)
            
            for size, path in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # Data loading
                result = benchmark.benchmark_data_loading(path, size)
                self.results.append(result)
                print(f"    Data loading: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Filtering
                result = benchmark.benchmark_filtering(path, size)
                self.results.append(result)
                print(f"    Filtering: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # GroupBy
                result = benchmark.benchmark_groupby(path, size)
                self.results.append(result)
                print(f"    GroupBy: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            spark.stop()
            
        except Exception as e:
            print(f"❌ Sabot benchmark failed: {e}")
    
    def _analyze_results(self):
        """Analyze benchmark results."""
        print("\n📊 Performance Analysis")
        print("=" * 60)
        
        # Group results by operation and dataset size
        by_operation = {}
        for result in self.results:
            if result.success:
                key = f"{result.operation}_{result.dataset_size}"
                if key not in by_operation:
                    by_operation[key] = {}
                by_operation[key][result.system] = result
        
        # Compare performance
        for key, systems in by_operation.items():
            parts = key.split('_')
            operation = parts[0]
            size = int(parts[-1])
            
            pyspark_result = systems.get('PySpark')
            sabot_result = systems.get('Sabot')
            
            print(f"\n{operation.title()} ({size:,} rows):")
            
            if pyspark_result and sabot_result:
                speedup = pyspark_result.execution_time / sabot_result.execution_time
                memory_reduction = (pyspark_result.memory_peak - sabot_result.memory_peak) / pyspark_result.memory_peak * 100
                
                print(f"  PySpark: {pyspark_result.execution_time:.2f}s, {pyspark_result.memory_peak:.1f}MB")
                print(f"  Sabot:   {sabot_result.execution_time:.2f}s, {sabot_result.memory_peak:.1f}MB")
                print(f"  Speedup: {speedup:.1f}x faster")
                print(f"  Memory:  {memory_reduction:.1f}% reduction")
            elif pyspark_result:
                print(f"  PySpark: {pyspark_result.execution_time:.2f}s, {pyspark_result.memory_peak:.1f}MB")
            elif sabot_result:
                print(f"  Sabot:   {sabot_result.execution_time:.2f}s, {sabot_result.memory_peak:.1f}MB")
    
    def _save_results(self):
        """Save results to files."""
        # Save JSON results
        json_path = self.output_dir / "simple_pyspark_vs_sabot_results.json"
        with open(json_path, 'w') as f:
            json.dump([result.to_dict() for result in self.results], f, indent=2)
        
        # Save CSV results
        csv_path = self.output_dir / "simple_pyspark_vs_sabot_results.csv"
        df = pd.DataFrame([result.to_dict() for result in self.results])
        df.to_csv(csv_path, index=False)
        
        print(f"\n💾 Results saved:")
        print(f"  JSON: {json_path}")
        print(f"  CSV:  {csv_path}")


def main():
    """Main benchmark execution."""
    print("Simple PySpark vs Sabot Benchmark")
    print("=" * 60)
    
    # Check availability
    print(f"PySpark available: {'✅' if PYSPARK_AVAILABLE else '❌'}")
    print(f"Sabot available: {'✅' if SABOT_AVAILABLE else '❌'}")
    print(f"PyArrow available: {'✅' if ARROW_AVAILABLE else '❌'}")
    
    if not PYSPARK_AVAILABLE and not SABOT_AVAILABLE:
        print("❌ Neither PySpark nor Sabot is available. Cannot run benchmark.")
        return
    
    # Run benchmark
    runner = BenchmarkRunner()
    runner.run_comprehensive_benchmark()


if __name__ == "__main__":
    main()
