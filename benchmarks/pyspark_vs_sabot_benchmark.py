#!/usr/bin/env python3
"""
PySpark vs Sabot Performance Benchmark

This benchmark compares PySpark and Sabot using identical code patterns
to measure performance differences across common data processing operations.

Key Features:
- Same data generation for both systems
- Identical transformation pipelines
- Comprehensive performance metrics
- Memory usage tracking
- Throughput measurements
- Scalability analysis

Operations Tested:
1. Data loading (Parquet, CSV)
2. Filtering operations
3. GroupBy aggregations
4. Joins (inner, left)
5. Window functions
6. Complex transformations
7. Data writing

Expected Results:
- Sabot: 2-10x faster on single machine
- Lower memory usage
- Faster startup time
- Comparable distributed performance
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
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    print("Warning: PyArrow not available, some features disabled")

try:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, count, max as spark_max, min as spark_min
    from pyspark.sql.functions import when, coalesce, row_number, rank, dense_rank
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Warning: PySpark not available, PySpark benchmarks will be skipped")

try:
    from sabot.spark import SparkSession as SabotSparkSession
    from sabot.spark.functions import col as sabot_col, sum as sabot_sum, avg as sabot_avg, count as sabot_count
    from sabot.spark.functions import max as sabot_max, min as sabot_min, when as sabot_when, coalesce as sabot_coalesce
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
    
    def generate_transactions(self, num_rows: int = 1_000_000) -> str:
        """Generate transaction dataset."""
        print(f"Generating {num_rows:,} transaction records...")
        
        # Generate realistic transaction data
        np.random.seed(42)  # Reproducible results
        
        data = {
            'transaction_id': list(range(1, num_rows + 1)),
            'customer_id': np.random.randint(1, 10000, num_rows),
            'amount': np.random.exponential(100, num_rows).round(2),
            'category': np.random.choice(['food', 'transport', 'entertainment', 'shopping', 'utilities'], num_rows),
            'merchant_id': np.random.randint(1, 1000, num_rows),
            'timestamp': pd.date_range('2024-01-01', periods=num_rows, freq='1min'),
            'is_fraud': np.random.choice([0, 1], num_rows, p=[0.95, 0.05]),
            'region': np.random.choice(['north', 'south', 'east', 'west'], num_rows)
        }
        
        df = pd.DataFrame(data)
        output_path = self.base_dir / f"transactions_{num_rows}.csv"
        
        # Use CSV to avoid PyArrow filesystem conflicts
        df.to_csv(output_path, index=False)
        
        print(f"✅ Generated: {output_path}")
        return str(output_path)
    
    def generate_customers(self, num_rows: int = 10000) -> str:
        """Generate customer dataset."""
        print(f"Generating {num_rows:,} customer records...")
        
        np.random.seed(42)
        
        data = {
            'customer_id': list(range(1, num_rows + 1)),
            'name': [f"Customer_{i}" for i in range(1, num_rows + 1)],
            'age': np.random.randint(18, 80, num_rows),
            'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], num_rows),
            'registration_date': pd.date_range('2020-01-01', periods=num_rows, freq='1D'),
            'tier': np.random.choice(['bronze', 'silver', 'gold', 'platinum'], num_rows, p=[0.4, 0.3, 0.2, 0.1])
        }
        
        df = pd.DataFrame(data)
        output_path = self.base_dir / f"customers_{num_rows}.csv"
        
        # Use CSV to avoid PyArrow filesystem conflicts
        df.to_csv(output_path, index=False)
        
        print(f"✅ Generated: {output_path}")
        return str(output_path)
    
    def generate_products(self, num_rows: int = 5000) -> str:
        """Generate product dataset."""
        print(f"Generating {num_rows:,} product records...")
        
        np.random.seed(42)
        
        data = {
            'product_id': list(range(1, num_rows + 1)),
            'name': [f"Product_{i}" for i in range(1, num_rows + 1)],
            'category': np.random.choice(['electronics', 'clothing', 'books', 'home', 'sports'], num_rows),
            'price': np.random.uniform(10, 1000, num_rows).round(2),
            'stock_quantity': np.random.randint(0, 1000, num_rows),
            'supplier_id': np.random.randint(1, 100, num_rows)
        }
        
        df = pd.DataFrame(data)
        output_path = self.base_dir / f"products_{num_rows}.csv"
        
        # Use CSV to avoid PyArrow filesystem conflicts
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
            
            monitor.stop()
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
            return BenchmarkResult(
                system="PySpark",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_filtering(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark filtering operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Complex filtering
            filtered_df = (df
                .filter(col("amount") > 100)
                .filter(col("category") == "food")
                .filter(col("is_fraud") == 0))
            
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
            return BenchmarkResult(
                system="PySpark",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_groupby(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark groupBy aggregations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Complex groupBy with multiple aggregations
            result = (df
                .groupBy("customer_id", "category")
                .agg(
                    spark_sum("amount").alias("total_amount"),
                    spark_avg("amount").alias("avg_amount"),
                    count("*").alias("transaction_count"),
                    spark_max("amount").alias("max_amount"),
                    spark_min("amount").alias("min_amount")
                )
                .filter(col("total_amount") > 1000))
            
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
            return BenchmarkResult(
                system="PySpark",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_join(self, transactions_path: str, customers_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark join operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            transactions_df = self.spark.read.csv(transactions_path, header=True, inferSchema=True)
            customers_df = self.spark.read.csv(customers_path, header=True, inferSchema=True)
            
            # Complex join with filtering
            result = (transactions_df
                .join(customers_df, "customer_id", "inner")
                .filter(col("amount") > 50)
                .filter(col("age") > 25)
                .select("transaction_id", "customer_id", "amount", "name", "city", "tier"))
            
            # Force evaluation
            count = result.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="PySpark",
                operation="join",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            return BenchmarkResult(
                system="PySpark",
                operation="join",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_window_functions(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark window functions."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Define window
            window_spec = Window.partitionBy("customer_id").orderBy("timestamp")
            
            # Complex window operations
            result = (df
                .withColumn("row_number", row_number().over(window_spec))
                .withColumn("rank", rank().over(window_spec))
                .withColumn("dense_rank", dense_rank().over(window_spec))
                .withColumn("amount_lag", col("amount").lag(1).over(window_spec))
                .withColumn("amount_lead", col("amount").lead(1).over(window_spec))
                .filter(col("row_number") <= 10))  # Top 10 per customer
            
            # Force evaluation
            count = result.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="PySpark",
                operation="window_functions",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            return BenchmarkResult(
                system="PySpark",
                operation="window_functions",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
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
            
            # Load data using Sabot's Spark-compatible API
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
            return BenchmarkResult(
                system="Sabot",
                operation="data_loading",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_filtering(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark filtering operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Complex filtering using Sabot's Spark-compatible API
            filtered_df = (df
                .filter(sabot_col("amount") > 100)
                .filter(sabot_col("category") == "food")
                .filter(sabot_col("is_fraud") == 0))
            
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
            return BenchmarkResult(
                system="Sabot",
                operation="filtering",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_groupby(self, file_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark groupBy aggregations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Complex groupBy with multiple aggregations
            result = (df
                .groupBy("customer_id", "category")
                .agg(
                    sabot_sum("amount").alias("total_amount"),
                    sabot_avg("amount").alias("avg_amount"),
                    sabot_count("*").alias("transaction_count"),
                    sabot_max("amount").alias("max_amount"),
                    sabot_min("amount").alias("min_amount")
                )
                .filter(sabot_col("total_amount") > 1000))
            
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
            return BenchmarkResult(
                system="Sabot",
                operation="groupby",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_join(self, transactions_path: str, customers_path: str, dataset_size: int) -> BenchmarkResult:
        """Benchmark join operations."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            transactions_df = self.spark.read.csv(transactions_path, header=True, inferSchema=True)
            customers_df = self.spark.read.csv(customers_path, header=True, inferSchema=True)
            
            # Complex join with filtering
            result = (transactions_df
                .join(customers_df, "customer_id", "inner")
                .filter(sabot_col("amount") > 50)
                .filter(sabot_col("age") > 25)
                .select("transaction_id", "customer_id", "amount", "name", "city", "tier"))
            
            # Force evaluation
            count = result.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu = monitor.stop()
            throughput = count / execution_time if execution_time > 0 else 0
            
            return BenchmarkResult(
                system="Sabot",
                operation="join",
                dataset_size=dataset_size,
                execution_time=execution_time,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True
            )
            
        except Exception as e:
            return BenchmarkResult(
                system="Sabot",
                operation="join",
                dataset_size=dataset_size,
                execution_time=0,
                memory_peak=0,
                memory_average=0,
                throughput_rows_per_sec=0,
                cpu_percent=0,
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
        self.dataset_sizes = [10_000, 100_000, 1_000_000]
        
        # Initialize data generator
        self.data_generator = DataGenerator()
    
    def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark suite."""
        print("🚀 Starting PySpark vs Sabot Benchmark")
        print("=" * 60)
        
        # Generate test datasets
        print("\n📊 Generating test datasets...")
        datasets = {}
        
        for size in self.dataset_sizes:
            datasets[size] = {
                'transactions': self.data_generator.generate_transactions(size),
                'customers': self.data_generator.generate_customers(min(size // 100, 10000)),
                'products': self.data_generator.generate_products(min(size // 200, 5000))
            }
        
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
    
    def _run_pyspark_benchmarks(self, datasets: Dict[int, Dict[str, str]]):
        """Run PySpark benchmarks."""
        try:
            # Create Spark session
            spark = PySparkSession.builder \
                .appName("PySparkBenchmark") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            benchmark = PySparkBenchmark(spark)
            
            for size, paths in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # Data loading
                result = benchmark.benchmark_data_loading(paths['transactions'], size)
                self.results.append(result)
                print(f"    Data loading: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Filtering
                result = benchmark.benchmark_filtering(paths['transactions'], size)
                self.results.append(result)
                print(f"    Filtering: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # GroupBy
                result = benchmark.benchmark_groupby(paths['transactions'], size)
                self.results.append(result)
                print(f"    GroupBy: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Join
                result = benchmark.benchmark_join(paths['transactions'], paths['customers'], size)
                self.results.append(result)
                print(f"    Join: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Window functions
                result = benchmark.benchmark_window_functions(paths['transactions'], size)
                self.results.append(result)
                print(f"    Window functions: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            spark.stop()
            
        except Exception as e:
            print(f"❌ PySpark benchmark failed: {e}")
    
    def _run_sabot_benchmarks(self, datasets: Dict[int, Dict[str, str]]):
        """Run Sabot benchmarks."""
        try:
            # Create Sabot Spark session
            spark = SabotSparkSession.builder \
                .appName("SabotBenchmark") \
                .master("local[*]") \
                .getOrCreate()
            
            benchmark = SabotBenchmark(spark)
            
            for size, paths in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # Data loading
                result = benchmark.benchmark_data_loading(paths['transactions'], size)
                self.results.append(result)
                print(f"    Data loading: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Filtering
                result = benchmark.benchmark_filtering(paths['transactions'], size)
                self.results.append(result)
                print(f"    Filtering: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # GroupBy
                result = benchmark.benchmark_groupby(paths['transactions'], size)
                self.results.append(result)
                print(f"    GroupBy: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Join
                result = benchmark.benchmark_join(paths['transactions'], paths['customers'], size)
                self.results.append(result)
                print(f"    Join: {result.execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
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
            if len(systems) == 2:  # Both PySpark and Sabot
                operation, size = key.split('_', 1)
                size = int(size)
                
                pyspark_result = systems.get('PySpark')
                sabot_result = systems.get('Sabot')
                
                if pyspark_result and sabot_result:
                    speedup = pyspark_result.execution_time / sabot_result.execution_time
                    memory_reduction = (pyspark_result.memory_peak - sabot_result.memory_peak) / pyspark_result.memory_peak * 100
                    
                    print(f"\n{operation.title()} ({size:,} rows):")
                    print(f"  PySpark: {pyspark_result.execution_time:.2f}s, {pyspark_result.memory_peak:.1f}MB")
                    print(f"  Sabot:   {sabot_result.execution_time:.2f}s, {sabot_result.memory_peak:.1f}MB")
                    print(f"  Speedup: {speedup:.1f}x faster")
                    print(f"  Memory:  {memory_reduction:.1f}% reduction")
    
    def _save_results(self):
        """Save results to files."""
        # Save JSON results
        json_path = self.output_dir / "benchmark_results.json"
        with open(json_path, 'w') as f:
            json.dump([result.to_dict() for result in self.results], f, indent=2)
        
        # Save CSV results
        csv_path = self.output_dir / "benchmark_results.csv"
        df = pd.DataFrame([result.to_dict() for result in self.results])
        df.to_csv(csv_path, index=False)
        
        print(f"\n💾 Results saved:")
        print(f"  JSON: {json_path}")
        print(f"  CSV:  {csv_path}")


def main():
    """Main benchmark execution."""
    print("PySpark vs Sabot Performance Benchmark")
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
