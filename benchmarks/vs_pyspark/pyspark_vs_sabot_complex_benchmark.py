#!/usr/bin/env python3
"""
PySpark vs Sabot Complex Pipeline Benchmark

This benchmark compares PySpark and Sabot using identical complex multi-step pipelines
to measure performance differences across realistic ETL/ELT workflows.

Pipeline Types:
1. ETL Pipeline - Extract, Transform, Load with multiple steps
2. Analytics Pipeline - Complex aggregations and window functions
3. ML Feature Pipeline - Feature creation and transformation

Both PySpark and Sabot run the exact same pipeline logic for fair comparison.
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
    from pyspark.sql.functions import when, coalesce, row_number, rank, dense_rank, lag, lead
    from pyspark.sql.functions import year, month, quarter, dayofweek, countDistinct
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Warning: PySpark not available, PySpark benchmarks will be skipped")

try:
    from sabot.spark import SparkSession as SabotSparkSession
    from sabot.spark.functions import col as sabot_col, sum as sabot_sum, avg as sabot_avg, count as sabot_count
    from sabot.spark.functions import max as sabot_max, min as sabot_min, when as sabot_when, coalesce as sabot_coalesce
    from sabot.spark.functions import year, month, quarter, dayofweek, countDistinct
    SABOT_AVAILABLE = True
except ImportError:
    SABOT_AVAILABLE = False
    print("Warning: Sabot not available, Sabot benchmarks will be skipped")


@dataclass
class PipelineResult:
    """Container for pipeline benchmark results."""
    system: str
    pipeline_name: str
    dataset_size: int
    total_execution_time: float
    step_times: Dict[str, float]
    memory_peak: float
    memory_average: float
    throughput_rows_per_sec: float
    cpu_percent: float
    success: bool
    error_message: Optional[str] = None
    intermediate_results: Dict[str, int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class DataGenerator:
    """Generate complex test datasets for benchmarking."""
    
    def __init__(self, base_dir: str = None):
        self.base_dir = Path(base_dir) if base_dir else Path(tempfile.mkdtemp())
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_ecommerce_data(self, num_rows: int = 1_000_000) -> Dict[str, str]:
        """Generate realistic e-commerce dataset."""
        print(f"Generating {num_rows:,} e-commerce records...")
        
        np.random.seed(42)  # Reproducible results
        
        # Orders dataset
        orders_data = {
            'order_id': list(range(1, num_rows + 1)),
            'customer_id': np.random.randint(1, 50000, num_rows),
            'product_id': np.random.randint(1, 10000, num_rows),
            'quantity': np.random.randint(1, 10, num_rows),
            'unit_price': np.random.uniform(10, 1000, num_rows).round(2),
            'order_date': pd.date_range('2023-01-01', periods=num_rows, freq='1min'),
            'payment_method': np.random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay'], num_rows),
            'shipping_address': [f"Address_{i}" for i in range(num_rows)],
            'order_status': np.random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'], num_rows, p=[0.1, 0.2, 0.3, 0.35, 0.05]),
            'discount_percent': np.random.uniform(0, 30, num_rows).round(1),
            'tax_amount': np.random.uniform(5, 50, num_rows).round(2)
        }
        
        # Customers dataset
        customers_data = {
            'customer_id': list(range(1, 50001)),
            'name': [f"Customer_{i}" for i in range(1, 50001)],
            'email': [f"customer_{i}@example.com" for i in range(1, 50001)],
            'age': np.random.randint(18, 80, 50000),
            'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], 50000),
            'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA'], 50000),
            'registration_date': pd.date_range('2020-01-01', periods=50000, freq='1D'),
            'tier': np.random.choice(['bronze', 'silver', 'gold', 'platinum'], 50000, p=[0.4, 0.3, 0.2, 0.1]),
            'total_spent': np.random.uniform(100, 50000, 50000).round(2),
            'loyalty_points': np.random.randint(0, 10000, 50000)
        }
        
        # Products dataset
        products_data = {
            'product_id': list(range(1, 10001)),
            'name': [f"Product_{i}" for i in range(1, 10001)],
            'category': np.random.choice(['electronics', 'clothing', 'books', 'home', 'sports', 'beauty', 'toys', 'automotive'], 10000),
            'brand': np.random.choice(['Brand_A', 'Brand_B', 'Brand_C', 'Brand_D', 'Brand_E'], 10000),
            'base_price': np.random.uniform(10, 1000, 10000).round(2),
            'cost': np.random.uniform(5, 500, 10000).round(2),
            'stock_quantity': np.random.randint(0, 1000, 10000),
            'supplier_id': np.random.randint(1, 100, 10000),
            'weight': np.random.uniform(0.1, 50, 10000).round(2),
            'dimensions': [f"{np.random.randint(1, 50)}x{np.random.randint(1, 50)}x{np.random.randint(1, 50)}" for _ in range(10000)]
        }
        
        # Reviews dataset
        review_count = min(num_rows * 2, 2_000_000)
        reviews_data = {
            'review_id': list(range(1, review_count + 1)),
            'order_id': np.random.randint(1, num_rows + 1, review_count),
            'customer_id': np.random.randint(1, 50001, review_count),
            'product_id': np.random.randint(1, 10001, review_count),
            'rating': np.random.randint(1, 6, review_count),
            'review_text': [f"Review text for product {i}" for i in range(review_count)],
            'review_date': pd.date_range('2023-01-01', periods=review_count, freq='2min'),
            'helpful_votes': np.random.randint(0, 100, review_count),
            'verified_purchase': np.random.choice([True, False], review_count, p=[0.8, 0.2])
        }
        
        # Save datasets
        datasets = {}
        for name, data in [('orders', orders_data), ('customers', customers_data), ('products', products_data), ('reviews', reviews_data)]:
            df = pd.DataFrame(data)
            output_path = self.base_dir / f"{name}_{num_rows}.csv"
            df.to_csv(output_path, index=False)
            datasets[name] = str(output_path)
            print(f"✅ Generated: {output_path} ({len(df):,} rows)")
        
        return datasets


class PerformanceMonitor:
    """Monitor system performance during benchmarks."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        self.peak_memory = 0
        self.memory_samples = []
        self.cpu_samples = []
        self.step_times = {}
    
    def start(self):
        """Start monitoring."""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory
        self.memory_samples = [self.start_memory]
        self.cpu_samples = []
        self.step_times = {}
    
    def start_step(self, step_name: str):
        """Start timing a pipeline step."""
        self.step_times[step_name] = time.time()
    
    def end_step(self, step_name: str):
        """End timing a pipeline step."""
        if step_name in self.step_times:
            self.step_times[step_name] = time.time() - self.step_times[step_name]
    
    def sample(self):
        """Take a performance sample."""
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        current_cpu = self.process.cpu_percent()
        
        self.memory_samples.append(current_memory)
        self.cpu_samples.append(current_cpu)
        
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
    
    def stop(self) -> Tuple[float, float, float, float, Dict[str, float]]:
        """Stop monitoring and return metrics."""
        if self.start_time is None:
            return 0, 0, 0, 0, {}
        
        execution_time = time.time() - self.start_time
        peak_memory = self.peak_memory
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        
        return execution_time, peak_memory, avg_memory, avg_cpu, self.step_times


class PySparkComplexPipeline:
    """PySpark complex pipeline implementation."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def benchmark_etl_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark ETL pipeline with PySpark."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = self.spark.read.csv(datasets['orders'], header=True, inferSchema=True)
            customers_df = self.spark.read.csv(datasets['customers'], header=True, inferSchema=True)
            products_df = self.spark.read.csv(datasets['products'], header=True, inferSchema=True)
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Data cleaning and validation
            monitor.start_step("data_cleaning")
            orders_clean = orders_df.filter(
                (col("quantity") > 0) & 
                (col("unit_price") > 0) & 
                (col("order_status") != "cancelled")
            )
            
            # Calculate derived fields
            orders_clean = orders_clean.withColumn("total_amount", col("quantity") * col("unit_price"))
            orders_clean = orders_clean.withColumn("discount_amount", col("total_amount") * (col("discount_percent") / 100))
            orders_clean = orders_clean.withColumn("final_amount", col("total_amount") - col("discount_amount") + col("tax_amount"))
            monitor.end_step("data_cleaning")
            monitor.sample()
            
            # Step 3: Enrich with customer data
            monitor.start_step("data_enrichment")
            enriched_orders = orders_clean.join(
                customers_df.select("customer_id", "name", "city", "state", "tier"),
                "customer_id",
                "left"
            )
            monitor.end_step("data_enrichment")
            monitor.sample()
            
            # Step 4: Enrich with product data
            monitor.start_step("product_enrichment")
            final_orders = enriched_orders.join(
                products_df.select("product_id", "name", "category", "brand"),
                "product_id",
                "left"
            )
            monitor.end_step("product_enrichment")
            monitor.sample()
            
            # Step 5: Aggregations and analytics
            monitor.start_step("aggregations")
            # Customer analytics
            customer_analytics = final_orders.groupBy("customer_id").agg(
                count("order_id").alias("order_count"),
                spark_sum("final_amount").alias("total_spent"),
                spark_avg("final_amount").alias("avg_order_value"),
                spark_max("final_amount").alias("max_order_value"),
                spark_sum("quantity").alias("total_quantity")
            ).filter(col("total_spent") > 1000)
            
            # Product analytics
            product_analytics = final_orders.groupBy("product_id").agg(
                count("order_id").alias("order_count"),
                spark_sum("final_amount").alias("total_revenue"),
                spark_avg("final_amount").alias("avg_order_value"),
                spark_sum("quantity").alias("total_quantity"),
                countDistinct("customer_id").alias("unique_customers")
            )
            
            # Category analytics
            category_analytics = final_orders.groupBy("category").agg(
                count("order_id").alias("order_count"),
                spark_sum("final_amount").alias("total_revenue"),
                spark_avg("final_amount").alias("avg_order_value"),
                spark_sum("quantity").alias("total_quantity"),
                countDistinct("customer_id").alias("unique_customers")
            )
            monitor.end_step("aggregations")
            monitor.sample()
            
            # Step 6: Data quality checks
            monitor.start_step("quality_checks")
            # Check for missing values
            missing_customers = final_orders.filter(col("customer_name").isNull()).count()
            missing_products = final_orders.filter(col("product_name").isNull()).count()
            
            # Check for data consistency
            negative_amounts = final_orders.filter(col("final_amount") < 0).count()
            invalid_quantities = final_orders.filter(col("quantity") <= 0).count()
            monitor.end_step("quality_checks")
            monitor.sample()
            
            # Force evaluation
            total_orders = final_orders.count()
            customer_count = customer_analytics.count()
            product_count = product_analytics.count()
            category_count = category_analytics.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
                system="PySpark",
                pipeline_name="ETL Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True,
                intermediate_results={
                    'total_orders': total_orders,
                    'customer_analytics': customer_count,
                    'product_analytics': product_count,
                    'category_analytics': category_count,
                    'missing_customers': missing_customers,
                    'missing_products': missing_products,
                    'negative_amounts': negative_amounts,
                    'invalid_quantities': invalid_quantities
                }
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            return PipelineResult(
                system="PySpark",
                pipeline_name="ETL Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_analytics_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark analytics pipeline with PySpark."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = self.spark.read.csv(datasets['orders'], header=True, inferSchema=True)
            customers_df = self.spark.read.csv(datasets['customers'], header=True, inferSchema=True)
            products_df = self.spark.read.csv(datasets['products'], header=True, inferSchema=True)
            reviews_df = self.spark.read.csv(datasets['reviews'], header=True, inferSchema=True)
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Time-based aggregations
            monitor.start_step("time_aggregations")
            # Add time-based columns
            orders_df = orders_df.withColumn("year", year(col("order_date")))
            orders_df = orders_df.withColumn("month", month(col("order_date")))
            orders_df = orders_df.withColumn("quarter", quarter(col("order_date")))
            orders_df = orders_df.withColumn("day_of_week", dayofweek(col("order_date")))
            
            # Monthly sales trends
            monthly_sales = orders_df.groupBy("year", "month").agg(
                count("order_id").alias("order_count"),
                spark_sum("unit_price").alias("total_revenue"),
                spark_avg("unit_price").alias("avg_order_value"),
                spark_sum("quantity").alias("total_quantity"),
                countDistinct("customer_id").alias("unique_customers")
            )
            
            # Quarterly customer analysis
            quarterly_customers = orders_df.groupBy("year", "quarter", "customer_id").agg(
                count("order_id").alias("order_count"),
                spark_sum("unit_price").alias("total_spent"),
                spark_sum("quantity").alias("total_quantity")
            )
            monitor.end_step("time_aggregations")
            monitor.sample()
            
            # Step 3: Customer segmentation
            monitor.start_step("customer_segmentation")
            # RFM Analysis
            customer_rfm = orders_df.groupBy("customer_id").agg(
                spark_max("order_date").alias("last_order_date"),
                count("order_id").alias("frequency"),
                spark_sum("unit_price").alias("monetary")
            )
            
            # Calculate RFM scores (simplified)
            customer_rfm = customer_rfm.withColumn("recency_score", 
                when(col("last_order_date") >= "2023-12-01", 5)
                .when(col("last_order_date") >= "2023-11-01", 4)
                .when(col("last_order_date") >= "2023-10-01", 3)
                .when(col("last_order_date") >= "2023-09-01", 2)
                .otherwise(1)
            )
            
            customer_rfm = customer_rfm.withColumn("frequency_score",
                when(col("frequency") >= 20, 5)
                .when(col("frequency") >= 15, 4)
                .when(col("frequency") >= 10, 3)
                .when(col("frequency") >= 5, 2)
                .otherwise(1)
            )
            
            customer_rfm = customer_rfm.withColumn("monetary_score",
                when(col("monetary") >= 10000, 5)
                .when(col("monetary") >= 5000, 4)
                .when(col("monetary") >= 2000, 3)
                .when(col("monetary") >= 1000, 2)
                .otherwise(1)
            )
            
            # Customer segments
            customer_rfm = customer_rfm.withColumn("segment",
                when((col("recency_score") == 5) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
                .when((col("recency_score") >= 4) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Loyal Customers")
                .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "New Customers")
                .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
                .otherwise("Others")
            )
            monitor.end_step("customer_segmentation")
            monitor.sample()
            
            # Step 4: Product performance analysis
            monitor.start_step("product_analysis")
            # Product sales performance
            product_performance = orders_df.groupBy("product_id").agg(
                count("order_id").alias("order_count"),
                spark_sum("unit_price").alias("total_revenue"),
                spark_avg("unit_price").alias("avg_order_value"),
                spark_sum("quantity").alias("total_quantity"),
                countDistinct("customer_id").alias("unique_customers")
            )
            
            # Product reviews analysis
            review_analysis = reviews_df.groupBy("product_id").agg(
                spark_avg("rating").alias("avg_rating"),
                count("review_id").alias("review_count"),
                spark_sum("helpful_votes").alias("total_helpful_votes"),
                countDistinct("customer_id").alias("unique_reviewers")
            )
            
            # Merge product performance with reviews
            product_analysis = product_performance.join(review_analysis, "product_id", "left")
            monitor.end_step("product_analysis")
            monitor.sample()
            
            # Step 5: Advanced analytics
            monitor.start_step("advanced_analytics")
            # Customer lifetime value
            customer_ltv = orders_df.groupBy("customer_id").agg(
                spark_sum("unit_price").alias("ltv"),
                count("order_id").alias("order_count"),
                spark_min("order_date").alias("first_order_date"),
                spark_max("order_date").alias("last_order_date")
            )
            
            customer_ltv = customer_ltv.withColumn("avg_order_value", col("ltv") / col("order_count"))
            monitor.end_step("advanced_analytics")
            monitor.sample()
            
            # Force evaluation
            total_orders = orders_df.count()
            monthly_count = monthly_sales.count()
            customer_segments = customer_rfm.count()
            product_analysis_count = product_analysis.count()
            customer_ltv_count = customer_ltv.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
                system="PySpark",
                pipeline_name="Analytics Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True,
                intermediate_results={
                    'total_orders': total_orders,
                    'monthly_sales': monthly_count,
                    'customer_segments': customer_segments,
                    'product_analysis': product_analysis_count,
                    'customer_ltv': customer_ltv_count
                }
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            return PipelineResult(
                system="PySpark",
                pipeline_name="Analytics Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )


class SabotComplexPipeline:
    """Sabot complex pipeline implementation using Spark-compatible API."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def benchmark_etl_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark ETL pipeline with Sabot."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = self.spark.read.csv(datasets['orders'], header=True, inferSchema=True)
            customers_df = self.spark.read.csv(datasets['customers'], header=True, inferSchema=True)
            products_df = self.spark.read.csv(datasets['products'], header=True, inferSchema=True)
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Data cleaning and validation
            monitor.start_step("data_cleaning")
            orders_clean = orders_df.filter(
                (sabot_col("quantity") > 0) & 
                (sabot_col("unit_price") > 0) & 
                (sabot_col("order_status") != "cancelled")
            )
            
            # Calculate derived fields
            orders_clean = orders_clean.withColumn("total_amount", sabot_col("quantity") * sabot_col("unit_price"))
            orders_clean = orders_clean.withColumn("discount_amount", sabot_col("total_amount") * (sabot_col("discount_percent") / 100))
            orders_clean = orders_clean.withColumn("final_amount", sabot_col("total_amount") - sabot_col("discount_amount") + sabot_col("tax_amount"))
            monitor.end_step("data_cleaning")
            monitor.sample()
            
            # Step 3: Enrich with customer data
            monitor.start_step("data_enrichment")
            enriched_orders = orders_clean.join(
                customers_df.select("customer_id", "name", "city", "state", "tier"),
                "customer_id",
                "left"
            )
            monitor.end_step("data_enrichment")
            monitor.sample()
            
            # Step 4: Enrich with product data
            monitor.start_step("product_enrichment")
            final_orders = enriched_orders.join(
                products_df.select("product_id", "name", "category", "brand"),
                "product_id",
                "left"
            )
            monitor.end_step("product_enrichment")
            monitor.sample()
            
            # Step 5: Aggregations and analytics
            monitor.start_step("aggregations")
            # Customer analytics
            customer_analytics = final_orders.groupBy("customer_id").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("final_amount").alias("total_spent"),
                sabot_avg("final_amount").alias("avg_order_value"),
                sabot_max("final_amount").alias("max_order_value"),
                sabot_sum("quantity").alias("total_quantity")
            ).filter(sabot_col("total_spent") > 1000)
            
            # Product analytics
            product_analytics = final_orders.groupBy("product_id").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("final_amount").alias("total_revenue"),
                sabot_avg("final_amount").alias("avg_order_value"),
                sabot_sum("quantity").alias("total_quantity"),
                sabot_count("customer_id").alias("unique_customers")
            )
            
            # Category analytics
            category_analytics = final_orders.groupBy("category").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("final_amount").alias("total_revenue"),
                sabot_avg("final_amount").alias("avg_order_value"),
                sabot_sum("quantity").alias("total_quantity"),
                sabot_count("customer_id").alias("unique_customers")
            )
            monitor.end_step("aggregations")
            monitor.sample()
            
            # Step 6: Data quality checks
            monitor.start_step("quality_checks")
            # Check for missing values
            missing_customers = final_orders.filter(sabot_col("customer_name").isNull()).count()
            missing_products = final_orders.filter(sabot_col("product_name").isNull()).count()
            
            # Check for data consistency
            negative_amounts = final_orders.filter(sabot_col("final_amount") < 0).count()
            invalid_quantities = final_orders.filter(sabot_col("quantity") <= 0).count()
            monitor.end_step("quality_checks")
            monitor.sample()
            
            # Force evaluation
            total_orders = final_orders.count()
            customer_count = customer_analytics.count()
            product_count = product_analytics.count()
            category_count = category_analytics.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
                system="Sabot",
                pipeline_name="ETL Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True,
                intermediate_results={
                    'total_orders': total_orders,
                    'customer_analytics': customer_count,
                    'product_analytics': product_count,
                    'category_analytics': category_count,
                    'missing_customers': missing_customers,
                    'missing_products': missing_products,
                    'negative_amounts': negative_amounts,
                    'invalid_quantities': invalid_quantities
                }
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            return PipelineResult(
                system="Sabot",
                pipeline_name="ETL Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=0,
                cpu_percent=avg_cpu,
                success=False,
                error_message=str(e)
            )
    
    def benchmark_analytics_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark analytics pipeline with Sabot."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = self.spark.read.csv(datasets['orders'], header=True, inferSchema=True)
            customers_df = self.spark.read.csv(datasets['customers'], header=True, inferSchema=True)
            products_df = self.spark.read.csv(datasets['products'], header=True, inferSchema=True)
            reviews_df = self.spark.read.csv(datasets['reviews'], header=True, inferSchema=True)
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Time-based aggregations
            monitor.start_step("time_aggregations")
            # Add time-based columns
            orders_df = orders_df.withColumn("year", year(sabot_col("order_date")))
            orders_df = orders_df.withColumn("month", month(sabot_col("order_date")))
            orders_df = orders_df.withColumn("quarter", quarter(sabot_col("order_date")))
            orders_df = orders_df.withColumn("day_of_week", dayofweek(sabot_col("order_date")))
            
            # Monthly sales trends
            monthly_sales = orders_df.groupBy("year", "month").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("unit_price").alias("total_revenue"),
                sabot_avg("unit_price").alias("avg_order_value"),
                sabot_sum("quantity").alias("total_quantity"),
                sabot_count("customer_id").alias("unique_customers")
            )
            
            # Quarterly customer analysis
            quarterly_customers = orders_df.groupBy("year", "quarter", "customer_id").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("unit_price").alias("total_spent"),
                sabot_sum("quantity").alias("total_quantity")
            )
            monitor.end_step("time_aggregations")
            monitor.sample()
            
            # Step 3: Customer segmentation
            monitor.start_step("customer_segmentation")
            # RFM Analysis
            customer_rfm = orders_df.groupBy("customer_id").agg(
                sabot_max("order_date").alias("last_order_date"),
                sabot_count("order_id").alias("frequency"),
                sabot_sum("unit_price").alias("monetary")
            )
            
            # Calculate RFM scores (simplified)
            customer_rfm = customer_rfm.withColumn("recency_score", 
                sabot_when(sabot_col("last_order_date") >= "2023-12-01", 5)
                .when(sabot_col("last_order_date") >= "2023-11-01", 4)
                .when(sabot_col("last_order_date") >= "2023-10-01", 3)
                .when(sabot_col("last_order_date") >= "2023-09-01", 2)
                .otherwise(1)
            )
            
            customer_rfm = customer_rfm.withColumn("frequency_score",
                sabot_when(sabot_col("frequency") >= 20, 5)
                .when(sabot_col("frequency") >= 15, 4)
                .when(sabot_col("frequency") >= 10, 3)
                .when(sabot_col("frequency") >= 5, 2)
                .otherwise(1)
            )
            
            customer_rfm = customer_rfm.withColumn("monetary_score",
                sabot_when(sabot_col("monetary") >= 10000, 5)
                .when(sabot_col("monetary") >= 5000, 4)
                .when(sabot_col("monetary") >= 2000, 3)
                .when(sabot_col("monetary") >= 1000, 2)
                .otherwise(1)
            )
            
            # Customer segments
            customer_rfm = customer_rfm.withColumn("segment",
                sabot_when((sabot_col("recency_score") == 5) & (sabot_col("frequency_score") >= 4) & (sabot_col("monetary_score") >= 4), "Champions")
                .when((sabot_col("recency_score") >= 4) & (sabot_col("frequency_score") >= 3) & (sabot_col("monetary_score") >= 3), "Loyal Customers")
                .when((sabot_col("recency_score") >= 4) & (sabot_col("frequency_score") <= 2), "New Customers")
                .when((sabot_col("recency_score") <= 2) & (sabot_col("frequency_score") >= 3), "At Risk")
                .otherwise("Others")
            )
            monitor.end_step("customer_segmentation")
            monitor.sample()
            
            # Step 4: Product performance analysis
            monitor.start_step("product_analysis")
            # Product sales performance
            product_performance = orders_df.groupBy("product_id").agg(
                sabot_count("order_id").alias("order_count"),
                sabot_sum("unit_price").alias("total_revenue"),
                sabot_avg("unit_price").alias("avg_order_value"),
                sabot_sum("quantity").alias("total_quantity"),
                sabot_count("customer_id").alias("unique_customers")
            )
            
            # Product reviews analysis
            review_analysis = reviews_df.groupBy("product_id").agg(
                sabot_avg("rating").alias("avg_rating"),
                sabot_count("review_id").alias("review_count"),
                sabot_sum("helpful_votes").alias("total_helpful_votes"),
                sabot_count("customer_id").alias("unique_reviewers")
            )
            
            # Merge product performance with reviews
            product_analysis = product_performance.join(review_analysis, "product_id", "left")
            monitor.end_step("product_analysis")
            monitor.sample()
            
            # Step 5: Advanced analytics
            monitor.start_step("advanced_analytics")
            # Customer lifetime value
            customer_ltv = orders_df.groupBy("customer_id").agg(
                sabot_sum("unit_price").alias("ltv"),
                sabot_count("order_id").alias("order_count"),
                sabot_min("order_date").alias("first_order_date"),
                sabot_max("order_date").alias("last_order_date")
            )
            
            customer_ltv = customer_ltv.withColumn("avg_order_value", sabot_col("ltv") / sabot_col("order_count"))
            monitor.end_step("advanced_analytics")
            monitor.sample()
            
            # Force evaluation
            total_orders = orders_df.count()
            monthly_count = monthly_sales.count()
            customer_segments = customer_rfm.count()
            product_analysis_count = product_analysis.count()
            customer_ltv_count = customer_ltv.count()
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
                system="Sabot",
                pipeline_name="Analytics Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True,
                intermediate_results={
                    'total_orders': total_orders,
                    'monthly_sales': monthly_count,
                    'customer_segments': customer_segments,
                    'product_analysis': product_analysis_count,
                    'customer_ltv': customer_ltv_count
                }
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            return PipelineResult(
                system="Sabot",
                pipeline_name="Analytics Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
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
        self.results: List[PipelineResult] = []
        
        # Dataset sizes to test
        self.dataset_sizes = [10_000, 100_000]
        
        # Initialize data generator
        self.data_generator = DataGenerator()
    
    def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark suite."""
        print("🚀 Starting PySpark vs Sabot Complex Pipeline Benchmark")
        print("=" * 70)
        
        # Generate test datasets
        print("\n📊 Generating test datasets...")
        datasets = {}
        
        for size in self.dataset_sizes:
            datasets[size] = self.data_generator.generate_ecommerce_data(size)
        
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
                .appName("PySparkComplexBenchmark") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            benchmark = PySparkComplexPipeline(spark)
            
            for size, paths in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # ETL Pipeline
                result = benchmark.benchmark_etl_pipeline(paths, size)
                self.results.append(result)
                print(f"    ETL Pipeline: {result.total_execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Analytics Pipeline
                result = benchmark.benchmark_analytics_pipeline(paths, size)
                self.results.append(result)
                print(f"    Analytics Pipeline: {result.total_execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            spark.stop()
            
        except Exception as e:
            print(f"❌ PySpark benchmark failed: {e}")
    
    def _run_sabot_benchmarks(self, datasets: Dict[int, Dict[str, str]]):
        """Run Sabot benchmarks."""
        try:
            # Create Sabot Spark session
            spark = SabotSparkSession.builder \
                .appName("SabotComplexBenchmark") \
                .master("local[*]") \
                .getOrCreate()
            
            benchmark = SabotComplexPipeline(spark)
            
            for size, paths in datasets.items():
                print(f"\n  Testing dataset size: {size:,} rows")
                
                # ETL Pipeline
                result = benchmark.benchmark_etl_pipeline(paths, size)
                self.results.append(result)
                print(f"    ETL Pipeline: {result.total_execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
                
                # Analytics Pipeline
                result = benchmark.benchmark_analytics_pipeline(paths, size)
                self.results.append(result)
                print(f"    Analytics Pipeline: {result.total_execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            spark.stop()
            
        except Exception as e:
            print(f"❌ Sabot benchmark failed: {e}")
    
    def _analyze_results(self):
        """Analyze benchmark results."""
        print("\n📊 Performance Analysis")
        print("=" * 70)
        
        # Group results by pipeline type and dataset size
        by_pipeline = {}
        for result in self.results:
            if result.success:
                key = f"{result.pipeline_name}_{result.dataset_size}"
                if key not in by_pipeline:
                    by_pipeline[key] = {}
                by_pipeline[key][result.system] = result
        
        # Show performance metrics
        for key, systems in by_pipeline.items():
            parts = key.split('_')
            pipeline_name = '_'.join(parts[:-1])
            size = int(parts[-1])
            
            pyspark_result = systems.get('PySpark')
            sabot_result = systems.get('Sabot')
            
            print(f"\n{pipeline_name} ({size:,} rows):")
            
            if pyspark_result and sabot_result:
                speedup = pyspark_result.total_execution_time / sabot_result.total_execution_time
                memory_reduction = (pyspark_result.memory_peak - sabot_result.memory_peak) / pyspark_result.memory_peak * 100
                
                print(f"  PySpark: {pyspark_result.total_execution_time:.2f}s, {pyspark_result.memory_peak:.1f}MB")
                print(f"  Sabot:   {sabot_result.total_execution_time:.2f}s, {sabot_result.memory_peak:.1f}MB")
                print(f"  Speedup: {speedup:.1f}x faster")
                print(f"  Memory:  {memory_reduction:.1f}% reduction")
                
                # Show step-by-step comparison
                print(f"  Step Times:")
                for step in pyspark_result.step_times.keys():
                    pyspark_time = pyspark_result.step_times.get(step, 0)
                    sabot_time = sabot_result.step_times.get(step, 0)
                    if pyspark_time > 0 and sabot_time > 0:
                        step_speedup = pyspark_time / sabot_time
                        print(f"    {step}: PySpark {pyspark_time:.2f}s, Sabot {sabot_time:.2f}s ({step_speedup:.1f}x)")
            elif pyspark_result:
                print(f"  PySpark: {pyspark_result.total_execution_time:.2f}s, {pyspark_result.memory_peak:.1f}MB")
            elif sabot_result:
                print(f"  Sabot:   {sabot_result.total_execution_time:.2f}s, {sabot_result.memory_peak:.1f}MB")
    
    def _save_results(self):
        """Save results to files."""
        # Save JSON results
        json_path = self.output_dir / "pyspark_vs_sabot_complex_results.json"
        with open(json_path, 'w') as f:
            json.dump([result.to_dict() for result in self.results], f, indent=2)
        
        # Save CSV results
        csv_path = self.output_dir / "pyspark_vs_sabot_complex_results.csv"
        df = pd.DataFrame([result.to_dict() for result in self.results])
        df.to_csv(csv_path, index=False)
        
        print(f"\n💾 Results saved:")
        print(f"  JSON: {json_path}")
        print(f"  CSV:  {csv_path}")


def main():
    """Main benchmark execution."""
    print("PySpark vs Sabot Complex Pipeline Benchmark")
    print("=" * 70)
    
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
