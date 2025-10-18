#!/usr/bin/env python3
"""
Complex Multi-Step Pipeline Benchmark

This benchmark tests Sabot's performance on realistic, multi-step data processing pipelines
that simulate real-world ETL/ELT workflows with complex transformations, joins, and aggregations.

Pipeline Types:
1. ETL Pipeline - Extract, Transform, Load with multiple steps
2. Analytics Pipeline - Complex aggregations and window functions
3. ML Feature Engineering - Feature creation and transformation
4. Data Quality Pipeline - Validation, cleaning, and enrichment
5. Real-time Processing - Streaming simulation with stateful operations
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
    from sabot import Sabot
    SABOT_AVAILABLE = True
except ImportError:
    SABOT_AVAILABLE = False
    print("Warning: Sabot not available, Sabot benchmarks will be skipped")


@dataclass
class PipelineResult:
    """Container for pipeline benchmark results."""
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


class ComplexPipelineBenchmark:
    """Complex pipeline benchmark implementation."""
    
    def __init__(self, engine: Sabot):
        self.engine = engine
    
    def benchmark_etl_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark ETL pipeline with multiple transformation steps."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load and validate data
            monitor.start_step("data_loading")
            orders_df = pd.read_csv(datasets['orders'])
            customers_df = pd.read_csv(datasets['customers'])
            products_df = pd.read_csv(datasets['products'])
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Data cleaning and validation
            monitor.start_step("data_cleaning")
            # Remove invalid orders
            orders_clean = orders_df[
                (orders_df['quantity'] > 0) & 
                (orders_df['unit_price'] > 0) & 
                (orders_df['order_status'] != 'cancelled')
            ].copy()
            
            # Calculate derived fields
            orders_clean['total_amount'] = orders_clean['quantity'] * orders_clean['unit_price']
            orders_clean['discount_amount'] = orders_clean['total_amount'] * (orders_clean['discount_percent'] / 100)
            orders_clean['final_amount'] = orders_clean['total_amount'] - orders_clean['discount_amount'] + orders_clean['tax_amount']
            monitor.end_step("data_cleaning")
            monitor.sample()
            
            # Step 3: Enrich with customer data
            monitor.start_step("data_enrichment")
            enriched_orders = orders_clean.merge(
                customers_df[['customer_id', 'name', 'city', 'state', 'tier']], 
                on='customer_id', 
                how='left'
            )
            monitor.end_step("data_enrichment")
            monitor.sample()
            
            # Step 4: Enrich with product data
            monitor.start_step("product_enrichment")
            final_orders = enriched_orders.merge(
                products_df[['product_id', 'name', 'category', 'brand']], 
                on='product_id', 
                how='left'
            )
            monitor.end_step("product_enrichment")
            monitor.sample()
            
            # Step 5: Aggregations and analytics
            monitor.start_step("aggregations")
            # Customer analytics
            customer_analytics = final_orders.groupby('customer_id').agg({
                'order_id': 'count',
                'final_amount': ['sum', 'mean', 'max'],
                'quantity': 'sum',
                'order_date': ['min', 'max']
            }).reset_index()
            
            # Product analytics
            product_analytics = final_orders.groupby('product_id').agg({
                'order_id': 'count',
                'final_amount': ['sum', 'mean'],
                'quantity': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            
            # Category analytics
            category_analytics = final_orders.groupby('category').agg({
                'order_id': 'count',
                'final_amount': ['sum', 'mean'],
                'quantity': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            monitor.end_step("aggregations")
            monitor.sample()
            
            # Step 6: Data quality checks
            monitor.start_step("quality_checks")
            # Check for missing values
            missing_customers = final_orders['name'].isna().sum()
            missing_products = final_orders['product_name'].isna().sum()
            
            # Check for data consistency
            negative_amounts = (final_orders['final_amount'] < 0).sum()
            invalid_quantities = (final_orders['quantity'] <= 0).sum()
            monitor.end_step("quality_checks")
            monitor.sample()
            
            # Force evaluation
            total_orders = len(final_orders)
            customer_count = len(customer_analytics)
            product_count = len(product_analytics)
            category_count = len(category_analytics)
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
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
        """Benchmark analytics pipeline with complex aggregations and window functions."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = pd.read_csv(datasets['orders'])
            customers_df = pd.read_csv(datasets['customers'])
            products_df = pd.read_csv(datasets['products'])
            reviews_df = pd.read_csv(datasets['reviews'])
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Time-based aggregations
            monitor.start_step("time_aggregations")
            orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
            orders_df['year'] = orders_df['order_date'].dt.year
            orders_df['month'] = orders_df['order_date'].dt.month
            orders_df['quarter'] = orders_df['order_date'].dt.quarter
            orders_df['day_of_week'] = orders_df['order_date'].dt.dayofweek
            
            # Monthly sales trends
            monthly_sales = orders_df.groupby(['year', 'month']).agg({
                'order_id': 'count',
                'unit_price': ['sum', 'mean', 'std'],
                'quantity': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            
            # Quarterly customer analysis
            quarterly_customers = orders_df.groupby(['year', 'quarter', 'customer_id']).agg({
                'order_id': 'count',
                'unit_price': 'sum',
                'quantity': 'sum'
            }).reset_index()
            monitor.end_step("time_aggregations")
            monitor.sample()
            
            # Step 3: Customer segmentation
            monitor.start_step("customer_segmentation")
            # RFM Analysis (Recency, Frequency, Monetary)
            customer_rfm = orders_df.groupby('customer_id').agg({
                'order_date': 'max',  # Recency
                'order_id': 'count',   # Frequency
                'unit_price': 'sum'    # Monetary
            }).reset_index()
            
            # Calculate RFM scores
            customer_rfm['recency_score'] = pd.qcut(customer_rfm['order_date'].rank(method='first'), 5, labels=[5,4,3,2,1])
            customer_rfm['frequency_score'] = pd.qcut(customer_rfm['order_id'].rank(method='first'), 5, labels=[1,2,3,4,5])
            customer_rfm['monetary_score'] = pd.qcut(customer_rfm['unit_price'].rank(method='first'), 5, labels=[1,2,3,4,5])
            
            # Customer segments
            customer_rfm['rfm_score'] = customer_rfm['recency_score'].astype(str) + customer_rfm['frequency_score'].astype(str) + customer_rfm['monetary_score'].astype(str)
            
            # Define segments
            def get_segment(rfm_score):
                if rfm_score in ['555', '554', '544', '545', '454', '455', '445']:
                    return 'Champions'
                elif rfm_score in ['543', '444', '435', '355', '354', '345', '344', '335']:
                    return 'Loyal Customers'
                elif rfm_score in ['512', '511', '422', '421', '412', '411', '311']:
                    return 'New Customers'
                elif rfm_score in ['155', '154', '144', '214', '215', '115', '114']:
                    return 'At Risk'
                else:
                    return 'Others'
            
            customer_rfm['segment'] = customer_rfm['rfm_score'].apply(get_segment)
            monitor.end_step("customer_segmentation")
            monitor.sample()
            
            # Step 4: Product performance analysis
            monitor.start_step("product_analysis")
            # Product sales performance
            product_performance = orders_df.groupby('product_id').agg({
                'order_id': 'count',
                'unit_price': ['sum', 'mean', 'std'],
                'quantity': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            
            # Product reviews analysis
            review_analysis = reviews_df.groupby('product_id').agg({
                'rating': ['mean', 'std', 'count'],
                'helpful_votes': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            
            # Merge product performance with reviews
            product_analysis = product_performance.merge(review_analysis, on='product_id', how='left')
            monitor.end_step("product_analysis")
            monitor.sample()
            
            # Step 5: Advanced analytics
            monitor.start_step("advanced_analytics")
            # Customer lifetime value
            customer_ltv = orders_df.groupby('customer_id').agg({
                'unit_price': 'sum',
                'order_id': 'count',
                'order_date': ['min', 'max']
            }).reset_index()
            
            customer_ltv['ltv'] = customer_ltv['unit_price']
            customer_ltv['avg_order_value'] = customer_ltv['unit_price'] / customer_ltv['order_id']
            customer_ltv['customer_age_days'] = (customer_ltv['order_date']['max'] - customer_ltv['order_date']['min']).dt.days
            
            # Market basket analysis (simplified)
            basket_analysis = orders_df.groupby('order_id')['product_id'].apply(list).reset_index()
            basket_analysis['basket_size'] = basket_analysis['product_id'].apply(len)
            monitor.end_step("advanced_analytics")
            monitor.sample()
            
            # Force evaluation
            total_orders = len(orders_df)
            monthly_count = len(monthly_sales)
            customer_segments = len(customer_rfm)
            product_analysis_count = len(product_analysis)
            customer_ltv_count = len(customer_ltv)
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_orders / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
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
    
    def benchmark_ml_feature_pipeline(self, datasets: Dict[str, str], dataset_size: int) -> PipelineResult:
        """Benchmark ML feature engineering pipeline."""
        monitor = PerformanceMonitor()
        
        try:
            monitor.start()
            
            # Step 1: Load data
            monitor.start_step("data_loading")
            orders_df = pd.read_csv(datasets['orders'])
            customers_df = pd.read_csv(datasets['customers'])
            products_df = pd.read_csv(datasets['products'])
            monitor.end_step("data_loading")
            monitor.sample()
            
            # Step 2: Feature engineering
            monitor.start_step("feature_engineering")
            # Time-based features
            orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
            orders_df['hour'] = orders_df['order_date'].dt.hour
            orders_df['day_of_week'] = orders_df['order_date'].dt.dayofweek
            orders_df['is_weekend'] = orders_df['day_of_week'].isin([5, 6])
            orders_df['is_business_hours'] = orders_df['hour'].between(9, 17)
            
            # Price features
            orders_df['total_amount'] = orders_df['quantity'] * orders_df['unit_price']
            orders_df['discount_amount'] = orders_df['total_amount'] * (orders_df['discount_percent'] / 100)
            orders_df['final_amount'] = orders_df['total_amount'] - orders_df['discount_amount'] + orders_df['tax_amount']
            orders_df['price_per_unit'] = orders_df['unit_price'] / orders_df['quantity']
            orders_df['is_high_value'] = orders_df['final_amount'] > orders_df['final_amount'].quantile(0.8)
            
            # Customer features
            customer_features = orders_df.groupby('customer_id').agg({
                'order_id': 'count',
                'final_amount': ['sum', 'mean', 'std'],
                'quantity': 'sum',
                'order_date': ['min', 'max']
            }).reset_index()
            
            customer_features['customer_frequency'] = customer_features['order_id']
            customer_features['customer_ltv'] = customer_features['final_amount']['sum']
            customer_features['avg_order_value'] = customer_features['final_amount']['mean']
            customer_features['order_std'] = customer_features['final_amount']['std'].fillna(0)
            customer_features['customer_age_days'] = (customer_features['order_date']['max'] - customer_features['order_date']['min']).dt.days
            
            # Product features
            product_features = orders_df.groupby('product_id').agg({
                'order_id': 'count',
                'final_amount': ['sum', 'mean', 'std'],
                'quantity': 'sum',
                'customer_id': 'nunique'
            }).reset_index()
            
            product_features['product_popularity'] = product_features['order_id']
            product_features['product_revenue'] = product_features['final_amount']['sum']
            product_features['avg_product_value'] = product_features['final_amount']['mean']
            product_features['product_std'] = product_features['final_amount']['std'].fillna(0)
            product_features['unique_customers'] = product_features['customer_id']
            monitor.end_step("feature_engineering")
            monitor.sample()
            
            # Step 3: Advanced features
            monitor.start_step("advanced_features")
            # Rolling window features
            orders_sorted = orders_df.sort_values(['customer_id', 'order_date'])
            orders_sorted['customer_order_rank'] = orders_sorted.groupby('customer_id').cumcount() + 1
            
            # Lag features
            orders_sorted['prev_order_amount'] = orders_sorted.groupby('customer_id')['final_amount'].shift(1)
            orders_sorted['prev_order_quantity'] = orders_sorted.groupby('customer_id')['quantity'].shift(1)
            orders_sorted['days_since_last_order'] = orders_sorted.groupby('customer_id')['order_date'].diff().dt.days
            
            # Rolling aggregations
            orders_sorted['rolling_3_order_avg'] = orders_sorted.groupby('customer_id')['final_amount'].rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)
            orders_sorted['rolling_7_order_sum'] = orders_sorted.groupby('customer_id')['final_amount'].rolling(window=7, min_periods=1).sum().reset_index(0, drop=True)
            
            # Time-based features
            orders_sorted['days_since_registration'] = (orders_sorted['order_date'] - orders_sorted['order_date'].min()).dt.days
            orders_sorted['month_since_registration'] = orders_sorted['days_since_registration'] // 30
            monitor.end_step("advanced_features")
            monitor.sample()
            
            # Step 4: Feature selection and transformation
            monitor.start_step("feature_transformation")
            # Select relevant features for ML
            ml_features = orders_sorted[[
                'customer_id', 'product_id', 'quantity', 'unit_price', 'final_amount',
                'hour', 'day_of_week', 'is_weekend', 'is_business_hours',
                'customer_order_rank', 'prev_order_amount', 'prev_order_quantity',
                'days_since_last_order', 'rolling_3_order_avg', 'rolling_7_order_sum',
                'days_since_registration', 'month_since_registration'
            ]].copy()
            
            # Handle missing values
            ml_features['prev_order_amount'] = ml_features['prev_order_amount'].fillna(0)
            ml_features['prev_order_quantity'] = ml_features['prev_order_quantity'].fillna(0)
            ml_features['days_since_last_order'] = ml_features['days_since_last_order'].fillna(0)
            
            # Feature scaling (simplified)
            numeric_features = ['quantity', 'unit_price', 'final_amount', 'prev_order_amount', 'prev_order_quantity']
            for feature in numeric_features:
                ml_features[f'{feature}_scaled'] = (ml_features[feature] - ml_features[feature].mean()) / ml_features[feature].std()
            
            # Categorical encoding
            ml_features['hour_encoded'] = pd.get_dummies(ml_features['hour'], prefix='hour').sum(axis=1)
            ml_features['day_encoded'] = pd.get_dummies(ml_features['day_of_week'], prefix='day').sum(axis=1)
            monitor.end_step("feature_transformation")
            monitor.sample()
            
            # Step 5: Feature validation
            monitor.start_step("feature_validation")
            # Check for feature quality
            feature_stats = ml_features.describe()
            
            # Check for correlations
            correlation_matrix = ml_features[numeric_features].corr()
            
            # Feature importance (simplified)
            feature_importance = ml_features[numeric_features].var().sort_values(ascending=False)
            monitor.end_step("feature_validation")
            monitor.sample()
            
            # Force evaluation
            total_features = len(ml_features)
            feature_count = len(ml_features.columns)
            correlation_count = len(correlation_matrix)
            
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            throughput = total_features / execution_time if execution_time > 0 else 0
            
            return PipelineResult(
                pipeline_name="ML Feature Pipeline",
                dataset_size=dataset_size,
                total_execution_time=execution_time,
                step_times=step_times,
                memory_peak=peak_memory,
                memory_average=avg_memory,
                throughput_rows_per_sec=throughput,
                cpu_percent=avg_cpu,
                success=True,
                intermediate_results={
                    'total_features': total_features,
                    'feature_count': feature_count,
                    'correlation_count': correlation_count,
                    'customer_features': len(customer_features),
                    'product_features': len(product_features)
                }
            )
            
        except Exception as e:
            execution_time, peak_memory, avg_memory, avg_cpu, step_times = monitor.stop()
            return PipelineResult(
                pipeline_name="ML Feature Pipeline",
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
        self.dataset_sizes = [10_000, 100_000, 500_000]
        
        # Initialize data generator
        self.data_generator = DataGenerator()
    
    def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark suite."""
        print("🚀 Starting Complex Multi-Step Pipeline Benchmark")
        print("=" * 70)
        
        # Generate test datasets
        print("\n📊 Generating test datasets...")
        datasets = {}
        
        for size in self.dataset_sizes:
            datasets[size] = self.data_generator.generate_ecommerce_data(size)
        
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
    
    def _run_sabot_benchmarks(self, datasets: Dict[int, Dict[str, str]]):
        """Run Sabot benchmarks."""
        try:
            # Create Sabot engine
            engine = Sabot(mode='local')
            
            benchmark = ComplexPipelineBenchmark(engine)
            
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
                
                # ML Feature Pipeline
                result = benchmark.benchmark_ml_feature_pipeline(paths, size)
                self.results.append(result)
                print(f"    ML Feature Pipeline: {result.total_execution_time:.2f}s ({result.throughput_rows_per_sec:,.0f} rows/sec)")
            
            engine.shutdown()
            
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
                by_pipeline[key][result.pipeline_name] = result
        
        # Show performance metrics
        for key, pipelines in by_pipeline.items():
            parts = key.split('_')
            pipeline_name = '_'.join(parts[:-1])
            size = int(parts[-1])
            
            pipeline_result = pipelines.get(pipeline_name)
            
            if pipeline_result:
                print(f"\n{pipeline_name} ({size:,} rows):")
                print(f"  Total Time: {pipeline_result.total_execution_time:.2f}s")
                print(f"  Throughput: {pipeline_result.throughput_rows_per_sec:,.0f} rows/sec")
                print(f"  Memory Peak: {pipeline_result.memory_peak:.1f}MB")
                print(f"  Step Times:")
                for step, time_taken in pipeline_result.step_times.items():
                    print(f"    {step}: {time_taken:.2f}s")
                if pipeline_result.intermediate_results:
                    print(f"  Results: {pipeline_result.intermediate_results}")
    
    def _save_results(self):
        """Save results to files."""
        # Save JSON results
        json_path = self.output_dir / "complex_pipeline_results.json"
        with open(json_path, 'w') as f:
            json.dump([result.to_dict() for result in self.results], f, indent=2)
        
        # Save CSV results
        csv_path = self.output_dir / "complex_pipeline_results.csv"
        df = pd.DataFrame([result.to_dict() for result in self.results])
        df.to_csv(csv_path, index=False)
        
        print(f"\n💾 Results saved:")
        print(f"  JSON: {json_path}")
        print(f"  CSV:  {csv_path}")


def main():
    """Main benchmark execution."""
    print("Complex Multi-Step Pipeline Benchmark")
    print("=" * 70)
    
    # Check availability
    print(f"Sabot available: {'✅' if SABOT_AVAILABLE else '❌'}")
    print(f"PyArrow available: {'✅' if ARROW_AVAILABLE else '❌'}")
    
    if not SABOT_AVAILABLE:
        print("❌ Sabot is not available. Cannot run benchmark.")
        return
    
    # Run benchmark
    runner = BenchmarkRunner()
    runner.run_comprehensive_benchmark()


if __name__ == "__main__":
    main()
