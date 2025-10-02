#!/usr/bin/env python3
"""
Configuration for Sabot Arrow Flight Operator Pipeline

Performance-tunable parameters for morsel-driven operator chains.
Based on Sabot's morsel parallelism and Arrow Flight IPC.
"""

import os

# ============================================================================
# Morsel Configuration (Cache-Friendly Chunking)
# ============================================================================

# Morsel size: Should fit in L2/L3 cache for optimal performance
# L2 cache typical: 256KB-1MB, L3 cache: 8-32MB
# Default: 64KB (fits comfortably in L2)
MORSEL_SIZE_BYTES = 65536  # 64KB

# Rows per morsel (estimated for financial data)
# Adjust based on record size - smaller records = more rows per morsel
MORSEL_ROWS_ESTIMATE = 1000  # ~64 bytes per row

# ============================================================================
# Parallelism Configuration
# ============================================================================

# Number of parallel workers per operator
# Default: CPU count (use all cores)
NUM_WORKERS = os.cpu_count() or 8

# NUMA-aware processing (if available)
NUMA_AWARE = True

# Work stealing threshold (when to steal from other workers)
WORK_STEALING_THRESHOLD = 4

# ============================================================================
# Arrow Flight Configuration
# ============================================================================

# Flight server ports for each operator stage
FLIGHT_PORTS = {
    'csv_source': 8815,
    'windowing': 8816,
    'filtering': 8817,
    'ranking': 8818,
    'enrichment': 8819,
    'spread_calculation': 8820,
    'aggregation': 8821,
}

# Flight server host
FLIGHT_HOST = 'localhost'

# Flight message size limit (Arrow IPC)
FLIGHT_MAX_MESSAGE_SIZE = 2 * 1024 * 1024  # 2MB

# ============================================================================
# Window Configuration (Flink SQL TUMBLE)
# ============================================================================

# Window size in milliseconds
# 1 day = 86400000 ms
WINDOW_SIZE_MS = 86400000

# Timestamp column name
TIMESTAMP_COLUMN = 'event_time'

# Window ID column name
WINDOW_COLUMN = 'window_id'

# ============================================================================
# Ranking Configuration (Flink SQL ROW_NUMBER)
# ============================================================================

# Top-N value for best bids/offers
TOP_N = 5

# Partition columns for ranking
RANK_PARTITION_BY = ['instrumentId', 'window_start']

# Order columns for ranking
RANK_ORDER_BID = [('price', 'DESC'), ('size', 'DESC')]  # Best bid = highest price
RANK_ORDER_OFFER = [('price', 'ASC'), ('size', 'DESC')]  # Best offer = lowest price

# ============================================================================
# Join Configuration
# ============================================================================

# Hash join buffer size (number of build-side records to buffer)
HASH_JOIN_BUFFER_SIZE = 100000

# Join keys
JOIN_KEYS_QUOTES_SECURITIES = ['instrumentId']

# ============================================================================
# Filtering Configuration
# ============================================================================

# Sentinel values to filter out (invalid prices)
FILTER_SENTINEL_VALUES = [0.0, 1.0, 1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0]

# Valid actions (filter out DELETE)
VALID_ACTIONS = ['UPDATE']

# ============================================================================
# Aggregation Configuration
# ============================================================================

# Group by columns for spread analysis
AGG_GROUP_BY = ['marketSegment', 'productCD', 'window_start']

# Aggregation functions
AGG_FUNCTIONS = {
    'bid_offer_spread': ['AVG', 'MIN', 'MAX', 'MEDIAN'],
    'spread_percentage': ['AVG', 'MIN', 'MAX'],
    'best_bid_size': ['SUM'],
    'best_offer_size': ['SUM'],
}

# ============================================================================
# Performance Monitoring
# ============================================================================

# Enable performance metrics collection
ENABLE_METRICS = True

# Metrics collection interval (seconds)
METRICS_INTERVAL = 1.0

# Log throughput every N morsels
LOG_THROUGHPUT_EVERY = 100

# ============================================================================
# Data Source Configuration
# ============================================================================

# CSV file paths
CSV_SECURITIES = 'master_security_10m.csv'
CSV_QUOTES = 'synthetic_inventory.csv'
CSV_TRADES = 'trax_trades_1m.csv'

# Batch sizes for CSV loading
BATCH_SIZE_SECURITIES = 100000
BATCH_SIZE_QUOTES = 50000
BATCH_SIZE_TRADES = 50000

# CSV reading (PyArrow native)
CSV_BLOCK_SIZE = 8 * 1024 * 1024  # 8MB
CSV_USE_THREADS = True

# ============================================================================
# Helper Functions
# ============================================================================

def get_flight_location(stage: str) -> str:
    """Get Arrow Flight location string for a stage."""
    port = FLIGHT_PORTS.get(stage)
    if not port:
        raise ValueError(f"Unknown stage: {stage}")
    return f"grpc://{FLIGHT_HOST}:{port}"


def estimate_morsels_for_table(num_rows: int, avg_row_size_bytes: int = 64) -> int:
    """Estimate number of morsels for a table."""
    table_size_bytes = num_rows * avg_row_size_bytes
    return max(1, table_size_bytes // MORSEL_SIZE_BYTES)


def get_operator_config(stage: str) -> dict:
    """Get configuration dict for an operator stage."""
    return {
        'morsel_size': MORSEL_SIZE_BYTES,
        'num_workers': NUM_WORKERS,
        'numa_aware': NUMA_AWARE,
        'flight_location': get_flight_location(stage),
        'enable_metrics': ENABLE_METRICS,
    }


# ============================================================================
# Configuration Display
# ============================================================================

def print_config():
    """Print current configuration."""
    print("=" * 70)
    print("SABOT ARROW FLIGHT OPERATOR PIPELINE - CONFIGURATION")
    print("=" * 70)
    print(f"Morsel Size: {MORSEL_SIZE_BYTES:,} bytes ({MORSEL_SIZE_BYTES//1024}KB)")
    print(f"Workers: {NUM_WORKERS}")
    print(f"NUMA Aware: {NUMA_AWARE}")
    print(f"Window Size: {WINDOW_SIZE_MS:,} ms ({WINDOW_SIZE_MS//86400000} days)")
    print(f"Top-N: {TOP_N}")
    print(f"Flight Servers: {len(FLIGHT_PORTS)}")
    for stage, port in FLIGHT_PORTS.items():
        print(f"  - {stage}: {FLIGHT_HOST}:{port}")
    print("=" * 70)


if __name__ == "__main__":
    print_config()
