#!/usr/bin/env python3
"""Test fraud detection throughput without Kafka."""

import asyncio
import time
import random
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Import the fraud app components
from examples.fraud_app import FraudDetector, BankTransaction

# City coordinates for test data
CITIES = ['Santiago', 'Valparaíso', 'Concepción', 'La Serena',
          'Antofagasta', 'Temuco', 'Arica', 'Punta Arenas']
ACCOUNTS = [f"ACC{i:06d}" for i in range(1000)]
USERS = [f"USER{i:05d}" for i in range(500)]


def generate_transaction(txn_id: int, timestamp_ms: int) -> BankTransaction:
    """Generate a random transaction."""
    return BankTransaction(
        transaction_id=txn_id,
        account_number=random.choice(ACCOUNTS),
        user_id=random.choice(USERS),
        transaction_date="2025-10-03",
        transaction_amount=random.uniform(10, 5000),
        currency="USD",
        transaction_type=random.choice(["purchase", "withdrawal", "transfer"]),
        branch_city=random.choice(CITIES),
        payment_method=random.choice(["credit_card", "debit_card", "cash"]),
        country="CL",
        timestamp_ms=timestamp_ms
    )


async def benchmark_fraud_detection(num_transactions: int = 10000):
    """Benchmark fraud detection throughput."""
    print("=" * 70)
    print("🧪 Fraud Detection Throughput Benchmark")
    print("=" * 70)
    print(f"Transactions: {num_transactions:,}")
    print()

    # Create detector
    detector = FraudDetector()

    # Generate transactions
    print("Generating test transactions...")
    start_time = time.time()
    base_timestamp = int(time.time() * 1000)

    transactions = [
        generate_transaction(i, base_timestamp + i * 100)
        for i in range(num_transactions)
    ]

    gen_time = time.time() - start_time
    print(f"✅ Generated {num_transactions:,} transactions in {gen_time:.2f}s\n")

    # Process transactions
    print("Processing transactions...")
    latencies = []
    alert_count = 0

    process_start = time.time()

    for txn in transactions:
        txn_start = time.time()
        alerts = await detector.detect_fraud(txn)
        txn_time = (time.time() - txn_start) * 1000  # ms

        latencies.append(txn_time)
        alert_count += len(alerts)

    process_end = time.time()
    elapsed = process_end - process_start

    # Calculate metrics
    throughput = num_transactions / elapsed

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    avg = sum(latencies) / len(latencies)

    # Display results
    print("\n" + "=" * 70)
    print("📊 Benchmark Results")
    print("=" * 70)
    print(f"Processed: {num_transactions:,} transactions")
    print(f"Time: {elapsed:.2f}s")
    print(f"Throughput: {throughput:,.0f} txn/s")
    print(f"Fraud Alerts: {alert_count}")
    print()
    print("Latency:")
    print(f"  Average: {avg:.2f}ms")
    print(f"  p50: {p50:.2f}ms")
    print(f"  p95: {p95:.2f}ms")
    print(f"  p99: {p99:.2f}ms")
    print("=" * 70)
    print()

    # State statistics
    print("State Statistics:")
    print(f"  Accounts tracked: {len(detector.account_stats)}")
    print(f"  Locations tracked: {len(detector.last_location)}")
    print(f"  Total alerts: {len(detector.alerts)}")
    print("=" * 70)


async def main():
    """Run benchmarks with different sizes."""
    # Small benchmark
    await benchmark_fraud_detection(1000)
    print("\n")

    # Medium benchmark
    await benchmark_fraud_detection(10000)
    print("\n")

    # Large benchmark
    await benchmark_fraud_detection(50000)


if __name__ == "__main__":
    asyncio.run(main())
