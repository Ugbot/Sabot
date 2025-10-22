#!/usr/bin/env python3
"""
PostgreSQL CDC Benchmark: wal2arrow vs pgoutput

Benchmarks both CDC connectors with real data and measures:
- Throughput (events/sec)
- Latency (p50, p95, p99)
- Memory usage
- CPU overhead
"""

import asyncio
import logging
import sys
import time
import psutil
import statistics
from dataclasses import dataclass
from typing import List, Dict
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')


@dataclass
class BenchmarkResult:
    """Benchmark results for a CDC connector."""
    plugin: str
    total_events: int
    duration_seconds: float
    throughput_events_per_sec: float
    latencies_ms: List[float]
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    memory_mb: float
    cpu_percent: float
    batch_count: int
    avg_batch_size: float


class PostgreSQLDataGenerator:
    """Generates test data in PostgreSQL."""

    def __init__(self, conn_str: str):
        from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection
        self.conn = PostgreSQLConnection(conn_str, replication=False)

    def setup_tables(self):
        """Create benchmark tables."""
        logger.info("Setting up benchmark tables...")

        # Drop existing tables
        self.conn.execute("DROP TABLE IF EXISTS benchmark_orders CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS benchmark_users CASCADE")

        # Create users table
        self.conn.execute("""
            CREATE TABLE benchmark_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                balance NUMERIC(10, 2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Create orders table
        self.conn.execute("""
            CREATE TABLE benchmark_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        logger.info("✅ Benchmark tables created")

    def generate_inserts(self, num_events: int) -> int:
        """
        Generate INSERT operations.

        Returns: Number of events generated
        """
        logger.info(f"Generating {num_events} INSERT events...")

        events_generated = 0
        batch_size = 100

        for i in range(0, num_events, batch_size):
            batch = min(batch_size, num_events - i)

            # Insert users
            values = []
            for j in range(batch):
                idx = i + j
                values.append(f"('user_{idx}', 'user_{idx}@example.com', {idx * 10.50})")

            query = f"INSERT INTO benchmark_users (username, email, balance) VALUES {','.join(values)}"
            self.conn.execute(query)
            events_generated += batch

            if (i + batch) % 1000 == 0:
                logger.info(f"Generated {i + batch}/{num_events} events")

        logger.info(f"✅ Generated {events_generated} INSERT events")
        return events_generated

    def generate_updates(self, num_events: int) -> int:
        """Generate UPDATE operations."""
        logger.info(f"Generating {num_events} UPDATE events...")

        events_generated = 0

        for i in range(num_events):
            user_id = (i % 100) + 1  # Update first 100 users
            new_balance = (i * 5.25) % 10000

            self.conn.execute(
                f"UPDATE benchmark_users SET balance = {new_balance} WHERE id = {user_id}"
            )
            events_generated += 1

            if (i + 1) % 1000 == 0:
                logger.info(f"Generated {i + 1}/{num_events} events")

        logger.info(f"✅ Generated {events_generated} UPDATE events")
        return events_generated

    def generate_deletes(self, num_events: int) -> int:
        """Generate DELETE operations."""
        logger.info(f"Generating {num_events} DELETE events...")

        events_generated = 0

        for i in range(num_events):
            user_id = i + 1

            try:
                self.conn.execute(f"DELETE FROM benchmark_users WHERE id = {user_id}")
                events_generated += 1
            except:
                break  # No more rows to delete

            if (i + 1) % 1000 == 0:
                logger.info(f"Generated {i + 1}/{num_events} events")

        logger.info(f"✅ Generated {events_generated} DELETE events")
        return events_generated

    def cleanup(self):
        """Cleanup benchmark tables."""
        try:
            self.conn.execute("DROP TABLE IF EXISTS benchmark_orders CASCADE")
            self.conn.execute("DROP TABLE IF EXISTS benchmark_users CASCADE")
            logger.info("✅ Cleaned up benchmark tables")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")

    def close(self):
        """Close connection."""
        self.conn.close()


async def benchmark_cdc_reader(
    reader,
    expected_events: int,
    timeout_seconds: int = 60
) -> BenchmarkResult:
    """
    Benchmark a CDC reader.

    Args:
        reader: CDC reader (wal2arrow or pgoutput)
        expected_events: Number of events to wait for
        timeout_seconds: Max time to wait

    Returns:
        BenchmarkResult with metrics
    """
    events_received = 0
    batch_count = 0
    latencies = []
    start_time = time.time()
    process = psutil.Process()

    # Get initial memory baseline
    initial_memory_mb = process.memory_info().rss / 1024 / 1024
    cpu_samples = []

    try:
        async def read_with_timeout():
            nonlocal events_received, batch_count

            async for batch in reader.read_batches():
                if batch and batch.num_rows > 0:
                    batch_start = time.time()
                    batch_count += 1
                    events_received += batch.num_rows

                    # Record latency (time from batch arrival to processing)
                    batch_latency_ms = (time.time() - batch_start) * 1000
                    latencies.append(batch_latency_ms)

                    # Sample CPU usage
                    cpu_samples.append(process.cpu_percent(interval=0.01))

                    logger.debug(
                        f"Batch {batch_count}: {batch.num_rows} rows "
                        f"(total: {events_received}/{expected_events})"
                    )

                    if events_received >= expected_events:
                        break

        await asyncio.wait_for(read_with_timeout(), timeout=timeout_seconds)

    except asyncio.TimeoutError:
        logger.warning(f"Timeout after {timeout_seconds}s, received {events_received}/{expected_events} events")

    end_time = time.time()
    duration = end_time - start_time

    # Calculate memory usage
    final_memory_mb = process.memory_info().rss / 1024 / 1024
    memory_delta_mb = final_memory_mb - initial_memory_mb

    # Calculate metrics
    throughput = events_received / duration if duration > 0 else 0
    avg_cpu = statistics.mean(cpu_samples) if cpu_samples else 0

    # Calculate latency percentiles
    if latencies:
        p50 = statistics.median(latencies)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
    else:
        p50 = p95 = p99 = 0

    avg_batch_size = events_received / batch_count if batch_count > 0 else 0

    plugin_name = "wal2arrow" if "ArrowCDC" in str(type(reader)) else "pgoutput"

    return BenchmarkResult(
        plugin=plugin_name,
        total_events=events_received,
        duration_seconds=duration,
        throughput_events_per_sec=throughput,
        latencies_ms=latencies,
        p50_latency_ms=p50,
        p95_latency_ms=p95,
        p99_latency_ms=p99,
        memory_mb=memory_delta_mb,
        cpu_percent=avg_cpu,
        batch_count=batch_count,
        avg_batch_size=avg_batch_size
    )


async def benchmark_wal2arrow(num_events: int) -> BenchmarkResult:
    """Benchmark wal2arrow CDC connector."""
    logger.info("=" * 80)
    logger.info("BENCHMARKING wal2arrow (Custom Plugin)")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.arrow_cdc_reader import (
            ArrowCDCReaderConfig
        )

        # Auto-configure with wal2arrow
        readers = ArrowCDCReaderConfig.auto_configure(
            host="localhost",
            database="sabot",
            user="sabot",
            password="sabot",
            port=5433,
            tables="benchmark_users",
            slot_name="benchmark_wal2arrow",
            plugin='wal2arrow',
            route_by_table=False  # Single reader for benchmark
        )

        logger.info("✅ wal2arrow reader configured")

        # Benchmark
        result = await benchmark_cdc_reader(readers, num_events, timeout_seconds=120)

        logger.info(f"✅ wal2arrow benchmark complete: {result.throughput_events_per_sec:.0f} events/sec")
        return result

    except Exception as e:
        logger.error(f"❌ wal2arrow benchmark failed: {e}", exc_info=True)
        return None


async def benchmark_pgoutput(num_events: int) -> BenchmarkResult:
    """Benchmark pgoutput CDC connector."""
    logger.info("=" * 80)
    logger.info("BENCHMARKING pgoutput (Plugin-Free)")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.arrow_cdc_reader import (
            ArrowCDCReaderConfig
        )

        # Auto-configure with pgoutput
        readers = ArrowCDCReaderConfig.auto_configure(
            host="localhost",
            database="sabot",
            user="sabot",
            password="sabot",
            port=5433,
            tables="benchmark_users",
            slot_name="benchmark_pgoutput",
            plugin='pgoutput',
            route_by_table=False  # Single reader for benchmark
        )

        logger.info("✅ pgoutput reader configured")

        # Benchmark
        result = await benchmark_cdc_reader(readers, num_events, timeout_seconds=120)

        logger.info(f"✅ pgoutput benchmark complete: {result.throughput_events_per_sec:.0f} events/sec")
        return result

    except Exception as e:
        logger.error(f"❌ pgoutput benchmark failed: {e}", exc_info=True)
        return None


def print_comparison(wal2arrow_result: BenchmarkResult, pgoutput_result: BenchmarkResult):
    """Print detailed comparison of results."""
    logger.info("\n" + "=" * 80)
    logger.info("BENCHMARK COMPARISON")
    logger.info("=" * 80)

    if not wal2arrow_result or not pgoutput_result:
        logger.error("Cannot compare - one or both benchmarks failed")
        return

    # Throughput comparison
    speedup = wal2arrow_result.throughput_events_per_sec / pgoutput_result.throughput_events_per_sec

    print("\n📊 THROUGHPUT")
    print(f"  wal2arrow:  {wal2arrow_result.throughput_events_per_sec:>10,.0f} events/sec")
    print(f"  pgoutput:   {pgoutput_result.throughput_events_per_sec:>10,.0f} events/sec")
    print(f"  Speedup:    {speedup:>10.2f}x")

    print("\n⏱️  LATENCY (milliseconds)")
    print(f"              wal2arrow    pgoutput")
    print(f"  p50:        {wal2arrow_result.p50_latency_ms:>8.2f}ms  {pgoutput_result.p50_latency_ms:>8.2f}ms")
    print(f"  p95:        {wal2arrow_result.p95_latency_ms:>8.2f}ms  {pgoutput_result.p95_latency_ms:>8.2f}ms")
    print(f"  p99:        {wal2arrow_result.p99_latency_ms:>8.2f}ms  {pgoutput_result.p99_latency_ms:>8.2f}ms")

    print("\n💾 MEMORY USAGE")
    print(f"  wal2arrow:  {wal2arrow_result.memory_mb:>10.1f} MB")
    print(f"  pgoutput:   {pgoutput_result.memory_mb:>10.1f} MB")
    print(f"  Difference: {abs(wal2arrow_result.memory_mb - pgoutput_result.memory_mb):>10.1f} MB")

    print("\n⚙️  CPU USAGE")
    print(f"  wal2arrow:  {wal2arrow_result.cpu_percent:>10.1f}%")
    print(f"  pgoutput:   {pgoutput_result.cpu_percent:>10.1f}%")

    print("\n📦 BATCHING")
    print(f"              wal2arrow    pgoutput")
    print(f"  Batches:    {wal2arrow_result.batch_count:>10}   {pgoutput_result.batch_count:>10}")
    print(f"  Avg size:   {wal2arrow_result.avg_batch_size:>10.1f}   {pgoutput_result.avg_batch_size:>10.1f}")

    print("\n" + "=" * 80)

    # Winner determination
    if speedup > 1.2:
        print(f"🏆 WINNER: wal2arrow ({speedup:.1f}x faster)")
    elif speedup < 0.8:
        print(f"🏆 WINNER: pgoutput ({1/speedup:.1f}x faster)")
    else:
        print("🏆 WINNER: TIE (performance within 20%)")

    print("=" * 80 + "\n")


async def run_benchmark():
    """Run complete benchmark suite."""
    logger.info("\n" + "=" * 80)
    logger.info("PostgreSQL CDC BENCHMARK")
    logger.info("Comparing wal2arrow vs pgoutput")
    logger.info("=" * 80 + "\n")

    # Configuration
    NUM_INSERT_EVENTS = 10000
    NUM_UPDATE_EVENTS = 5000
    NUM_DELETE_EVENTS = 1000
    TOTAL_EVENTS = NUM_INSERT_EVENTS + NUM_UPDATE_EVENTS + NUM_DELETE_EVENTS

    conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"

    # Setup data generator
    generator = PostgreSQLDataGenerator(conn_str)

    try:
        # Setup tables
        generator.setup_tables()

        # Benchmark 1: wal2arrow
        logger.info(f"\n📝 Generating {TOTAL_EVENTS} events for wal2arrow benchmark...")
        generator.generate_inserts(NUM_INSERT_EVENTS)
        generator.generate_updates(NUM_UPDATE_EVENTS)
        generator.generate_deletes(NUM_DELETE_EVENTS)

        wal2arrow_result = await benchmark_wal2arrow(TOTAL_EVENTS)

        # Cleanup for next benchmark
        generator.cleanup()
        generator.setup_tables()

        # Benchmark 2: pgoutput
        logger.info(f"\n📝 Generating {TOTAL_EVENTS} events for pgoutput benchmark...")
        generator.generate_inserts(NUM_INSERT_EVENTS)
        generator.generate_updates(NUM_UPDATE_EVENTS)
        generator.generate_deletes(NUM_DELETE_EVENTS)

        pgoutput_result = await benchmark_pgoutput(TOTAL_EVENTS)

        # Compare results
        print_comparison(wal2arrow_result, pgoutput_result)

        # Cleanup
        generator.cleanup()

    finally:
        generator.close()

    return wal2arrow_result, pgoutput_result


if __name__ == "__main__":
    try:
        wal2arrow_result, pgoutput_result = asyncio.run(run_benchmark())

        # Exit with success if both benchmarks completed
        if wal2arrow_result and pgoutput_result:
            sys.exit(0)
        else:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Benchmark failed: {e}", exc_info=True)
        sys.exit(1)
