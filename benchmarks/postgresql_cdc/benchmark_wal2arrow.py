#!/usr/bin/env python3
"""
PostgreSQL CDC Benchmark: wal2arrow Performance Test

Writes 10K rows to an 8-column table and measures CDC throughput.
"""

import asyncio
import logging
import sys
import time
import psutil
import statistics
from dataclasses import dataclass
from typing import List
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
    """Benchmark results for CDC connector."""
    connector: str
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

    def setup_table(self):
        """Create benchmark table with 8 columns."""
        logger.info("Setting up benchmark table...")

        # Drop existing table
        self.conn.execute("DROP TABLE IF EXISTS benchmark_data CASCADE")

        # Create 8-column table
        self.conn.execute("""
            CREATE TABLE benchmark_data (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                balance NUMERIC(10, 2) DEFAULT 0.00,
                status VARCHAR(20) DEFAULT 'active',
                last_login TIMESTAMP DEFAULT NOW(),
                metadata JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        logger.info("✅ Benchmark table created (8 columns)")

    def generate_data(self, num_rows: int = 10000) -> int:
        """
        Generate INSERT operations.

        Args:
            num_rows: Number of rows to insert (default: 10,000)

        Returns:
            Number of rows generated
        """
        logger.info(f"Generating {num_rows} rows...")

        rows_generated = 0
        batch_size = 1000
        start_time = time.time()

        for i in range(0, num_rows, batch_size):
            batch = min(batch_size, num_rows - i)

            # Build batch insert
            values = []
            for j in range(batch):
                idx = i + j
                metadata = f'{{"user_type": "test", "batch": {i // batch_size}}}'
                values.append(
                    f"({idx}, 'user_{idx}', 'user_{idx}@example.com', "
                    f"{idx * 10.50}, 'active', NOW(), '{metadata}'::jsonb)"
                )

            query = f"""
                INSERT INTO benchmark_data
                (user_id, username, email, balance, status, last_login, metadata)
                VALUES {','.join(values)}
            """
            self.conn.execute(query)
            rows_generated += batch

            if (i + batch) % 5000 == 0:
                logger.info(f"Generated {i + batch}/{num_rows} rows")

        duration = time.time() - start_time
        throughput = rows_generated / duration

        logger.info(
            f"✅ Generated {rows_generated} rows in {duration:.2f}s "
            f"({throughput:.0f} rows/sec)"
        )
        return rows_generated

    def cleanup(self):
        """Cleanup benchmark table."""
        try:
            self.conn.execute("DROP TABLE IF EXISTS benchmark_data CASCADE")
            logger.info("✅ Cleaned up benchmark table")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")

    def close(self):
        """Close connection."""
        self.conn.close()


async def benchmark_wal2arrow(num_events: int) -> BenchmarkResult:
    """Benchmark wal2arrow CDC connector."""
    logger.info("=" * 80)
    logger.info("BENCHMARKING wal2arrow CDC Connector")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.arrow_cdc_reader import (
            ArrowCDCReader,
            ArrowCDCReaderConfig
        )

        # Auto-configure with wal2arrow
        readers = ArrowCDCReaderConfig.auto_configure(
            host="localhost",
            database="sabot",
            user="sabot",
            password="sabot",
            port=5433,
            tables="benchmark_data",
            slot_name="benchmark_wal2arrow",
            plugin='wal2arrow',
            route_by_table=False  # Single reader for benchmark
        )

        logger.info("✅ wal2arrow reader configured")

        # Benchmark metrics
        events_received = 0
        batch_count = 0
        latencies = []
        start_time = time.time()
        process = psutil.Process()

        # Get initial memory baseline
        initial_memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_samples = []

        logger.info(f"Starting to receive {num_events} CDC events...")

        # Read batches with timeout
        try:
            async def read_with_timeout():
                nonlocal events_received, batch_count

                async for batch in readers.read_batches():
                    if batch and batch.num_rows > 0:
                        batch_start = time.time()
                        batch_count += 1
                        events_received += batch.num_rows

                        # Record latency
                        batch_latency_ms = (time.time() - batch_start) * 1000
                        latencies.append(batch_latency_ms)

                        # Sample CPU usage
                        cpu_samples.append(process.cpu_percent(interval=0.01))

                        logger.info(
                            f"Batch {batch_count}: {batch.num_rows} rows "
                            f"(total: {events_received}/{num_events})"
                        )

                        if events_received >= num_events:
                            break

            await asyncio.wait_for(read_with_timeout(), timeout=120)

        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout after 120s, received {events_received}/{num_events} events"
            )

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

        result = BenchmarkResult(
            connector="wal2arrow",
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

        logger.info(
            f"✅ wal2arrow benchmark complete: {result.throughput_events_per_sec:.0f} events/sec"
        )
        return result

    except Exception as e:
        logger.error(f"❌ wal2arrow benchmark failed: {e}", exc_info=True)
        return None


def print_results(result: BenchmarkResult):
    """Print detailed benchmark results."""
    logger.info("\n" + "=" * 80)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 80)

    if not result:
        logger.error("Benchmark failed")
        return

    print("\n📊 THROUGHPUT")
    print(f"  Connector:  {result.connector}")
    print(f"  Events:     {result.total_events:>10,}")
    print(f"  Duration:   {result.duration_seconds:>10.2f}s")
    print(f"  Rate:       {result.throughput_events_per_sec:>10,.0f} events/sec")

    print("\n⏱️  LATENCY (milliseconds)")
    print(f"  p50:        {result.p50_latency_ms:>10.2f}ms")
    print(f"  p95:        {result.p95_latency_ms:>10.2f}ms")
    print(f"  p99:        {result.p99_latency_ms:>10.2f}ms")

    print("\n💾 MEMORY USAGE")
    print(f"  Delta:      {result.memory_mb:>10.1f} MB")

    print("\n⚙️  CPU USAGE")
    print(f"  Average:    {result.cpu_percent:>10.1f}%")

    print("\n📦 BATCHING")
    print(f"  Batches:    {result.batch_count:>10}")
    print(f"  Avg size:   {result.avg_batch_size:>10.1f} rows/batch")

    print("\n" + "=" * 80)

    # Performance rating
    if result.throughput_events_per_sec > 100000:
        print("🏆 EXCELLENT: >100K events/sec")
    elif result.throughput_events_per_sec > 50000:
        print("✅ GOOD: >50K events/sec")
    elif result.throughput_events_per_sec > 10000:
        print("⚠️  FAIR: >10K events/sec")
    else:
        print("❌ POOR: <10K events/sec")

    print("=" * 80 + "\n")


async def run_benchmark():
    """Run complete benchmark suite."""
    logger.info("\n" + "=" * 80)
    logger.info("PostgreSQL CDC BENCHMARK - wal2arrow")
    logger.info("Test: 10,000 rows × 8 columns")
    logger.info("=" * 80 + "\n")

    # Configuration
    NUM_ROWS = 10000
    conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"

    # Setup data generator
    generator = PostgreSQLDataGenerator(conn_str)

    try:
        # Setup table
        generator.setup_table()

        # Generate data
        logger.info(f"\n📝 Generating {NUM_ROWS} rows in benchmark_data table...")
        rows_generated = generator.generate_data(NUM_ROWS)

        # Wait a moment for WAL to flush
        await asyncio.sleep(1)

        # Benchmark wal2arrow
        result = await benchmark_wal2arrow(rows_generated)

        # Print results
        print_results(result)

        # Cleanup
        generator.cleanup()

        return result

    finally:
        generator.close()


if __name__ == "__main__":
    try:
        result = asyncio.run(run_benchmark())

        # Exit with success if benchmark completed
        if result and result.total_events > 0:
            sys.exit(0)
        else:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Benchmark failed: {e}", exc_info=True)
        sys.exit(1)
