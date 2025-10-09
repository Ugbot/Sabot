#!/usr/bin/env python3
"""
MySQL CDC Performance Benchmark.

Benchmarks CDC throughput by:
1. Writing 10K records to MySQL
2. Streaming them through Sabot CDC
3. Measuring end-to-end latency and throughput

Setup:
    1. Start MySQL with binlog:
       docker compose up mysql -d

    2. Run benchmark:
       python examples/mysql_cdc_benchmark.py

Metrics:
- Write throughput (records/sec)
- CDC throughput (records/sec)
- End-to-end latency (ms)
- Batch processing time
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List
import statistics

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector, MySQLCDCConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BenchmarkMetrics:
    """Track benchmark metrics."""

    def __init__(self):
        self.write_start_time = None
        self.write_end_time = None
        self.write_count = 0

        self.cdc_start_time = None
        self.cdc_end_time = None
        self.cdc_count = 0

        self.batch_sizes = []
        self.batch_times = []
        self.first_event_latency = None

    def start_write(self):
        """Start write timing."""
        self.write_start_time = time.time()

    def end_write(self, count: int):
        """End write timing."""
        self.write_end_time = time.time()
        self.write_count = count

    def start_cdc(self):
        """Start CDC timing."""
        self.cdc_start_time = time.time()

    def record_batch(self, batch_size: int, batch_time: float):
        """Record batch metrics."""
        self.batch_sizes.append(batch_size)
        self.batch_times.append(batch_time)
        self.cdc_count += batch_size

    def end_cdc(self):
        """End CDC timing."""
        self.cdc_end_time = time.time()

    def record_first_event_latency(self):
        """Record time to first event."""
        if self.first_event_latency is None and self.write_end_time:
            self.first_event_latency = time.time() - self.write_end_time

    def print_report(self):
        """Print benchmark report."""
        print("\n" + "="*70)
        print("MySQL CDC Performance Benchmark Results")
        print("="*70 + "\n")

        # Write metrics
        write_duration = self.write_end_time - self.write_start_time
        write_throughput = self.write_count / write_duration

        print("Write Performance:")
        print(f"  Records written: {self.write_count:,}")
        print(f"  Duration: {write_duration:.2f}s")
        print(f"  Throughput: {write_throughput:,.0f} records/sec")
        print()

        # CDC metrics
        cdc_duration = self.cdc_end_time - self.cdc_start_time
        cdc_throughput = self.cdc_count / cdc_duration

        print("CDC Performance:")
        print(f"  Records received: {self.cdc_count:,}")
        print(f"  Duration: {cdc_duration:.2f}s")
        print(f"  Throughput: {cdc_throughput:,.0f} records/sec")
        print(f"  Time to first event: {self.first_event_latency*1000:.1f}ms")
        print()

        # Batch metrics
        if self.batch_times:
            avg_batch_size = statistics.mean(self.batch_sizes)
            avg_batch_time = statistics.mean(self.batch_times)
            max_batch_time = max(self.batch_times)
            min_batch_time = min(self.batch_times)

            print("Batch Processing:")
            print(f"  Batches processed: {len(self.batch_sizes)}")
            print(f"  Avg batch size: {avg_batch_size:.1f} records")
            print(f"  Avg batch time: {avg_batch_time*1000:.2f}ms")
            print(f"  Min batch time: {min_batch_time*1000:.2f}ms")
            print(f"  Max batch time: {max_batch_time*1000:.2f}ms")
            print()

        # End-to-end metrics
        total_duration = self.cdc_end_time - self.write_start_time
        e2e_throughput = self.cdc_count / total_duration

        print("End-to-End Performance:")
        print(f"  Total duration: {total_duration:.2f}s")
        print(f"  Throughput: {e2e_throughput:,.0f} records/sec")
        print()

        # Efficiency
        if self.cdc_count == self.write_count:
            print("✅ All records processed successfully")
        else:
            print(f"⚠️  Processed {self.cdc_count}/{self.write_count} records")

        print("\n" + "="*70 + "\n")


async def setup_database():
    """Setup benchmark database and tables."""
    import pymysql

    logger.info("Setting up benchmark database...")

    connection = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot"
    )

    try:
        with connection.cursor() as cursor:
            # Drop table if exists
            cursor.execute("DROP TABLE IF EXISTS cdc_benchmark")

            # Create benchmark table
            cursor.execute("""
                CREATE TABLE cdc_benchmark (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    event_type VARCHAR(50),
                    event_data VARCHAR(200),
                    amount DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_event_type (event_type)
                ) ENGINE=InnoDB
            """)
            connection.commit()

            logger.info("✅ Database setup complete")

    finally:
        connection.close()


async def write_benchmark_data(num_records: int, metrics: BenchmarkMetrics):
    """
    Write benchmark data to MySQL.

    Args:
        num_records: Number of records to write
        metrics: Metrics tracker
    """
    import pymysql

    logger.info(f"Writing {num_records:,} records to MySQL...")

    connection = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot"
    )

    metrics.start_write()

    try:
        with connection.cursor() as cursor:
            # Batch insert for better performance
            batch_size = 100
            event_types = ['click', 'view', 'purchase', 'signup', 'logout']

            for i in range(0, num_records, batch_size):
                values = []
                for j in range(batch_size):
                    if i + j >= num_records:
                        break

                    user_id = (i + j) % 1000 + 1
                    event_type = event_types[(i + j) % len(event_types)]
                    event_data = f"Event {i+j} data"
                    amount = ((i + j) % 100) + 0.99

                    values.append(f"({user_id}, '{event_type}', '{event_data}', {amount})")

                if values:
                    query = f"""
                        INSERT INTO cdc_benchmark (user_id, event_type, event_data, amount)
                        VALUES {','.join(values)}
                    """
                    cursor.execute(query)

                # Commit periodically
                if (i + batch_size) % 1000 == 0:
                    connection.commit()

            # Final commit
            connection.commit()

    finally:
        connection.close()

    metrics.end_write(num_records)

    write_duration = metrics.write_end_time - metrics.write_start_time
    write_throughput = num_records / write_duration

    logger.info(f"✅ Wrote {num_records:,} records in {write_duration:.2f}s " +
               f"({write_throughput:,.0f} records/sec)")


async def stream_and_process(num_records: int, metrics: BenchmarkMetrics):
    """
    Stream CDC events and measure performance.

    Args:
        num_records: Expected number of records
        metrics: Metrics tracker
    """
    logger.info("Starting CDC stream...")

    # Configure CDC
    config = MySQLCDCConfig(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot",
        only_tables=["sabot.cdc_benchmark"],
        batch_size=100,  # Process in batches of 100
        max_poll_interval=1.0,
        track_ddl=False  # Disable DDL tracking for benchmark
    )

    connector = MySQLCDCConnector(config)

    metrics.start_cdc()

    try:
        async with connector:
            async for batch in connector.stream_batches():
                batch_start = time.time()

                # Record first event latency
                metrics.record_first_event_latency()

                # Process batch (simulate work)
                batch_dict = batch.to_pydict()
                insert_count = sum(1 for evt in batch_dict['event_type'] if evt == 'insert')

                batch_time = time.time() - batch_start
                metrics.record_batch(insert_count, batch_time)

                logger.info(f"Processed batch: {insert_count} records in {batch_time*1000:.1f}ms " +
                           f"(Total: {metrics.cdc_count}/{num_records})")

                # Stop when we've received all records
                if metrics.cdc_count >= num_records:
                    logger.info(f"✅ Received all {num_records:,} records")
                    break

    except Exception as e:
        logger.error(f"CDC error: {e}", exc_info=True)
    finally:
        metrics.end_cdc()


async def run_benchmark(num_records: int = 10000):
    """
    Run complete benchmark.

    Args:
        num_records: Number of records to benchmark
    """
    metrics = BenchmarkMetrics()

    print("\n" + "="*70)
    print(f"MySQL CDC Benchmark - {num_records:,} Records")
    print("="*70 + "\n")

    # Setup
    await setup_database()

    # Write data
    await write_benchmark_data(num_records, metrics)

    # Small delay to ensure writes are committed
    await asyncio.sleep(1)

    # Stream and process
    await stream_and_process(num_records, metrics)

    # Print report
    metrics.print_report()


async def main():
    """Main benchmark entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='MySQL CDC Performance Benchmark')
    parser.add_argument(
        '--records',
        type=int,
        default=10000,
        help='Number of records to benchmark (default: 10000)'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Quick benchmark with 1000 records'
    )

    args = parser.parse_args()

    num_records = 1000 if args.quick else args.records

    try:
        await run_benchmark(num_records)
    except KeyboardInterrupt:
        logger.info("\n\nBenchmark interrupted")
    except Exception as e:
        logger.error(f"Benchmark failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
