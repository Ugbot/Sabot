#!/usr/bin/env python3
"""
Working PostgreSQL CDC Benchmark

Creates replication slot FIRST, then generates data to capture CDC events.
"""

import asyncio
import logging
import sys
import time
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')


def setup_table():
    """Create benchmark table."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    logger.info("Setting up benchmark table...")
    conn.execute("DROP TABLE IF EXISTS benchmark_data CASCADE")
    conn.execute("""
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

    # Set REPLICA IDENTITY FULL for wal2arrow CDC
    conn.execute("ALTER TABLE benchmark_data REPLICA IDENTITY FULL")

    logger.info("✅ Created benchmark_data table (8 columns) with REPLICA IDENTITY FULL")
    conn.close()


def generate_data_async(num_rows=10000):
    """Generate data in background thread."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    # No delay - start immediately to avoid replication timeout
    # time.sleep(0.5)  # REMOVED: causes 60s timeout

    try:
        conn = PostgreSQLConnection(
            "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
            replication=False
        )

        logger.info(f"📝 Generating {num_rows} rows...")

        batch_size = 100  # Smaller batches for better CDC granularity
        total_inserted = 0

        for i in range(0, num_rows, batch_size):
            batch = min(batch_size, num_rows - i)

            try:
                # Start transaction
                conn.execute("BEGIN")

                values = []
                for j in range(batch):
                    idx = i + j
                    values.append(
                        f"({idx}, 'user_{idx}', 'user_{idx}@test.com', "
                        f"{idx * 10.50}, 'active', NOW(), '{{}}'::jsonb)"
                    )

                query = f"""
                    INSERT INTO benchmark_data
                    (user_id, username, email, balance, status, last_login, metadata)
                    VALUES {','.join(values)}
                """
                conn.execute(query)

                # COMMIT to generate WAL event (required for CDC)
                conn.execute("COMMIT")

                total_inserted += batch

                if (i + batch) % 1000 == 0 or (i + batch) == num_rows:
                    logger.info(f"  Generated {i + batch}/{num_rows} rows")

            except Exception as e:
                logger.error(f"❌ Error inserting batch {i}-{i+batch}: {e}", exc_info=True)
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                break

            # Small delay to allow CDC to keep up
            time.sleep(0.01)

        logger.info(f"✅ Generated {total_inserted} rows (target was {num_rows})")
        conn.close()

    except Exception as e:
        logger.error(f"❌ Fatal error in data generation: {e}", exc_info=True)


async def benchmark_cdc(num_rows=10000):
    """Benchmark CDC replication."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    # Create replication connection
    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot replication=database",
        replication=True
    )

    logger.info("✅ Created replication connection")

    # Drop existing slot if it exists
    try:
        conn.execute("SELECT pg_drop_replication_slot('bench_wal2arrow')")
        logger.info("Dropped existing slot")
    except:
        pass

    # Create replication slot
    logger.info("Creating replication slot with wal2arrow...")
    try:
        slot_info = conn.create_replication_slot('bench_wal2arrow', output_plugin='wal2arrow')
        logger.info(f"✅ Replication slot created: {slot_info}")
    except Exception as e:
        logger.error(f"❌ Failed to create wal2arrow slot: {e}")
        logger.info("This benchmark requires wal2arrow plugin installed")
        return

    # Start data generation in background thread
    data_thread = threading.Thread(target=generate_data_async, args=(num_rows,))
    data_thread.start()

    # Start replication and count events
    logger.info("Starting replication stream...")
    events_received = 0
    bytes_received = 0
    batches_received = 0
    start_time = time.time()

    try:
        async def read_stream():
            nonlocal events_received, bytes_received, batches_received

            async for msg in conn.start_replication('bench_wal2arrow'):
                if msg.data:
                    batches_received += 1
                    data_len = len(msg.data)
                    bytes_received += data_len

                    # Assume each message is a batch (we'll count actual rows if we can parse)
                    events_received += 1

                    logger.info(
                        f"Batch {batches_received}: {data_len} bytes "
                        f"(WAL: {msg.wal_start})"
                    )

                    # Stop when we've likely received all data
                    if batches_received >= num_rows / 10:  # Expect ~100 batches for 10K rows
                        break

        await asyncio.wait_for(read_stream(), timeout=60)

    except asyncio.TimeoutError:
        logger.warning("Timeout - stopping")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)

    # Wait for data generation to complete
    data_thread.join(timeout=10)

    duration = time.time() - start_time
    throughput_events = events_received / duration if duration > 0 else 0
    throughput_bytes = bytes_received / duration if duration > 0 else 0

    # Print results
    logger.info("\n" + "=" * 80)
    logger.info("CDC BENCHMARK RESULTS")
    logger.info("=" * 80)
    logger.info(f"Plugin:          wal2arrow")
    logger.info(f"Batches:         {batches_received}")
    logger.info(f"Bytes received:  {bytes_received:,}")
    logger.info(f"Duration:        {duration:.2f}s")
    logger.info(f"Throughput:      {throughput_events:.0f} batches/sec")
    logger.info(f"Bandwidth:       {throughput_bytes / 1024:.1f} KB/sec")
    logger.info("=" * 80)

    # Cleanup
    try:
        conn.execute("SELECT pg_drop_replication_slot('bench_wal2arrow')")
    except:
        pass

    conn.close()


async def main():
    """Main benchmark."""
    logger.info("\n" + "=" * 80)
    logger.info("PostgreSQL CDC Benchmark - wal2arrow")
    logger.info("=" * 80 + "\n")

    # Setup
    setup_table()

    # Run benchmark
    await benchmark_cdc(10000)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Failed: {e}", exc_info=True)
        sys.exit(1)
