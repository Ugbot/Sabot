#!/usr/bin/env python3
"""
Simple PostgreSQL CDC Benchmark using libpq directly

Tests raw CDC throughput with wal2arrow plugin.
"""

import asyncio
import logging
import sys
import time
import psutil

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')


def setup_table():
    """Create benchmark table with 8 columns."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    logger.info("Setting up benchmark table...")

    # Drop existing
    conn.execute("DROP TABLE IF EXISTS benchmark_data CASCADE")

    # Create 8-column table
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

    logger.info("✅ Created benchmark_data table (8 columns)")
    conn.close()


def generate_data(num_rows=10000):
    """Generate test data."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    logger.info(f"Generating {num_rows} rows...")

    batch_size = 1000
    for i in range(0, num_rows, batch_size):
        batch = min(batch_size, num_rows - i)

        values = []
        for j in range(batch):
            idx = i + j
            metadata = f'{{"batch": {i // batch_size}}}'
            values.append(
                f"({idx}, 'user_{idx}', 'user_{idx}@example.com', "
                f"{idx * 10.50}, 'active', NOW(), '{metadata}'::jsonb)"
            )

        query = f"""
            INSERT INTO benchmark_data
            (user_id, username, email, balance, status, last_login, metadata)
            VALUES {','.join(values)}
        """
        conn.execute(query)

        if (i + batch) % 5000 == 0:
            logger.info(f"  Generated {i + batch}/{num_rows} rows")

    logger.info(f"✅ Generated {num_rows} rows")
    conn.close()


async def test_replication():
    """Test basic replication stream."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    # Create replication connection
    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot replication=database",
        replication=True
    )

    logger.info("✅ Created replication connection")

    # Check if wal2arrow plugin exists
    try:
        # Try to create slot with wal2arrow
        logger.info("Creating replication slot with wal2arrow plugin...")
        conn.create_replication_slot('test_wal2arrow', output_plugin='wal2arrow')
        logger.info("✅ Replication slot created with wal2arrow")
    except Exception as e:
        logger.error(f"❌ Failed to create slot with wal2arrow: {e}")
        logger.info("Trying pgoutput instead...")
        try:
            conn.create_replication_slot('test_pgoutput', output_plugin='pgoutput')
            logger.info("✅ Replication slot created with pgoutput")
        except Exception as e2:
            logger.error(f"❌ pgoutput also failed: {e2}")
            return

    # Start replication
    logger.info("Starting replication stream...")
    events_received = 0
    batches_received = 0
    start_time = time.time()

    try:
        async def read_stream():
            nonlocal events_received, batches_received

            async for msg in conn.start_replication('test_wal2arrow'):
                if msg.data:
                    batches_received += 1
                    data_len = len(msg.data)
                    events_received += 1  # Count each message as 1 event for now

                    logger.info(
                        f"Received batch {batches_received}: "
                        f"{data_len} bytes (WAL: {msg.wal_start}..{msg.wal_end})"
                    )

                    if events_received >= 100:  # Stop after 100 messages
                        break

        await asyncio.wait_for(read_stream(), timeout=30)

    except asyncio.TimeoutError:
        logger.warning("Timeout after 30s")
    except Exception as e:
        logger.error(f"Error during replication: {e}", exc_info=True)

    duration = time.time() - start_time
    throughput = events_received / duration if duration > 0 else 0

    logger.info("\n" + "=" * 80)
    logger.info("REPLICATION TEST RESULTS")
    logger.info("=" * 80)
    logger.info(f"Events received: {events_received}")
    logger.info(f"Batches received: {batches_received}")
    logger.info(f"Duration: {duration:.2f}s")
    logger.info(f"Throughput: {throughput:.0f} events/sec")
    logger.info("=" * 80)

    conn.close()


async def main():
    """Main benchmark."""
    logger.info("\n" + "=" * 80)
    logger.info("PostgreSQL CDC Simple Benchmark")
    logger.info("=" * 80 + "\n")

    # Setup
    setup_table()

    # Generate data
    logger.info("\n📝 Generating test data...")
    generate_data(10000)

    # Wait for WAL to flush
    await asyncio.sleep(2)

    # Test replication
    logger.info("\n🔄 Testing replication stream...")
    await test_replication()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Failed: {e}", exc_info=True)
        sys.exit(1)
