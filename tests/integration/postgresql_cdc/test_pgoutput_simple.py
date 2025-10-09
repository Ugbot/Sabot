#!/usr/bin/env python3
"""
Simple test using pgoutput (built-in plugin) instead of wal2arrow.
This will confirm the replication plumbing works.
"""

import asyncio
import logging
import sys
import time
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.insert(0, '/Users/bengamble/Sabot')


def setup():
    """Setup table and publication."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS test_cdc CASCADE")
    conn.execute("""
        CREATE TABLE test_cdc (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    # Set replica identity to FULL (required for CDC)
    conn.execute("ALTER TABLE test_cdc REPLICA IDENTITY FULL")

    # Create publication
    try:
        conn.execute("DROP PUBLICATION IF EXISTS test_pub")
    except:
        pass

    conn.execute("CREATE PUBLICATION test_pub FOR TABLE test_cdc")

    logger.info("✅ Table and publication created")
    conn.close()


def generate_data():
    """Generate data in background."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    time.sleep(3)  # Wait for replication to start

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    logger.info("📝 Generating data...")
    for i in range(10):
        # BEGIN transaction to generate WAL events
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO test_cdc (name, value) VALUES ('test_{i}', {i * 100})")
        # COMMIT to flush to WAL (required for CDC)
        conn.execute("COMMIT")
        logger.info(f"  Inserted row {i+1}/10")
        time.sleep(0.5)

    logger.info("✅ Generated 10 rows")
    conn.close()


async def test_pgoutput():
    """Test pgoutput CDC."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot replication=database",
        replication=True
    )

    # Drop existing slot
    try:
        conn.execute("SELECT pg_drop_replication_slot('test_slot')")
    except:
        pass

    # Create slot with pgoutput
    logger.info("Creating replication slot with pgoutput...")
    slot = conn.create_replication_slot('test_slot', output_plugin='pgoutput')
    logger.info(f"✅ Slot created: {slot}")

    # Start data generation thread
    data_thread = threading.Thread(target=generate_data)
    data_thread.start()

    # Read replication stream
    logger.info("Starting replication...")
    messages = 0

    try:
        async def read_stream():
            nonlocal messages

            # pgoutput requires publication_names option
            async for msg in conn.start_replication(
                'test_slot',
                options={'proto_version': '1', 'publication_names': 'test_pub'}
            ):
                if msg.data:
                    messages += 1
                    logger.info(f"Message {messages}: {len(msg.data)} bytes")

                    if messages >= 20:  # Stop after receiving some messages
                        break

        await asyncio.wait_for(read_stream(), timeout=30)

    except asyncio.TimeoutError:
        logger.info("Timeout")

    data_thread.join(timeout=5)

    logger.info(f"\n✅ Received {messages} CDC messages")

    # Cleanup
    try:
        conn.execute("SELECT pg_drop_replication_slot('test_slot')")
    except:
        pass

    conn.close()


if __name__ == "__main__":
    try:
        setup()
        asyncio.run(test_pgoutput())
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
