#!/usr/bin/env python3
"""
PostgreSQL CDC Benchmark using pgoutput (built-in plugin)

Demonstrates working CDC replication with pgoutput protocol.
No plugin dependencies - uses PostgreSQL's built-in logical decoding.
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
    """Create benchmark table and publication."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
        replication=False
    )

    logger.info("Setting up benchmark table...")

    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS benchmark_cdc CASCADE")
    conn.execute("""
        CREATE TABLE benchmark_cdc (
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

    # Set REPLICA IDENTITY FULL (required for pgoutput CDC)
    conn.execute("ALTER TABLE benchmark_cdc REPLICA IDENTITY FULL")

    # Create publication
    try:
        conn.execute("DROP PUBLICATION IF EXISTS bench_pub")
    except:
        pass

    conn.execute("CREATE PUBLICATION bench_pub FOR TABLE benchmark_cdc")

    logger.info("✅ Created benchmark_cdc table (8 columns) with REPLICA IDENTITY FULL")
    logger.info("✅ Created publication 'bench_pub'")
    conn.close()


def generate_data_async(num_rows=10000):
    """Generate data in background thread."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    try:
        conn = PostgreSQLConnection(
            "host=localhost port=5433 dbname=sabot user=sabot password=sabot",
            replication=False
        )

        logger.info(f"📝 Generating {num_rows} rows...")

        # Hybrid approach: smaller multi-value INSERTs for balance between
        # CDC granularity and performance
        batch_size = 100  # Number of transactions
        rows_per_insert = 10  # Rows per multi-value INSERT (CDC events per transaction)
        total_inserted = 0

        for i in range(0, num_rows, batch_size):
            batch = min(batch_size, num_rows - i)

            try:
                # BEGIN transaction
                conn.execute("BEGIN")

                # Execute multiple small multi-value INSERTs to get more CDC events
                # while maintaining reasonable performance
                for j in range(0, batch, rows_per_insert):
                    insert_count = min(rows_per_insert, batch - j)
                    values = []
                    for k in range(insert_count):
                        idx = i + j + k
                        values.append(
                            f"({idx}, 'user_{idx}', 'user_{idx}@test.com', "
                            f"{idx * 10.50}, 'active', NOW(), '{{}}'::jsonb)"
                        )

                    query = f"""
                        INSERT INTO benchmark_cdc
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

            # No delay for maximum throughput
            # time.sleep(0.01)  # Removed for performance

        logger.info(f"✅ Generated {total_inserted} rows (target was {num_rows})")
        conn.close()

    except Exception as e:
        logger.error(f"❌ Fatal error in data generation: {e}", exc_info=True)


async def benchmark_cdc(num_rows=10000):
    """Benchmark pgoutput CDC replication."""
    from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

    # Create replication connection
    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot replication=database",
        replication=True
    )

    logger.info("✅ Created replication connection")

    # Drop existing slot if it exists
    try:
        conn.execute("SELECT pg_drop_replication_slot('bench_pgoutput')")
        logger.info("Dropped existing slot")
    except:
        pass

    # Create replication slot with pgoutput
    logger.info("Creating replication slot with pgoutput...")
    try:
        slot_info = conn.create_replication_slot('bench_pgoutput', output_plugin='pgoutput')
        logger.info(f"✅ Replication slot created: {slot_info}")
    except Exception as e:
        logger.error(f"❌ Failed to create pgoutput slot: {e}")
        return

    # Start data generation in background thread
    data_thread = threading.Thread(target=generate_data_async, args=(num_rows,))
    data_thread.start()

    # Start replication and count events
    logger.info("Starting replication stream...")
    messages_received = 0
    bytes_received = 0
    start_time = time.time()

    try:
        async def read_stream():
            nonlocal messages_received, bytes_received

            # pgoutput requires publication_names option
            async for msg in conn.start_replication(
                'bench_pgoutput',
                options={'proto_version': '1', 'publication_names': 'bench_pub'}
            ):
                if msg.data:
                    messages_received += 1
                    data_len = len(msg.data)
                    bytes_received += data_len

                    # Log every 1000 messages to avoid overwhelming output
                    if messages_received % 1000 == 0:
                        logger.info(
                            f"Message {messages_received}: {data_len} bytes "
                            f"(WAL: {msg.wal_start})"
                        )

                    # Stop after receiving all CDC messages
                    # With 10-row multi-value INSERTs in 100-row transactions:
                    # - num_transactions = num_rows / 100 = 10 for 1000 rows
                    # - inserts_per_transaction = 100 / 10 = 10 INSERT statements
                    # - Total INSERTs = (num_rows / 10) = 100 INSERT messages
                    # Per transaction: 1 BEGIN + 1 RELATION + 10 INSERTs + 1 COMMIT = 13 messages
                    # Total = 10 transactions × 13 messages = 130 messages for 1000 rows
                    rows_per_insert = 10
                    batch_size = 100
                    num_transactions = (num_rows + batch_size - 1) // batch_size
                    inserts_per_transaction = (batch_size + rows_per_insert - 1) // rows_per_insert
                    expected_messages = num_transactions * (3 + inserts_per_transaction)  # BEGIN/RELATION/COMMIT + INSERTs
                    if messages_received >= expected_messages:
                        break

        await asyncio.wait_for(read_stream(), timeout=60)

    except asyncio.TimeoutError:
        logger.warning("Timeout - stopping")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)

    # Wait for data generation to complete
    data_thread.join(timeout=10)

    duration = time.time() - start_time
    throughput_messages = messages_received / duration if duration > 0 else 0
    throughput_bytes = bytes_received / duration if duration > 0 else 0

    # Print results
    logger.info("\n" + "=" * 80)
    logger.info("PGOUTPUT CDC BENCHMARK RESULTS")
    logger.info("=" * 80)
    logger.info(f"Plugin:          pgoutput (built-in)")
    logger.info(f"Messages:        {messages_received:,}")
    logger.info(f"Bytes received:  {bytes_received:,}")
    logger.info(f"Duration:        {duration:.2f}s")
    logger.info(f"Throughput:      {throughput_messages:.0f} messages/sec")
    logger.info(f"Bandwidth:       {throughput_bytes / 1024:.1f} KB/sec")
    logger.info("=" * 80)

    # Cleanup
    try:
        conn.execute("SELECT pg_drop_replication_slot('bench_pgoutput')")
    except:
        pass

    conn.close()


async def main():
    """Main benchmark."""
    logger.info("\n" + "=" * 80)
    logger.info("PostgreSQL CDC Benchmark - pgoutput (built-in)")
    logger.info("=" * 80 + "\n")

    # Setup
    setup_table()

    # Run benchmark with 10K rows
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
