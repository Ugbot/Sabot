#!/usr/bin/env python3
"""
MySQL CDC DDL Tracking Demo.

Demonstrates DDL change tracking with schema evolution:
- CREATE TABLE detection
- ALTER TABLE tracking
- DROP TABLE detection
- RENAME TABLE handling
- Schema cache invalidation

Setup:
    1. Start MySQL with binlog enabled:
       docker compose up mysql -d

    2. Run this demo:
       python examples/mysql_cdc_ddl_demo.py

The demo will:
- Monitor DDL changes in real-time
- Track schema evolution
- Show schema cache invalidation
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector, MySQLCDCConfig
from sabot._cython.connectors.mysql_schema_tracker import DDLType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_ddl_operations():
    """
    Run DDL operations in background to generate schema changes.

    This simulates schema evolution:
    1. CREATE TABLE
    2. ALTER TABLE (add columns)
    3. RENAME TABLE
    4. DROP TABLE
    """
    import pymysql
    await asyncio.sleep(5)  # Wait for CDC to start

    logger.info("\n" + "="*70)
    logger.info("Running DDL Operations")
    logger.info("="*70)

    connection = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot"
    )

    try:
        with connection.cursor() as cursor:
            # Step 1: CREATE TABLE
            logger.info("\n[DDL Operation 1] Creating table 'test_ddl'...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_ddl (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            connection.commit()
            await asyncio.sleep(2)

            # Step 2: INSERT data
            logger.info("\n[DDL Operation 2] Inserting data...")
            cursor.execute("INSERT INTO test_ddl (name) VALUES ('Alice'), ('Bob')")
            connection.commit()
            await asyncio.sleep(2)

            # Step 3: ALTER TABLE (add column)
            logger.info("\n[DDL Operation 3] Adding column 'email'...")
            cursor.execute("ALTER TABLE test_ddl ADD COLUMN email VARCHAR(255)")
            connection.commit()
            await asyncio.sleep(2)

            # Step 4: UPDATE data
            logger.info("\n[DDL Operation 4] Updating data...")
            cursor.execute("UPDATE test_ddl SET email = 'alice@example.com' WHERE name = 'Alice'")
            connection.commit()
            await asyncio.sleep(2)

            # Step 5: ALTER TABLE (add another column)
            logger.info("\n[DDL Operation 5] Adding column 'age'...")
            cursor.execute("ALTER TABLE test_ddl ADD COLUMN age INT")
            connection.commit()
            await asyncio.sleep(2)

            # Step 6: RENAME TABLE
            logger.info("\n[DDL Operation 6] Renaming table to 'test_ddl_renamed'...")
            cursor.execute("RENAME TABLE test_ddl TO test_ddl_renamed")
            connection.commit()
            await asyncio.sleep(2)

            # Step 7: DROP TABLE
            logger.info("\n[DDL Operation 7] Dropping table...")
            cursor.execute("DROP TABLE IF EXISTS test_ddl_renamed")
            connection.commit()
            await asyncio.sleep(2)

            logger.info("\n" + "="*70)
            logger.info("DDL Operations Complete")
            logger.info("="*70 + "\n")

    finally:
        connection.close()


async def stream_ddl_events():
    """Stream CDC events and track DDL changes."""
    logger.info("\n" + "="*70)
    logger.info("MySQL CDC DDL Tracking Demo")
    logger.info("="*70 + "\n")

    # Configure CDC with DDL tracking enabled
    config = MySQLCDCConfig(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot",
        only_tables=["sabot.test_ddl", "sabot.test_ddl_renamed"],  # Track both old and new names
        track_ddl=True,  # Enable DDL tracking
        emit_ddl_events=True,  # Include DDL events in stream
        batch_size=10,
        max_poll_interval=2.0
    )

    connector = MySQLCDCConnector(config)
    schema_tracker = connector.get_schema_tracker()

    logger.info("Starting CDC stream with DDL tracking...")
    logger.info("Monitoring tables: sabot.test_ddl, sabot.test_ddl_renamed")
    logger.info("\nWaiting for DDL events...\n")

    event_count = 0
    ddl_count = 0

    try:
        async with connector:
            async for batch in connector.stream_batches():
                event_count += batch.num_rows

                # Convert to Python for display
                batch_dict = batch.to_pydict()

                for i in range(batch.num_rows):
                    event_type = batch_dict['event_type'][i]
                    schema = batch_dict['schema'][i]
                    table = batch_dict['table'][i]
                    timestamp = batch_dict['timestamp'][i]

                    if event_type == 'ddl':
                        ddl_count += 1
                        query = batch_dict['query'][i]
                        log_file = batch_dict['log_file'][i]
                        log_pos = batch_dict['log_pos'][i]

                        logger.info(f"\n{'='*70}")
                        logger.info(f"DDL Event #{ddl_count}")
                        logger.info(f"{'='*70}")
                        logger.info(f"Schema: {schema}")
                        logger.info(f"Timestamp: {timestamp}")
                        logger.info(f"Position: {log_file}:{log_pos}")
                        logger.info(f"Query: {query}")

                        # Show schema tracker status
                        if schema_tracker:
                            tracked_tables = schema_tracker.get_tracked_tables()
                            logger.info(f"\nSchema Tracker Status:")
                            logger.info(f"  Tracked tables: {tracked_tables}")

                            for db, tbl in tracked_tables:
                                cached_schema = schema_tracker.get_schema(db, tbl)
                                if cached_schema:
                                    logger.info(f"  {db}.{tbl}: version={cached_schema.schema_version}, " +
                                               f"columns={list(cached_schema.columns.keys())}")
                        logger.info(f"{'='*70}\n")

                    elif event_type in ('insert', 'update', 'delete'):
                        data = batch_dict['data'][i]
                        log_file = batch_dict['log_file'][i]
                        log_pos = batch_dict['log_pos'][i]

                        logger.info(f"\nData Event: {event_type.upper()}")
                        logger.info(f"  Table: {schema}.{table}")
                        logger.info(f"  Data: {data}")
                        logger.info(f"  Position: {log_file}:{log_pos}\n")

                # Stop after we've seen some DDL events
                if ddl_count >= 5:
                    logger.info("\n✅ Demo complete - seen enough DDL events")
                    break

    except KeyboardInterrupt:
        logger.info("\n\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"\nError: {e}", exc_info=True)
    finally:
        logger.info(f"\nTotal events: {event_count} (DDL: {ddl_count})")
        logger.info("\nDemo finished.")


async def main():
    """Run DDL tracking demo."""
    # Run DDL operations in background
    ddl_task = asyncio.create_task(run_ddl_operations())

    # Stream DDL events
    await stream_ddl_events()

    # Wait for DDL operations to complete
    try:
        await asyncio.wait_for(ddl_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
