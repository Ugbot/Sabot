#!/usr/bin/env python3
"""
01: Basic PostgreSQL CDC Example.

Demonstrates the fundamentals of PostgreSQL Change Data Capture:
- Connecting to PostgreSQL logical replication
- Streaming INSERT/UPDATE/DELETE events
- Processing events in real-time
- Basic error handling

Setup:
    1. Start PostgreSQL:
       docker compose up -d postgres

    2. Initialize CDC:
       docker compose exec postgres psql -U sabot -d sabot -f /docker-entrypoint-initdb.d/01_init_cdc.sql

    3. Create test table:
       docker compose exec postgres psql -U sabot -d sabot -c "
           CREATE TABLE IF NOT EXISTS products (
               id SERIAL PRIMARY KEY,
               name VARCHAR(100),
               price DECIMAL(10,2),
               stock INT DEFAULT 0,
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
       "

    4. Run this example:
       python examples/postgresql_cdc/01_basic_cdc.py

    5. In another terminal, insert data:
       docker compose exec postgres psql -U sabot -d sabot -c "
           INSERT INTO products (name, price, stock) VALUES
               ('Laptop', 999.99, 10),
               ('Mouse', 29.99, 50),
               ('Keyboard', 79.99, 25);

           UPDATE products SET stock = stock - 1 WHERE name = 'Laptop';
           DELETE FROM products WHERE name = 'Mouse';
       "
"""

import asyncio
import logging
import sys
from pathlib import Path
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot._cython.connectors.postgresql import (
    PostgreSQLCDCConnector,
    PostgreSQLCDCConfig
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def process_cdc_events():
    """
    Process CDC events from PostgreSQL logical replication.

    This function demonstrates:
    1. Configuring CDC connector
    2. Connecting to PostgreSQL logical replication
    3. Streaming events
    4. Processing different event types
    5. Graceful shutdown
    """
    logger.info("="*70)
    logger.info("Basic PostgreSQL CDC Example")
    logger.info("="*70)

    # Step 1: Configure CDC Connector
    config = PostgreSQLCDCConfig(
        host="localhost",
        port=5433,
        user="sabot",
        password="sabot",
        database="sabot",
        slot_name="sabot_cdc_slot",
        publication_name="sabot_cdc",
        tables=["public.products"],  # Monitor only products table
        batch_size=10,  # Process up to 10 events per batch
        max_poll_interval=2.0,  # Wait max 2 seconds for new data
    )

    connector = PostgreSQLCDCConnector(config)

    logger.info("\n✅ CDC Connector configured")
    logger.info(f"   Host: {config.host}:{config.port}")
    logger.info(f"   Database: {config.database}")
    logger.info(f"   Slot: {config.slot_name}")
    logger.info(f"   Publication: {config.publication_name}")
    logger.info(f"   Tables: {config.tables}")
    logger.info("\n📡 Starting CDC stream...")
    logger.info("   Waiting for events... (Press Ctrl+C to stop)\n")

    event_count = 0

    try:
        # Step 2: Start streaming
        async with connector:
            async for batch in connector.stream_batches():
                # Step 3: Process each batch
                batch_dict = batch.to_pydict()

                for i in range(batch.num_rows):
                    event_count += 1

                    # Extract event details
                    event_type = batch_dict['event_type'][i]
                    schema = batch_dict['schema'][i]
                    table = batch_dict['table'][i]
                    timestamp = batch_dict['timestamp'][i]
                    data_json = batch_dict['data'][i]
                    old_data_json = batch_dict['old_data'][i]
                    lsn = batch_dict['lsn'][i]

                    # Step 4: Process based on event type
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Event #{event_count}: {event_type.upper()}")
                    logger.info(f"{'='*60}")
                    logger.info(f"Table: {schema}.{table}")
                    logger.info(f"Time: {timestamp}")
                    logger.info(f"LSN: {lsn}")

                    if event_type == 'insert':
                        # New record created
                        data = json.loads(data_json)
                        logger.info(f"\n✨ New Record Created:")
                        for key, value in data.items():
                            logger.info(f"   {key}: {value}")

                    elif event_type == 'update':
                        # Record updated
                        old_data = json.loads(old_data_json) if old_data_json else {}
                        new_data = json.loads(data_json)

                        logger.info(f"\n🔄 Record Updated:")
                        logger.info(f"   Changed fields:")
                        for key in new_data.keys():
                            if old_data.get(key) != new_data.get(key):
                                logger.info(f"      {key}: {old_data.get(key)} → {new_data.get(key)}")

                    elif event_type == 'delete':
                        # Record deleted
                        data = json.loads(data_json)
                        logger.info(f"\n🗑️  Record Deleted:")
                        for key, value in data.items():
                            logger.info(f"   {key}: {value}")

    except KeyboardInterrupt:
        logger.info("\n\n⏹️  Stopping CDC stream...")
    except Exception as e:
        logger.error(f"\n❌ Error: {e}", exc_info=True)
    finally:
        logger.info(f"\n{'='*70}")
        logger.info(f"Summary")
        logger.info(f"{'='*70}")
        logger.info(f"Total events processed: {event_count}")
        logger.info(f"{'='*70}\n")


async def main():
    """Main entry point."""
    await process_cdc_events()


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
