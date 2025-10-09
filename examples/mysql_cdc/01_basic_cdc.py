#!/usr/bin/env python3
"""
01: Basic MySQL CDC Example.

Demonstrates the fundamentals of MySQL Change Data Capture:
- Connecting to MySQL binlog
- Streaming INSERT/UPDATE/DELETE events
- Processing events in real-time
- Basic error handling

Setup:
    1. Start MySQL:
       docker compose up -d mysql

    2. Create test table:
       docker compose exec mysql mysql -u root -psabot -e "
           USE sabot;
           CREATE TABLE IF NOT EXISTS products (
               id INT PRIMARY KEY AUTO_INCREMENT,
               name VARCHAR(100),
               price DECIMAL(10,2),
               stock INT DEFAULT 0,
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
           );
       "

    3. Run this example:
       python examples/mysql_cdc/01_basic_cdc.py

    4. In another terminal, insert data:
       docker compose exec mysql mysql -u root -psabot sabot -e "
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

from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector, MySQLCDCConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def process_cdc_events():
    """
    Process CDC events from MySQL binlog.

    This function demonstrates:
    1. Configuring CDC connector
    2. Connecting to MySQL binlog
    3. Streaming events
    4. Processing different event types
    5. Graceful shutdown
    """
    logger.info("="*70)
    logger.info("Basic MySQL CDC Example")
    logger.info("="*70)

    # Step 1: Configure CDC Connector
    config = MySQLCDCConfig(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot",
        only_tables=["sabot.products"],  # Monitor only products table
        batch_size=10,  # Process up to 10 events per batch
        max_poll_interval=2.0,  # Wait max 2 seconds for new data
    )

    connector = MySQLCDCConnector(config)

    logger.info("\n✅ CDC Connector configured")
    logger.info(f"   Host: {config.host}:{config.port}")
    logger.info(f"   Database: {config.database}")
    logger.info(f"   Tables: {config.only_tables}")
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
                    log_file = batch_dict['log_file'][i]
                    log_pos = batch_dict['log_pos'][i]

                    # Step 4: Process based on event type
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Event #{event_count}: {event_type.upper()}")
                    logger.info(f"{'='*60}")
                    logger.info(f"Table: {schema}.{table}")
                    logger.info(f"Time: {timestamp}")
                    logger.info(f"Position: {log_file}:{log_pos}")

                    if event_type == 'insert':
                        # New record created
                        data = json.loads(data_json)
                        logger.info(f"\n✨ New Record Created:")
                        for key, value in data.items():
                            logger.info(f"   {key}: {value}")

                    elif event_type == 'update':
                        # Record updated
                        old_data = json.loads(old_data_json)
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

                    elif event_type == 'ddl':
                        # Schema change
                        query = batch_dict['query'][i]
                        logger.info(f"\n🔧 Schema Change:")
                        logger.info(f"   Query: {query}")

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
