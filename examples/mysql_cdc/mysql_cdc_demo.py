#!/usr/bin/env python3
"""
MySQL CDC Demo for Sabot.

Demonstrates Change Data Capture from MySQL using binlog replication.

Setup:
    1. Start MySQL with binlog enabled:
       docker compose up mysql -d

    2. Create test table and insert data:
       docker compose exec mysql mysql -u root -psabot -e "
           USE sabot;
           CREATE TABLE orders (
               id INT PRIMARY KEY AUTO_INCREMENT,
               customer_id INT NOT NULL,
               product VARCHAR(100),
               amount DECIMAL(10,2),
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
       "

    3. Run this demo:
       python examples/mysql_cdc_demo.py

    4. In another terminal, insert/update/delete data:
       docker compose exec mysql mysql -u root -psabot sabot -e "
           INSERT INTO orders (customer_id, product, amount)
           VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99);
       "

The demo will print CDC events in real-time.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot._cython.connectors.mysql_cdc import MySQLCDCConfig, MySQLCDCConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Run MySQL CDC demo."""
    logger.info("Starting MySQL CDC demo...")

    # Configure CDC connector
    config = MySQLCDCConfig(
        host="localhost",
        port=3307,  # Docker port
        user="root",
        password="sabot",
        database="sabot",  # Monitor only 'sabot' database
        server_id=1000001,  # Unique replication client ID
        only_tables=["sabot.orders"],  # Monitor only 'orders' table
        batch_size=10,
        max_poll_interval=2.0,
    )

    # Create connector
    connector = MySQLCDCConnector(config)

    logger.info(f"Connecting to MySQL: {config.host}:{config.port}")
    logger.info(f"Monitoring database: {config.database}")
    logger.info(f"Monitoring tables: {config.only_tables}")
    logger.info("Waiting for CDC events... (Press Ctrl+C to stop)")
    logger.info("")

    try:
        async with connector:
            event_count = 0

            # Stream CDC events as Arrow batches
            async for batch in connector.stream_batches():
                event_count += batch.num_rows

                logger.info(f"=== Received CDC Batch ({batch.num_rows} events) ===")

                # Convert to Python for display
                batch_dict = batch.to_pydict()

                for i in range(batch.num_rows):
                    event_type = batch_dict['event_type'][i]
                    schema = batch_dict['schema'][i]
                    table = batch_dict['table'][i]
                    timestamp = batch_dict['timestamp'][i]
                    data = batch_dict['data'][i]
                    old_data = batch_dict['old_data'][i]
                    log_file = batch_dict['log_file'][i]
                    log_pos = batch_dict['log_pos'][i]

                    logger.info(f"Event #{event_count - batch.num_rows + i + 1}:")
                    logger.info(f"  Type: {event_type}")
                    logger.info(f"  Table: {schema}.{table}")
                    logger.info(f"  Timestamp: {timestamp}")
                    logger.info(f"  Position: {log_file}:{log_pos}")

                    if event_type == 'insert':
                        logger.info(f"  New Data: {data}")
                    elif event_type == 'update':
                        logger.info(f"  Old Data: {old_data}")
                        logger.info(f"  New Data: {data}")
                    elif event_type == 'delete':
                        logger.info(f"  Deleted Data: {data}")
                    elif event_type == 'ddl':
                        query = batch_dict['query'][i]
                        logger.info(f"  Query: {query}")

                    logger.info("")

                # Show connector status
                status = connector.get_status()
                logger.info(f"Status: {status}")
                logger.info("")

    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        logger.info(f"Total events processed: {event_count}")


if __name__ == "__main__":
    asyncio.run(main())
