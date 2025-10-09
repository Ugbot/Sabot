#!/usr/bin/env python3
"""
MySQL CDC Auto-Configuration Demo.

Demonstrates automatic MySQL CDC setup with:
- Configuration validation
- Wildcard table selection
- Automatic table discovery
- User creation (optional)

Setup:
    1. Start MySQL with binlog enabled:
       docker compose up mysql -d

    2. Create test tables:
       docker compose exec mysql mysql -u root -psabot -e "
           USE sabot;

           -- Orders table
           CREATE TABLE IF NOT EXISTS orders (
               id INT PRIMARY KEY AUTO_INCREMENT,
               customer_id INT NOT NULL,
               product VARCHAR(100),
               amount DECIMAL(10,2),
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );

           -- Users table
           CREATE TABLE IF NOT EXISTS users (
               id INT PRIMARY KEY AUTO_INCREMENT,
               name VARCHAR(100),
               email VARCHAR(100),
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );

           -- Events table
           CREATE TABLE IF NOT EXISTS events_click (
               id INT PRIMARY KEY AUTO_INCREMENT,
               user_id INT,
               page VARCHAR(200),
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
       "

    3. Run this demo:
       python examples/mysql_cdc_auto_demo.py

The demo will:
- Validate MySQL configuration
- Discover tables matching patterns
- Stream CDC events
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector
from sabot._cython.connectors.mysql_auto_config import MySQLAutoConfigurator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def validate_mysql_configuration():
    """Validate MySQL configuration before starting CDC."""
    logger.info("="*70)
    logger.info("Step 1: Validating MySQL Configuration")
    logger.info("="*70 + "\n")

    configurator = MySQLAutoConfigurator(
        host="localhost",
        port=3307,
        admin_user="root",
        admin_password="sabot"
    )

    issues = configurator.validate_configuration()

    if issues:
        configurator.print_fix_instructions(issues)

        # Check for errors
        from sabot._cython.connectors.mysql_auto_config import Severity
        errors = [i for i in issues if i.severity == Severity.ERROR]
        if errors:
            logger.error("\n❌ MySQL has configuration errors. Please fix and retry.")
            return False
    else:
        logger.info("✅ MySQL is properly configured for CDC\n")

    return True


async def discover_tables():
    """Discover tables matching patterns."""
    logger.info("="*70)
    logger.info("Step 2: Discovering Tables with Patterns")
    logger.info("="*70 + "\n")

    from sabot._cython.connectors.mysql_discovery import MySQLTableDiscovery

    discovery = MySQLTableDiscovery(
        host="localhost",
        port=3307,
        user="root",
        password="sabot"
    )

    # Discover all tables in sabot database
    logger.info("Pattern: sabot.*")
    all_tables = discovery.discover_tables(database="sabot")
    logger.info(f"Found {len(all_tables)} tables: {all_tables}\n")

    # Test wildcard patterns
    patterns = [
        "sabot.orders",         # Single table
        "sabot.events_*",       # Wildcard
    ]

    logger.info(f"Expanding patterns: {patterns}")
    matched = discovery.expand_patterns(patterns, database="sabot")
    logger.info(f"Matched {len(matched)} tables: {matched}\n")

    return matched


async def stream_cdc_events():
    """Stream CDC events with auto-configuration."""
    logger.info("="*70)
    logger.info("Step 3: Streaming CDC Events (Auto-Configured)")
    logger.info("="*70 + "\n")

    # Auto-configure CDC with wildcard patterns
    logger.info("Auto-configuring CDC connector...")
    connector = MySQLCDCConnector.auto_configure(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot",
        table_patterns=["sabot.orders", "sabot.users", "sabot.events_*"],
        validate_config=True,
        batch_size=10,
        max_poll_interval=2.0
    )

    logger.info("✅ CDC connector auto-configured")
    logger.info(f"Monitoring tables: {connector.config.only_tables}")
    logger.info("\nWaiting for CDC events... (Press Ctrl+C to stop)")
    logger.info("Insert/update/delete data to see events:\n")
    logger.info("  docker compose exec mysql mysql -u root -psabot sabot -e \"")
    logger.info("      INSERT INTO orders (customer_id, product, amount)")
    logger.info("      VALUES (1, 'Laptop', 999.99);")
    logger.info("  \"\n")

    event_count = 0

    try:
        async with connector:
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

                    logger.info(f"\nEvent #{event_count - batch.num_rows + i + 1}:")
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

                # Show status
                status = connector.get_status()
                logger.info(f"\nStatus: Running={status['running']}, " +
                           f"Position={status['last_log_file']}:{status['last_log_pos']}\n")

    except KeyboardInterrupt:
        logger.info("\n\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"\nError: {e}", exc_info=True)
    finally:
        logger.info(f"\nTotal events processed: {event_count}")


async def main():
    """Run MySQL CDC auto-configuration demo."""
    logger.info("\n" + "="*70)
    logger.info("MySQL CDC Auto-Configuration Demo")
    logger.info("="*70 + "\n")

    # Step 1: Validate configuration
    if not await validate_mysql_configuration():
        return

    # Step 2: Discover tables
    await discover_tables()

    # Step 3: Stream events
    await stream_cdc_events()


if __name__ == "__main__":
    asyncio.run(main())
