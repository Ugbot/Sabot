#!/usr/bin/env python3
"""
MySQL CDC Per-Table Stream Routing Demo.

Demonstrates routing CDC events from different tables to separate streams:
- Orders → Order processing pipeline
- Users → User enrichment pipeline
- Events → Analytics pipeline

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
       python examples/mysql_cdc_routing_demo.py

The demo will:
- Route events from orders, users, and events tables
- Process each table's events separately
- Show routing statistics
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector
from sabot._cython.connectors.mysql_stream_router import MySQLStreamRouter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderProcessor:
    """Process order events."""

    def __init__(self):
        self.order_count = 0
        self.total_amount = 0.0

    async def process_batch(self, batch):
        """Process order batch."""
        batch_dict = batch.to_pydict()

        for i in range(batch.num_rows):
            event_type = batch_dict['event_type'][i]
            data = batch_dict['data'][i]

            if event_type == 'insert':
                self.order_count += 1
                # Parse amount from JSON data
                import json
                data_obj = json.loads(data) if isinstance(data, str) else data
                if 'amount' in data_obj:
                    self.total_amount += float(data_obj['amount'])

        logger.info(f"[ORDERS] Processed {batch.num_rows} events | " +
                   f"Total orders: {self.order_count}, Total amount: ${self.total_amount:.2f}")


class UserProcessor:
    """Process user events."""

    def __init__(self):
        self.user_count = 0
        self.users = set()

    async def process_batch(self, batch):
        """Process user batch."""
        batch_dict = batch.to_pydict()

        for i in range(batch.num_rows):
            event_type = batch_dict['event_type'][i]
            data = batch_dict['data'][i]

            if event_type == 'insert':
                import json
                data_obj = json.loads(data) if isinstance(data, str) else data
                if 'id' in data_obj:
                    self.users.add(data_obj['id'])
                self.user_count += 1

        logger.info(f"[USERS] Processed {batch.num_rows} events | " +
                   f"Total users: {len(self.users)}, Total events: {self.user_count}")


class EventProcessor:
    """Process analytics events."""

    def __init__(self):
        self.event_count = 0
        self.page_views = {}

    async def process_batch(self, batch):
        """Process event batch."""
        batch_dict = batch.to_pydict()

        for i in range(batch.num_rows):
            event_type = batch_dict['event_type'][i]
            data = batch_dict['data'][i]

            if event_type == 'insert':
                import json
                data_obj = json.loads(data) if isinstance(data, str) else data
                if 'page' in data_obj:
                    page = data_obj['page']
                    self.page_views[page] = self.page_views.get(page, 0) + 1
                self.event_count += 1

        logger.info(f"[EVENTS] Processed {batch.num_rows} events | " +
                   f"Total events: {self.event_count}, Pages tracked: {len(self.page_views)}")


async def insert_test_data():
    """Insert test data to generate CDC events."""
    import pymysql
    await asyncio.sleep(5)  # Wait for CDC to start

    logger.info("\n" + "="*70)
    logger.info("Inserting Test Data")
    logger.info("="*70 + "\n")

    connection = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="sabot",
        database="sabot"
    )

    try:
        with connection.cursor() as cursor:
            # Insert orders
            logger.info("Inserting orders...")
            for i in range(5):
                cursor.execute("""
                    INSERT INTO orders (customer_id, product, amount)
                    VALUES (%s, %s, %s)
                """, (i % 3 + 1, f"Product-{i}", 99.99 + i * 10))
            connection.commit()
            await asyncio.sleep(2)

            # Insert users
            logger.info("Inserting users...")
            for i in range(3):
                cursor.execute("""
                    INSERT INTO users (name, email)
                    VALUES (%s, %s)
                """, (f"User-{i}", f"user{i}@example.com"))
            connection.commit()
            await asyncio.sleep(2)

            # Insert events
            logger.info("Inserting events...")
            for i in range(10):
                cursor.execute("""
                    INSERT INTO events_click (user_id, page)
                    VALUES (%s, %s)
                """, (i % 3 + 1, f"/page-{i % 4}"))
            connection.commit()
            await asyncio.sleep(2)

            logger.info("Test data insertion complete\n")

    finally:
        connection.close()


async def stream_with_routing():
    """Stream CDC events with per-table routing."""
    logger.info("\n" + "="*70)
    logger.info("MySQL CDC Per-Table Routing Demo")
    logger.info("="*70 + "\n")

    # Auto-configure CDC connector
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

    # Create stream router
    router = MySQLStreamRouter()
    router.add_route("sabot", "orders", output_key="orders")
    router.add_route("sabot", "users", output_key="users")
    router.add_route("sabot", "events_click", output_key="events")

    # Create processors
    processors = {
        "orders": OrderProcessor(),
        "users": UserProcessor(),
        "events": EventProcessor()
    }

    logger.info("Starting CDC with table routing...")
    logger.info(f"Routes configured:")
    for route in router.get_routes():
        logger.info(f"  {route.table_spec} -> {route.output_key}")
    logger.info("\n")

    total_events = 0
    route_stats = {key: 0 for key in processors.keys()}

    try:
        async with connector:
            async for batch in connector.stream_batches():
                total_events += batch.num_rows

                # Route batch to separate streams
                routed_batches = router.route_batch(batch)

                # Process each route
                for output_key, routed_batch in routed_batches.items():
                    if output_key in processors:
                        route_stats[output_key] += routed_batch.num_rows
                        await processors[output_key].process_batch(routed_batch)
                    elif output_key == 'default':
                        logger.warning(f"Unrouted events: {routed_batch.num_rows} rows")

                # Stop after processing enough events
                if total_events >= 20:
                    logger.info("\n✅ Demo complete - processed enough events")
                    break

    except KeyboardInterrupt:
        logger.info("\n\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"\nError: {e}", exc_info=True)
    finally:
        logger.info("\n" + "="*70)
        logger.info("Final Statistics")
        logger.info("="*70)
        logger.info(f"Total events processed: {total_events}")
        logger.info(f"\nRoute statistics:")
        for key, count in route_stats.items():
            logger.info(f"  {key}: {count} events")

        logger.info(f"\nProcessor statistics:")
        logger.info(f"  Orders: {processors['orders'].order_count} orders, " +
                   f"${processors['orders'].total_amount:.2f} total")
        logger.info(f"  Users: {len(processors['users'].users)} unique users, " +
                   f"{processors['users'].user_count} events")
        logger.info(f"  Events: {processors['events'].event_count} clicks, " +
                   f"{len(processors['events'].page_views)} pages")
        logger.info("="*70 + "\n")


async def main():
    """Run routing demo."""
    # Insert test data in background
    data_task = asyncio.create_task(insert_test_data())

    # Stream with routing
    await stream_with_routing()

    # Wait for data insertion to complete
    try:
        await asyncio.wait_for(data_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
