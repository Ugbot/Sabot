#!/usr/bin/env python3
"""
PostgreSQL CDC (Change Data Capture) Demo for Sabot

This example demonstrates how to use Sabot's PostgreSQL CDC connector to stream
database changes in real-time using logical replication with wal2json.

Requirements:
- PostgreSQL 10+ with logical replication enabled
- wal2json extension installed
- A database with some tables to monitor

Setup PostgreSQL for CDC:
```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Restart PostgreSQL
-- sudo systemctl restart postgresql

-- Create a user with replication privileges
CREATE USER cdc_user WITH PASSWORD 'cdc_password' REPLICATION;

-- Grant necessary permissions
GRANT CONNECT ON DATABASE your_database TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Install wal2json extension
CREATE EXTENSION wal2json;
```

Usage:
    python examples/postgres_cdc_demo.py
"""

import asyncio
import logging
from sabot import Stream

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_postgres_cdc():
    """
    Demonstrate PostgreSQL CDC streaming.

    This will connect to PostgreSQL and stream all changes from monitored tables.
    """
    print("🚀 Starting PostgreSQL CDC Demo")
    print("=" * 50)

    # Configuration - adjust these for your PostgreSQL setup
    config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'postgres',  # Change to your database
        'user': 'cdc_user',      # User with replication privileges
        'password': 'cdc_password',
        'replication_slot': 'sabot_cdc_demo',
        'table_filter': ['public.*'],  # Monitor all tables in public schema
        'batch_size': 10,        # Process events in small batches for demo
    }

    print(f"📡 Connecting to PostgreSQL: {config['host']}:{config['port']}/{config['database']}")
    print(f"🎯 Replication slot: {config['replication_slot']}")
    print(f"📋 Table filter: {config['table_filter']}")
    print()

    try:
        # Create CDC stream
        stream = Stream.from_postgres_cdc(**config)

        print("🎬 Streaming CDC events... (Press Ctrl+C to stop)")
        print("-" * 50)

        event_count = 0
        max_events = 50  # Limit for demo

        # Process the stream
        async for batch in stream:
            for event in batch.to_pylist():
                event_count += 1

                # Format event for display
                event_type = event['event_type'].upper()
                schema = event['schema']
                table = event['table']
                lsn = event.get('lsn', 'N/A')
                timestamp = event.get('timestamp', 'N/A')

                print(f"#{event_count} [{event_type}] {schema}.{table} @ {lsn}")

                # Show data based on event type
                if event_type == 'INSERT':
                    data = event.get('data', {})
                    print(f"  ➕ INSERTED: {list(data.keys()) if data else 'No data'}")
                    if data and len(str(data)) < 100:
                        print(f"     {data}")

                elif event_type == 'UPDATE':
                    old_data = event.get('old_data', {})
                    new_data = event.get('data', {})
                    print(f"  🔄 UPDATED: {list(new_data.keys()) if new_data else 'No data'}")
                    if old_data and len(str(old_data)) < 50:
                        print(f"     FROM: {old_data}")
                    if new_data and len(str(new_data)) < 50:
                        print(f"     TO:   {new_data}")

                elif event_type == 'DELETE':
                    key_data = event.get('key_data', {})
                    print(f"  ➖ DELETED: {list(key_data.keys()) if key_data else 'No key'}")
                    if key_data and len(str(key_data)) < 100:
                        print(f"     KEY: {key_data}")

                elif event_type == 'BEGIN':
                    print(f"  🏁 TRANSACTION BEGIN: {event.get('transaction_id', 'N/A')}")

                elif event_type == 'COMMIT':
                    print(f"  ✅ TRANSACTION COMMIT: {event.get('transaction_id', 'N/A')}")

                elif event_type == 'MESSAGE':
                    prefix = event.get('message_prefix', '')
                    content = event.get('message_content', '')
                    print(f"  💬 MESSAGE: {prefix}")
                    if len(content) < 100:
                        print(f"     {content}")

                print()

                # Stop after max events for demo
                if event_count >= max_events:
                    print(f"🎯 Demo complete: Processed {event_count} events")
                    return

    except KeyboardInterrupt:
        print("\n⏹️  Demo stopped by user")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is running and accessible")
        print("2. Verify wal2json extension is installed")
        print("3. Check user has replication privileges")
        print("4. Ensure logical replication is enabled (wal_level = logical)")
        raise


async def demo_with_filtering():
    """
    Demonstrate CDC with event filtering and processing.
    """
    print("🎯 PostgreSQL CDC with Filtering Demo")
    print("=" * 50)

    config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'cdc_user',
        'password': 'cdc_password',
        'replication_slot': 'sabot_filtered_cdc',
        'table_filter': ['public.users', 'public.orders'],  # Only these tables
        'batch_size': 5,
    }

    try:
        stream = Stream.from_postgres_cdc(**config)

        # Add filtering and processing
        processed_stream = (
            stream
            # Only process actual data changes (not transaction markers)
            .filter(lambda batch: batch.column('event_type').isin(['insert', 'update', 'delete']))
            # Add processing logic here
        )

        print("🎬 Processing filtered CDC events...")
        event_count = 0

        async for batch in processed_stream:
            for event in batch.to_pylist():
                event_count += 1
                print(f"📊 Processed event #{event_count}: {event['event_type']} on {event['schema']}.{event['table']}")

                if event_count >= 20:  # Shorter demo
                    return

    except Exception as e:
        print(f"❌ Filtered demo failed: {e}")


async def main():
    """Main demo function."""
    print("🐘 PostgreSQL CDC Connector Demo")
    print("This demo shows real-time change data capture from PostgreSQL")
    print()

    # Run basic demo
    await demo_postgres_cdc()

    print("\n" + "="*50)
    print("🎯 Trying filtered demo...")

    # Run filtered demo
    try:
        await demo_with_filtering()
    except Exception:
        print("Filtered demo skipped (requires specific table setup)")


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main())






