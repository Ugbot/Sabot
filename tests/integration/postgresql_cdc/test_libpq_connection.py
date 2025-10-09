#!/usr/bin/env python3
"""
Test libpq PostgreSQL CDC connection.

Tests basic replication connectivity and slot management.
Since wal2arrow plugin isn't built yet, we'll use wal2json to verify the plumbing works.
"""

import asyncio
import logging
import sys

# Add Sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection, PostgreSQLError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_basic_connection():
    """Test basic PostgreSQL connection."""
    logger.info("Testing basic PostgreSQL connection...")

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=cdc_user password=cdc_password",
        replication=False
    )

    try:
        # Execute simple query
        result = conn.execute("SELECT version()")
        logger.info(f"✅ Connected to PostgreSQL: {result}")

        # Check tables
        result = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        logger.info(f"✅ Tables in database: {result}")

        return True

    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False
    finally:
        conn.close()


async def test_replication_connection():
    """Test replication connection setup."""
    logger.info("Testing replication connection...")

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=cdc_user password=cdc_password replication=database",
        replication=True
    )

    try:
        # Test replication connection
        logger.info("✅ Replication connection established")

        # List existing replication slots
        result = conn.execute("SELECT slot_name, plugin, slot_type FROM pg_replication_slots")
        logger.info(f"📋 Existing replication slots: {result}")

        return True

    except Exception as e:
        logger.error(f"❌ Replication connection failed: {e}")
        return False
    finally:
        conn.close()


async def test_replication_slot_management():
    """Test creating and dropping replication slots."""
    logger.info("Testing replication slot management...")

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=cdc_user password=cdc_password replication=database",
        replication=True
    )

    slot_name = "test_slot"

    try:
        # Drop slot if exists
        try:
            conn.drop_replication_slot(slot_name)
            logger.info(f"🗑️  Dropped existing slot '{slot_name}'")
        except PostgreSQLError:
            pass  # Slot doesn't exist

        # Create slot with wal2json (available in standard PostgreSQL)
        conn.create_replication_slot(slot_name, 'wal2json')
        logger.info(f"✅ Created replication slot '{slot_name}' with wal2json plugin")

        # Verify slot exists
        result = conn.execute(
            f"SELECT slot_name, plugin, active FROM pg_replication_slots WHERE slot_name='{slot_name}'"
        )
        logger.info(f"✅ Slot verified: {result}")

        # Drop slot
        conn.drop_replication_slot(slot_name)
        logger.info(f"✅ Dropped replication slot '{slot_name}'")

        return True

    except Exception as e:
        logger.error(f"❌ Slot management failed: {e}", exc_info=True)
        return False
    finally:
        conn.close()


async def test_logical_replication_stream():
    """Test streaming logical replication data (with wal2json)."""
    logger.info("Testing logical replication streaming...")

    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=cdc_user password=cdc_password replication=database",
        replication=True
    )

    slot_name = "test_stream_slot"

    try:
        # Create slot
        try:
            conn.drop_replication_slot(slot_name)
        except PostgreSQLError:
            pass

        conn.create_replication_slot(slot_name, 'wal2json')
        logger.info(f"✅ Created slot '{slot_name}'")

        # Start replication stream
        logger.info("📡 Starting replication stream...")
        logger.info("   (Will timeout after 5 seconds if no data)")

        batch_count = 0

        try:
            async for msg in conn.start_replication(slot_name):
                batch_count += 1
                logger.info(
                    f"📨 Received message #{batch_count}: "
                    f"{len(msg.data)} bytes at LSN {msg.wal_end}"
                )

                # Show first 200 chars of JSON data
                if msg.data:
                    data_preview = msg.data[:200].decode('utf-8', errors='ignore')
                    logger.info(f"   Data preview: {data_preview}...")

                # Stop after 3 messages or timeout
                if batch_count >= 3:
                    logger.info("✅ Successfully received replication messages")
                    break

        except asyncio.TimeoutError:
            if batch_count == 0:
                logger.info("⏱️  No CDC events (database idle)")
            else:
                logger.info(f"✅ Received {batch_count} messages before timeout")

        # Clean up slot
        conn.drop_replication_slot(slot_name)

        return True

    except Exception as e:
        logger.error(f"❌ Streaming failed: {e}", exc_info=True)
        return False
    finally:
        conn.close()


async def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("PostgreSQL CDC Connection Tests")
    logger.info("=" * 60)

    tests = [
        ("Basic Connection", test_basic_connection),
        ("Replication Connection", test_replication_connection),
        ("Slot Management", test_replication_slot_management),
        ("Logical Replication Stream", test_logical_replication_stream),
    ]

    results = []

    for test_name, test_func in tests:
        logger.info("")
        logger.info(f"▶️  Running: {test_name}")
        logger.info("-" * 60)

        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"❌ Test '{test_name}' crashed: {e}", exc_info=True)
            results.append((test_name, False))

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Test Results Summary")
    logger.info("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status}: {test_name}")

    logger.info("")
    logger.info(f"Total: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed!")
        return 0
    else:
        logger.error(f"⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
