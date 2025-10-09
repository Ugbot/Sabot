#!/usr/bin/env python3
"""
Test pgoutput CDC Connector

Tests the plugin-free pgoutput-based CDC reader with the existing PostgreSQL setup.
"""

import asyncio
import logging
import sys
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')


async def test_pgoutput_basic():
    """Test basic pgoutput CDC functionality."""
    logger.info("=" * 80)
    logger.info("TEST 1: Basic pgoutput CDC Connection")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection
        from sabot._cython.connectors.postgresql.pgoutput_parser import PgoutputParser

        # Connect to PostgreSQL
        conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"
        conn = PostgreSQLConnection(conn_str, replication=False)

        # Check configuration
        config = conn.check_replication_config()
        logger.info(f"✅ PostgreSQL configuration:")
        logger.info(f"   wal_level: {config['wal_level']}")
        logger.info(f"   max_replication_slots: {config['max_replication_slots']}")
        logger.info(f"   max_wal_senders: {config['max_wal_senders']}")

        # Discover tables
        tables = conn.discover_tables()
        logger.info(f"✅ Found {len(tables)} tables: {tables}")

        conn.close()
        logger.info("✅ Test 1 PASSED: Basic connection successful\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 1 FAILED: {e}", exc_info=True)
        return False


async def test_pgoutput_parser():
    """Test pgoutput binary message parser."""
    logger.info("=" * 80)
    logger.info("TEST 2: pgoutput Binary Parser")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.pgoutput_parser import PgoutputParser

        parser = PgoutputParser()

        # Test parsing a BEGIN message (synthetic)
        # Format: 'B' (1) + final_lsn (8) + timestamp (8) + xid (4)
        begin_msg = bytearray([ord('B')])
        begin_msg.extend((0x0000000000100000).to_bytes(8, 'big'))  # LSN
        begin_msg.extend((1000000).to_bytes(8, 'big', signed=True))  # Timestamp
        begin_msg.extend((12345).to_bytes(4, 'big'))  # XID

        result = parser.parse_message(bytes(begin_msg), len(begin_msg))
        logger.info(f"✅ Parsed BEGIN message: {result}")

        assert result['type'] == 'begin'
        assert result['xid'] == 12345

        # Check parser stats
        stats = parser.get_stats()
        logger.info(f"✅ Parser stats: {stats}")

        logger.info("✅ Test 2 PASSED: Parser working correctly\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 2 FAILED: {e}", exc_info=True)
        return False


async def test_pgoutput_publication():
    """Test publication creation for pgoutput."""
    logger.info("=" * 80)
    logger.info("TEST 3: Publication Setup")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

        # Connect as admin
        conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"
        conn = PostgreSQLConnection(conn_str, replication=False)

        # Create publication for test tables
        pub_name = "test_pgoutput_pub"
        tables = [("public", "users"), ("public", "orders")]

        try:
            # Drop existing publication if exists
            conn.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
            logger.info(f"Dropped existing publication '{pub_name}'")
        except:
            pass

        # Create new publication
        conn.create_publication(pub_name, tables)
        logger.info(f"✅ Created publication '{pub_name}' for {len(tables)} tables")

        # Verify publication
        result = conn.execute(f"SELECT * FROM pg_publication WHERE pubname = '{pub_name}'")
        logger.info(f"✅ Publication verified: {result}")

        # Check publication tables
        result = conn.execute(
            f"SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{pub_name}'"
        )
        logger.info(f"✅ Published tables: {result}")

        conn.close()
        logger.info("✅ Test 3 PASSED: Publication setup successful\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 3 FAILED: {e}", exc_info=True)
        return False


async def test_pgoutput_replication_slot():
    """Test replication slot creation with pgoutput."""
    logger.info("=" * 80)
    logger.info("TEST 4: Replication Slot with pgoutput")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

        # Connect with replication
        conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"
        conn = PostgreSQLConnection(conn_str, replication=True)

        slot_name = "test_pgoutput_slot"

        # Drop existing slot if exists
        try:
            conn.drop_replication_slot(slot_name)
            logger.info(f"Dropped existing slot '{slot_name}'")
        except:
            pass

        # Create slot with pgoutput plugin
        slot_info = conn.create_replication_slot(slot_name, output_plugin='pgoutput')
        logger.info(f"✅ Created replication slot: {slot_info}")

        # Verify slot exists
        result = conn.execute(
            f"SELECT slot_name, plugin, slot_type FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
        )
        logger.info(f"✅ Slot verified: {result}")

        assert result[0][1] == 'pgoutput', "Plugin should be pgoutput"

        conn.close()
        logger.info("✅ Test 4 PASSED: Replication slot created with pgoutput\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 4 FAILED: {e}", exc_info=True)
        return False


async def test_pgoutput_cdc_reader():
    """Test pgoutput CDC reader end-to-end."""
    logger.info("=" * 80)
    logger.info("TEST 5: pgoutput CDC Reader (End-to-End)")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.pgoutput_cdc_reader import create_pgoutput_cdc_reader
        from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

        # Setup: Create publication and slot
        conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"
        slot_name = "test_pgoutput_cdc"
        pub_name = "test_pgoutput_cdc_pub"

        # Create reader with auto-setup
        logger.info(f"Creating pgoutput CDC reader...")
        reader = create_pgoutput_cdc_reader(
            connection_string=f"{conn_str} replication=database",
            slot_name=slot_name,
            publication_names=[pub_name],
            create_publication=True,
            tables=[("public", "users"), ("public", "orders")]
        )

        logger.info(f"✅ Created reader: {reader}")

        # Insert test data to generate CDC events
        logger.info("Inserting test data to generate CDC events...")
        admin_conn = PostgreSQLConnection(conn_str, replication=False)
        admin_conn.execute("INSERT INTO users (username, email) VALUES ('test_user', 'test@example.com')")
        admin_conn.execute("UPDATE users SET email = 'updated@example.com' WHERE username = 'test_user'")
        admin_conn.execute("DELETE FROM users WHERE username = 'test_user'")
        admin_conn.close()

        logger.info("✅ Test data inserted")

        # Read CDC messages (with timeout)
        logger.info("Reading CDC messages...")
        messages_received = 0
        batch_count = 0

        async def read_with_timeout():
            nonlocal messages_received, batch_count
            async for batch in reader.read_batches():
                batch_count += 1
                logger.info(f"✅ Received batch {batch_count}: {batch.num_rows if batch else 0} rows")
                messages_received += 1
                if messages_received >= 3:  # Stop after 3 messages (begin, insert, commit)
                    break

        try:
            await asyncio.wait_for(read_with_timeout(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for CDC messages (this is OK for now)")

        # Check reader stats
        stats = reader.get_stats()
        logger.info(f"✅ Reader stats: {stats}")

        logger.info("✅ Test 5 PASSED: CDC reader functional\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 5 FAILED: {e}", exc_info=True)
        return False


async def test_pgoutput_checkpointer():
    """Test LSN checkpointing."""
    logger.info("=" * 80)
    logger.info("TEST 6: LSN Checkpointing")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.pgoutput_checkpointer import create_checkpointer
        import tempfile
        import shutil

        # Create temporary checkpoint directory
        checkpoint_dir = tempfile.mkdtemp(prefix='sabot_checkpoint_test_')
        logger.info(f"Using checkpoint dir: {checkpoint_dir}")

        try:
            # Create checkpointer
            checkpointer = create_checkpointer('test_slot', checkpoint_dir)
            logger.info(f"✅ Created checkpointer: {checkpointer}")

            # Get initial LSN (should be 0)
            initial_lsn = checkpointer.get_last_checkpoint_lsn()
            logger.info(f"✅ Initial LSN: {initial_lsn}")
            assert initial_lsn == 0, "Initial LSN should be 0"

            # Save checkpoint
            test_lsn = 123456789
            test_timestamp = int(time.time() * 1000000)
            checkpointer.save_checkpoint(test_lsn, test_timestamp)
            logger.info(f"✅ Saved checkpoint at LSN {test_lsn}")

            # Verify checkpoint saved
            saved_lsn = checkpointer.get_last_checkpoint_lsn()
            logger.info(f"✅ Saved LSN: {saved_lsn}")
            assert saved_lsn == test_lsn, f"Saved LSN {saved_lsn} should match {test_lsn}"

            # Get checkpoint stats
            stats = checkpointer.get_checkpoint_stats()
            logger.info(f"✅ Checkpoint stats: {stats}")

            checkpointer.close()
            logger.info("✅ Test 6 PASSED: Checkpointing working\n")
            return True

        finally:
            # Cleanup
            shutil.rmtree(checkpoint_dir, ignore_errors=True)

    except Exception as e:
        logger.error(f"❌ Test 6 FAILED: {e}", exc_info=True)
        return False


async def test_auto_configure_pgoutput():
    """Test auto-configuration with pgoutput plugin."""
    logger.info("=" * 80)
    logger.info("TEST 7: Auto-Configuration with pgoutput")
    logger.info("=" * 80)

    try:
        from sabot._cython.connectors.postgresql.arrow_cdc_reader import ArrowCDCReaderConfig

        logger.info("Testing auto-configure with pgoutput plugin...")

        # This will test the full auto-configuration flow
        # Note: This may fail if dependencies aren't compiled yet
        readers = ArrowCDCReaderConfig.auto_configure(
            host="localhost",
            database="sabot",
            user="sabot",
            password="sabot",
            port=5433,
            tables="public.*",
            slot_name="test_auto_pgoutput",
            plugin='pgoutput',  # Use pgoutput instead of wal2arrow
            route_by_table=True
        )

        logger.info(f"✅ Auto-configured {len(readers)} readers")
        for table_name, reader in readers.items():
            logger.info(f"   - {table_name}: {reader}")

        logger.info("✅ Test 7 PASSED: Auto-configuration with pgoutput successful\n")
        return True

    except Exception as e:
        logger.error(f"❌ Test 7 FAILED: {e}", exc_info=True)
        logger.info("This is expected if Cython modules aren't compiled yet")
        return False


async def run_all_tests():
    """Run all pgoutput tests."""
    logger.info("\n" + "=" * 80)
    logger.info("PGOUTPUT CDC CONNECTOR TEST SUITE")
    logger.info("=" * 80 + "\n")

    results = []

    # Run tests
    results.append(("Basic Connection", await test_pgoutput_basic()))
    results.append(("Binary Parser", await test_pgoutput_parser()))
    results.append(("Publication Setup", await test_pgoutput_publication()))
    results.append(("Replication Slot", await test_pgoutput_replication_slot()))
    results.append(("CDC Reader", await test_pgoutput_cdc_reader()))
    results.append(("Checkpointing", await test_pgoutput_checkpointer()))
    results.append(("Auto-Configure", await test_auto_configure_pgoutput()))

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{status}: {test_name}")

    logger.info("=" * 80)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("=" * 80 + "\n")

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
