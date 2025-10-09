#!/usr/bin/env python3
"""
Integration tests for MySQL CDC connector.

Requires a running MySQL instance with binlog enabled.
Use: docker compose up mysql -d

These tests will:
1. Create test tables
2. Insert/update/delete data
3. Verify CDC events are captured correctly
"""

import pytest
import asyncio
import time
from datetime import datetime

# Skip if no MySQL connection available
try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

from sabot._cython.connectors.mysql_cdc import (
    MySQLCDCConfig,
    MySQLCDCConnector,
)


@pytest.mark.skipif(not MYSQL_AVAILABLE, reason="pymysql not available")
@pytest.mark.integration
class TestMySQLCDCIntegration:
    """Integration tests for MySQL CDC."""

    @pytest.fixture
    async def mysql_connection(self):
        """Create MySQL connection for test setup/teardown."""
        conn = pymysql.connect(
            host="localhost",
            port=3307,
            user="root",
            password="sabot",
            database="sabot",
        )
        try:
            yield conn
        finally:
            conn.close()

    @pytest.fixture
    async def clean_test_table(self, mysql_connection):
        """Create and clean up test table."""
        cursor = mysql_connection.cursor()

        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS cdc_test_users")

        # Create test table
        cursor.execute("""
            CREATE TABLE cdc_test_users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql_connection.commit()

        yield

        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS cdc_test_users")
        mysql_connection.commit()
        cursor.close()

    @pytest.mark.asyncio
    async def test_insert_events(self, mysql_connection, clean_test_table):
        """Test capturing INSERT events."""
        # Configure CDC
        config = MySQLCDCConfig(
            host="localhost",
            port=3307,
            user="root",
            password="sabot",
            database="sabot",
            only_tables=["sabot.cdc_test_users"],
            batch_size=10,
        )

        connector = MySQLCDCConnector(config)
        events_received = []

        async def consume_events():
            """Consume CDC events."""
            async with connector:
                async for batch in connector.stream_batches():
                    batch_dict = batch.to_pydict()
                    for i in range(batch.num_rows):
                        if batch_dict['event_type'][i] == 'insert':
                            events_received.append({
                                'event_type': batch_dict['event_type'][i],
                                'table': batch_dict['table'][i],
                                'data': batch_dict['data'][i],
                            })
                    if len(events_received) >= 2:
                        break

        # Start consuming in background
        consumer_task = asyncio.create_task(consume_events())

        # Wait for connector to start
        await asyncio.sleep(2)

        # Insert test data
        cursor = mysql_connection.cursor()
        cursor.execute("""
            INSERT INTO cdc_test_users (name, email, age)
            VALUES ('Alice', 'alice@example.com', 30)
        """)
        cursor.execute("""
            INSERT INTO cdc_test_users (name, email, age)
            VALUES ('Bob', 'bob@example.com', 25)
        """)
        mysql_connection.commit()
        cursor.close()

        # Wait for events
        await asyncio.wait_for(consumer_task, timeout=10)

        # Verify events
        assert len(events_received) == 2
        assert events_received[0]['event_type'] == 'insert'
        assert events_received[0]['table'] == 'cdc_test_users'
        assert 'Alice' in events_received[0]['data']
        assert 'Bob' in events_received[1]['data']

    @pytest.mark.asyncio
    async def test_update_events(self, mysql_connection, clean_test_table):
        """Test capturing UPDATE events."""
        # Insert initial data
        cursor = mysql_connection.cursor()
        cursor.execute("""
            INSERT INTO cdc_test_users (name, email, age)
            VALUES ('Charlie', 'charlie@example.com', 35)
        """)
        mysql_connection.commit()
        user_id = cursor.lastrowid
        cursor.close()

        # Configure CDC
        config = MySQLCDCConfig(
            host="localhost",
            port=3307,
            user="root",
            password="sabot",
            database="sabot",
            only_tables=["sabot.cdc_test_users"],
        )

        connector = MySQLCDCConnector(config)
        update_events = []

        async def consume_updates():
            """Consume UPDATE events."""
            async with connector:
                async for batch in connector.stream_batches():
                    batch_dict = batch.to_pydict()
                    for i in range(batch.num_rows):
                        if batch_dict['event_type'][i] == 'update':
                            update_events.append({
                                'event_type': batch_dict['event_type'][i],
                                'data': batch_dict['data'][i],
                                'old_data': batch_dict['old_data'][i],
                            })
                    if len(update_events) >= 1:
                        break

        # Start consuming
        consumer_task = asyncio.create_task(consume_updates())
        await asyncio.sleep(2)

        # Update data
        cursor = mysql_connection.cursor()
        cursor.execute(f"""
            UPDATE cdc_test_users
            SET email = 'charlie.new@example.com'
            WHERE id = {user_id}
        """)
        mysql_connection.commit()
        cursor.close()

        # Wait for events
        await asyncio.wait_for(consumer_task, timeout=10)

        # Verify
        assert len(update_events) == 1
        assert update_events[0]['event_type'] == 'update'
        assert 'charlie.new@example.com' in update_events[0]['data']
        assert 'charlie@example.com' in update_events[0]['old_data']

    @pytest.mark.asyncio
    async def test_delete_events(self, mysql_connection, clean_test_table):
        """Test capturing DELETE events."""
        # Insert initial data
        cursor = mysql_connection.cursor()
        cursor.execute("""
            INSERT INTO cdc_test_users (name, email, age)
            VALUES ('David', 'david@example.com', 40)
        """)
        mysql_connection.commit()
        user_id = cursor.lastrowid
        cursor.close()

        # Configure CDC
        config = MySQLCDCConfig(
            host="localhost",
            port=3307,
            user="root",
            password="sabot",
            database="sabot",
            only_tables=["sabot.cdc_test_users"],
        )

        connector = MySQLCDCConnector(config)
        delete_events = []

        async def consume_deletes():
            """Consume DELETE events."""
            async with connector:
                async for batch in connector.stream_batches():
                    batch_dict = batch.to_pydict()
                    for i in range(batch.num_rows):
                        if batch_dict['event_type'][i] == 'delete':
                            delete_events.append({
                                'event_type': batch_dict['event_type'][i],
                                'data': batch_dict['data'][i],
                            })
                    if len(delete_events) >= 1:
                        break

        # Start consuming
        consumer_task = asyncio.create_task(consume_deletes())
        await asyncio.sleep(2)

        # Delete data
        cursor = mysql_connection.cursor()
        cursor.execute(f"DELETE FROM cdc_test_users WHERE id = {user_id}")
        mysql_connection.commit()
        cursor.close()

        # Wait for events
        await asyncio.wait_for(consumer_task, timeout=10)

        # Verify
        assert len(delete_events) == 1
        assert delete_events[0]['event_type'] == 'delete'
        assert 'David' in delete_events[0]['data']


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
