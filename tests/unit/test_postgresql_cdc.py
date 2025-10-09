#!/usr/bin/env python3
"""
Unit tests for PostgreSQL CDC connector.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from sabot._cython.connectors.postgresql.cdc_connector import (
    PostgreSQLCDCConfig,
    PostgreSQLCDCConnector,
    CDCRecord
)
from sabot._cython.connectors.postgresql.wal2json_parser import (
    Wal2JsonParser,
    CDCEvent,
    CDCTransaction
)


class TestPostgreSQLCDCConfig:
    """Test CDC configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = PostgreSQLCDCConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.replication_slot == "sabot_cdc_slot"
        assert config.format_version == 2
        assert config.batch_size == 100

    def test_connection_string(self):
        """Test connection string generation."""
        config = PostgreSQLCDCConfig(
            host="prod-db.example.com",
            port=5433,
            database="analytics",
            user="cdc_user",
            password="secret123"
        )

        conn_str = config.to_connection_string()
        expected_parts = [
            "host=prod-db.example.com",
            "port=5433",
            "dbname=analytics",
            "user=cdc_user",
            "password=secret123",
            "connect_timeout=30",
            "replication=database",
            "fallback_application_name=sabot_cdc"
        ]

        for part in expected_parts:
            assert part in conn_str

    def test_plugin_args(self):
        """Test wal2json plugin arguments generation."""
        config = PostgreSQLCDCConfig(
            format_version=2,
            include_xids=True,
            include_timestamps=True,
            include_types=False,
            pretty_print=True,
            filter_tables=["public.users", "public.orders"]
        )

        args = config.to_plugin_args()

        assert "format-version=2" in args
        assert "include-xids=1" in args
        assert "include-timestamp=1" in args
        assert "include-types=0" in args
        assert "pretty-print=1" in args
        assert "filter-tables=public.users" in args
        assert "filter-tables=public.orders" in args


class TestWal2JsonParser:
    """Test wal2json parser."""

    def test_parse_format_v2_insert(self):
        """Test parsing v2 format INSERT event."""
        parser = Wal2JsonParser(format_version=2)

        json_data = '''{
            "action": "I",
            "schema": "public",
            "table": "users",
            "columns": [
                {"name": "id", "type": "integer", "value": 123},
                {"name": "name", "type": "text", "value": "Alice"}
            ]
        }'''

        events = parser.parse_message(json_data.encode('utf-8'))
        assert len(events) == 1

        event = events[0]
        assert isinstance(event, CDCEvent)
        assert event.action == 'I'
        assert event.schema == 'public'
        assert event.table == 'users'
        assert len(event.columns) == 2
        assert event.columns[0].name == 'id'
        assert event.columns[0].value == 123
        assert event.columns[1].name == 'name'
        assert event.columns[1].value == 'Alice'

    def test_parse_format_v2_update(self):
        """Test parsing v2 format UPDATE event."""
        parser = Wal2JsonParser(format_version=2)

        json_data = '''{
            "action": "U",
            "schema": "public",
            "table": "users",
            "columns": [
                {"name": "id", "type": "integer", "value": 123},
                {"name": "name", "type": "text", "value": "Alice Updated"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 123}
            ]
        }'''

        events = parser.parse_message(json_data.encode('utf-8'))
        assert len(events) == 1

        event = events[0]
        assert isinstance(event, CDCEvent)
        assert event.action == 'U'
        assert event.schema == 'public'
        assert event.table == 'users'
        assert event.identity.names == ['id']
        assert event.identity.values == [123]

    def test_parse_format_v1_transaction(self):
        """Test parsing v1 format transaction."""
        parser = Wal2JsonParser(format_version=1)

        json_data = '''{
            "xid": 12345,
            "timestamp": "2025-01-15 10:30:45.123456+00",
            "lsn": "0/15D6B88",
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "users",
                    "columnnames": ["id", "name"],
                    "columntypes": ["integer", "text"],
                    "columnvalues": [123, "Alice"]
                }
            ]
        }'''

        events = parser.parse_message(json_data.encode('utf-8'))
        assert len(events) == 1

        transaction = events[0]
        assert isinstance(transaction, CDCTransaction)
        assert transaction.xid == 12345
        assert len(transaction.changes) == 1

        change = transaction.changes[0]
        assert change.action == 'I'
        assert change.schema == 'public'
        assert change.table == 'users'


class TestCDCRecord:
    """Test CDC record conversion."""

    def test_from_insert_event(self):
        """Test converting INSERT event to CDC record."""
        event = CDCEvent(
            action='I',
            schema='public',
            table='users',
            columns=[
                CDCEvent.CDCColumn(name='id', type='integer', value=123),
                CDCEvent.CDCColumn(name='name', type='text', value='Alice')
            ],
            lsn='0/15D6B88'
        )

        records = CDCRecord.from_cdc_event(event)
        assert len(records) == 1

        record = records[0]
        assert record.event_type == 'insert'
        assert record.schema == 'public'
        assert record.table == 'users'
        assert record.data == {'id': 123, 'name': 'Alice'}
        assert record.lsn == '0/15D6B88'

    def test_from_update_event(self):
        """Test converting UPDATE event to CDC record."""
        event = CDCEvent(
            action='U',
            schema='public',
            table='users',
            columns=[
                CDCEvent.CDCColumn(name='id', type='integer', value=123),
                CDCEvent.CDCColumn(name='name', type='text', value='Alice Updated')
            ],
            identity=CDCEvent.CDCIdentity(
                names=['id'],
                types=['integer'],
                values=[123]
            )
        )

        records = CDCRecord.from_cdc_event(event)
        assert len(records) == 1

        record = records[0]
        assert record.event_type == 'update'
        assert record.schema == 'public'
        assert record.table == 'users'
        assert record.data == {'id': 123, 'name': 'Alice Updated'}
        assert record.key_data == {'id': 123}

    def test_from_delete_event(self):
        """Test converting DELETE event to CDC record."""
        event = CDCEvent(
            action='D',
            schema='public',
            table='users',
            identity=CDCEvent.CDCIdentity(
                names=['id'],
                types=['integer'],
                values=[123]
            )
        )

        records = CDCRecord.from_cdc_event(event)
        assert len(records) == 1

        record = records[0]
        assert record.event_type == 'delete'
        assert record.schema == 'public'
        assert record.table == 'users'
        assert record.key_data == {'id': 123}
        assert record.data is None


class TestPostgreSQLCDCConnector:
    """Test CDC connector (mocked)."""

    @pytest.mark.asyncio
    async def test_connector_initialization(self):
        """Test connector initialization."""
        config = PostgreSQLCDCConfig(
            host="localhost",
            database="test",
            replication_slot="test_slot"
        )

        connector = PostgreSQLCDCConnector(config)
        assert connector.config == config
        assert not connector._running
        assert connector._parser is not None

    @pytest.mark.asyncio
    @patch('sabot._cython.connectors.postgresql.cdc_connector.PostgreSQLConnection')
    async def test_start_stop(self, mock_conn_class):
        """Test connector start/stop lifecycle."""
        # Mock connection
        mock_conn = Mock()
        mock_conn.is_connected = False
        mock_conn.is_replication = False
        mock_conn.connect = Mock()
        mock_conn.execute = Mock(return_value=[])
        mock_conn.create_replication_slot = Mock()
        mock_conn.start_replication = Mock()
        mock_conn.close = Mock()
        mock_conn_class.return_value = mock_conn

        config = PostgreSQLCDCConfig()
        connector = PostgreSQLCDCConnector(config)

        # Test start
        await connector.start()
        assert connector._running
        mock_conn.connect.assert_called_once()
        mock_conn.start_replication.assert_called_once()

        # Test stop
        await connector.stop()
        assert not connector._running
        mock_conn.close.assert_called_once()

    def test_status(self):
        """Test connector status reporting."""
        config = PostgreSQLCDCConfig()
        connector = PostgreSQLCDCConnector(config)

        status = connector.get_status()
        assert not status['running']
        assert not status['connected']
        assert not status['replication_active']
        assert status['slot_name'] == config.replication_slot
        assert status['database'] == config.database


# Integration test (requires PostgreSQL)
@pytest.mark.integration
@pytest.mark.asyncio
class TestPostgreSQLCDCIntegration:
    """Integration tests (require PostgreSQL)."""

    @pytest.fixture
    def pg_config(self):
        """PostgreSQL test configuration."""
        return PostgreSQLCDCConfig(
            host="localhost",
            database="test_cdc",
            user="cdc_test",
            password="cdc_test",
            replication_slot="test_cdc_slot"
        )

    def test_connection_string_generation(self, pg_config):
        """Test connection string includes replication parameters."""
        conn_str = pg_config.to_connection_string()
        assert "replication=database" in conn_str
        assert "fallback_application_name=sabot_cdc" in conn_str

    def test_plugin_args_includes_format(self, pg_config):
        """Test plugin args include format version."""
        args = pg_config.to_plugin_args()
        assert f"format-version={pg_config.format_version}" in args


if __name__ == "__main__":
    pytest.main([__file__])






