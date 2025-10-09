#!/usr/bin/env python3
"""
Unit tests for MySQL CDC connector.

Tests the configuration, event parsing, and Arrow conversion
without requiring a live MySQL connection.
"""

import pytest
from datetime import datetime
from sabot._cython.connectors.mysql_cdc import (
    MySQLCDCConfig,
    CDCRecord,
    cdc_records_to_arrow_batch,
)


class TestMySQLCDCConfig:
    """Test MySQL CDC configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MySQLCDCConfig()
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.user == "root"
        assert config.password == ""
        assert config.server_id == 1000001
        assert config.batch_size == 100
        assert config.blocking is False
        assert config.resume_stream is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = MySQLCDCConfig(
            host="mysql-server",
            port=3307,
            user="cdc_user",
            password="secret",
            database="production",
            server_id=2000000,
            batch_size=500,
        )
        assert config.host == "mysql-server"
        assert config.port == 3307
        assert config.user == "cdc_user"
        assert config.password == "secret"
        assert config.database == "production"
        assert config.server_id == 2000000
        assert config.batch_size == 500

    def test_connection_params(self):
        """Test conversion to connection parameters."""
        config = MySQLCDCConfig(
            host="localhost",
            port=3306,
            user="root",
            password="pass",
            database="test",
            only_tables=["test.users", "test.orders"],
        )

        params = config.to_connection_params()

        assert params["connection_settings"]["host"] == "localhost"
        assert params["connection_settings"]["port"] == 3306
        assert params["connection_settings"]["user"] == "root"
        assert params["connection_settings"]["passwd"] == "pass"
        assert params["connection_settings"]["database"] == "test"
        assert params["server_id"] == 1000001
        assert params["only_tables"] == ["test.users", "test.orders"]

    def test_gtid_config(self):
        """Test GTID-based configuration."""
        config = MySQLCDCConfig(
            auto_position=True,
            gtid_set="00000000-0000-0000-0000-000000000000:1-100",
        )

        params = config.to_connection_params()

        assert params["auto_position"] is True
        assert params["gtid_set"] == "00000000-0000-0000-0000-000000000000:1-100"

    def test_log_pos_config(self):
        """Test file+position configuration."""
        config = MySQLCDCConfig(
            log_file="mysql-bin.000001",
            log_pos=1234,
        )

        params = config.to_connection_params()

        assert params["log_file"] == "mysql-bin.000001"
        assert params["log_pos"] == 1234


class TestCDCRecord:
    """Test CDC record representation."""

    def test_insert_record(self):
        """Test INSERT record."""
        record = CDCRecord(
            event_type='insert',
            schema='test',
            table='users',
            timestamp=datetime(2025, 10, 9, 12, 0, 0),
            data={'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            log_file='mysql-bin.000001',
            log_pos=1000,
        )

        assert record.event_type == 'insert'
        assert record.schema == 'test'
        assert record.table == 'users'
        assert record.data['id'] == 1
        assert record.data['name'] == 'Alice'
        assert record.old_data is None

    def test_update_record(self):
        """Test UPDATE record."""
        record = CDCRecord(
            event_type='update',
            schema='test',
            table='users',
            timestamp=datetime(2025, 10, 9, 12, 0, 0),
            data={'id': 1, 'name': 'Alice Smith', 'email': 'alice.smith@example.com'},
            old_data={'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            log_file='mysql-bin.000001',
            log_pos=2000,
        )

        assert record.event_type == 'update'
        assert record.old_data['name'] == 'Alice'
        assert record.data['name'] == 'Alice Smith'

    def test_delete_record(self):
        """Test DELETE record."""
        record = CDCRecord(
            event_type='delete',
            schema='test',
            table='users',
            timestamp=datetime(2025, 10, 9, 12, 0, 0),
            data={'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            log_file='mysql-bin.000001',
            log_pos=3000,
        )

        assert record.event_type == 'delete'
        assert record.data['id'] == 1


class TestArrowConversion:
    """Test conversion to Arrow RecordBatch."""

    def test_single_record_conversion(self):
        """Test converting single record to Arrow batch."""
        records = [
            CDCRecord(
                event_type='insert',
                schema='test',
                table='users',
                timestamp=datetime(2025, 10, 9, 12, 0, 0),
                data={'id': 1, 'name': 'Alice'},
                log_file='mysql-bin.000001',
                log_pos=1000,
            )
        ]

        batch = cdc_records_to_arrow_batch(records)

        assert batch.num_rows == 1
        assert batch.num_columns == 10

        # Check schema
        assert batch.schema.field('event_type').name == 'event_type'
        assert batch.schema.field('schema').name == 'schema'
        assert batch.schema.field('table').name == 'table'
        assert batch.schema.field('timestamp').name == 'timestamp'

        # Check data
        batch_dict = batch.to_pydict()
        assert batch_dict['event_type'][0] == 'insert'
        assert batch_dict['schema'][0] == 'test'
        assert batch_dict['table'][0] == 'users'
        assert '{"id": 1, "name": "Alice"}' in batch_dict['data'][0]

    def test_multiple_records_conversion(self):
        """Test converting multiple records to Arrow batch."""
        records = [
            CDCRecord(
                event_type='insert',
                schema='test',
                table='users',
                timestamp=datetime(2025, 10, 9, 12, 0, 0),
                data={'id': 1, 'name': 'Alice'},
                log_file='mysql-bin.000001',
                log_pos=1000,
            ),
            CDCRecord(
                event_type='update',
                schema='test',
                table='users',
                timestamp=datetime(2025, 10, 9, 12, 1, 0),
                data={'id': 1, 'name': 'Alice Smith'},
                old_data={'id': 1, 'name': 'Alice'},
                log_file='mysql-bin.000001',
                log_pos=2000,
            ),
            CDCRecord(
                event_type='delete',
                schema='test',
                table='users',
                timestamp=datetime(2025, 10, 9, 12, 2, 0),
                data={'id': 1},
                log_file='mysql-bin.000001',
                log_pos=3000,
            ),
        ]

        batch = cdc_records_to_arrow_batch(records)

        assert batch.num_rows == 3

        batch_dict = batch.to_pydict()
        assert batch_dict['event_type'][0] == 'insert'
        assert batch_dict['event_type'][1] == 'update'
        assert batch_dict['event_type'][2] == 'delete'

    def test_null_handling(self):
        """Test handling of NULL values in conversion."""
        records = [
            CDCRecord(
                event_type='insert',
                schema='test',
                table='users',
                timestamp=datetime(2025, 10, 9, 12, 0, 0),
                data={'id': 1, 'name': 'Alice'},
                old_data=None,
                log_file='mysql-bin.000001',
                log_pos=1000,
                gtid=None,
                query=None,
            )
        ]

        batch = cdc_records_to_arrow_batch(records)

        batch_dict = batch.to_pydict()
        assert batch_dict['old_data'][0] is None
        assert batch_dict['gtid'][0] is None
        assert batch_dict['query'][0] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
