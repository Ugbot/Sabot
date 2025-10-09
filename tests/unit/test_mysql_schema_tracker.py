"""Unit tests for MySQL schema tracker."""

import pytest
from datetime import datetime
from sabot._cython.connectors.mysql_schema_tracker import (
    MySQLSchemaTracker,
    DDLType,
    DDLEvent
)


class TestMySQLSchemaTracker:
    """Test MySQLSchemaTracker."""

    def test_create_table_detection(self):
        """Test CREATE TABLE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.CREATE_TABLE
        assert ddl_event.database == "ecommerce"
        assert ddl_event.table == "users"

    def test_create_table_if_not_exists(self):
        """Test CREATE TABLE IF NOT EXISTS detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="CREATE TABLE IF NOT EXISTS users (id INT)",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.CREATE_TABLE
        assert ddl_event.table == "users"

    def test_drop_table_detection(self):
        """Test DROP TABLE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="DROP TABLE users",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.DROP_TABLE
        assert ddl_event.database == "ecommerce"
        assert ddl_event.table == "users"

    def test_alter_table_add_column(self):
        """Test ALTER TABLE ADD COLUMN detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="ALTER TABLE users ADD COLUMN email VARCHAR(255)",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.ALTER_TABLE
        assert ddl_event.table == "users"
        assert "email" in ddl_event.affected_columns

    def test_alter_table_drop_column(self):
        """Test ALTER TABLE DROP COLUMN detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="ALTER TABLE users DROP COLUMN age",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.ALTER_TABLE
        assert "age" in ddl_event.affected_columns

    def test_alter_table_modify_column(self):
        """Test ALTER TABLE MODIFY COLUMN detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="ALTER TABLE users MODIFY COLUMN name VARCHAR(200)",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.ALTER_TABLE
        assert "name" in ddl_event.affected_columns

    def test_alter_table_change_column(self):
        """Test ALTER TABLE CHANGE COLUMN detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="ALTER TABLE users CHANGE COLUMN old_name new_name VARCHAR(100)",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.ALTER_TABLE
        assert "old_name" in ddl_event.affected_columns
        assert "new_name" in ddl_event.affected_columns

    def test_rename_table_detection(self):
        """Test RENAME TABLE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="RENAME TABLE old_users TO new_users",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.RENAME_TABLE
        assert "old_users" in ddl_event.old_table_name
        assert "new_users" in ddl_event.new_table_name

    def test_truncate_table_detection(self):
        """Test TRUNCATE TABLE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="TRUNCATE TABLE users",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.TRUNCATE_TABLE
        assert ddl_event.table == "users"

    def test_create_database_detection(self):
        """Test CREATE DATABASE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="",
            query="CREATE DATABASE new_db",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.CREATE_DATABASE
        assert ddl_event.database == "new_db"
        assert ddl_event.table is None

    def test_drop_database_detection(self):
        """Test DROP DATABASE detection."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="",
            query="DROP DATABASE old_db",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.ddl_type == DDLType.DROP_DATABASE
        assert ddl_event.database == "old_db"

    def test_non_ddl_query(self):
        """Test that non-DDL queries return None."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="ecommerce",
            query="INSERT INTO users VALUES (1, 'Alice')",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is None

    def test_invalidate_table(self):
        """Test table schema invalidation."""
        tracker = MySQLSchemaTracker()

        # Add schema
        tracker.update_schema(
            "ecommerce",
            "users",
            {"id": "INT", "name": "VARCHAR(100)"},
            ["id"]
        )

        schema = tracker.get_schema("ecommerce", "users")
        assert schema is not None
        initial_version = schema.schema_version

        # Invalidate
        tracker.invalidate_table("ecommerce", "users")

        schema = tracker.get_schema("ecommerce", "users")
        assert schema.schema_version == initial_version + 1

    def test_remove_table(self):
        """Test table removal."""
        tracker = MySQLSchemaTracker()

        tracker.update_schema(
            "ecommerce",
            "users",
            {"id": "INT"},
            ["id"]
        )

        assert tracker.get_schema("ecommerce", "users") is not None

        tracker.remove_table("ecommerce", "users")

        assert tracker.get_schema("ecommerce", "users") is None

    def test_rename_table(self):
        """Test table rename."""
        tracker = MySQLSchemaTracker()

        tracker.update_schema(
            "ecommerce",
            "old_users",
            {"id": "INT"},
            ["id"]
        )

        tracker.rename_table("ecommerce", "old_users", "ecommerce", "new_users")

        assert tracker.get_schema("ecommerce", "old_users") is None
        assert tracker.get_schema("ecommerce", "new_users") is not None

    def test_track_table(self):
        """Test table tracking."""
        tracker = MySQLSchemaTracker()

        assert not tracker.is_tracked("ecommerce", "users")

        tracker.track_table("ecommerce", "users")

        assert tracker.is_tracked("ecommerce", "users")
        assert ("ecommerce", "users") in tracker.get_tracked_tables()

    def test_untrack_table(self):
        """Test table untracking."""
        tracker = MySQLSchemaTracker()

        tracker.track_table("ecommerce", "users")
        assert tracker.is_tracked("ecommerce", "users")

        tracker.untrack_table("ecommerce", "users")
        assert not tracker.is_tracked("ecommerce", "users")

    def test_update_schema(self):
        """Test schema update."""
        tracker = MySQLSchemaTracker()

        tracker.update_schema(
            "ecommerce",
            "users",
            {"id": "INT", "name": "VARCHAR(100)"},
            ["id"]
        )

        schema = tracker.get_schema("ecommerce", "users")
        assert schema is not None
        assert schema.database == "ecommerce"
        assert schema.table == "users"
        assert schema.columns == {"id": "INT", "name": "VARCHAR(100)"}
        assert schema.primary_key == ["id"]

    def test_qualified_table_name(self):
        """Test queries with qualified table names (database.table)."""
        tracker = MySQLSchemaTracker()

        ddl_event = tracker.process_query_event(
            database="other",
            query="CREATE TABLE ecommerce.users (id INT)",
            timestamp=datetime.now(),
            log_file="mysql-bin.000001",
            log_pos=1000
        )

        assert ddl_event is not None
        assert ddl_event.database == "ecommerce"
        assert ddl_event.table == "users"

    def test_clear_cache(self):
        """Test cache clearing."""
        tracker = MySQLSchemaTracker()

        tracker.update_schema("db1", "table1", {"id": "INT"}, ["id"])
        tracker.update_schema("db2", "table2", {"id": "INT"}, ["id"])

        assert tracker.get_schema("db1", "table1") is not None
        assert tracker.get_schema("db2", "table2") is not None

        tracker.clear_cache()

        assert tracker.get_schema("db1", "table1") is None
        assert tracker.get_schema("db2", "table2") is None
