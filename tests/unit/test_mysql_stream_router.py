"""Unit tests for MySQL stream router."""

import pytest
from sabot import cyarrow as ca
from sabot._cython.connectors.mysql_stream_router import (
    MySQLStreamRouter,
    TableRoute,
    create_table_router
)


def create_test_batch(events):
    """
    Create test CDC batch.

    Args:
        events: List of (schema, table, event_type, data) tuples

    Returns:
        Arrow RecordBatch
    """
    event_types = []
    schemas = []
    tables = []
    timestamps = []
    data = []
    old_data = []
    log_files = []
    log_positions = []
    gtids = []
    queries = []

    for schema, table, event_type, data_str in events:
        event_types.append(event_type)
        schemas.append(schema)
        tables.append(table)
        timestamps.append(0)
        data.append(data_str)
        old_data.append("")
        log_files.append("mysql-bin.000001")
        log_positions.append(1000)
        gtids.append("")
        queries.append("")

    arrays = [
        ca.array(event_types, type=ca.string()),
        ca.array(schemas, type=ca.string()),
        ca.array(tables, type=ca.string()),
        ca.array(timestamps, type=ca.timestamp('us')),
        ca.array(data, type=ca.string()),
        ca.array(old_data, type=ca.string()),
        ca.array(log_files, type=ca.string()),
        ca.array(log_positions, type=ca.int64()),
        ca.array(gtids, type=ca.string()),
        ca.array(queries, type=ca.string()),
    ]

    schema = ca.schema([
        ('event_type', ca.string()),
        ('schema', ca.string()),
        ('table', ca.string()),
        ('timestamp', ca.timestamp('us')),
        ('data', ca.string()),
        ('old_data', ca.string()),
        ('log_file', ca.string()),
        ('log_pos', ca.int64()),
        ('gtid', ca.string()),
        ('query', ca.string()),
    ])

    return ca.RecordBatch.from_arrays(arrays, schema=schema)


class TestTableRoute:
    """Test TableRoute dataclass."""

    def test_table_spec(self):
        """Test table_spec property."""
        route = TableRoute(
            database="ecommerce",
            table="users",
            output_key="users"
        )
        assert route.table_spec == "ecommerce.users"


class TestMySQLStreamRouter:
    """Test MySQLStreamRouter."""

    def test_add_route(self):
        """Test adding routes."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")
        router.add_route("ecommerce", "orders", output_key="orders")

        assert router.get_output_key("ecommerce", "users") == "users"
        assert router.get_output_key("ecommerce", "orders") == "orders"
        assert router.get_output_key("ecommerce", "unknown") is None

    def test_remove_route(self):
        """Test removing routes."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")
        assert router.get_output_key("ecommerce", "users") == "users"

        router.remove_route("ecommerce", "users")
        assert router.get_output_key("ecommerce", "users") is None

    def test_get_route(self):
        """Test getting route."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")

        route = router.get_route("ecommerce", "users")
        assert route is not None
        assert route.database == "ecommerce"
        assert route.table == "users"
        assert route.output_key == "users"

    def test_route_batch_single_table(self):
        """Test routing batch with single table."""
        router = MySQLStreamRouter()
        router.add_route("ecommerce", "users", output_key="users")

        batch = create_test_batch([
            ("ecommerce", "users", "insert", '{"id": 1}'),
            ("ecommerce", "users", "insert", '{"id": 2}'),
        ])

        routed = router.route_batch(batch)

        assert "users" in routed
        assert routed["users"].num_rows == 2

    def test_route_batch_multiple_tables(self):
        """Test routing batch with multiple tables."""
        router = MySQLStreamRouter()
        router.add_route("ecommerce", "users", output_key="users")
        router.add_route("ecommerce", "orders", output_key="orders")

        batch = create_test_batch([
            ("ecommerce", "users", "insert", '{"id": 1}'),
            ("ecommerce", "orders", "insert", '{"id": 100}'),
            ("ecommerce", "users", "insert", '{"id": 2}'),
        ])

        routed = router.route_batch(batch)

        assert "users" in routed
        assert "orders" in routed
        assert routed["users"].num_rows == 2
        assert routed["orders"].num_rows == 1

    def test_route_batch_default_output(self):
        """Test routing batch with unrouted tables (default output)."""
        router = MySQLStreamRouter()
        router.add_route("ecommerce", "users", output_key="users")

        batch = create_test_batch([
            ("ecommerce", "users", "insert", '{"id": 1}'),
            ("ecommerce", "unknown", "insert", '{"id": 2}'),
        ])

        routed = router.route_batch(batch)

        assert "users" in routed
        assert "default" in routed
        assert routed["users"].num_rows == 1
        assert routed["default"].num_rows == 1

    def test_route_batch_empty(self):
        """Test routing empty batch."""
        router = MySQLStreamRouter()
        router.add_route("ecommerce", "users", output_key="users")

        batch = create_test_batch([])

        routed = router.route_batch(batch)

        assert len(routed) == 0

    def test_route_batch_with_predicate(self):
        """Test routing with predicate filter."""
        router = MySQLStreamRouter()

        # Only route insert events
        router.add_route(
            "ecommerce",
            "users",
            output_key="users",
            predicate=lambda row: row['event_type'] == 'insert'
        )

        batch = create_test_batch([
            ("ecommerce", "users", "insert", '{"id": 1}'),
            ("ecommerce", "users", "update", '{"id": 2}'),
            ("ecommerce", "users", "insert", '{"id": 3}'),
        ])

        routed = router.route_batch(batch)

        assert "users" in routed
        assert routed["users"].num_rows == 2  # Only inserts

        # Update goes to default
        assert "default" in routed
        assert routed["default"].num_rows == 1

    def test_get_routes(self):
        """Test getting all routes."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")
        router.add_route("ecommerce", "orders", output_key="orders")

        routes = router.get_routes()
        assert len(routes) == 2

    def test_get_output_keys(self):
        """Test getting all output keys."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")
        router.add_route("ecommerce", "orders", output_key="orders")

        keys = router.get_output_keys()
        assert sorted(keys) == ["orders", "users"]

    def test_get_tables_for_output(self):
        """Test getting tables for output key."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="user_data")
        router.add_route("ecommerce", "user_profiles", output_key="user_data")

        tables = router.get_tables_for_output("user_data")
        assert sorted(tables) == ["ecommerce.user_profiles", "ecommerce.users"]

    def test_clear_routes(self):
        """Test clearing all routes."""
        router = MySQLStreamRouter()

        router.add_route("ecommerce", "users", output_key="users")
        router.add_route("ecommerce", "orders", output_key="orders")

        assert len(router.get_routes()) == 2

        router.clear_routes()

        assert len(router.get_routes()) == 0
        assert len(router.get_output_keys()) == 0


class TestCreateTableRouter:
    """Test create_table_router convenience function."""

    def test_create_simple(self):
        """Test creating router from dictionary."""
        router = create_table_router({
            "ecommerce.users": "users",
            "ecommerce.orders": "orders"
        })

        assert router.get_output_key("ecommerce", "users") == "users"
        assert router.get_output_key("ecommerce", "orders") == "orders"

    def test_create_invalid_spec(self):
        """Test creating router with invalid table spec."""
        with pytest.raises(ValueError) as exc_info:
            create_table_router({"users": "users"})

        assert "must be database.table" in str(exc_info.value)

    def test_create_multiple_to_same_output(self):
        """Test creating router with multiple tables to same output."""
        router = create_table_router({
            "ecommerce.users": "user_data",
            "ecommerce.user_profiles": "user_data"
        })

        assert router.get_output_key("ecommerce", "users") == "user_data"
        assert router.get_output_key("ecommerce", "user_profiles") == "user_data"

        tables = router.get_tables_for_output("user_data")
        assert len(tables) == 2
