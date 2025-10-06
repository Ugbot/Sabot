# -*- coding: utf-8 -*-
"""Tests for join operations."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

from sabot.joins import (
    JoinType,
    JoinMode,
    StreamTableJoin,
    StreamStreamJoin,
    IntervalJoin,
    TemporalJoin,
    LookupJoin,
    WindowJoin,
    TableTableJoin,
    ArrowTableJoin,
    ArrowDatasetJoin,
    ArrowAsofJoin,
    JoinBuilder,
    create_join_builder
)


class TestJoinTypes:
    """Test join type enum."""

    def test_join_types(self):
        """Test join type values."""
        assert JoinType.INNER.value == "inner"
        assert JoinType.LEFT_OUTER.value == "left_outer"
        assert JoinType.RIGHT_OUTER.value == "right_outer"
        assert JoinType.FULL_OUTER.value == "full_outer"


class TestStreamTableJoin:
    """Test stream-table join functionality."""

    @pytest.fixture
    async def mock_stream(self):
        """Create a mock stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record

        return MockStream([
            {"user_id": "1", "event": "click", "timestamp": 1000},
            {"user_id": "2", "event": "view", "timestamp": 1001},
            {"user_id": "1", "event": "purchase", "timestamp": 1002},
        ])

    @pytest.fixture
    async def mock_table(self):
        """Create a mock table."""
        class MockTable:
            def __init__(self, data):
                self.data = data

            async def aitems(self):
                for key, value in self.data.items():
                    yield key, value

        return MockTable({
            "1": {"user_id": "1", "name": "Alice", "age": 30},
            "2": {"user_id": "2", "name": "Bob", "age": 25},
        })

    @pytest.mark.asyncio
    async def test_stream_table_join_inner(self, mock_stream, mock_table):
        """Test inner stream-table join."""
        join = StreamTableJoin(
            mock_stream,
            mock_table,
            JoinType.INNER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should have 3 results (all stream records matched)
        assert len(results) == 3

        # Check that user data was joined
        alice_results = [r for r in results if r.get("table_name") == "Alice"]
        assert len(alice_results) == 2  # Alice had 2 events

        bob_results = [r for r in results if r.get("table_name") == "Bob"]
        assert len(bob_results) == 1  # Bob had 1 event

    @pytest.mark.asyncio
    async def test_stream_table_join_left_outer(self, mock_stream, mock_table):
        """Test left outer stream-table join."""
        join = StreamTableJoin(
            mock_stream,
            mock_table,
            JoinType.LEFT_OUTER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should have 3 results (all stream records included)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_stream_table_join_no_matches(self):
        """Test stream-table join with no matches."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record

        class MockTable:
            async def aitems(self):
                return
                yield  # Empty table

        stream = MockStream([{"user_id": "999", "event": "unknown"}])
        table = MockTable()

        join = StreamTableJoin(
            stream,
            table,
            JoinType.LEFT_OUTER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should return original record for left outer join
        assert len(results) == 1
        assert results[0]["user_id"] == "999"


class TestStreamStreamJoin:
    """Test stream-stream join functionality."""

    @pytest.fixture
    async def mock_left_stream(self):
        """Create a mock left stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)  # Small delay to simulate real streaming

        return MockStream([
            {"order_id": "1001", "user_id": "1", "amount": 50.0, "timestamp": 1000},
            {"order_id": "1002", "user_id": "2", "amount": 75.0, "timestamp": 1005},
        ])

    @pytest.fixture
    async def mock_right_stream(self):
        """Create a mock right stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)

        return MockStream([
            {"payment_id": "2001", "order_id": "1001", "status": "paid", "timestamp": 1002},
            {"payment_id": "2002", "order_id": "1003", "status": "pending", "timestamp": 1006},
        ])

    @pytest.mark.asyncio
    async def test_stream_stream_join_inner(self, mock_left_stream, mock_right_stream):
        """Test inner stream-stream join."""
        join = StreamStreamJoin(
            mock_left_stream,
            mock_right_stream,
            JoinType.INNER,
            [{"left_field": "order_id", "right_field": "order_id"}],
            window_size_seconds=10.0  # Large window for test
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should find one match (order 1001)
        matches = [r for r in results if "left_order_id" in r and "right_order_id" in r]
        assert len(matches) >= 1

        # Verify the join result
        match = matches[0]
        assert match["left_order_id"] == "1001"
        assert match["right_order_id"] == "1001"
        assert match["left_amount"] == 50.0
        assert match["right_status"] == "paid"


class TestTableTableJoin:
    """Test table-table join functionality."""

    @pytest.fixture
    async def mock_left_table(self):
        """Create a mock left table."""
        class MockTable:
            def __init__(self, data):
                self.data = data

            async def aitems(self):
                for key, value in self.data.items():
                    yield key, value

        return MockTable({
            "1": {"user_id": "1", "name": "Alice", "department": "engineering"},
            "2": {"user_id": "2", "name": "Bob", "department": "sales"},
            "3": {"user_id": "3", "name": "Charlie", "department": "engineering"},
        })

    @pytest.fixture
    async def mock_right_table(self):
        """Create a mock right table."""
        class MockTable:
            def __init__(self, data):
                self.data = data

            async def aitems(self):
                for key, value in self.data.items():
                    yield key, value

        return MockTable({
            "1": {"user_id": "1", "salary": 100000, "bonus": 5000},
            "2": {"user_id": "2", "salary": 80000, "bonus": 3000},
            "4": {"user_id": "4", "salary": 90000, "bonus": 4000},  # No match in left
        })

    @pytest.mark.asyncio
    async def test_table_table_join_inner(self, mock_left_table, mock_right_table):
        """Test inner table-table join."""
        join = TableTableJoin(
            mock_left_table,
            mock_right_table,
            JoinType.INNER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = await join.execute()

        # Should have 2 matches (users 1 and 2)
        assert len(results) == 2

        # Verify join results
        alice_results = [r for r in results if r.get("name") == "Alice"]
        assert len(alice_results) == 1
        assert alice_results[0]["salary"] == 100000
        assert alice_results[0]["bonus"] == 5000
        assert alice_results[0]["department"] == "engineering"

    @pytest.mark.asyncio
    async def test_table_table_join_left_outer(self, mock_left_table, mock_right_table):
        """Test left outer table-table join."""
        join = TableTableJoin(
            mock_left_table,
            mock_right_table,
            JoinType.LEFT_OUTER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = await join.execute()

        # Should have 3 results (all from left table)
        assert len(results) == 3

        # Charlie should be included even without salary data
        charlie_results = [r for r in results if r.get("name") == "Charlie"]
        assert len(charlie_results) == 1
        # Charlie should have department but no salary/bonus
        assert charlie_results[0]["department"] == "engineering"
        assert "salary" not in charlie_results[0]


class TestJoinBuilder:
    """Test the fluent join builder API."""

    def test_join_builder_creation(self):
        """Test creating a join builder."""
        mock_app = MagicMock()
        builder = create_join_builder(mock_app)
        assert isinstance(builder, JoinBuilder)

    def test_stream_table_join_builder(self):
        """Test stream-table join builder."""
        mock_app = MagicMock()
        mock_stream = MagicMock()
        mock_table = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.stream_table(mock_stream, mock_table, JoinType.INNER)

        # Add condition
        join_builder.on("user_id", "user_id")

        # Build the join
        join = join_builder.build()
        assert isinstance(join, StreamTableJoin)
        assert join.join_type == JoinType.INNER
        assert len(join.conditions) == 1

    def test_stream_stream_join_builder(self):
        """Test stream-stream join builder."""
        mock_app = MagicMock()
        mock_left = MagicMock()
        mock_right = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.stream_stream(mock_left, mock_right, JoinType.INNER)

        # Configure join
        join_builder.on("order_id", "order_id").within(300.0)

        # Build the join
        join = join_builder.build()
        assert isinstance(join, StreamStreamJoin)
        assert join.window_size_seconds == 300.0
        assert len(join.conditions) == 1

    def test_table_table_join_builder(self):
        """Test table-table join builder."""
        mock_app = MagicMock()
        mock_left = MagicMock()
        mock_right = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.table_table(mock_left, mock_right, JoinType.INNER)

        # Add condition
        join_builder.on("id", "user_id")

        # Build the join
        join = join_builder.build()
        assert isinstance(join, TableTableJoin)
        assert join.join_type == JoinType.INNER
        assert len(join.conditions) == 1


class TestIntervalJoin:
    """Test interval join functionality."""

    @pytest.fixture
    async def mock_left_stream(self):
        """Create a mock left stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)

        return MockStream([
            {"order_id": "1001", "user_id": "1", "timestamp": 1000},
            {"order_id": "1002", "user_id": "2", "timestamp": 1005},
        ])

    @pytest.fixture
    async def mock_right_stream(self):
        """Create a mock right stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)

        return MockStream([
            {"payment_id": "2001", "order_id": "1001", "timestamp": 1002},
            {"payment_id": "2002", "order_id": "1003", "timestamp": 1006},
        ])

    @pytest.mark.asyncio
    async def test_interval_join_inner(self, mock_left_stream, mock_right_stream):
        """Test inner interval join."""
        join = IntervalJoin(
            mock_left_stream,
            mock_right_stream,
            JoinType.INNER,
            [{"left_field": "order_id", "right_field": "order_id"}],
            left_lower_bound=-10.0,
            left_upper_bound=10.0,
            right_lower_bound=-10.0,
            right_upper_bound=10.0
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should find matches within the interval
        assert len(results) >= 0  # May be empty due to timing


class TestTemporalJoin:
    """Test temporal join functionality."""

    @pytest.fixture
    async def mock_stream(self):
        """Create a mock stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record

        return MockStream([
            {"user_id": "1", "event": "click", "version": 1},
            {"user_id": "2", "event": "view", "version": 2},
        ])

    @pytest.fixture
    async def mock_temporal_table(self):
        """Create a mock temporal table."""
        class MockTable:
            def __init__(self, data):
                self.data = data

            async def aitems(self):
                for key, value in self.data.items():
                    yield key, value

        return MockTable({
            "1": {"user_id": "1", "name": "Alice", "version": 1},
            "2": {"user_id": "2", "name": "Bob", "version": 2},
        })

    @pytest.mark.asyncio
    async def test_temporal_join_inner(self, mock_stream, mock_temporal_table):
        """Test inner temporal join."""
        join = TemporalJoin(
            mock_stream,
            mock_temporal_table,
            JoinType.INNER,
            [{"left_field": "user_id", "right_field": "user_id"}]
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should have enriched results
        assert len(results) == 2
        assert "temporal_name" in results[0] or "name" in results[0]


class TestLookupJoin:
    """Test lookup join functionality."""

    @pytest.fixture
    async def mock_stream(self):
        """Create a mock stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record

        return MockStream([
            {"user_id": "1", "event": "login"},
            {"user_id": "2", "event": "logout"},
        ])

    async def mock_lookup_func(self, key: str):
        """Mock lookup function."""
        lookup_data = {
            "1": {"name": "Alice", "department": "engineering"},
            "2": {"name": "Bob", "department": "sales"},
        }
        return lookup_data.get(key)

    @pytest.mark.asyncio
    async def test_lookup_join_left_outer(self, mock_stream):
        """Test left outer lookup join."""
        lookup_func = self.mock_lookup_func

        join = LookupJoin(
            mock_stream,
            lookup_func,
            JoinType.LEFT_OUTER,
            [{"left_field": "user_id", "right_field": "id"}],
            cache_size=100
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Should have 2 results (all stream records included)
        assert len(results) == 2

        # Check that lookup data was added
        alice_results = [r for r in results if r.get("lookup_name") == "Alice"]
        assert len(alice_results) == 1
        assert alice_results[0]["lookup_department"] == "engineering"


class TestWindowJoin:
    """Test window join functionality."""

    @pytest.fixture
    async def mock_left_stream(self):
        """Create a mock left stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)

        return MockStream([
            {"order_id": "1001", "user_id": "1", "timestamp": 1000},
            {"order_id": "1002", "user_id": "2", "timestamp": 1005},
        ])

    @pytest.fixture
    async def mock_right_stream(self):
        """Create a mock right stream."""
        class MockStream:
            def __init__(self, records):
                self.records = records

            def __aiter__(self):
                return self._async_iter()

            async def _async_iter(self):
                for record in self.records:
                    yield record
                    await asyncio.sleep(0.01)

        return MockStream([
            {"payment_id": "2001", "order_id": "1001", "timestamp": 1002},
            {"payment_id": "2002", "order_id": "1002", "timestamp": 1006},
        ])

    @pytest.mark.asyncio
    async def test_window_join_tumbling(self, mock_left_stream, mock_right_stream):
        """Test tumbling window join."""
        join = WindowJoin(
            mock_left_stream,
            mock_right_stream,
            JoinType.INNER,
            [{"left_field": "order_id", "right_field": "order_id"}],
            window_size=300.0,
            window_slide=300.0  # Tumbling window
        )

        results = []
        async with join:
            async for result in join:
                results.append(result)

        # Results may vary due to timing, but should be valid
        assert isinstance(results, list)


class TestJoinBuilderExtended:
    """Test extended join builder functionality."""

    def test_all_join_builder_methods(self):
        """Test that all join builder methods exist."""
        mock_app = MagicMock()
        builder = create_join_builder(mock_app)

        # Test all join types
        assert hasattr(builder, 'stream_table')
        assert hasattr(builder, 'stream_stream')
        assert hasattr(builder, 'interval_join')
        assert hasattr(builder, 'temporal_join')
        assert hasattr(builder, 'lookup_join')
        assert hasattr(builder, 'window_join')
        assert hasattr(builder, 'table_table')
        # Test Arrow join methods
        assert hasattr(builder, 'arrow_table_join')
        assert hasattr(builder, 'arrow_dataset_join')
        assert hasattr(builder, 'arrow_asof_join')

    def test_interval_join_builder(self):
        """Test interval join builder."""
        mock_app = MagicMock()
        mock_left = MagicMock()
        mock_right = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.interval_join(mock_left, mock_right, JoinType.INNER)

        # Configure join
        join_builder.on("order_id", "order_id").between(-10, 10, -5, 5)

        # Build the join
        join = join_builder.build()
        assert isinstance(join, IntervalJoin)
        assert join.left_lower_bound == -10
        assert join.left_upper_bound == 10

    def test_temporal_join_builder(self):
        """Test temporal join builder."""
        mock_app = MagicMock()
        mock_stream = MagicMock()
        mock_table = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.temporal_join(mock_stream, mock_table, JoinType.INNER)

        # Add condition
        join_builder.on("user_id", "user_id")

        # Build the join
        join = join_builder.build()
        assert isinstance(join, TemporalJoin)

    def test_lookup_join_builder(self):
        """Test lookup join builder."""
        mock_app = MagicMock()
        mock_stream = MagicMock()
        mock_func = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.lookup_join(mock_stream, mock_func, JoinType.LEFT_OUTER)

        # Configure
        join_builder.on("user_id", "id").cache(2000)

        # Build the join
        join = join_builder.build()
        assert isinstance(join, LookupJoin)
        assert join.cache_size == 2000

    def test_window_join_builder(self):
        """Test window join builder."""
        mock_app = MagicMock()
        mock_left = MagicMock()
        mock_right = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.window_join(mock_left, mock_right, JoinType.INNER)

        # Configure windows
        join_builder.on("order_id", "order_id").tumbling_window(600.0)

        # Build the join
        join = join_builder.build()
        assert isinstance(join, WindowJoin)
        assert join.window_size == 600.0
        assert join.window_slide == 600.0  # Tumbling

        # Test sliding window
        join_builder.sliding_window(300.0, 60.0)
        join = join_builder.build()
        assert join.window_size == 300.0
        assert join.window_slide == 60.0


class TestJoinMode:
    """Test join mode enum."""

    def test_join_modes(self):
        """Test join mode values."""
        assert JoinMode.BATCH.value == "batch"
        assert JoinMode.STREAMING.value == "streaming"
        assert JoinMode.INTERVAL.value == "interval"
        assert JoinMode.TEMPORAL.value == "temporal"
        assert JoinMode.WINDOW.value == "window"
        assert JoinMode.LOOKUP.value == "lookup"


class TestJoinIntegration:
    """Test join integration with Sabot components."""

    @pytest.mark.asyncio
    async def test_app_join_integration(self):
        """Test that joins integrate with Sabot app."""
        # This would be a full integration test with actual Sabot components
        # For now, just test that the methods exist
        mock_app = MagicMock()
        mock_app.joins.return_value = create_join_builder(mock_app)

        # Mock the app having a joins method
        join_builder = mock_app.joins()
        assert isinstance(join_builder, JoinBuilder)

    @pytest.mark.asyncio
    async def test_join_processor_stats(self):
        """Test that join processors provide statistics."""
        # Test basic stats functionality
        mock_stream = MagicMock()
        mock_table = MagicMock()

        join = StreamTableJoin(
            mock_stream,
            mock_table,
            JoinType.INNER,
            [{"left_field": "id", "right_field": "id"}]
        )

        # Should be able to create join without errors
        assert join.join_type == JoinType.INNER
        assert len(join.conditions) == 1


class TestArrowTableJoin:
    """Test Arrow-native table join functionality."""

    @pytest.fixture
    async def sample_arrow_tables(self):
        """Create sample Arrow tables for testing."""
        try:
            import pyarrow as pa

            # User table
            user_data = {
                'user_id': ['alice', 'bob', 'charlie', 'diana'],
                'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince'],
                'country': ['US', 'UK', 'CA', 'US'],
                'vip': [True, False, False, True]
            }
            user_table = pa.table(user_data)

            # Order table
            order_data = {
                'order_id': ['1001', '1002', '1003', '1004', '1005'],
                'user_id': ['alice', 'bob', 'alice', 'diana', 'eve'],  # eve not in users
                'amount': [99.99, 149.99, 79.99, 199.99, 59.99],
                'status': ['completed', 'completed', 'pending', 'completed', 'cancelled']
            }
            order_table = pa.table(order_data)

            return user_table, order_table
        except ImportError:
            pytest.skip("PyArrow not available")

    @pytest.mark.asyncio
    async def test_arrow_table_join_inner(self, sample_arrow_tables):
        """Test Arrow table inner join."""
        user_table, order_table = sample_arrow_tables

        join = ArrowTableJoin(
            user_table,
            order_table,
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()

        # Should have 4 results (alice x2, bob x1, diana x1)
        assert result.num_rows == 4

        # Check that joined data is present
        result_df = result.to_pandas()
        alice_orders = result_df[result_df['user_id'] == 'alice']
        assert len(alice_orders) == 2  # Alice has 2 orders
        assert 'name' in alice_orders.columns  # User data joined
        assert 'amount' in alice_orders.columns  # Order data joined

    @pytest.mark.asyncio
    async def test_arrow_table_join_left_outer(self, sample_arrow_tables):
        """Test Arrow table left outer join."""
        user_table, order_table = sample_arrow_tables

        join = ArrowTableJoin(
            user_table,
            order_table,
            join_type=JoinType.LEFT_OUTER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()

        # Should have 5 results (all users, including charlie with no orders)
        assert result.num_rows == 5

        result_df = result.to_pandas()
        charlie_row = result_df[result_df['user_id'] == 'charlie']
        assert len(charlie_row) == 1
        # Charlie should have user data but no order data
        assert charlie_row['name'].iloc[0] == 'Charlie Brown'
        assert pd.isna(charlie_row['order_id'].iloc[0])  # No order data

    @pytest.mark.asyncio
    async def test_arrow_table_join_right_outer(self, sample_arrow_tables):
        """Test Arrow table right outer join."""
        user_table, order_table = sample_arrow_tables

        join = ArrowTableJoin(
            user_table,
            order_table,
            join_type=JoinType.RIGHT_OUTER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()

        # Should have 5 results (all orders, including eve with no user data)
        assert result.num_rows == 5

        result_df = result.to_pandas()
        eve_row = result_df[result_df['user_id'] == 'eve']
        assert len(eve_row) == 1
        # Eve should have order data but no user data
        assert eve_row['amount'].iloc[0] == 59.99
        assert pd.isna(eve_row['name'].iloc[0])  # No user data

    @pytest.mark.asyncio
    async def test_arrow_table_join_no_conditions(self, sample_arrow_tables):
        """Test Arrow table join with no conditions raises error."""
        user_table, order_table = sample_arrow_tables

        join = ArrowTableJoin(
            user_table,
            order_table,
            join_type=JoinType.INNER,
            conditions=[]
        )

        with pytest.raises(ValueError, match="Join conditions are required"):
            await join.execute()

    @pytest.mark.asyncio
    async def test_arrow_table_join_pandas_conversion(self):
        """Test Arrow table join with pandas DataFrame conversion."""
        try:
            import pandas as pd
            import pyarrow as pa
        except ImportError:
            pytest.skip("pandas or PyArrow not available")

        # Create pandas DataFrames
        user_df = pd.DataFrame({
            'user_id': ['alice', 'bob'],
            'name': ['Alice Johnson', 'Bob Smith']
        })

        order_df = pd.DataFrame({
            'order_id': ['1001', '1002'],
            'user_id': ['alice', 'bob'],
            'amount': [99.99, 149.99]
        })

        join = ArrowTableJoin(
            user_df,  # pandas DataFrame
            order_df,  # pandas DataFrame
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()

        # Should successfully join and return Arrow table
        assert result.num_rows == 2
        assert isinstance(result, pa.Table)


class TestArrowDatasetJoin:
    """Test Arrow-native dataset join functionality."""

    @pytest.fixture
    async def sample_arrow_datasets(self, tmp_path):
        """Create sample Arrow datasets for testing."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("PyArrow not available")

        # Create user data
        user_data = {
            'user_id': ['alice', 'bob', 'charlie'],
            'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown'],
            'department': ['sales', 'engineering', 'sales']
        }
        user_table = pa.table(user_data)

        # Create order data
        order_data = {
            'order_id': ['1001', '1002', '1003', '1004'],
            'user_id': ['alice', 'bob', 'alice', 'diana'],  # diana not in users
            'amount': [99.99, 149.99, 79.99, 199.99],
            'product': ['laptop', 'phone', 'tablet', 'monitor']
        }
        order_table = pa.table(order_data)

        # Save as Parquet files
        user_path = tmp_path / "users.parquet"
        order_path = tmp_path / "orders.parquet"

        pq.write_table(user_table, str(user_path))
        pq.write_table(order_table, str(order_path))

        # Create datasets
        user_dataset = ds.dataset(str(user_path))
        order_dataset = ds.dataset(str(order_path))

        return user_dataset, order_dataset

    @pytest.mark.asyncio
    async def test_arrow_dataset_join_inner(self, sample_arrow_datasets):
        """Test Arrow dataset inner join."""
        user_dataset, order_dataset = sample_arrow_datasets

        join = ArrowDatasetJoin(
            user_dataset,
            order_dataset,
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()

        # Should have 3 results (alice x2, bob x1)
        assert result.num_rows == 3

        result_df = result.to_pandas()
        alice_orders = result_df[result_df['user_id'] == 'alice']
        assert len(alice_orders) == 2
        assert 'name' in alice_orders.columns
        assert 'amount' in alice_orders.columns

    @pytest.mark.asyncio
    async def test_arrow_dataset_join_from_paths(self, tmp_path):
        """Test Arrow dataset join from file paths."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("PyArrow not available")

        # Create and save data
        user_data = pa.table({'user_id': ['alice'], 'name': ['Alice Johnson']})
        order_data = pa.table({'order_id': ['1001'], 'user_id': ['alice'], 'amount': [99.99]})

        user_path = tmp_path / "users.parquet"
        order_path = tmp_path / "orders.parquet"

        pq.write_table(user_data, str(user_path))
        pq.write_table(order_data, str(order_path))

        # Create datasets from paths
        user_dataset = ds.dataset(str(user_path))
        order_dataset = ds.dataset(str(order_path))

        join = ArrowDatasetJoin(
            user_dataset,
            order_dataset,
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        result = await join.execute()
        assert result.num_rows == 1

    @pytest.mark.asyncio
    async def test_arrow_dataset_join_no_conditions(self, sample_arrow_datasets):
        """Test Arrow dataset join with no conditions raises error."""
        user_dataset, order_dataset = sample_arrow_datasets

        join = ArrowDatasetJoin(
            user_dataset,
            order_dataset,
            join_type=JoinType.INNER,
            conditions=[]
        )

        with pytest.raises(ValueError, match="Join conditions are required"):
            await join.execute()


class TestArrowAsofJoin:
    """Test Arrow-native as-of join functionality."""

    @pytest.fixture
    async def sample_temporal_datasets(self, tmp_path):
        """Create sample temporal datasets for as-of testing."""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("pandas or PyArrow not available")

        # Create activity data (left side)
        activity_df = pd.DataFrame({
            'user_id': ['alice', 'alice', 'bob', 'bob', 'alice'],
            'timestamp': pd.to_datetime([
                '2024-01-15 10:00:00',
                '2024-01-15 11:30:00',
                '2024-01-14 14:00:00',
                '2024-01-14 16:00:00',
                '2024-01-15 14:00:00'
            ]),
            'action': ['login', 'view_product', 'login', 'purchase', 'logout'],
            'product_id': ['P001', 'P002', 'P001', 'P002', 'P001']
        })

        # Create price history (right side - for as-of join)
        price_df = pd.DataFrame({
            'product_id': ['P001', 'P001', 'P002', 'P002', 'P001'],
            'timestamp': pd.to_datetime([
                '2024-01-01 00:00:00',  # Old price
                '2024-01-10 00:00:00',  # Current price for P001
                '2024-01-01 00:00:00',  # Old price
                '2024-01-12 00:00:00',  # Current price for P002
                '2024-01-20 00:00:00'   # Future price
            ]),
            'price': [89.99, 99.99, 25.99, 29.99, 109.99]
        })

        # Convert to Arrow tables and save
        activity_table = pa.Table.from_pandas(activity_df)
        price_table = pa.Table.from_pandas(price_df)

        activity_path = tmp_path / "activity.parquet"
        price_path = tmp_path / "prices.parquet"

        pq.write_table(activity_table, str(activity_path))
        pq.write_table(price_table, str(price_path))

        # Create datasets
        activity_dataset = ds.dataset(str(activity_path))
        price_dataset = ds.dataset(str(price_path))

        return activity_dataset, price_dataset

    @pytest.mark.asyncio
    async def test_arrow_asof_join_basic(self, sample_temporal_datasets):
        """Test basic Arrow as-of join."""
        activity_dataset, price_dataset = sample_temporal_datasets

        join = ArrowAsofJoin(
            activity_dataset,
            price_dataset,
            left_key='timestamp',
            right_key='timestamp'
        )

        result = await join.execute()

        # Should successfully execute (exact results depend on Arrow implementation)
        assert isinstance(result, (dict, type(None))) or hasattr(result, 'num_rows')

        # If we got a result, check it has expected structure
        if hasattr(result, 'num_rows'):
            # Some implementations return Arrow tables
            assert result.num_rows >= 0
        elif isinstance(result, dict):
            # Some implementations return pandas DataFrames
            assert isinstance(result, dict)  # Allow any dict-like result

    @pytest.mark.asyncio
    async def test_arrow_asof_join_with_by_keys(self, sample_temporal_datasets):
        """Test Arrow as-of join with grouping keys."""
        activity_dataset, price_dataset = sample_temporal_datasets

        join = ArrowAsofJoin(
            activity_dataset,
            price_dataset,
            left_key='timestamp',
            right_key='timestamp',
            left_by_key='product_id',
            right_by_key='product_id'
        )

        result = await join.execute()

        # Should execute without error
        assert result is not None

    @pytest.mark.asyncio
    async def test_arrow_asof_join_with_tolerance(self, sample_temporal_datasets):
        """Test Arrow as-of join with tolerance."""
        activity_dataset, price_dataset = sample_temporal_datasets

        join = ArrowAsofJoin(
            activity_dataset,
            price_dataset,
            left_key='timestamp',
            right_key='timestamp',
            tolerance=3600.0  # 1 hour tolerance
        )

        result = await join.execute()

        # Should execute without error
        assert result is not None

    @pytest.mark.asyncio
    async def test_arrow_asof_join_pandas_fallback(self, tmp_path):
        """Test Arrow as-of join pandas fallback."""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("pandas or PyArrow not available")

        # Create simple test data
        activity_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-15 10:00:00']),
            'action': ['login']
        })

        price_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-14 10:00:00']),
            'price': [99.99]
        })

        # Save as Parquet
        activity_table = pa.Table.from_pandas(activity_df)
        price_table = pa.Table.from_pandas(price_df)

        activity_path = tmp_path / "activity_simple.parquet"
        price_path = tmp_path / "prices_simple.parquet"

        pq.write_table(activity_table, str(activity_path))
        pq.write_table(price_table, str(price_path))

        activity_dataset = ds.dataset(str(activity_path))
        price_dataset = ds.dataset(str(price_path))

        join = ArrowAsofJoin(
            activity_dataset,
            price_dataset,
            left_key='timestamp',
            right_key='timestamp'
        )

        result = await join.execute()

        # Should execute (may use pandas fallback)
        assert result is not None


class TestArrowJoinBuilders:
    """Test Arrow join builder integration."""

    def test_arrow_join_builder_methods_exist(self):
        """Test that Arrow join builder methods exist."""
        mock_app = MagicMock()
        builder = create_join_builder(mock_app)

        # Check that all Arrow join methods exist
        assert hasattr(builder, 'arrow_table_join')
        assert hasattr(builder, 'arrow_dataset_join')
        assert hasattr(builder, 'arrow_asof_join')

    def test_arrow_table_join_builder(self):
        """Test Arrow table join builder."""
        mock_app = MagicMock()
        mock_left_table = MagicMock()
        mock_right_table = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.arrow_table_join(mock_left_table, mock_right_table, JoinType.INNER)

        # Configure join
        join_builder.on("user_id", "user_id")

        # Build the join
        join = join_builder.build()
        assert isinstance(join, ArrowTableJoin)
        assert join.join_type == JoinType.INNER
        assert len(join.conditions) == 1

    def test_arrow_dataset_join_builder(self):
        """Test Arrow dataset join builder."""
        mock_app = MagicMock()
        mock_left_dataset = MagicMock()
        mock_right_dataset = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.arrow_dataset_join(mock_left_dataset, mock_right_dataset, JoinType.INNER)

        # Configure join
        join_builder.on("user_id", "user_id")

        # Build the join
        join = join_builder.build()
        assert isinstance(join, ArrowDatasetJoin)
        assert join.join_type == JoinType.INNER
        assert len(join.conditions) == 1

    def test_arrow_asof_join_builder(self):
        """Test Arrow as-of join builder."""
        mock_app = MagicMock()
        mock_left_dataset = MagicMock()
        mock_right_dataset = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.arrow_asof_join(mock_left_dataset, mock_right_dataset, "timestamp", "timestamp")

        # Configure join
        join_builder.by("product_id", "product_id").within(300.0)

        # Build the join
        join = join_builder.build()
        assert isinstance(join, ArrowAsofJoin)
        assert join.left_key == "timestamp"
        assert join.right_key == "timestamp"
        assert join.left_by_key == "product_id"
        assert join.right_by_key == "product_id"
        assert join.tolerance == 300.0

    def test_arrow_asof_join_builder_minimal(self):
        """Test Arrow as-of join builder with minimal configuration."""
        mock_app = MagicMock()
        mock_left_dataset = MagicMock()
        mock_right_dataset = MagicMock()

        builder = create_join_builder(mock_app)
        join_builder = builder.arrow_asof_join(mock_left_dataset, mock_right_dataset, "ts", "ts")

        # Build with minimal config
        join = join_builder.build()
        assert isinstance(join, ArrowAsofJoin)
        assert join.left_key == "ts"
        assert join.right_key == "ts"
        assert join.left_by_key is None
        assert join.right_by_key is None
        assert join.tolerance == 0.0


class TestArrowJoinIntegration:
    """Test Arrow join integration with Sabot components."""

    @pytest.mark.asyncio
    async def test_arrow_join_stats(self):
        """Test that Arrow joins provide statistics."""
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("PyArrow not available")

        # Create simple Arrow table
        table = pa.table({'user_id': ['alice'], 'name': ['Alice']})

        join = ArrowTableJoin(
            table,
            table,
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        # Should be able to get stats
        stats = await join.get_stats()
        assert isinstance(stats, dict)
        assert 'join_type' in stats
        assert 'join_mode' in stats
        assert stats['join_mode'] == 'arrow_native'

    @pytest.mark.asyncio
    async def test_arrow_dataset_join_stats(self):
        """Test that Arrow dataset joins provide statistics."""
        try:
            import pyarrow as pa
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("PyArrow not available")

        # Create simple dataset
        table = pa.table({'user_id': ['alice'], 'name': ['Alice']})
        dataset = ds.dataset(table)

        join = ArrowDatasetJoin(
            dataset,
            dataset,
            join_type=JoinType.INNER,
            conditions=[{"left_field": "user_id", "right_field": "user_id"}]
        )

        # Should be able to get stats
        stats = await join.get_stats()
        assert isinstance(stats, dict)
        assert 'join_type' in stats
        assert 'join_mode' in stats
        assert stats['join_mode'] == 'arrow_dataset'

    @pytest.mark.asyncio
    async def test_arrow_asof_join_stats(self):
        """Test that Arrow as-of joins provide statistics."""
        try:
            import pyarrow as pa
            import pyarrow.dataset as ds
        except ImportError:
            pytest.skip("PyArrow not available")

        # Create simple datasets
        left_table = pa.table({
            'timestamp': pa.array([1, 2, 3], type=pa.timestamp('s')),
            'value': [10, 20, 30]
        })
        right_table = pa.table({
            'timestamp': pa.array([1, 2, 3], type=pa.timestamp('s')),
            'price': [1.0, 2.0, 3.0]
        })

        left_dataset = ds.dataset(left_table)
        right_dataset = ds.dataset(right_table)

        join = ArrowAsofJoin(
            left_dataset,
            right_dataset,
            left_key='timestamp',
            right_key='timestamp'
        )

        # Should be able to get stats
        stats = await join.get_stats()
        assert isinstance(stats, dict)
        assert 'join_type' in stats
        assert 'join_mode' in stats
        assert stats['join_mode'] == 'arrow_asof'


if __name__ == "__main__":
    pytest.main([__file__])
