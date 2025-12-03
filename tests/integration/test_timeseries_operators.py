"""
Time Series Operator Tests for Sabot SQL Controller.

Tests all time series operators:
- ASOF Join (join with most recent match)
- Interval Join (join within time interval)
- Time Bucket (tumbling windows)
- Session Window (gap-based sessions)
- Rolling Window (moving aggregates)
- Resample (up/downsample)
- Fill (forward/backward fill, interpolation)
- Time Diff (calculate time differences)
- EWMA (exponentially weighted moving average)
- Log Returns (financial log returns)
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any

# Configure pytest-asyncio
pytest_plugins = ['pytest_asyncio']
pytestmark = pytest.mark.asyncio(loop_scope="function")

# Import cyarrow and sabot modules
from sabot import cyarrow as ca

import pandas as pd
import numpy as np

from sabot.sql.controller import SQLController
from sabot.sql.distributed_plan import OperatorSpec


class TestAsofJoin:
    """Tests for ASOF Join operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def trades_table(self):
        """Create sample trades data."""
        data = {
            'symbol': ['AAPL', 'AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
            'trade_time': pd.to_datetime([
                '2024-01-01 10:00:00',
                '2024-01-01 10:05:00',
                '2024-01-01 10:10:00',
                '2024-01-01 10:02:00',
                '2024-01-01 10:07:00'
            ]),
            'price': [150.0, 151.0, 152.0, 2800.0, 2810.0],
            'quantity': [100, 200, 150, 50, 75]
        }
        return ca.table(data)

    @pytest.fixture
    def quotes_table(self):
        """Create sample quotes data."""
        data = {
            'symbol': ['AAPL', 'AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
            'trade_time': pd.to_datetime([  # Same column name as trades
                '2024-01-01 09:59:00',
                '2024-01-01 10:04:00',
                '2024-01-01 10:09:00',
                '2024-01-01 10:01:00',
                '2024-01-01 10:06:00'
            ]),
            'bid': [149.5, 150.5, 151.5, 2795.0, 2805.0],
            'ask': [150.5, 151.5, 152.5, 2805.0, 2815.0]
        }
        return ca.table(data)

    async def test_asof_join_backward(self, controller, trades_table, quotes_table):
        """Test ASOF join with backward direction (most recent before)."""
        spec = OperatorSpec(
            type="AsofJoin",
            name="asof_join",
            estimated_cardinality=5,
            time_column='trade_time',
            time_direction='backward'
        )

        operator = controller._default_operator_builder(spec, trades_table)
        result = operator(trades_table, quotes_table)

        # Should have matched quotes with most recent quote before each trade
        assert result is not None
        assert result.num_rows == 5

    async def test_asof_join_forward(self, controller, trades_table, quotes_table):
        """Test ASOF join with forward direction (nearest after)."""
        spec = OperatorSpec(
            type="AsofJoin",
            name="asof_join",
            estimated_cardinality=5,
            time_column='trade_time',
            time_direction='forward'
        )

        operator = controller._default_operator_builder(spec, trades_table)
        result = operator(trades_table, quotes_table)

        assert result is not None


class TestIntervalJoin:
    """Tests for Interval Join operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def events_a(self):
        """Create events A."""
        data = {
            'event_id': [1, 2, 3],
            'timestamp': pd.to_datetime([
                '2024-01-01 10:00:00',
                '2024-01-01 10:05:00',
                '2024-01-01 10:10:00'
            ]),
            'type': ['click', 'view', 'click']
        }
        return ca.table(data)

    @pytest.fixture
    def events_b(self):
        """Create events B."""
        data = {
            'session_id': [101, 102, 103, 104],
            'timestamp': pd.to_datetime([
                '2024-01-01 09:59:30',  # 30s before event 1
                '2024-01-01 10:00:30',  # 30s after event 1
                '2024-01-01 10:05:30',  # 30s after event 2
                '2024-01-01 10:12:00'   # 2m after event 3
            ]),
            'category': ['A', 'B', 'A', 'C']
        }
        return ca.table(data)

    async def test_interval_join_within_range(self, controller, events_a, events_b):
        """Test interval join within specified time range."""
        spec = OperatorSpec(
            type="IntervalJoin",
            name="interval_join",
            estimated_cardinality=10,
            time_column='timestamp',
            interval_lower=-60000,  # -60 seconds
            interval_upper=60000    # +60 seconds
        )

        operator = controller._default_operator_builder(spec, events_a)
        result = operator(events_a, events_b)

        assert result is not None
        # Event 1 should match events within 60 seconds (sessions 101, 102)
        # Event 2 should match session 103
        # Event 3 might match session 104 if within range


class TestTimeBucket:
    """Tests for Time Bucket operator (tumbling windows)."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def sensor_data(self):
        """Create sensor data with timestamps."""
        timestamps = pd.date_range('2024-01-01', periods=100, freq='1min')
        data = {
            'timestamp': timestamps,
            'sensor_id': ['S1'] * 50 + ['S2'] * 50,
            'temperature': np.random.uniform(20, 30, 100),
            'humidity': np.random.uniform(40, 60, 100)
        }
        return ca.table(data)

    async def test_time_bucket_hourly(self, controller, sensor_data):
        """Test hourly time buckets."""
        spec = OperatorSpec(
            type="TimeBucket",
            name="time_bucket_1h",
            estimated_cardinality=10,
            time_column='timestamp',
            bucket_size='1h',
            aggregate_functions=['mean', 'max'],
            aggregate_columns=['temperature', 'humidity']
        )

        operator = controller._default_operator_builder(spec, sensor_data)
        result = operator(sensor_data)

        assert result is not None
        # Should bucket into fewer rows
        assert result.num_rows < sensor_data.num_rows

    async def test_time_bucket_with_group_by(self, controller, sensor_data):
        """Test time buckets with additional grouping."""
        spec = OperatorSpec(
            type="TimeBucket",
            name="time_bucket_grouped",
            estimated_cardinality=20,
            time_column='timestamp',
            bucket_size='30m',
            group_by_keys=['sensor_id'],
            aggregate_functions=['mean'],
            aggregate_columns=['temperature']
        )

        operator = controller._default_operator_builder(spec, sensor_data)
        result = operator(sensor_data)

        assert result is not None
        # Should have buckets per sensor
        result_df = result.to_pandas()
        assert 'sensor_id' in result_df.columns

    async def test_time_bucket_5_minute(self, controller, sensor_data):
        """Test 5-minute time buckets."""
        spec = OperatorSpec(
            type="TimeBucket",
            name="time_bucket_5m",
            estimated_cardinality=20,
            time_column='timestamp',
            bucket_size='5m',
            aggregate_functions=['sum'],
            aggregate_columns=['temperature']
        )

        operator = controller._default_operator_builder(spec, sensor_data)
        result = operator(sensor_data)

        assert result is not None


class TestSessionWindow:
    """Tests for Session Window operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def user_events(self):
        """Create user event data with session gaps."""
        # Create events with gaps to form sessions
        timestamps = [
            # Session 1 (user1): events within 30s
            '2024-01-01 10:00:00',
            '2024-01-01 10:00:15',
            '2024-01-01 10:00:25',
            # Gap > 30s
            # Session 2 (user1): new session
            '2024-01-01 10:01:30',
            '2024-01-01 10:01:45',
            # Session 1 (user2): different user
            '2024-01-01 10:00:05',
            '2024-01-01 10:00:20',
        ]
        data = {
            'timestamp': pd.to_datetime(timestamps),
            'user_id': ['user1', 'user1', 'user1', 'user1', 'user1', 'user2', 'user2'],
            'event_type': ['click', 'view', 'click', 'purchase', 'logout', 'click', 'view'],
            'value': [1, 1, 1, 100, 0, 1, 1]
        }
        return ca.table(data)

    async def test_session_window_basic(self, controller, user_events):
        """Test basic session window detection."""
        spec = OperatorSpec(
            type="SessionWindow",
            name="session_window",
            estimated_cardinality=10,
            time_column='timestamp',
            session_gap=30000  # 30 seconds
        )

        operator = controller._default_operator_builder(spec, user_events)
        result = operator(user_events)

        assert result is not None
        # Should have aggregated into sessions
        assert result.num_rows < user_events.num_rows

    async def test_session_window_with_aggregation(self, controller, user_events):
        """Test session window with aggregation."""
        spec = OperatorSpec(
            type="SessionWindow",
            name="session_window_agg",
            estimated_cardinality=10,
            time_column='timestamp',
            session_gap=30000,
            aggregate_functions=['sum', 'count'],
            aggregate_columns=['value', 'event_type']
        )

        operator = controller._default_operator_builder(spec, user_events)
        result = operator(user_events)

        assert result is not None


class TestRollingWindow:
    """Tests for Rolling Window operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def stock_prices(self):
        """Create stock price data."""
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        np.random.seed(42)  # For reproducibility
        data = {
            'date': dates,
            'symbol': ['AAPL'] * 100,
            'close': 150 + np.cumsum(np.random.randn(100) * 2),
            'volume': np.random.randint(1000000, 5000000, 100)
        }
        return ca.table(data)

    async def test_rolling_mean(self, controller, stock_prices):
        """Test rolling mean calculation."""
        spec = OperatorSpec(
            type="RollingWindow",
            name="rolling_mean",
            estimated_cardinality=100,
            time_column='date',
            rolling_window_size=20,
            rolling_min_periods=1,
            aggregate_functions=['mean'],
            aggregate_columns=['close']
        )

        operator = controller._default_operator_builder(spec, stock_prices)
        result = operator(stock_prices)

        assert result is not None
        result_df = result.to_pandas()
        assert 'close_rolling_mean' in result_df.columns
        # Rolling mean should smooth the data
        assert not result_df['close_rolling_mean'].isna().all()

    async def test_rolling_std(self, controller, stock_prices):
        """Test rolling standard deviation."""
        spec = OperatorSpec(
            type="RollingWindow",
            name="rolling_std",
            estimated_cardinality=100,
            time_column='date',
            rolling_window_size=20,
            rolling_min_periods=5,
            aggregate_functions=['std'],
            aggregate_columns=['close']
        )

        operator = controller._default_operator_builder(spec, stock_prices)
        result = operator(stock_prices)

        assert result is not None
        result_df = result.to_pandas()
        assert 'close_rolling_std' in result_df.columns

    async def test_rolling_multiple_aggregations(self, controller, stock_prices):
        """Test multiple rolling aggregations."""
        spec = OperatorSpec(
            type="RollingWindow",
            name="rolling_multi",
            estimated_cardinality=100,
            time_column='date',
            rolling_window_size=10,
            aggregate_functions=['min', 'max'],
            aggregate_columns=['close', 'close']
        )

        operator = controller._default_operator_builder(spec, stock_prices)
        result = operator(stock_prices)

        assert result is not None
        result_df = result.to_pandas()
        # Should have both rolling columns
        assert 'close_rolling_min' in result_df.columns
        assert 'close_rolling_max' in result_df.columns


class TestResample:
    """Tests for Resample operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def minute_data(self):
        """Create minute-level data for resampling."""
        timestamps = pd.date_range('2024-01-01', periods=1440, freq='1min')  # 1 day
        data = {
            'timestamp': timestamps,
            'value': np.random.uniform(0, 100, 1440),
            'category': np.random.choice(['A', 'B'], 1440)
        }
        return ca.table(data)

    async def test_resample_to_hourly(self, controller, minute_data):
        """Test resampling from minute to hour."""
        spec = OperatorSpec(
            type="Resample",
            name="resample_hourly",
            estimated_cardinality=24,
            time_column='timestamp',
            resample_rule='1h',
            resample_agg='mean'
        )

        operator = controller._default_operator_builder(spec, minute_data)
        result = operator(minute_data)

        assert result is not None
        # Should have ~24 rows (1 per hour)
        assert result.num_rows <= 24

    async def test_resample_sum(self, controller, minute_data):
        """Test resampling with sum aggregation."""
        spec = OperatorSpec(
            type="Resample",
            name="resample_sum",
            estimated_cardinality=24,
            time_column='timestamp',
            resample_rule='1h',
            resample_agg='sum'
        )

        operator = controller._default_operator_builder(spec, minute_data)
        result = operator(minute_data)

        assert result is not None

    async def test_resample_with_groups(self, controller, minute_data):
        """Test resampling within groups."""
        spec = OperatorSpec(
            type="Resample",
            name="resample_grouped",
            estimated_cardinality=48,
            time_column='timestamp',
            resample_rule='1h',
            resample_agg='mean',
            group_by_keys=['category']
        )

        operator = controller._default_operator_builder(spec, minute_data)
        result = operator(minute_data)

        assert result is not None
        # Should have more rows due to grouping
        result_df = result.to_pandas()
        assert 'category' in result_df.columns


class TestFill:
    """Tests for Fill/Interpolate operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def data_with_gaps(self):
        """Create data with missing values."""
        data = {
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='1h'),
            'value': [1.0, np.nan, np.nan, 4.0, 5.0, np.nan, 7.0, 8.0, np.nan, 10.0],
            'category': ['A', 'A', 'B', 'B', 'A', 'A', 'B', 'B', 'A', 'A']
        }
        return ca.table(data)

    async def test_forward_fill(self, controller, data_with_gaps):
        """Test forward fill."""
        spec = OperatorSpec(
            type="Fill",
            name="forward_fill",
            estimated_cardinality=10,
            fill_method='forward',
            aggregate_columns=['value']
        )

        operator = controller._default_operator_builder(spec, data_with_gaps)
        result = operator(data_with_gaps)

        result_df = result.to_pandas()
        # After ffill: [1, 1, 1, 4, 5, 5, 7, 8, 8, 10]
        assert not result_df['value'].isna().any()
        assert result_df['value'].iloc[1] == 1.0  # Filled from previous
        assert result_df['value'].iloc[5] == 5.0  # Filled from previous

    async def test_backward_fill(self, controller, data_with_gaps):
        """Test backward fill."""
        spec = OperatorSpec(
            type="Fill",
            name="backward_fill",
            estimated_cardinality=10,
            fill_method='backward',
            aggregate_columns=['value']
        )

        operator = controller._default_operator_builder(spec, data_with_gaps)
        result = operator(data_with_gaps)

        result_df = result.to_pandas()
        # After bfill: [1, 4, 4, 4, 5, 7, 7, 8, 10, 10]
        assert not result_df['value'].isna().any()
        assert result_df['value'].iloc[1] == 4.0  # Filled from next

    async def test_linear_interpolation(self, controller, data_with_gaps):
        """Test linear interpolation."""
        spec = OperatorSpec(
            type="Fill",
            name="linear_interp",
            estimated_cardinality=10,
            fill_method='linear',
            aggregate_columns=['value']
        )

        operator = controller._default_operator_builder(spec, data_with_gaps)
        result = operator(data_with_gaps)

        result_df = result.to_pandas()
        # After linear interp: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert not result_df['value'].isna().any()

    async def test_fill_with_limit(self, controller, data_with_gaps):
        """Test fill with limit on consecutive fills."""
        spec = OperatorSpec(
            type="Fill",
            name="fill_limited",
            estimated_cardinality=10,
            fill_method='forward',
            fill_limit=1,  # Only fill 1 consecutive gap
            aggregate_columns=['value']
        )

        operator = controller._default_operator_builder(spec, data_with_gaps)
        result = operator(data_with_gaps)

        result_df = result.to_pandas()
        # Should only fill 1 consecutive NaN
        assert result_df['value'].iloc[1] == 1.0  # First gap filled
        assert pd.isna(result_df['value'].iloc[2])  # Second gap not filled


class TestTimeDiff:
    """Tests for Time Diff operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def event_data(self):
        """Create event data with timestamps."""
        data = {
            'timestamp': pd.to_datetime([
                '2024-01-01 10:00:00',
                '2024-01-01 10:00:30',
                '2024-01-01 10:01:15',
                '2024-01-01 10:05:00',
            ]),
            'user_id': ['user1', 'user1', 'user1', 'user1'],
            'event': ['start', 'click', 'view', 'end']
        }
        return ca.table(data)

    async def test_time_diff_basic(self, controller, event_data):
        """Test basic time diff calculation."""
        spec = OperatorSpec(
            type="TimeDiff",
            name="time_diff",
            estimated_cardinality=4,
            time_column='timestamp'
        )

        operator = controller._default_operator_builder(spec, event_data)
        result = operator(event_data)

        assert result is not None
        result_df = result.to_pandas()
        assert 'time_diff' in result_df.columns or 'time_diff_seconds' in result_df.columns

        # First diff should be NaT, second should be 30s, third should be 45s
        if 'time_diff_seconds' in result_df.columns:
            assert result_df['time_diff_seconds'].iloc[1] == 30.0
            assert result_df['time_diff_seconds'].iloc[2] == 45.0


class TestEWMA:
    """Tests for EWMA (Exponentially Weighted Moving Average) operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def price_data(self):
        """Create price data for EWMA."""
        np.random.seed(42)
        data = {
            'timestamp': pd.date_range('2024-01-01', periods=50, freq='D'),
            'price': 100 + np.cumsum(np.random.randn(50) * 2),
            'symbol': ['AAPL'] * 50
        }
        return ca.table(data)

    async def test_ewma_basic(self, controller, price_data):
        """Test basic EWMA calculation."""
        spec = OperatorSpec(
            type="EWMA",
            name="ewma",
            estimated_cardinality=50,
            ewma_alpha=0.3,
            ewma_adjust=True,
            aggregate_columns=['price']
        )

        operator = controller._default_operator_builder(spec, price_data)
        result = operator(price_data)

        assert result is not None
        result_df = result.to_pandas()
        assert 'price_ewma' in result_df.columns

        # EWMA should smooth the data
        assert not result_df['price_ewma'].isna().any()

    async def test_ewma_different_alphas(self, controller, price_data):
        """Test EWMA with different alpha values."""
        for alpha in [0.1, 0.5, 0.9]:
            spec = OperatorSpec(
                type="EWMA",
                name=f"ewma_{alpha}",
                estimated_cardinality=50,
                ewma_alpha=alpha,
                aggregate_columns=['price']
            )

            operator = controller._default_operator_builder(spec, price_data)
            result = operator(price_data)

            assert result is not None
            result_df = result.to_pandas()
            # Higher alpha = more weight on recent values = closer to actual prices
            assert 'price_ewma' in result_df.columns


class TestLogReturns:
    """Tests for Log Returns operator."""

    @pytest.fixture
    def controller(self):
        return SQLController()

    @pytest.fixture
    def price_series(self):
        """Create price series for log returns."""
        data = {
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'price': [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 110.0],
            'symbol': ['AAPL'] * 10
        }
        return ca.table(data)

    async def test_log_returns_basic(self, controller, price_series):
        """Test basic log returns calculation."""
        spec = OperatorSpec(
            type="LogReturns",
            name="log_returns",
            estimated_cardinality=10,
            aggregate_columns=['price']
        )

        operator = controller._default_operator_builder(spec, price_series)
        result = operator(price_series)

        assert result is not None
        result_df = result.to_pandas()
        assert 'price_log_returns' in result_df.columns

        # First value should be NaN (no previous price)
        assert pd.isna(result_df['price_log_returns'].iloc[0])

        # Log return from 100 to 102 should be log(102/100) ≈ 0.0198
        expected = np.log(102.0 / 100.0)
        assert abs(result_df['price_log_returns'].iloc[1] - expected) < 0.001


class TestEndToEnd:
    """End-to-end tests combining multiple time series operators."""

    @pytest.fixture
    def controller(self):
        controller = SQLController()
        # Store tables directly in controller.tables dict
        controller.tables = {
            'trades': self._create_trades(),
            'quotes': self._create_quotes()
        }
        return controller

    def _create_trades(self):
        """Create sample trades data."""
        np.random.seed(42)
        data = {
            'trade_time': pd.date_range('2024-01-01 09:30:00', periods=1000, freq='1min'),
            'symbol': np.random.choice(['AAPL', 'GOOGL', 'MSFT'], 1000),
            'price': np.random.uniform(100, 200, 1000),
            'quantity': np.random.randint(10, 1000, 1000)
        }
        return ca.table(data)

    def _create_quotes(self):
        """Create sample quotes data."""
        np.random.seed(43)
        data = {
            'quote_time': pd.date_range('2024-01-01 09:30:00', periods=1000, freq='1min'),
            'symbol': np.random.choice(['AAPL', 'GOOGL', 'MSFT'], 1000),
            'bid': np.random.uniform(99, 199, 1000),
            'ask': np.random.uniform(101, 201, 1000)
        }
        return ca.table(data)

    async def test_timeseries_pipeline(self, controller):
        """Test a pipeline of time series operations."""
        trades = controller.tables['trades']

        # 1. Calculate log returns
        log_returns_spec = OperatorSpec(
            type="LogReturns",
            name="log_returns",
            estimated_cardinality=1000,
            aggregate_columns=['price']
        )
        op1 = controller._default_operator_builder(log_returns_spec, trades)
        result = op1(trades)

        # 2. Calculate EWMA on returns
        ewma_spec = OperatorSpec(
            type="EWMA",
            name="ewma",
            estimated_cardinality=1000,
            ewma_alpha=0.1,
            aggregate_columns=['price_log_returns']
        )
        op2 = controller._default_operator_builder(ewma_spec, result)
        result = op2(result)

        # 3. Bucket into 5-minute intervals
        bucket_spec = OperatorSpec(
            type="TimeBucket",
            name="bucket_5m",
            estimated_cardinality=200,
            time_column='trade_time',
            bucket_size='5m',
            aggregate_functions=['mean', 'std'],
            aggregate_columns=['price', 'price_log_returns']
        )
        op3 = controller._default_operator_builder(bucket_spec, result)
        result = op3(result)

        assert result is not None
        assert result.num_rows < 1000  # Should be bucketed

    async def test_vwap_calculation(self, controller):
        """Test VWAP-like calculation using time buckets."""
        trades = controller.tables['trades']

        # Calculate VWAP per 5-minute bucket
        bucket_spec = OperatorSpec(
            type="TimeBucket",
            name="vwap_bucket",
            estimated_cardinality=200,
            time_column='trade_time',
            bucket_size='5m',
            group_by_keys=['symbol'],
            aggregate_functions=['sum', 'sum'],  # sum(price*qty) / sum(qty)
            aggregate_columns=['price', 'quantity']
        )

        op = controller._default_operator_builder(bucket_spec, trades)
        result = op(trades)

        assert result is not None
        result_df = result.to_pandas()
        assert 'symbol' in result_df.columns


# Run tests
if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
