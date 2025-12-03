#!/usr/bin/env python3
"""
SQL API Facade

Unified SQL API that wraps existing sabot_sql implementation.
Provides integration with unified Sabot engine.

Includes:
- Standard SQL execution via DuckDB
- Distributed SQL execution via SQLController
- Time series operators (ASOF join, rolling windows, etc.)
"""

import logging
from typing import Optional, Any, Dict, List, Callable

logger = logging.getLogger(__name__)


class SQLAPI:
    """
    SQL processing API facade.
    
    Wraps sabot_sql (DuckDB fork) and integrates with unified engine.
    Provides SQL execution with streaming semantics.
    
    Example:
        engine = Sabot()
        result = engine.sql("SELECT * FROM table WHERE x > 10")
        
        # Or call execute directly
        result = engine.sql.execute("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id")
    """
    
    def __init__(self, engine):
        """
        Initialize SQL API.

        Args:
            engine: Parent Sabot engine instance
        """
        self.engine = engine
        self._sql_bridge = None
        self._distributed_controller = None
        self._timeseries = None
    
    def _get_bridge(self):
        """Lazy-load SQL bridge."""
        if self._sql_bridge is None:
            try:
                from sabot_sql import create_sabot_sql_bridge
                self._sql_bridge = create_sabot_sql_bridge()
                logger.info("Initialized sabot_sql bridge")
            except ImportError as e:
                logger.error(f"Failed to load sabot_sql: {e}")
                raise ImportError(
                    "sabot_sql not available. Ensure it's built and installed."
                ) from e

        return self._sql_bridge

    def _get_distributed_controller(self):
        """Lazy-load distributed SQL controller."""
        if self._distributed_controller is None:
            try:
                from sabot.sql.controller import SQLController
                self._distributed_controller = SQLController()
                logger.info("Initialized distributed SQL controller")
            except ImportError as e:
                logger.error(f"Failed to load SQLController: {e}")
                raise ImportError(
                    "SQLController not available."
                ) from e

        return self._distributed_controller

    @property
    def timeseries(self):
        """
        Access time series operations.

        Returns:
            TimeSeriesAPI: Time series processing interface

        Example:
            # ASOF join trades with quotes
            result = engine.sql.timeseries.asof_join(
                trades, quotes,
                time_column='timestamp',
                direction='backward'
            )

            # Rolling window
            result = engine.sql.timeseries.rolling(
                prices,
                window_size=20,
                column='close',
                agg='mean'
            )
        """
        if self._timeseries is None:
            self._timeseries = TimeSeriesAPI(self)
        return self._timeseries
    
    def execute(self, query: str, **options) -> Any:
        """
        Execute SQL query.
        
        Args:
            query: SQL query string
            **options: Execution options
            
        Returns:
            Query result (Arrow table or stream)
            
        Example:
            result = engine.sql.execute("SELECT * FROM table WHERE x > 10")
            
            # With options
            result = engine.sql.execute(
                "SELECT * FROM large_table",
                streaming=True,
                batch_size=10000
            )
        """
        bridge = self._get_bridge()
        
        try:
            # Execute via sabot_sql
            result = bridge.execute_sql(query)
            
            # Wrap result to integrate with engine
            return self._wrap_result(result, options)
            
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise
    
    def _wrap_result(self, result, options):
        """Wrap SQL result for unified API."""
        # If streaming mode, return as Stream
        if options.get('streaming', False):
            from sabot.api.stream import Stream
            return Stream.from_table(result, batch_size=options.get('batch_size', 100000))
        
        # Otherwise return raw table
        return result
    
    def register_table(self, name: str, table: Any):
        """
        Register table for SQL queries.
        
        Args:
            name: Table name
            table: Arrow table or compatible object
            
        Example:
            engine.sql.register_table('events', events_table)
            result = engine.sql.execute("SELECT * FROM events WHERE status = 'active'")
        """
        bridge = self._get_bridge()
        
        try:
            bridge.register_table(name, table)
            logger.info(f"Registered SQL table: {name}")
        except Exception as e:
            logger.error(f"Failed to register table {name}: {e}")
            raise
    
    def unregister_table(self, name: str):
        """
        Unregister table.
        
        Args:
            name: Table name to unregister
        """
        bridge = self._get_bridge()
        
        try:
            bridge.unregister_table(name)
            logger.info(f"Unregistered SQL table: {name}")
        except Exception as e:
            logger.error(f"Failed to unregister table {name}: {e}")
            raise
    
    def list_tables(self) -> list:
        """
        List registered tables.
        
        Returns:
            List of table names
        """
        bridge = self._get_bridge()
        
        try:
            return bridge.list_tables()
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def explain(self, query: str) -> str:
        """
        Get query execution plan.
        
        Args:
            query: SQL query
            
        Returns:
            Execution plan as string
            
        Example:
            plan = engine.sql.explain("SELECT * FROM table WHERE x > 10")
            print(plan)
        """
        bridge = self._get_bridge()
        
        try:
            return bridge.explain(query)
        except Exception as e:
            logger.error(f"Failed to explain query: {e}")
            raise
    
    def __call__(self, query: str, **options) -> Any:
        """
        Convenience: Call SQL API directly.
        
        Args:
            query: SQL query
            **options: Execution options
            
        Returns:
            Query result
            
        Example:
            result = engine.sql("SELECT * FROM table")
        """
        return self.execute(query, **options)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get SQL engine statistics.
        
        Returns:
            Dict with SQL stats
        """
        stats = {'api': 'sql'}
        
        if self._sql_bridge:
            try:
                stats['tables_registered'] = len(self.list_tables())
                stats['bridge_loaded'] = True
            except:
                stats['bridge_loaded'] = False
        else:
            stats['bridge_loaded'] = False
        
        return stats


class TimeSeriesAPI:
    """
    Time series operations API.

    Provides direct access to time series operators:
    - ASOF Join (join with most recent match)
    - Interval Join (join within time interval)
    - Time Bucket (tumbling windows)
    - Rolling Window (moving aggregates)
    - Session Window (gap-based sessions)
    - Resample (up/downsample)
    - Fill (forward/backward fill, interpolation)
    - EWMA (exponentially weighted moving average)
    - Log Returns (financial returns)

    Example:
        engine = Sabot()

        # ASOF join trades with quotes
        result = engine.sql.timeseries.asof_join(
            trades, quotes,
            time_column='timestamp'
        )

        # Rolling mean
        result = engine.sql.timeseries.rolling(
            prices, window_size=20, column='close', agg='mean'
        )
    """

    def __init__(self, sql_api: SQLAPI):
        """Initialize time series API."""
        self._sql_api = sql_api

    def _get_controller(self):
        """Get the distributed SQL controller."""
        return self._sql_api._get_distributed_controller()

    def _build_operator(self, spec):
        """Build an operator from spec."""
        controller = self._get_controller()
        return controller._default_operator_builder(spec, None)

    def asof_join(
        self,
        left_table,
        right_table,
        time_column: str = 'timestamp',
        direction: str = 'backward',
        left_keys: List[str] = None,
        right_keys: List[str] = None
    ):
        """
        ASOF Join - join with most recent match by timestamp.

        Args:
            left_table: Left Arrow table
            right_table: Right Arrow table
            time_column: Timestamp column name
            direction: 'backward' (most recent before) or 'forward' (nearest after)
            left_keys: Additional join keys from left table
            right_keys: Additional join keys from right table

        Returns:
            Joined Arrow table

        Example:
            # Join trades with most recent quote
            result = engine.sql.timeseries.asof_join(
                trades, quotes,
                time_column='timestamp',
                direction='backward'
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="AsofJoin",
            name="asof_join",
            estimated_cardinality=left_table.num_rows,
            time_column=time_column,
            time_direction=direction,
            left_keys=left_keys or [],
            right_keys=right_keys or []
        )

        operator = self._build_operator(spec)
        return operator(left_table, right_table)

    def interval_join(
        self,
        left_table,
        right_table,
        time_column: str = 'timestamp',
        lower_ms: int = 0,
        upper_ms: int = 0
    ):
        """
        Interval Join - join within time interval.

        Args:
            left_table: Left Arrow table
            right_table: Right Arrow table
            time_column: Timestamp column name
            lower_ms: Lower bound in milliseconds (can be negative)
            upper_ms: Upper bound in milliseconds

        Returns:
            Joined Arrow table

        Example:
            # Join events within 5 seconds
            result = engine.sql.timeseries.interval_join(
                events_a, events_b,
                time_column='timestamp',
                lower_ms=-5000,
                upper_ms=5000
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="IntervalJoin",
            name="interval_join",
            estimated_cardinality=left_table.num_rows,
            time_column=time_column,
            interval_lower=lower_ms,
            interval_upper=upper_ms
        )

        operator = self._build_operator(spec)
        return operator(left_table, right_table)

    def time_bucket(
        self,
        table,
        time_column: str = 'timestamp',
        bucket_size: str = '1h',
        group_by: List[str] = None,
        agg_functions: List[str] = None,
        agg_columns: List[str] = None
    ):
        """
        Time Bucket - group into time buckets (tumbling windows).

        Args:
            table: Arrow table
            time_column: Timestamp column name
            bucket_size: Bucket size (e.g., '1h', '5m', '1d')
            group_by: Additional grouping columns
            agg_functions: Aggregation functions ('sum', 'mean', 'min', 'max', etc.)
            agg_columns: Columns to aggregate

        Returns:
            Bucketed Arrow table

        Example:
            # Hourly OHLC bars
            result = engine.sql.timeseries.time_bucket(
                trades,
                time_column='timestamp',
                bucket_size='1h',
                group_by=['symbol'],
                agg_functions=['first', 'max', 'min', 'last'],
                agg_columns=['price', 'price', 'price', 'price']
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="TimeBucket",
            name="time_bucket",
            estimated_cardinality=table.num_rows // 10,
            time_column=time_column,
            bucket_size=bucket_size,
            group_by_keys=group_by or [],
            aggregate_functions=agg_functions or [],
            aggregate_columns=agg_columns or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def rolling(
        self,
        table,
        window_size: int,
        column: str,
        agg: str = 'mean',
        time_column: str = 'timestamp',
        min_periods: int = 1,
        group_by: List[str] = None
    ):
        """
        Rolling Window - calculate moving aggregates.

        Args:
            table: Arrow table
            window_size: Number of rows in window
            column: Column to aggregate
            agg: Aggregation function ('mean', 'sum', 'min', 'max', 'std', 'var')
            time_column: Timestamp column for sorting
            min_periods: Minimum observations required
            group_by: Optional grouping columns

        Returns:
            Table with rolling column added

        Example:
            # 20-day moving average
            result = engine.sql.timeseries.rolling(
                prices,
                window_size=20,
                column='close',
                agg='mean'
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="RollingWindow",
            name="rolling_window",
            estimated_cardinality=table.num_rows,
            time_column=time_column,
            rolling_window_size=window_size,
            rolling_min_periods=min_periods,
            aggregate_functions=[agg],
            aggregate_columns=[column],
            group_by_keys=group_by or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def session_window(
        self,
        table,
        time_column: str = 'timestamp',
        gap_ms: int = 30000,
        group_by: List[str] = None,
        agg_functions: List[str] = None,
        agg_columns: List[str] = None
    ):
        """
        Session Window - group by session gaps.

        Args:
            table: Arrow table
            time_column: Timestamp column name
            gap_ms: Session gap in milliseconds
            group_by: Additional grouping columns
            agg_functions: Aggregation functions
            agg_columns: Columns to aggregate

        Returns:
            Aggregated sessions

        Example:
            # User sessions with 30s gap
            result = engine.sql.timeseries.session_window(
                events,
                time_column='timestamp',
                gap_ms=30000,
                group_by=['user_id'],
                agg_functions=['count', 'sum'],
                agg_columns=['event_id', 'value']
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="SessionWindow",
            name="session_window",
            estimated_cardinality=table.num_rows // 5,
            time_column=time_column,
            session_gap=gap_ms,
            group_by_keys=group_by or [],
            aggregate_functions=agg_functions or [],
            aggregate_columns=agg_columns or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def resample(
        self,
        table,
        time_column: str = 'timestamp',
        rule: str = '1h',
        agg: str = 'mean',
        group_by: List[str] = None
    ):
        """
        Resample - up/downsample time series.

        Args:
            table: Arrow table
            time_column: Timestamp column name
            rule: Resample frequency (e.g., '1h', '5m', '1d')
            agg: Aggregation method ('mean', 'sum', 'min', 'max', 'first', 'last', 'ohlc')
            group_by: Optional grouping columns

        Returns:
            Resampled table

        Example:
            # Downsample to hourly
            result = engine.sql.timeseries.resample(
                minute_data,
                time_column='timestamp',
                rule='1h',
                agg='mean'
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="Resample",
            name="resample",
            estimated_cardinality=table.num_rows // 10,
            time_column=time_column,
            resample_rule=rule,
            resample_agg=agg,
            group_by_keys=group_by or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def fill(
        self,
        table,
        method: str = 'forward',
        columns: List[str] = None,
        limit: int = None
    ):
        """
        Fill - handle missing values.

        Args:
            table: Arrow table
            method: Fill method ('forward', 'backward', 'linear', 'zero', 'null')
            columns: Columns to fill (None = all)
            limit: Max consecutive fills

        Returns:
            Table with filled values

        Example:
            # Forward fill missing prices
            result = engine.sql.timeseries.fill(
                prices,
                method='forward',
                columns=['close'],
                limit=5
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="Fill",
            name="fill",
            estimated_cardinality=table.num_rows,
            fill_method=method,
            fill_limit=limit or 0,
            aggregate_columns=columns or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def ewma(
        self,
        table,
        column: str,
        alpha: float = 0.5,
        adjust: bool = True,
        group_by: List[str] = None
    ):
        """
        EWMA - Exponentially Weighted Moving Average.

        Args:
            table: Arrow table
            column: Column to smooth
            alpha: Decay factor (0-1, higher = more weight on recent)
            adjust: Adjust for initial values
            group_by: Optional grouping columns

        Returns:
            Table with EWMA column added

        Example:
            # Exponential smoothing
            result = engine.sql.timeseries.ewma(
                prices,
                column='close',
                alpha=0.1
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="EWMA",
            name="ewma",
            estimated_cardinality=table.num_rows,
            ewma_alpha=alpha,
            ewma_adjust=adjust,
            aggregate_columns=[column],
            group_by_keys=group_by or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def log_returns(
        self,
        table,
        column: str = 'price',
        group_by: List[str] = None
    ):
        """
        Log Returns - calculate financial log returns.

        Formula: r_t = log(p_t / p_{t-1})

        Args:
            table: Arrow table
            column: Price column name
            group_by: Optional grouping columns (e.g., ['symbol'])

        Returns:
            Table with log returns column added

        Example:
            # Calculate log returns per symbol
            result = engine.sql.timeseries.log_returns(
                prices,
                column='close',
                group_by=['symbol']
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="LogReturns",
            name="log_returns",
            estimated_cardinality=table.num_rows,
            aggregate_columns=[column],
            group_by_keys=group_by or []
        )

        operator = self._build_operator(spec)
        return operator(table)

    def time_diff(
        self,
        table,
        time_column: str = 'timestamp',
        group_by: List[str] = None
    ):
        """
        Time Diff - calculate time differences between rows.

        Args:
            table: Arrow table
            time_column: Timestamp column name
            group_by: Optional grouping columns

        Returns:
            Table with time_diff and time_diff_seconds columns

        Example:
            # Calculate inter-event times
            result = engine.sql.timeseries.time_diff(
                events,
                time_column='timestamp',
                group_by=['user_id']
            )
        """
        from sabot.sql.distributed_plan import OperatorSpec

        spec = OperatorSpec(
            type="TimeDiff",
            name="time_diff",
            estimated_cardinality=table.num_rows,
            time_column=time_column,
            group_by_keys=group_by or []
        )

        operator = self._build_operator(spec)
        return operator(table)
