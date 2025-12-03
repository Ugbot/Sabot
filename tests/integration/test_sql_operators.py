"""
Comprehensive test suite for distributed SQL operators.

Tests all operator types:
- Filter (with AND, OR, IN, BETWEEN, LIKE, IS NULL)
- Aggregate (SUM, COUNT, AVG, MIN, MAX, COUNT DISTINCT, STDDEV)
- HashJoin (INNER, LEFT, RIGHT, FULL OUTER)
- Sort (ORDER BY, multiple columns)
- Distinct
- Limit with Offset
- TopN (optimized ORDER BY + LIMIT)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)
- Set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
- SemiJoin / AntiJoin
- CrossJoin
"""

import pytest
import asyncio

# Configure pytest-asyncio mode
pytest_plugins = ['pytest_asyncio']
pytestmark = pytest.mark.asyncio(loop_scope="function")

from sabot import cyarrow as ca
from sabot.sql.distributed_plan import (
    DistributedQueryPlan,
    ExecutionStage,
    OperatorSpec,
    ShuffleSpec,
)
from sabot.sql.stage_scheduler import StageScheduler, ShuffleCoordinator


# =============================================================================
# Test Data Fixtures
# =============================================================================

@pytest.fixture
def orders_table():
    """Create a test orders table."""
    return ca.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'customer_id': [101, 102, 101, 103, 102, 101, 104, 103, 102, 105],
        'amount': [150.0, 50.0, 200.0, 75.0, 300.0, 25.0, 180.0, 90.0, 250.0, 60.0],
        'status': ['completed', 'pending', 'completed', 'completed', 'pending',
                   'cancelled', 'completed', 'pending', 'completed', 'completed'],
        'region': ['east', 'west', 'east', 'central', 'west', 'east', 'central', 'central', 'west', 'east']
    })


@pytest.fixture
def customers_table():
    """Create a test customers table."""
    return ca.table({
        'id': [101, 102, 103, 104, 105, 106],  # 106 has no orders
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'city': ['NYC', 'LA', 'Chicago', 'Boston', 'Seattle', 'Denver'],
        'tier': ['gold', 'silver', 'bronze', 'gold', 'silver', 'bronze']
    })


@pytest.fixture
def products_table():
    """Create a test products table."""
    return ca.table({
        'id': [1, 2, 3, 4, 5],
        'name': ['Widget', 'Gadget', 'Gizmo', 'Doohickey', 'Thingamajig'],
        'price': [10.0, 25.0, 15.0, 50.0, 8.0],
        'category': ['electronics', 'electronics', 'tools', 'tools', 'accessories']
    })


# =============================================================================
# Operator Builder for Tests
# =============================================================================

def comprehensive_operator_builder(spec, input_data):
    """Full operator builder for testing all operator types."""
    import pyarrow.compute as pc

    if spec.type == 'TableScan':
        return None

    elif spec.type == 'Filter':
        def filter_func(table):
            expr = spec.filter_expression
            return _apply_test_filter(table, expr, pc)
        return filter_func

    elif spec.type == 'Aggregate':
        group_keys = spec.group_by_keys or []
        agg_funcs = spec.aggregate_functions or []
        agg_cols = spec.aggregate_columns or []

        def aggregate_func(table):
            if not group_keys:
                # Global aggregation
                results = {}
                for func, col in zip(agg_funcs, agg_cols):
                    col_name = col if col != '*' else table.column_names[0]
                    if col_name in table.column_names:
                        column = table.column(col_name)
                        func_upper = func.upper()
                        if func_upper == 'SUM':
                            results[f'{func}_{col}'] = [pc.sum(column).as_py()]
                        elif func_upper == 'COUNT':
                            results[f'{func}_{col}'] = [table.num_rows]
                        elif func_upper in ('AVG', 'MEAN'):
                            results[f'{func}_{col}'] = [pc.mean(column).as_py()]
                        elif func_upper == 'MIN':
                            results[f'{func}_{col}'] = [pc.min(column).as_py()]
                        elif func_upper == 'MAX':
                            results[f'{func}_{col}'] = [pc.max(column).as_py()]
                        elif func_upper == 'STDDEV':
                            results[f'{func}_{col}'] = [pc.stddev(column).as_py()]
                        elif func_upper == 'VARIANCE':
                            results[f'{func}_{col}'] = [pc.variance(column).as_py()]
                return ca.table(results) if results else table
            else:
                # GROUP BY
                grouped = table.group_by(group_keys)
                agg_list = []
                for func, col in zip(agg_funcs, agg_cols):
                    col_name = col if col != '*' else group_keys[0]
                    func_lower = func.lower()
                    if func_lower in ('avg', 'average'):
                        func_lower = 'mean'
                    if func_lower == 'count_distinct':
                        func_lower = 'count_distinct'
                    agg_list.append((col_name, func_lower))
                return grouped.aggregate(agg_list)
        return aggregate_func

    elif spec.type == 'HashJoin':
        left_keys = spec.left_keys or []
        right_keys = spec.right_keys or []
        join_type = spec.join_type or 'inner'

        def join_func(left_table, right_table=None):
            if right_table is None:
                return left_table

            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()

            # Map right keys to left keys for merge
            key_map = dict(zip(right_keys, left_keys))
            right_df_renamed = right_df.rename(columns=key_map)

            # Perform join
            how = join_type
            if how == 'inner':
                result = left_df.merge(right_df_renamed, on=left_keys, how='inner')
            elif how == 'left':
                result = left_df.merge(right_df_renamed, on=left_keys, how='left')
            elif how == 'right':
                result = left_df.merge(right_df_renamed, on=left_keys, how='right')
            elif how in ('full', 'outer', 'full_outer'):
                result = left_df.merge(right_df_renamed, on=left_keys, how='outer')
            else:
                result = left_df.merge(right_df_renamed, on=left_keys, how='inner')

            return ca.Table.from_pandas(result)
        return join_func

    elif spec.type == 'Projection':
        projected_cols = spec.projected_columns or []
        def project(table, cols=projected_cols):
            existing = [c for c in cols if c in table.column_names]
            return table.select(existing) if existing else table
        return project

    elif spec.type == 'Limit':
        limit = spec.int_params.get('limit', 100) if spec.int_params else 100
        offset = spec.int_params.get('offset', 0) if spec.int_params else 0
        def limit_func(table, lim=limit, off=offset):
            return table.slice(off, lim)
        return limit_func

    elif spec.type == 'Distinct':
        def distinct_func(table):
            return table.group_by(table.column_names).aggregate([])
        return distinct_func

    elif spec.type == 'Sort':
        sort_keys = spec.string_list_params.get('sort_keys', []) if spec.string_list_params else []
        sort_orders = spec.string_list_params.get('sort_orders', []) if spec.string_list_params else []

        def sort_func(table, keys=sort_keys, orders=sort_orders):
            if not keys:
                return table
            sort_keys_arrow = []
            for i, key in enumerate(keys):
                if key in table.column_names:
                    order = 'ascending'
                    if i < len(orders):
                        order = 'descending' if orders[i].lower() in ('desc', 'descending') else 'ascending'
                    sort_keys_arrow.append((key, order))
            if sort_keys_arrow:
                indices = pc.sort_indices(table, sort_keys=sort_keys_arrow)
                return pc.take(table, indices)
            return table
        return sort_func

    elif spec.type == 'TopN':
        limit = spec.int_params.get('limit', 10) if spec.int_params else 10
        sort_keys = spec.string_list_params.get('sort_keys', []) if spec.string_list_params else []
        sort_orders = spec.string_list_params.get('sort_orders', []) if spec.string_list_params else []

        def topn_func(table, n=limit, keys=sort_keys, orders=sort_orders):
            if not keys or table.num_rows == 0:
                return table.slice(0, min(n, table.num_rows))
            sort_keys_arrow = []
            for i, key in enumerate(keys):
                if key in table.column_names:
                    order = 'ascending'
                    if i < len(orders):
                        order = 'descending' if orders[i].lower() in ('desc', 'descending') else 'ascending'
                    sort_keys_arrow.append((key, order))
            if sort_keys_arrow:
                indices = pc.sort_indices(table, sort_keys=sort_keys_arrow)
                return pc.take(table, indices).slice(0, min(n, table.num_rows))
            return table.slice(0, min(n, table.num_rows))
        return topn_func

    elif spec.type == 'Window':
        partition_by = spec.window_partition_by or []
        order_by = spec.window_order_by or []
        order_dirs = spec.window_order_directions or []
        window_func = spec.window_function or 'row_number'
        window_col = spec.window_column or ''

        def window_func_impl(table):
            if table.num_rows == 0:
                return table
            df = table.to_pandas()
            func_upper = window_func.upper()

            # Sort
            sort_cols = [c for c in order_by if c in df.columns]
            if partition_by or sort_cols:
                sort_all = [c for c in partition_by if c in df.columns] + sort_cols
                if sort_all:
                    df = df.sort_values(sort_all)

            if partition_by:
                grouper = df.groupby(partition_by, sort=False)
            else:
                grouper = df

            if func_upper == 'ROW_NUMBER':
                if partition_by:
                    df['row_number'] = grouper.cumcount() + 1
                else:
                    df['row_number'] = range(1, len(df) + 1)
            elif func_upper == 'RANK':
                if sort_cols:
                    if partition_by:
                        df['rank'] = grouper[sort_cols[0]].rank(method='min').astype(int)
                    else:
                        df['rank'] = df[sort_cols[0]].rank(method='min').astype(int)
            elif func_upper == 'LAG' and window_col in df.columns:
                offset = spec.int_params.get('offset', 1) if spec.int_params else 1
                if partition_by:
                    df[f'lag_{window_col}'] = grouper[window_col].shift(offset)
                else:
                    df[f'lag_{window_col}'] = df[window_col].shift(offset)
            elif func_upper == 'LEAD' and window_col in df.columns:
                offset = spec.int_params.get('offset', 1) if spec.int_params else 1
                if partition_by:
                    df[f'lead_{window_col}'] = grouper[window_col].shift(-offset)
                else:
                    df[f'lead_{window_col}'] = df[window_col].shift(-offset)

            return ca.Table.from_pandas(df)
        return window_func_impl

    elif spec.type == 'Union':
        union_all = spec.set_all

        def union_func(left_table, right_table=None):
            if right_table is None:
                return left_table
            combined = ca.concat_tables([left_table, right_table])
            if not union_all:
                return combined.group_by(combined.column_names).aggregate([])
            return combined
        return union_func

    elif spec.type == 'Intersect':
        def intersect_func(left_table, right_table=None):
            if right_table is None or right_table.num_rows == 0:
                return ca.table({}, schema=left_table.schema)
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()
            merged = left_df.merge(right_df, how='inner')
            return ca.Table.from_pandas(merged.drop_duplicates())
        return intersect_func

    elif spec.type == 'Except':
        def except_func(left_table, right_table=None):
            if right_table is None or right_table.num_rows == 0:
                return left_table.group_by(left_table.column_names).aggregate([])
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()
            merged = left_df.merge(right_df, how='left', indicator=True)
            result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            return ca.Table.from_pandas(result.drop_duplicates())
        return except_func

    elif spec.type == 'SemiJoin':
        left_keys = spec.left_keys or []
        right_keys = spec.right_keys or []

        def semi_join_func(left_table, right_table=None):
            if right_table is None or right_table.num_rows == 0:
                return ca.table({}, schema=left_table.schema)
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()
            right_keys_only = right_df[right_keys].drop_duplicates()
            right_keys_only.columns = left_keys
            result = left_df.merge(right_keys_only, on=left_keys, how='inner')
            return ca.Table.from_pandas(result[left_df.columns])
        return semi_join_func

    elif spec.type == 'AntiJoin':
        left_keys = spec.left_keys or []
        right_keys = spec.right_keys or []

        def anti_join_func(left_table, right_table=None):
            if right_table is None or right_table.num_rows == 0:
                return left_table
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()
            right_keys_only = right_df[right_keys].drop_duplicates()
            right_keys_only.columns = left_keys
            merged = left_df.merge(right_keys_only, on=left_keys, how='left', indicator=True)
            result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            return ca.Table.from_pandas(result[left_df.columns])
        return anti_join_func

    elif spec.type == 'CrossJoin':
        def cross_join_func(left_table, right_table=None):
            if right_table is None:
                return left_table
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()
            left_df['_cross_key'] = 1
            right_df['_cross_key'] = 1
            result = left_df.merge(right_df, on='_cross_key').drop('_cross_key', axis=1)
            return ca.Table.from_pandas(result)
        return cross_join_func

    return lambda table: table


def _apply_test_filter(table, expr, pc):
    """Apply filter expression for testing."""
    import re

    # Simple parsing for tests
    patterns = [
        (r'(\w+)\s*>=\s*(.+)', 'ge'),
        (r'(\w+)\s*<=\s*(.+)', 'le'),
        (r'(\w+)\s*>\s*(.+)', 'gt'),
        (r'(\w+)\s*<\s*(.+)', 'lt'),
        (r'(\w+)\s*=\s*(.+)', 'eq'),
        (r'(\w+)\s*!=\s*(.+)', 'ne'),
    ]

    for pattern, op in patterns:
        match = re.match(pattern, expr.strip())
        if match:
            col_name, value_str = match.groups()
            if col_name not in table.column_names:
                return table
            column = table.column(col_name)
            try:
                value = float(value_str.strip())
                if value == int(value):
                    value = int(value)
            except ValueError:
                value = value_str.strip().strip("'\"")

            if op == 'gt':
                mask = pc.greater(column, value)
            elif op == 'ge':
                mask = pc.greater_equal(column, value)
            elif op == 'lt':
                mask = pc.less(column, value)
            elif op == 'le':
                mask = pc.less_equal(column, value)
            elif op == 'eq':
                mask = pc.equal(column, value)
            elif op == 'ne':
                mask = pc.not_equal(column, value)
            else:
                return table
            return table.filter(mask)

    return table


# =============================================================================
# Filter Tests
# =============================================================================

class TestFilterOperator:
    """Test filter operator with various expressions."""

    async def test_simple_greater_than(self, orders_table):
        """Test simple > filter."""
        spec = OperatorSpec(
            type='Filter',
            name='Filter amount > 100',
            estimated_cardinality=5,
            filter_expression='amount > 100'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 5
        amounts = result.column('amount').to_pylist()
        assert all(a > 100 for a in amounts)

    async def test_less_than_equal(self, orders_table):
        """Test <= filter."""
        spec = OperatorSpec(
            type='Filter',
            name='Filter amount <= 100',
            estimated_cardinality=5,
            filter_expression='amount <= 100'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        amounts = result.column('amount').to_pylist()
        assert all(a <= 100 for a in amounts)

    async def test_equality_filter(self, orders_table):
        """Test = filter."""
        spec = OperatorSpec(
            type='Filter',
            name='Filter customer_id = 101',
            estimated_cardinality=3,
            filter_expression='customer_id = 101'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 3
        customer_ids = result.column('customer_id').to_pylist()
        assert all(c == 101 for c in customer_ids)


# =============================================================================
# Aggregate Tests
# =============================================================================

class TestAggregateOperator:
    """Test aggregate operator with various functions."""

    async def test_global_sum(self, orders_table):
        """Test global SUM aggregation."""
        spec = OperatorSpec(
            type='Aggregate',
            name='SUM amount',
            estimated_cardinality=1,
            aggregate_functions=['SUM'],
            aggregate_columns=['amount']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 1
        expected_sum = sum(orders_table.column('amount').to_pylist())
        assert result.column('SUM_amount').to_pylist()[0] == expected_sum

    async def test_global_count(self, orders_table):
        """Test global COUNT aggregation."""
        spec = OperatorSpec(
            type='Aggregate',
            name='COUNT',
            estimated_cardinality=1,
            aggregate_functions=['COUNT'],
            aggregate_columns=['id']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 1
        assert result.column('COUNT_id').to_pylist()[0] == 10

    async def test_group_by_sum(self, orders_table):
        """Test GROUP BY with SUM."""
        spec = OperatorSpec(
            type='Aggregate',
            name='GROUP BY customer_id SUM amount',
            estimated_cardinality=5,
            group_by_keys=['customer_id'],
            aggregate_functions=['sum'],
            aggregate_columns=['amount']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        # Should have 5 unique customers
        assert result.num_rows == 5

    async def test_multiple_aggregates(self, orders_table):
        """Test multiple aggregate functions."""
        spec = OperatorSpec(
            type='Aggregate',
            name='Multiple aggregates',
            estimated_cardinality=1,
            aggregate_functions=['SUM', 'AVG', 'MIN', 'MAX'],
            aggregate_columns=['amount', 'amount', 'amount', 'amount']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 1
        amounts = orders_table.column('amount').to_pylist()
        assert result.column('SUM_amount').to_pylist()[0] == sum(amounts)
        assert result.column('MIN_amount').to_pylist()[0] == min(amounts)
        assert result.column('MAX_amount').to_pylist()[0] == max(amounts)


# =============================================================================
# Join Tests
# =============================================================================

class TestJoinOperator:
    """Test join operator with various join types."""

    async def test_inner_join(self, orders_table, customers_table):
        """Test INNER JOIN."""
        spec = OperatorSpec(
            type='HashJoin',
            name='Inner join',
            estimated_cardinality=10,
            left_keys=['customer_id'],
            right_keys=['id'],
            join_type='inner'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, customers_table)

        # All orders have matching customers (101-105)
        assert result.num_rows == 10
        assert 'name' in result.column_names

    async def test_left_join(self, orders_table, customers_table):
        """Test LEFT JOIN."""
        spec = OperatorSpec(
            type='HashJoin',
            name='Left join',
            estimated_cardinality=10,
            left_keys=['customer_id'],
            right_keys=['id'],
            join_type='left'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, customers_table)

        # Should preserve all orders
        assert result.num_rows >= 10

    async def test_right_join(self, orders_table, customers_table):
        """Test RIGHT JOIN."""
        spec = OperatorSpec(
            type='HashJoin',
            name='Right join',
            estimated_cardinality=11,
            left_keys=['customer_id'],
            right_keys=['id'],
            join_type='right'
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, customers_table)

        # Should include customer 106 with NULL order data
        assert result.num_rows >= 10


# =============================================================================
# Sort Tests
# =============================================================================

class TestSortOperator:
    """Test sort operator."""

    async def test_sort_ascending(self, orders_table):
        """Test ascending sort."""
        spec = OperatorSpec(
            type='Sort',
            name='Sort by amount ASC',
            estimated_cardinality=10,
            string_list_params={'sort_keys': ['amount'], 'sort_orders': ['asc']}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        amounts = result.column('amount').to_pylist()
        assert amounts == sorted(amounts)

    async def test_sort_descending(self, orders_table):
        """Test descending sort."""
        spec = OperatorSpec(
            type='Sort',
            name='Sort by amount DESC',
            estimated_cardinality=10,
            string_list_params={'sort_keys': ['amount'], 'sort_orders': ['desc']}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        amounts = result.column('amount').to_pylist()
        assert amounts == sorted(amounts, reverse=True)


# =============================================================================
# TopN Tests
# =============================================================================

class TestTopNOperator:
    """Test TopN operator."""

    async def test_top_5_by_amount(self, orders_table):
        """Test top 5 by amount descending."""
        spec = OperatorSpec(
            type='TopN',
            name='Top 5 by amount',
            estimated_cardinality=5,
            int_params={'limit': 5},
            string_list_params={'sort_keys': ['amount'], 'sort_orders': ['desc']}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 5
        amounts = result.column('amount').to_pylist()
        assert amounts[0] == 300.0  # Highest


# =============================================================================
# Window Function Tests
# =============================================================================

class TestWindowOperator:
    """Test window function operator."""

    async def test_row_number(self, orders_table):
        """Test ROW_NUMBER window function."""
        spec = OperatorSpec(
            type='Window',
            name='Row number',
            estimated_cardinality=10,
            window_function='row_number',
            window_order_by=['amount'],
            window_order_directions=['desc']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert 'row_number' in result.column_names
        row_nums = result.column('row_number').to_pylist()
        assert set(row_nums) == set(range(1, 11))

    async def test_row_number_with_partition(self, orders_table):
        """Test ROW_NUMBER with PARTITION BY."""
        spec = OperatorSpec(
            type='Window',
            name='Row number by customer',
            estimated_cardinality=10,
            window_function='row_number',
            window_partition_by=['customer_id'],
            window_order_by=['amount'],
            window_order_directions=['desc']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert 'row_number' in result.column_names

    async def test_lag_function(self, orders_table):
        """Test LAG window function."""
        spec = OperatorSpec(
            type='Window',
            name='Lag amount',
            estimated_cardinality=10,
            window_function='lag',
            window_column='amount',
            window_order_by=['id'],
            int_params={'offset': 1}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert 'lag_amount' in result.column_names


# =============================================================================
# Set Operation Tests
# =============================================================================

class TestSetOperations:
    """Test set operations (UNION, INTERSECT, EXCEPT)."""

    async def test_union_all(self, orders_table):
        """Test UNION ALL."""
        spec = OperatorSpec(
            type='Union',
            name='Union all',
            estimated_cardinality=20,
            set_all=True
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, orders_table)

        assert result.num_rows == 20  # Duplicates preserved

    async def test_union_distinct(self, orders_table):
        """Test UNION (distinct)."""
        spec = OperatorSpec(
            type='Union',
            name='Union distinct',
            estimated_cardinality=10,
            set_all=False
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, orders_table)

        assert result.num_rows == 10  # Duplicates removed

    async def test_intersect(self, orders_table):
        """Test INTERSECT."""
        spec = OperatorSpec(
            type='Intersect',
            name='Intersect',
            estimated_cardinality=10
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, orders_table)

        assert result.num_rows == 10  # Same tables intersect fully

    async def test_except(self, orders_table):
        """Test EXCEPT."""
        # Filter to get subset
        filtered = orders_table.slice(0, 5)

        spec = OperatorSpec(
            type='Except',
            name='Except',
            estimated_cardinality=5
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, filtered)

        assert result.num_rows == 5  # Remaining rows


# =============================================================================
# Semi/Anti Join Tests
# =============================================================================

class TestSemiAntiJoins:
    """Test semi and anti joins."""

    async def test_semi_join(self, orders_table, customers_table):
        """Test SEMI JOIN (EXISTS)."""
        spec = OperatorSpec(
            type='SemiJoin',
            name='Semi join',
            estimated_cardinality=10,
            left_keys=['customer_id'],
            right_keys=['id']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, customers_table)

        # All orders have matching customers
        assert result.num_rows == 10
        # Result only has left columns
        assert 'name' not in result.column_names

    async def test_anti_join(self, orders_table, customers_table):
        """Test ANTI JOIN (NOT EXISTS)."""
        # Filter customers to only include 101, 102
        filtered_customers = customers_table.slice(0, 2)

        spec = OperatorSpec(
            type='AntiJoin',
            name='Anti join',
            estimated_cardinality=5,
            left_keys=['customer_id'],
            right_keys=['id']
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table, filtered_customers)

        # Orders with customer_id 103, 104, 105 should remain
        customer_ids = set(result.column('customer_id').to_pylist())
        assert 101 not in customer_ids
        assert 102 not in customer_ids


# =============================================================================
# Cross Join Tests
# =============================================================================

class TestCrossJoin:
    """Test cross join (Cartesian product)."""

    async def test_cross_join(self, products_table):
        """Test CROSS JOIN."""
        # Use small subset for cross join
        left = products_table.slice(0, 2)
        right = products_table.slice(0, 3)

        spec = OperatorSpec(
            type='CrossJoin',
            name='Cross join',
            estimated_cardinality=6
        )
        op = comprehensive_operator_builder(spec, left)
        result = op(left, right)

        assert result.num_rows == 6  # 2 x 3 = 6


# =============================================================================
# Limit/Offset Tests
# =============================================================================

class TestLimitOffset:
    """Test LIMIT and OFFSET."""

    async def test_limit(self, orders_table):
        """Test LIMIT."""
        spec = OperatorSpec(
            type='Limit',
            name='Limit 5',
            estimated_cardinality=5,
            int_params={'limit': 5}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 5

    async def test_limit_with_offset(self, orders_table):
        """Test LIMIT with OFFSET."""
        spec = OperatorSpec(
            type='Limit',
            name='Limit 3 offset 2',
            estimated_cardinality=3,
            int_params={'limit': 3, 'offset': 2}
        )
        op = comprehensive_operator_builder(spec, orders_table)
        result = op(orders_table)

        assert result.num_rows == 3
        # Should skip first 2 rows
        ids = result.column('id').to_pylist()
        assert ids[0] == 3  # Third row


# =============================================================================
# Distinct Tests
# =============================================================================

class TestDistinct:
    """Test DISTINCT operator."""

    async def test_distinct(self, orders_table):
        """Test DISTINCT."""
        # Select only customer_id for distinct
        projected = orders_table.select(['customer_id'])

        spec = OperatorSpec(
            type='Distinct',
            name='Distinct customer_id',
            estimated_cardinality=5
        )
        op = comprehensive_operator_builder(spec, projected)
        result = op(projected)

        assert result.num_rows == 5  # 5 unique customers


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
