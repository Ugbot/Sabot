# -*- coding: utf-8 -*-
"""Pytest configuration for Sabot tests."""

import asyncio
import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Generator, AsyncGenerator

import pyarrow as pa

from sabot.app import App


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def test_app():
    """Create a test Sabot application."""
    app = App(
        id="test-app",
        broker="memory://",  # Use in-memory broker for tests
        enable_flight=False,  # Disable flight for tests
        enable_interactive=False,  # Disable interactive for tests
    )
    yield app


@pytest.fixture
def sample_arrow_batch():
    """Create a sample Arrow RecordBatch for testing."""
    data = {
        'user_id': [1, 2, 3, 4, 5],
        'amount': [100.0, 250.0, 75.0, 1200.0, 50.0],
        'category': ['A', 'B', 'A', 'C', 'B'],
        'timestamp': pa.array([1, 2, 3, 4, 5], type=pa.timestamp('s'))
    }
    return pa.RecordBatch.from_arrays(
        list(data.values()),
        names=list(data.keys())
    )


@pytest.fixture
def sample_arrow_table(sample_arrow_batch):
    """Create a sample Arrow Table for testing."""
    return pa.Table.from_batches([sample_arrow_batch])


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
async def mock_kafka_topic():
    """Mock Kafka topic for testing."""
    class MockTopic:
        def __init__(self, name):
            self.name = name
            self.messages = []

        async def send(self, value, key=None):
            self.messages.append({'value': value, 'key': key})

        def get_messages(self):
            return self.messages[:]

        def clear(self):
            self.messages.clear()

    return MockTopic("test-topic")


@pytest.fixture
async def agent_test_context(test_app):
    """Context for testing agents."""
    class AgentTestContext:
        def __init__(self, app):
            self.app = app
            self.results = []
            self.errors = []

        async def collect_results(self, agent_name, timeout=5.0):
            """Collect results from an agent."""
            # This would integrate with the agent manager
            # For now, return mock results
            await asyncio.sleep(0.1)
            return self.results

        def add_result(self, result):
            self.results.append(result)

        def add_error(self, error):
            self.errors.append(error)

    context = AgentTestContext(test_app)
    yield context


# Custom markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance benchmarks"
    )
    config.addinivalue_line(
        "markers", "arrow: marks tests that require Arrow"
    )


# Test utilities
def assert_arrow_batch_equal(batch1, batch2, check_order=True):
    """Assert that two Arrow RecordBatches are equal."""
    assert batch1.num_columns == batch2.num_columns
    assert batch1.num_rows == batch2.num_rows
    assert batch1.schema == batch2.schema

    if check_order:
        for i in range(batch1.num_columns):
            col1 = batch1.column(i)
            col2 = batch2.column(i)
            assert col1 == col2


def assert_arrow_table_equal(table1, table2, check_order=True):
    """Assert that two Arrow Tables are equal."""
    assert table1.num_columns == table2.num_columns
    assert table1.num_rows == table2.num_rows
    assert table1.schema == table2.schema

    if check_order:
        for i in range(table1.num_columns):
            col1 = table1.column(i)
            col2 = table2.column(i)
            assert col1 == col2
