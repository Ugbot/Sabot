# -*- coding: utf-8 -*-
"""Tests for Sabot App."""

import pytest
import asyncio
from unittest.mock import Mock, patch

from sabot.app import App, Topic, Table, Stream
from sabot.types import AgentT


class TestApp:
    """Test the main Sabot application."""

    def test_app_creation(self):
        """Test app creation with default settings."""
        app = App("test-app")

        assert app.id == "test-app"
        assert app.broker == "kafka://localhost:9092"
        assert app.value_serializer == "arrow"
        assert app.key_serializer == "raw"

    def test_app_creation_with_options(self):
        """Test app creation with custom options."""
        app = App(
            "custom-app",
            broker="kafka://custom:9092",
            value_serializer="json",
            key_serializer="str",
            flight_port=9999
        )

        assert app.id == "custom-app"
        assert app.broker == "kafka://custom:9092"
        assert app.value_serializer == "json"
        assert app.key_serializer == "str"
        assert app.flight_port == 9999

    def test_topic_creation(self):
        """Test topic creation."""
        app = App("test-app")
        topic = app.topic("test-topic")

        assert isinstance(topic, Topic)
        assert topic.name == "test-topic"
        assert topic.app == app
        assert topic.partitions == 1
        assert topic.replication_factor == 1

    def test_topic_creation_with_options(self):
        """Test topic creation with custom options."""
        app = App("test-app")
        topic = app.topic(
            "custom-topic",
            partitions=3,
            replication_factor=2
        )

        assert topic.name == "custom-topic"
        assert topic.partitions == 3
        assert topic.replication_factor == 2

    def test_table_creation(self):
        """Test table creation."""
        app = App("test-app")
        table = app.table("test-table")

        assert isinstance(table, Table)
        assert table.name == "test-table"
        assert table.app == app
        assert table.default == {}

    def test_table_creation_with_options(self):
        """Test table creation with custom default."""
        app = App("test-app")
        default_value = {"count": 0, "sum": 0.0}
        table = app.table("custom-table", default=default_value)

        assert table.name == "custom-table"
        assert table.default == default_value

    @pytest.mark.asyncio
    async def test_app_lifecycle(self):
        """Test app start/stop lifecycle."""
        app = App("test-app")

        # Mock the underlying services
        with patch.object(app, '_flight_app', None), \
             patch.object(app, '_interactive_app', None):

            await app.start()
            await app.stop()

            # Should not raise any exceptions
            assert True

    def test_agent_decorator(self):
        """Test agent decorator creation."""
        app = App("test-app")

        # Create a mock agent function
        async def mock_agent(stream):
            async for batch in stream:
                yield batch

        # Apply decorator
        decorated_agent = app.agent("test-topic")(mock_agent)

        assert decorated_agent is not None
        assert hasattr(decorated_agent, 'name')
        assert hasattr(decorated_agent, 'func')

    def test_arrow_agent_decorator(self):
        """Test Arrow agent decorator creation."""
        app = App("test-app")

        # Create a mock agent function
        async def mock_arrow_agent(stream):
            async for batch in stream:
                yield batch

        # Apply decorator
        decorated_agent = app.arrow_agent("test-topic")(mock_arrow_agent)

        assert decorated_agent is not None
        assert hasattr(decorated_agent, 'name')
        assert hasattr(decorated_agent, 'func')

    def test_pipeline_creation(self):
        """Test pipeline creation."""
        app = App("test-app")

        # Mock interactive app
        with patch.object(app, '_interactive_app') as mock_interactive:
            mock_pipeline = Mock()
            mock_interactive.pipeline.return_value = mock_pipeline

            pipeline = app.pipeline("test-pipeline")

            assert pipeline == mock_pipeline
            mock_interactive.pipeline.assert_called_once_with("test-pipeline")

    def test_pipeline_creation_fallback(self):
        """Test pipeline creation fallback when interactive is disabled."""
        app = App("test-app", enable_interactive=False)

        pipeline = app.pipeline("test-pipeline")

        # Should return a basic pipeline
        assert hasattr(pipeline, '_pipeline')
        assert pipeline._pipeline._name == "test-pipeline"

    def test_stats_collection(self):
        """Test application statistics collection."""
        app = App("test-app")

        # Add some mock components
        app._topics["topic1"] = Mock()
        app._topics["topic2"] = Mock()
        app._tables["table1"] = Mock()
        app._agents["agent1"] = Mock()

        stats = app.get_stats()

        assert stats["app_id"] == "test-app"
        assert stats["topics"] == 2
        assert stats["tables"] == 1
        assert stats["agents"] == 1
        assert "metrics" in stats


class TestTopic:
    """Test Topic functionality."""

    def test_topic_initialization(self):
        """Test topic initialization."""
        app = Mock()
        topic = Topic("test-topic", app)

        assert topic.name == "test-topic"
        assert topic.app == app
        assert topic.partitions == 1
        assert topic.replication_factor == 1
        assert topic.value_serializer == "arrow"

    def test_topic_initialization_with_options(self):
        """Test topic initialization with custom options."""
        app = Mock()
        topic = Topic(
            "custom-topic",
            app,
            partitions=3,
            replication_factor=2,
            value_serializer="json"
        )

        assert topic.name == "custom-topic"
        assert topic.partitions == 3
        assert topic.replication_factor == 2
        assert topic.value_serializer == "json"

    @pytest.mark.asyncio
    async def test_topic_send(self):
        """Test sending messages to topic."""
        app = Mock()
        topic = Topic("test-topic", app)

        # This would normally send to Kafka
        # For now, just ensure it doesn't raise
        await topic.send({"key": "value"}, key="test-key")

        # Should not raise any exceptions
        assert True


class TestTable:
    """Test Table functionality."""

    def test_table_initialization(self):
        """Test table initialization."""
        app = Mock()
        table = Table("test-table", app)

        assert table.name == "test-table"
        assert table.app == app
        assert table.default == {}
        assert table._data == {}

    def test_table_initialization_with_default(self):
        """Test table initialization with custom default."""
        app = Mock()
        default_value = {"count": 0}
        table = Table("test-table", app, default=default_value)

        assert table.default == default_value

    def test_table_get_set(self):
        """Test table get/set operations."""
        app = Mock()
        table = Table("test-table", app)

        # Test setting and getting values
        table["key1"] = {"value": 123}
        assert table["key1"] == {"value": 123}

        # Test default values
        assert table.get("nonexistent") == {}
        assert table.get("nonexistent", "custom_default") == "custom_default"

    def test_table_default_fallback(self):
        """Test table default value fallback."""
        app = Mock()
        default_value = {"count": 0, "sum": 0.0}
        table = Table("test-table", app, default=default_value)

        # Non-existent key should return default
        assert table["nonexistent"] == default_value
        assert table.get("nonexistent") == default_value


class TestStream:
    """Test Stream functionality."""

    def test_stream_initialization(self):
        """Test stream initialization."""
        topic = Mock()
        topic.name = "test-topic"
        stream = Stream(topic)

        assert stream.topic == topic
        assert stream.batch_size == 1000
        assert stream.timeout == 1.0

    def test_stream_initialization_with_options(self):
        """Test stream initialization with custom options."""
        topic = Mock()
        topic.name = "test-topic"
        stream = Stream(topic, batch_size=500, timeout=2.0)

        assert stream.topic == topic
        assert stream.batch_size == 500
        assert stream.timeout == 2.0

    @pytest.mark.asyncio
    async def test_stream_iteration(self):
        """Test stream async iteration."""
        topic = Mock()
        topic.name = "test-topic"
        stream = Stream(topic)

        # This would normally iterate over Kafka messages
        # For now, just test the async iterator protocol
        async_iter = stream.__aiter__()
        assert async_iter is not None
