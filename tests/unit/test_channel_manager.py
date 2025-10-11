#!/usr/bin/env python3
"""Test for Sabot channel manager system."""

import pytest
from unittest.mock import MagicMock
import sys

# Mock dependencies for basic testing
sys.modules['typer'] = MagicMock()
sys.modules['rich.console'] = MagicMock()
sys.modules['rich.table'] = MagicMock()
sys.modules['rich.panel'] = MagicMock()
sys.modules['rich.text'] = MagicMock()
sys.modules['rich.live'] = MagicMock()
sys.modules['rich.spinner'] = MagicMock()
sys.modules['rich.progress'] = MagicMock()
sys.modules['rich.prompt'] = MagicMock()
sys.modules['rich'] = MagicMock()

# Mock typer
mock_typer = sys.modules['typer']
mock_app = MagicMock()
mock_typer.Typer.return_value = mock_app
mock_typer.Argument = lambda x, **kwargs: x
mock_typer.Option = lambda *args, **kwargs: args[0] if args else None

# Mock rich components
mock_console = MagicMock()
sys.modules['rich.console'].Console.return_value = mock_console


@pytest.fixture
def mock_app():
    """Create mock app for testing."""
    class MockApp:
        def __init__(self):
            self.conf = MagicMock()
            self.conf.stream_buffer_maxsize = 1000
    return MockApp()


def test_channel_manager_imports():
    """Test channel manager imports."""
    from sabot.channel_manager import (
        ChannelManager, ChannelBackend, ChannelPolicy,
        MemoryChannelFactory, KafkaChannelFactory
    )
    # If imports succeed, test passes
    assert ChannelManager is not None
    assert ChannelBackend is not None
    assert ChannelPolicy is not None


def test_channel_manager_creation(mock_app):
    """Test channel manager creation."""
    from sabot.channel_manager import ChannelManager
    manager = ChannelManager(mock_app)
    assert manager is not None


def test_backend_registration(mock_app):
    """Test backend registration."""
    from sabot.channel_manager import ChannelManager, ChannelBackend
    manager = ChannelManager(mock_app)
    assert ChannelBackend.MEMORY in manager._backends


def test_policy_selection(mock_app):
    """Test policy-based backend selection."""
    from sabot.channel_manager import ChannelManager, ChannelBackend, ChannelPolicy
    manager = ChannelManager(mock_app)
    backend = manager._select_backend_by_policy(ChannelPolicy.PERFORMANCE)
    assert backend == ChannelBackend.MEMORY


def test_synchronous_channel_creation(mock_app):
    """Test synchronous channel creation."""
    channel = mock_app.channel("test-memory")
    assert channel is not None


def test_auto_generated_channel_names(mock_app):
    """Test auto-generated channel names."""
    auto_channel = mock_app.channel()  # Should get auto-generated name
    assert auto_channel is not None


def test_memory_channel_method(mock_app):
    """Test memory channel method."""
    memory_channel = mock_app.memory_channel("test-memory-explicit", maxsize=500)
    assert memory_channel is not None


def test_channel_retrieval(mock_app):
    """Test channel retrieval."""
    from sabot.channel_manager import ChannelManager
    manager = ChannelManager(mock_app)
    mock_app.channel("test-memory")
    retrieved = manager.get_channel("test-memory")
    assert retrieved is not None


def test_channel_listing(mock_app):
    """Test channel listing."""
    from sabot.channel_manager import ChannelManager
    manager = ChannelManager(mock_app)
    mock_app.channel("test-memory")
    mock_app.channel()  # Auto-generated name
    channels = manager.list_channels()
    assert len(channels) >= 2  # test-memory and auto-generated


def test_backend_listing(mock_app):
    """Test backend listing."""
    from sabot.channel_manager import ChannelManager, ChannelBackend
    manager = ChannelManager(mock_app)
    backends = manager.list_backends()
    assert ChannelBackend.MEMORY in backends
    assert len(backends) > 0
