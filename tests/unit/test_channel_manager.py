#!/usr/bin/env python3
"""Test for Sabot channel manager system."""

import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Mock dependencies for basic testing
from unittest.mock import MagicMock
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

try:
    # Test channel manager imports
    from sabot.channel_manager import (
        ChannelManager, ChannelBackend, ChannelPolicy,
        MemoryChannelFactory, KafkaChannelFactory
    )
    print("✓ Channel manager imports successful")

    # Create mock app
    class MockApp:
        def __init__(self):
            self.conf = MagicMock()
            self.conf.stream_buffer_maxsize = 1000

    app = MockApp()

    # Test channel manager creation
    manager = ChannelManager(app)
    print("✓ Channel manager creation successful")

    # Test backend registration
    assert ChannelBackend.MEMORY in manager._backends
    print("✓ Memory backend registered")

    # Test policy selection
    backend = manager._select_backend_by_policy(ChannelPolicy.PERFORMANCE)
    assert backend == ChannelBackend.MEMORY
    print("✓ Policy-based backend selection works")

    # Test synchronous channel creation (memory only)
    channel = app.channel("test-memory")
    assert channel is not None
    print("✓ Synchronous channel creation works")

    # Test channel naming
    auto_channel = app.channel()  # Should get auto-generated name
    assert auto_channel is not None
    print("✓ Auto-generated channel names work")

    # Test memory channel method
    memory_channel = app.memory_channel("test-memory-explicit", maxsize=500)
    assert memory_channel is not None
    print("✓ Memory channel method works")

    # Test channel retrieval
    retrieved = manager.get_channel("test-memory")
    assert retrieved is not None
    print("✓ Channel retrieval works")

    # Test channel listing
    channels = manager.list_channels()
    assert len(channels) >= 2  # test-memory and auto-generated
    print(f"✓ Channel listing works: {len(channels)} channels")

    # Test backend listing
    backends = manager.list_backends()
    assert ChannelBackend.MEMORY in backends
    print(f"✓ Backend listing works: {len(backends)} backends available")

    print("\n🎉 Channel manager tests passed!")
    print("\nVerified functionality:")
    print("• Channel manager creation and backend registration")
    print("• Policy-based backend selection")
    print("• Synchronous channel creation (memory)")
    print("• Auto-generated channel names")
    print("• Memory channel convenience method")
    print("• Channel retrieval and listing")
    print("• Backend enumeration")

except Exception as e:
    print(f"❌ Channel manager test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
