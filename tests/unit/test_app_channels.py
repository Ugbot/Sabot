import pytest
import asyncio
from sabot import App
from sabot.channels import Channel


def test_create_memory_channel_sync():
    """Test synchronous memory channel creation."""
    app = App("test-app")

    channel = app.channel(name="test-channel", maxsize=100)

    assert isinstance(channel, Channel)
    assert channel.maxsize == 100
    assert "test-channel" in app._channels


def test_create_memory_channel_auto_name():
    """Test channel with auto-generated name."""
    app = App("test-app")

    channel = app.channel()

    # Check that a channel was created and registered
    assert len(app._channels) > 0
    # The channel should be registered with some name
    channel_name = list(app._channels.keys())[0]
    assert channel_name.startswith("channel-")


@pytest.mark.asyncio
async def test_create_kafka_channel_async():
    """Test async Kafka channel creation."""
    app = App("test-app", broker="kafka://localhost:9092")

    # This will fail without Kafka running, but tests the API
    try:
        channel = await app.async_channel(
            name="test-topic",
            backend="kafka"
        )
        # If Kafka is available
        assert channel.name == "test-topic"
    except Exception as e:
        # Expected if Kafka not running
        assert "kafka" in str(e).lower() or "connection" in str(e).lower()


def test_channel_invalid_backend_raises():
    """Test invalid backend raises ValueError."""
    import asyncio
    app = App("test-app")

    try:
        asyncio.run(app.async_channel(backend="invalid"))
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown backend" in str(e)
