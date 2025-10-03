"""End-to-end tests for channel creation and usage."""
import pytest
import asyncio
from sabot import App


def test_memory_channel_send_receive():
    """Test memory channel can send and receive."""
    app = App("test-app")
    channel = app.channel(name="test")

    # Send message
    asyncio.run(channel.put({"key": "value"}))

    # Receive message
    msg = asyncio.run(channel.get())
    assert msg == {"key": "value"}


def test_multiple_channels():
    """Test creating multiple channels."""
    app = App("test-app")

    ch1 = app.channel(name="channel-1")
    ch2 = app.channel(name="channel-2")
    ch3 = app.channel(name="channel-3")

    assert ch1.name == "channel-1"
    assert ch2.name == "channel-2"
    assert ch3.name == "channel-3"
    assert len(app._channels) == 3


@pytest.mark.asyncio
@pytest.mark.skipif(
    not pytest.config.getoption("--run-kafka"),
    reason="Requires --run-kafka flag and running Kafka"
)
async def test_kafka_channel_send_receive():
    """Test Kafka channel can send and receive (requires Kafka)."""
    app = App("test-app", broker="kafka://localhost:19092")

    channel = await app.async_channel(
        name="test-topic",
        backend="kafka"
    )

    # Send message
    await channel.put({"test": "data"})

    # Receive message
    msg = await channel.get()
    assert msg == {"test": "data"}


def test_channel_configuration_options():
    """Test channel creation with various options."""
    app = App("test-app")

    channel = app.channel(
        name="configured",
        maxsize=5000,
        key_type=str,
        value_type=dict
    )

    assert channel.maxsize == 5000
    # Additional assertions based on implementation

