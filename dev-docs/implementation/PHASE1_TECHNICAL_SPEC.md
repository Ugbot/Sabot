# Phase 1: Fix Critical Blockers - Technical Specification
**Status:** Ready to implement
**Dependencies:** None (can start immediately)

---

## Step 1.1: Fix CLI App Loading

### Current Implementation
**File:** `sabot/cli.py` lines 45-61

```python
def create_app(id: str = "sabot", broker: str = "memory://", **kwargs):
    """Mock create_app function for CLI testing."""
    class MockApp:
        def __init__(self, app_id, broker):
            self.id = app_id
            self.broker = broker

        async def run(self):
            console.print(f"[bold green]Mock app '{self.id}' running...")
            # ... mock implementation
    return MockApp(id, broker)
```

### Required Changes

#### Change 1: Remove Mock Implementation
**File:** `sabot/cli.py`
**Location:** Lines 45-61

**Delete:**
```python
def create_app(id: str = "sabot", broker: str = "memory://", **kwargs):
    """Mock create_app function for CLI testing."""
    class MockApp:
        # ... entire mock class
    return MockApp(id, broker)
```

#### Change 2: Add Real App Loader
**File:** `sabot/cli.py`
**Location:** After line 42 (after imports)

**Add:**
```python
import importlib
import sys
from pathlib import Path

def load_app_from_spec(app_spec: str):
    """
    Load Sabot App from module:attribute specification.

    Args:
        app_spec: String in format 'module.path:app_name'
                  Example: 'examples.fraud_app:app'

    Returns:
        App instance

    Raises:
        ValueError: If app_spec format is invalid
        ImportError: If module cannot be imported
        AttributeError: If app attribute doesn't exist
    """
    if ':' not in app_spec:
        raise ValueError(
            f"Invalid app specification '{app_spec}'. "
            "Expected format: 'module.path:attribute'"
        )

    module_name, app_name = app_spec.split(':', 1)

    # Add current directory to path for local imports
    cwd = Path.cwd()
    if str(cwd) not in sys.path:
        sys.path.insert(0, str(cwd))

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(
            f"Cannot import module '{module_name}': {e}"
        ) from e

    try:
        app = getattr(module, app_name)
    except AttributeError as e:
        raise AttributeError(
            f"Module '{module_name}' has no attribute '{app_name}'"
        ) from e

    # Verify it's an App instance
    from sabot.app import App
    if not isinstance(app, App):
        raise TypeError(
            f"'{app_name}' is not a Sabot App instance. "
            f"Got {type(app).__name__}"
        )

    return app
```

#### Change 3: Update Worker Command
**File:** `sabot/cli.py`
**Location:** Function `worker()` around line 117

**Replace:**
```python
@app.command()
def worker(
    app_spec: str = typer.Option(..., "-A", "--app", help="App module (e.g., myapp:app)"),
    workers: int = typer.Option(1, "-c", "--concurrency", help="Number of worker processes"),
    broker: Optional[str] = typer.Option(None, "-b", "--broker", help="Override broker URL"),
    log_level: str = typer.Option("INFO", "-l", "--loglevel", help="Logging level"),
):
    """Start Sabot worker (Faust-style: sabot -A myapp:app worker)."""
    # Delegate to workers_start
    workers_start(
        app_module=None,
        app_flag=app_spec,
        workers=workers,
        broker=broker,
        redis=None,
        rocksdb=None,
        daemon=False,
        log_level=log_level
    )
```

**With:**
```python
@app.command()
def worker(
    app_spec: str = typer.Option(..., "-A", "--app", help="App module (e.g., myapp:app)"),
    workers: int = typer.Option(1, "-c", "--concurrency", help="Number of worker processes"),
    broker: Optional[str] = typer.Option(None, "-b", "--broker", help="Override broker URL"),
    log_level: str = typer.Option("INFO", "-l", "--loglevel", help="Logging level"),
):
    """Start Sabot worker (Faust-style: sabot -A myapp:app worker)."""
    import logging

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Load the app
        logger.info(f"Loading app from: {app_spec}")
        app_instance = load_app_from_spec(app_spec)

        # Override broker if specified
        if broker:
            app_instance.broker = broker
            logger.info(f"Broker overridden to: {broker}")

        # Display app info
        console.print(Panel(
            f"[bold green]Starting Sabot Worker[/bold green]\n"
            f"App: {app_instance.id}\n"
            f"Broker: {app_instance.broker}\n"
            f"Workers: {workers}\n"
            f"Log Level: {log_level}",
            title="Worker Configuration"
        ))

        # Start the app
        import asyncio
        asyncio.run(app_instance.run())

    except (ValueError, ImportError, AttributeError, TypeError) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        console.print("\n[dim]Worker stopped by user[/dim]")
        raise typer.Exit(code=0)
    except Exception as e:
        logger.exception("Unexpected error starting worker")
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(code=1)
```

### Verification

#### Manual Test
```bash
# From project root
sabot -A examples.fraud_app:app worker

# Should see:
# - "Loading app from: examples.fraud_app:app"
# - Worker configuration panel
# - App starting (not mock)
```

#### Test Cases
**File:** `tests/unit/test_cli_app_loading.py` (new file)

```python
import pytest
from sabot.cli import load_app_from_spec
from sabot.app import App

def test_load_app_from_spec_valid():
    """Test loading a valid app."""
    app = load_app_from_spec("examples.fraud_app:app")
    assert isinstance(app, App)
    assert app.id is not None

def test_load_app_from_spec_invalid_format():
    """Test invalid spec format raises ValueError."""
    with pytest.raises(ValueError, match="Invalid app specification"):
        load_app_from_spec("invalid_spec")

def test_load_app_from_spec_module_not_found():
    """Test nonexistent module raises ImportError."""
    with pytest.raises(ImportError, match="Cannot import module"):
        load_app_from_spec("nonexistent.module:app")

def test_load_app_from_spec_attribute_not_found():
    """Test nonexistent attribute raises AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute"):
        load_app_from_spec("examples.fraud_app:nonexistent")

def test_load_app_from_spec_not_an_app():
    """Test non-App object raises TypeError."""
    with pytest.raises(TypeError, match="not a Sabot App instance"):
        load_app_from_spec("sys:path")  # sys.path is a list, not App
```

---

## Step 1.2: Test CLI with Fraud Demo

### Test Implementation
**File:** `tests/integration/test_cli_fraud_demo.py` (new file)

```python
"""Integration tests for CLI with fraud detection demo."""
import subprocess
import time
import signal
import pytest
from pathlib import Path


@pytest.fixture
def project_root():
    """Get project root directory."""
    return Path(__file__).parent.parent.parent


def test_cli_loads_fraud_app(project_root):
    """Test CLI can load fraud detection app."""
    cmd = [
        "sabot",
        "-A", "examples.fraud_app:app",
        "worker",
        "--loglevel", "INFO"
    ]

    # Start process
    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    try:
        # Give it 5 seconds to start
        time.sleep(5)

        # Check it's still running
        assert process.poll() is None, "Worker process died unexpectedly"

        # Check stdout for expected messages
        # Note: This is basic, may need adjustment based on actual output

    finally:
        # Clean shutdown
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def test_cli_invalid_app_spec(project_root):
    """Test CLI handles invalid app spec gracefully."""
    cmd = [
        "sabot",
        "-A", "invalid_module:app",
        "worker"
    ]

    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Should exit with error
    returncode = process.wait(timeout=5)
    assert returncode != 0, "Should exit with error code"

    stderr = process.stderr.read()
    assert "Cannot import module" in stderr or "Error" in stderr


def test_cli_broker_override(project_root):
    """Test CLI can override broker URL."""
    cmd = [
        "sabot",
        "-A", "examples.fraud_app:app",
        "worker",
        "--broker", "kafka://test-broker:9092",
        "--loglevel", "DEBUG"
    ]

    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    try:
        time.sleep(3)

        # Check process started
        assert process.poll() is None

        # Would need to check logs for broker override
        # This is a basic smoke test

    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
```

### Verification
```bash
pytest tests/integration/test_cli_fraud_demo.py -v
```

---

## Step 1.3: Fix Channel Creation

### Current Implementation
**File:** `sabot/app.py` line 381

```python
def channel(self, name: Optional[str] = None, ...) -> ChannelT:
    # Line 381
    raise NotImplementedError("Non-memory channels require async creation...")
```

### Analysis
**Issue:** `channel()` is a synchronous method but channel creation may require async operations (e.g., connecting to Kafka/Redis).

**Options:**
1. Make `channel()` async → breaks API compatibility
2. Add `async_channel()` method → dual API
3. Create channels lazily → complex lifecycle
4. Use sync wrappers → may block event loop

**Recommendation:** Option 2 - Add `async_channel()` for async backends, keep `channel()` for memory-only

### Required Changes

#### Change 1: Add async_channel Method
**File:** `sabot/app.py`
**Location:** After existing `channel()` method

**Add:**
```python
async def async_channel(
    self,
    name: Optional[str] = None,
    *,
    backend: str = "memory",
    key_type: Optional[Type] = None,
    value_type: Optional[Type] = None,
    maxsize: int = 1000,
    **backend_options
) -> ChannelT:
    """
    Create a channel asynchronously (for Kafka/Redis backends).

    Args:
        name: Channel name (auto-generated if None)
        backend: Backend type ('memory', 'kafka', 'redis', 'flight')
        key_type: Type for keys (optional)
        value_type: Type for values (optional)
        maxsize: Max channel size (for memory backend)
        **backend_options: Backend-specific options

    Returns:
        Channel instance

    Example:
        # Create Kafka channel
        channel = await app.async_channel(
            name='my-topic',
            backend='kafka',
            broker='localhost:9092'
        )
    """
    from sabot.channels import (
        MemoryChannel,
        create_kafka_channel,
        create_redis_channel,
        create_flight_channel
    )

    # Generate name if not provided
    if name is None:
        name = f"channel-{len(self._channels)}"

    # Create based on backend
    if backend == "memory":
        channel = MemoryChannel(
            name=name,
            key_type=key_type,
            value_type=value_type,
            maxsize=maxsize
        )

    elif backend == "kafka":
        # Import here to avoid dependency if not using Kafka
        broker = backend_options.get('broker', self.broker)
        topic = backend_options.get('topic', name)

        channel = await create_kafka_channel(
            topic=topic,
            broker=broker,
            key_type=key_type,
            value_type=value_type,
            **backend_options
        )

    elif backend == "redis":
        redis_url = backend_options.get('redis_url', 'redis://localhost:6379')

        channel = await create_redis_channel(
            name=name,
            redis_url=redis_url,
            key_type=key_type,
            value_type=value_type,
            **backend_options
        )

    elif backend == "flight":
        flight_url = backend_options.get('flight_url')
        if not flight_url:
            raise ValueError("flight_url required for Flight backend")

        channel = await create_flight_channel(
            name=name,
            flight_url=flight_url,
            key_type=key_type,
            value_type=value_type,
            **backend_options
        )

    else:
        raise ValueError(f"Unknown backend: {backend}")

    # Register channel
    self._channels[name] = channel

    return channel
```

#### Change 2: Update Sync channel() Method
**File:** `sabot/app.py`
**Location:** Existing `channel()` method

**Replace:**
```python
def channel(self, name: Optional[str] = None, ...) -> ChannelT:
    raise NotImplementedError("Non-memory channels require async creation...")
```

**With:**
```python
def channel(
    self,
    name: Optional[str] = None,
    *,
    key_type: Optional[Type] = None,
    value_type: Optional[Type] = None,
    maxsize: int = 1000,
) -> ChannelT:
    """
    Create a memory channel (synchronous).

    For Kafka/Redis/Flight channels, use async_channel() instead.

    Args:
        name: Channel name (auto-generated if None)
        key_type: Type for keys (optional)
        value_type: Type for values (optional)
        maxsize: Max channel size

    Returns:
        MemoryChannel instance

    Example:
        channel = app.channel(name='my-channel', maxsize=5000)
    """
    from sabot.channels import MemoryChannel

    # Generate name if not provided
    if name is None:
        name = f"channel-{len(self._channels)}"

    # Create memory channel
    channel = MemoryChannel(
        name=name,
        key_type=key_type,
        value_type=value_type,
        maxsize=maxsize
    )

    # Register channel
    self._channels[name] = channel

    return channel
```

#### Change 3: Add Helper Functions
**File:** `sabot/channels.py`
**Location:** End of file

**Add:**
```python
async def create_kafka_channel(
    topic: str,
    broker: str,
    key_type: Optional[Type] = None,
    value_type: Optional[Type] = None,
    **options
):
    """
    Create and initialize a Kafka channel.

    Args:
        topic: Kafka topic name
        broker: Kafka broker URL
        key_type: Type for keys
        value_type: Type for values
        **options: Additional Kafka options

    Returns:
        Initialized KafkaChannel
    """
    from .kafka.source import KafkaSource
    from .kafka.sink import KafkaSink

    # Create source and sink
    source = KafkaSource(
        topic=topic,
        broker=broker,
        **options
    )

    sink = KafkaSink(
        topic=topic,
        broker=broker,
        **options
    )

    # Initialize connection
    await source.start()
    await sink.start()

    # Create channel wrapper
    channel = KafkaChannel(
        name=topic,
        source=source,
        sink=sink,
        key_type=key_type,
        value_type=value_type
    )

    return channel


async def create_redis_channel(
    name: str,
    redis_url: str,
    key_type: Optional[Type] = None,
    value_type: Optional[Type] = None,
    **options
):
    """Create and initialize a Redis channel."""
    # Implementation similar to Kafka
    raise NotImplementedError("Redis channel creation not yet implemented")


async def create_flight_channel(
    name: str,
    flight_url: str,
    key_type: Optional[Type] = None,
    value_type: Optional[Type] = None,
    **options
):
    """Create and initialize an Arrow Flight channel."""
    # Implementation similar to Kafka
    raise NotImplementedError("Flight channel creation not yet implemented")
```

### Verification

#### Unit Test
**File:** `tests/unit/test_app_channels.py` (new file)

```python
import pytest
import asyncio
from sabot import App
from sabot.channels import MemoryChannel


def test_create_memory_channel_sync():
    """Test synchronous memory channel creation."""
    app = App("test-app")

    channel = app.channel(name="test-channel", maxsize=100)

    assert isinstance(channel, MemoryChannel)
    assert channel.name == "test-channel"
    assert channel.maxsize == 100
    assert "test-channel" in app._channels


def test_create_memory_channel_auto_name():
    """Test channel with auto-generated name."""
    app = App("test-app")

    channel = app.channel()

    assert channel.name is not None
    assert channel.name in app._channels


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
    app = App("test-app")

    with pytest.raises(ValueError, match="Unknown backend"):
        asyncio.run(app.async_channel(backend="invalid"))
```

---

## Step 1.4: Test Channel Creation

### Test Implementation
**File:** `tests/integration/test_channels_e2e.py` (new file)

```python
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
```

### Verification
```bash
# Run basic tests
pytest tests/unit/test_app_channels.py -v

# Run with Kafka integration (if available)
pytest tests/integration/test_channels_e2e.py -v --run-kafka
```

---

## Dependencies

### Required Imports
**File:** `sabot/cli.py`
```python
import importlib
import sys
from pathlib import Path
```

**File:** `sabot/app.py`
```python
from typing import Type, Optional
```

### External Dependencies
- typer (already in dependencies)
- rich (already in dependencies)

### Internal Dependencies
- `sabot.app.App` must exist and be importable
- `sabot.channels.MemoryChannel` must exist
- `sabot.kafka.source.KafkaSource` for Kafka channels
- `sabot.kafka.sink.KafkaSink` for Kafka channels

---

## Completion Criteria

### Step 1.1 Complete When:
- [ ] Mock `create_app()` removed from `sabot/cli.py`
- [ ] `load_app_from_spec()` function added
- [ ] `worker()` command updated to use real app loader
- [ ] Error handling covers all edge cases
- [ ] Manual test: `sabot -A examples.fraud_app:app worker` works

### Step 1.2 Complete When:
- [ ] Unit tests for `load_app_from_spec()` pass
- [ ] Integration test for CLI with fraud demo passes
- [ ] Invalid app specs handled gracefully
- [ ] Broker override tested

### Step 1.3 Complete When:
- [ ] `async_channel()` method added to `App`
- [ ] `channel()` method updated for memory-only
- [ ] Helper functions for Kafka/Redis/Flight channels added
- [ ] Documentation strings complete

### Step 1.4 Complete When:
- [ ] Memory channel tests pass
- [ ] Multiple channel creation tested
- [ ] Channel configuration options tested
- [ ] Kafka integration test exists (may skip if no Kafka)

### Phase 1 Complete When:
- [ ] All steps 1.1-1.4 complete
- [ ] CLI loads real applications
- [ ] Memory channels work
- [ ] Kafka channels can be created (API exists)
- [ ] Tests verify functionality
- [ ] Documentation updated

---

**Next:** See PHASE2_TECHNICAL_SPEC.md for Agent Runtime Integration
