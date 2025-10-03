# Phase 2: Agent Runtime Integration - Technical Specification
**Status:** Ready to implement
**Dependencies:** Phase 1 complete (CLI loads real apps)

---

## Overview

Wire the agent runtime to actual Kafka consumers and state backends. Currently, the agent runtime exists as a 657-line structure but doesn't execute agents or connect to data sources.

**Goal:** Agents consume from Kafka, process messages, use state, and coordinate checkpoints.

---

## Step 2.1: Wire Agent to Kafka Consumer

### Current State

**File:** `sabot/agents/runtime.py`
- `AgentProcess` dataclass exists
- No actual Kafka consumer integration
- No message processing loop

**File:** `sabot/kafka/source.py`
- `KafkaSource` exists but not connected to agents

### Required Changes

#### Change 1: Add Kafka Consumer to AgentProcess
**File:** `sabot/agents/runtime.py`

**Add to AgentProcess:**
```python
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
import asyncio
import logging

logger = logging.getLogger(__name__)


@dataclass
class AgentProcess:
    """Represents a running agent process."""
    agent_id: str
    agent_func: Callable  # The user's @app.agent() function
    topic: str  # Kafka topic to consume from
    broker: str  # Kafka broker URL

    # Runtime state
    process: Optional[asyncio.Task] = None
    state: 'AgentState' = field(default_factory=lambda: AgentState.STOPPED)
    consumer: Optional['KafkaSource'] = None

    # Statistics
    messages_processed: int = 0
    errors_count: int = 0
    last_message_time: Optional[float] = None

    async def start(self):
        """Start the agent process."""
        from sabot.kafka.source import KafkaSource

        logger.info(f"Starting agent {self.agent_id}")

        # Create Kafka consumer
        self.consumer = KafkaSource(
            topic=self.topic,
            broker=self.broker,
            group_id=f"{self.agent_id}-group"
        )

        # Start consumer
        await self.consumer.start()

        # Create processing task
        self.process = asyncio.create_task(self._processing_loop())
        self.state = AgentState.RUNNING

        logger.info(f"Agent {self.agent_id} started")

    async def stop(self):
        """Stop the agent process."""
        logger.info(f"Stopping agent {self.agent_id}")

        self.state = AgentState.STOPPING

        # Cancel processing task
        if self.process:
            self.process.cancel()
            try:
                await self.process
            except asyncio.CancelledError:
                pass

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        self.state = AgentState.STOPPED
        logger.info(f"Agent {self.agent_id} stopped")

    async def _processing_loop(self):
        """Main message processing loop."""
        logger.info(f"Agent {self.agent_id} processing loop started")

        try:
            async for message in self.consumer:
                await self._process_message(message)
        except asyncio.CancelledError:
            logger.info(f"Agent {self.agent_id} processing loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Agent {self.agent_id} processing loop error: {e}")
            self.state = AgentState.FAILED
            raise

    async def _process_message(self, message):
        """Process a single message."""
        import time

        try:
            # Call user's agent function
            result = await self.agent_func(message)

            # Update statistics
            self.messages_processed += 1
            self.last_message_time = time.time()

            # Handle result (if any)
            if result is not None:
                # Agent returned a result (e.g., for forwarding)
                logger.debug(f"Agent {self.agent_id} produced result: {result}")
                # TODO: Forward to output topic/channel

        except Exception as e:
            self.errors_count += 1
            logger.error(f"Agent {self.agent_id} error processing message: {e}")
            # TODO: Error handling policy (skip, retry, fail)
            raise
```

#### Change 2: Add AgentRuntime Manager
**File:** `sabot/agents/runtime.py`

**Add:**
```python
class AgentRuntime:
    """
    Manages multiple agent processes.

    Responsibilities:
    - Start/stop agents
    - Monitor health
    - Coordinate checkpoints
    """

    def __init__(self, app):
        self.app = app
        self.agents: Dict[str, AgentProcess] = {}
        self._running = False
        logger.info("AgentRuntime initialized")

    async def register_agent(
        self,
        agent_id: str,
        agent_func: Callable,
        topic: str
    ):
        """Register an agent for execution."""
        agent = AgentProcess(
            agent_id=agent_id,
            agent_func=agent_func,
            topic=topic,
            broker=self.app.broker
        )

        self.agents[agent_id] = agent
        logger.info(f"Registered agent: {agent_id} for topic: {topic}")

    async def start_all(self):
        """Start all registered agents."""
        logger.info(f"Starting {len(self.agents)} agents")

        self._running = True

        # Start all agents concurrently
        await asyncio.gather(
            *[agent.start() for agent in self.agents.values()]
        )

        logger.info("All agents started")

    async def stop_all(self):
        """Stop all agents."""
        logger.info("Stopping all agents")

        self._running = False

        # Stop all agents
        await asyncio.gather(
            *[agent.stop() for agent in self.agents.values()],
            return_exceptions=True
        )

        logger.info("All agents stopped")

    async def run(self):
        """Run the agent runtime (blocks until stopped)."""
        try:
            await self.start_all()

            # Wait indefinitely (until interrupted)
            while self._running:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            await self.stop_all()

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all agents."""
        return {
            agent_id: {
                "state": agent.state.value,
                "messages_processed": agent.messages_processed,
                "errors": agent.errors_count,
                "last_message_time": agent.last_message_time
            }
            for agent_id, agent in self.agents.items()
        }
```

#### Change 3: Update App to Use AgentRuntime
**File:** `sabot/app.py`

**Add to App.__init__:**
```python
from sabot.agents.runtime import AgentRuntime

class App:
    def __init__(self, id: str, broker: str = "memory://", **kwargs):
        self.id = id
        self.broker = broker

        # ... existing init ...

        # Add agent runtime
        self.agent_runtime = AgentRuntime(self)
```

**Update agent decorator:**
```python
def agent(self, topic: Optional[str] = None, **kwargs):
    """Decorator to register an agent."""
    def decorator(func):
        # Generate topic from function name if not provided
        agent_topic = topic or func.__name__
        agent_id = f"{self.id}.{func.__name__}"

        # Register agent with runtime
        import asyncio
        asyncio.create_task(
            self.agent_runtime.register_agent(
                agent_id=agent_id,
                agent_func=func,
                topic=agent_topic
            )
        )

        return func
    return decorator
```

**Update run method:**
```python
async def run(self):
    """Run the application."""
    logger.info(f"Starting Sabot app: {self.id}")

    try:
        # Run agent runtime
        await self.agent_runtime.run()
    except KeyboardInterrupt:
        logger.info("App stopped by user")
    finally:
        logger.info("App shutdown complete")
```

### Verification

#### Unit Test
**File:** `tests/unit/test_agent_process.py` (new file)

```python
import pytest
import asyncio
from sabot.agents.runtime import AgentProcess, AgentState


@pytest.mark.asyncio
async def test_agent_process_lifecycle():
    """Test agent can start and stop."""
    processed_messages = []

    async def test_agent(message):
        processed_messages.append(message)

    agent = AgentProcess(
        agent_id="test-agent",
        agent_func=test_agent,
        topic="test-topic",
        broker="memory://"
    )

    # Initially stopped
    assert agent.state == AgentState.STOPPED

    # Start agent
    await agent.start()
    assert agent.state == AgentState.RUNNING

    # Stop agent
    await agent.stop()
    assert agent.state == AgentState.STOPPED


@pytest.mark.asyncio
async def test_agent_processes_messages():
    """Test agent processes messages from Kafka."""
    processed = []

    async def counting_agent(msg):
        processed.append(msg)

    agent = AgentProcess(
        agent_id="counter",
        agent_func=counting_agent,
        topic="test",
        broker="memory://"
    )

    await agent.start()

    # Give it time to process (in real test, would send messages)
    await asyncio.sleep(0.1)

    await agent.stop()

    # Verify statistics updated
    # (actual count depends on mock consumer)
```

---

## Step 2.2: Test Agent Kafka Integration

### Test Implementation
**File:** `tests/integration/test_agent_kafka.py` (new file)

```python
"""Integration tests for agent Kafka integration."""
import pytest
import asyncio
from sabot import App


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agent_receives_kafka_messages():
    """Test agent can receive messages from Kafka."""
    app = App("test-app", broker="kafka://localhost:19092")

    received_messages = []

    @app.agent(topic="test-agent-topic")
    async def test_agent(message):
        received_messages.append(message)

    # Start app (would need to run in background)
    # Send test messages to Kafka
    # Verify agent received them

    # This is a framework for the test
    # Actual implementation depends on Kafka test setup


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agent_handles_json_messages():
    """Test agent deserializes JSON messages correctly."""
    app = App("test-app", broker="kafka://localhost:19092")

    @app.agent(topic="json-topic")
    async def json_agent(message):
        assert isinstance(message, dict)
        assert "key" in message

    # Framework for JSON deserialization test


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agent_error_handling():
    """Test agent handles processing errors."""
    app = App("test-app", broker="kafka://localhost:19092")

    errors = []

    @app.agent(topic="error-topic")
    async def failing_agent(message):
        if message.get("trigger_error"):
            raise ValueError("Test error")
        return message

    # Test error handling policies
```

### Verification
```bash
# Requires Kafka running
docker compose up -d kafka

# Run integration tests
pytest tests/integration/test_agent_kafka.py -v
```

---

## Step 2.3: Connect State Backend to Agents

### Current State
- State backends exist (`MemoryBackend`, `RocksDBBackend`)
- Agents don't have access to state

### Required Changes

#### Change 1: Add State to AgentProcess
**File:** `sabot/agents/runtime.py`

**Update AgentProcess:**
```python
@dataclass
class AgentProcess:
    # ... existing fields ...

    state_backend: Optional['StateBackend'] = None

    async def start(self):
        """Start the agent process."""
        # ... existing start code ...

        # Initialize state backend if configured
        if self.state_backend:
            logger.info(f"Agent {self.agent_id} using state backend: {type(self.state_backend).__name__}")

    async def _process_message(self, message):
        """Process a single message with state access."""
        try:
            # Make state available to agent function
            if self.state_backend:
                # Inject state into message context
                message._state = self.state_backend

            # Call user's agent function
            result = await self.agent_func(message)

            # ... rest of processing ...
```

#### Change 2: Add State Access Pattern
**File:** `sabot/api/context.py` (new file)

```python
"""Agent execution context with state access."""
from typing import Any, Optional
from sabot.state import StateBackend


class AgentContext:
    """
    Execution context for agent functions.

    Provides access to state, checkpoints, etc.
    """

    def __init__(
        self,
        message: Any,
        state_backend: Optional[StateBackend] = None
    ):
        self.message = message
        self._state_backend = state_backend

    def state(self, namespace: str = "default"):
        """Get state accessor for namespace."""
        if not self._state_backend:
            raise RuntimeError("No state backend configured")

        return StateAccessor(
            backend=self._state_backend,
            namespace=namespace
        )


class StateAccessor:
    """State accessor for a specific namespace."""

    def __init__(self, backend: StateBackend, namespace: str):
        self.backend = backend
        self.namespace = namespace

    async def get(self, key: str) -> Any:
        """Get value from state."""
        from sabot.state import ValueState
        state = ValueState(self.backend, self.namespace)
        return await state.get(key)

    async def put(self, key: str, value: Any):
        """Put value into state."""
        from sabot.state import ValueState
        state = ValueState(self.backend, self.namespace)
        await state.put(key, value)

    async def delete(self, key: str):
        """Delete value from state."""
        from sabot.state import ValueState
        state = ValueState(self.backend, self.namespace)
        await state.delete(key)
```

#### Change 3: Update Agent Function Signature
**File:** `sabot/agents/runtime.py`

**Update _process_message:**
```python
async def _process_message(self, message):
    """Process a single message."""
    from sabot.api.context import AgentContext
    import time

    try:
        # Create context with state access
        ctx = AgentContext(
            message=message,
            state_backend=self.state_backend
        )

        # Call user's agent function with context
        result = await self.agent_func(ctx)

        # ... rest of processing ...
```

### Verification

#### Unit Test
**File:** `tests/unit/test_agent_state.py` (new file)

```python
import pytest
import asyncio
from sabot.agents.runtime import AgentProcess
from sabot.state import MemoryBackend, BackendConfig


@pytest.mark.asyncio
async def test_agent_can_access_state():
    """Test agent can read/write state."""
    backend = MemoryBackend(BackendConfig(backend_type="memory"))

    async def stateful_agent(ctx):
        # Write to state
        await ctx.state().put("counter", 1)

        # Read from state
        value = await ctx.state().get("counter")
        assert value == 1

    agent = AgentProcess(
        agent_id="stateful",
        agent_func=stateful_agent,
        topic="test",
        broker="memory://",
        state_backend=backend
    )

    await agent.start()

    # Process a message (would come from Kafka in real scenario)
    from sabot.api.context import AgentContext
    ctx = AgentContext(message={"test": "data"}, state_backend=backend)
    await stateful_agent(ctx)

    await agent.stop()
```

---

## Step 2.4: Test Agent State Integration

### Test Implementation
**File:** `tests/integration/test_agent_state_e2e.py` (new file)

```python
"""End-to-end tests for agent state integration."""
import pytest
import asyncio
from sabot import App
from sabot.state import MemoryBackend, BackendConfig


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agent_state_persistence():
    """Test agent state persists across messages."""
    backend = MemoryBackend(BackendConfig(backend_type="memory"))
    app = App("test-app", broker="memory://")
    app.state_backend = backend

    @app.agent(topic="counter-topic")
    async def counter_agent(ctx):
        # Increment counter in state
        current = await ctx.state().get("count") or 0
        await ctx.state().put("count", current + 1)
        return current + 1

    # Process multiple messages
    # Verify counter increments correctly

    # Framework for test


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_agents_share_state():
    """Test multiple agents can share state."""
    backend = MemoryBackend(BackendConfig(backend_type="memory"))
    app = App("test-app", broker="memory://")
    app.state_backend = backend

    @app.agent(topic="writer-topic")
    async def writer_agent(ctx):
        await ctx.state().put("shared", ctx.message)

    @app.agent(topic="reader-topic")
    async def reader_agent(ctx):
        value = await ctx.state().get("shared")
        return value

    # Test state sharing between agents
```

### Verification
```bash
pytest tests/integration/test_agent_state_e2e.py -v
```

---

## Step 2.5: Connect Checkpoint Coordinator

### Required Changes

#### Change 1: Add Checkpoint Coordinator to AgentRuntime
**File:** `sabot/agents/runtime.py`

**Update AgentRuntime:**
```python
from sabot.checkpoint import Coordinator

class AgentRuntime:
    def __init__(self, app):
        self.app = app
        self.agents: Dict[str, AgentProcess] = {}
        self._running = False

        # Add checkpoint coordinator
        self.checkpoint_coordinator = Coordinator()
        self._checkpoint_interval = 60.0  # seconds
        self._checkpoint_task = None

        logger.info("AgentRuntime initialized with checkpoint coordinator")

    async def start_all(self):
        """Start all registered agents."""
        # ... existing start code ...

        # Start checkpoint timer
        self._checkpoint_task = asyncio.create_task(
            self._checkpoint_loop()
        )

    async def stop_all(self):
        """Stop all agents."""
        # Cancel checkpoint task
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # ... existing stop code ...

    async def _checkpoint_loop(self):
        """Periodic checkpoint triggering."""
        while self._running:
            try:
                await asyncio.sleep(self._checkpoint_interval)
                await self.trigger_checkpoint()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Checkpoint loop error: {e}")

    async def trigger_checkpoint(self) -> int:
        """Trigger a checkpoint across all agents."""
        logger.info("Triggering checkpoint")

        # Generate checkpoint ID
        checkpoint_id = self.checkpoint_coordinator.trigger()

        logger.info(f"Checkpoint {checkpoint_id} triggered")

        # Send barriers to all agents
        for agent in self.agents.values():
            await agent.receive_barrier(checkpoint_id)

        # Wait for all agents to acknowledge
        # (simplified - real implementation more complex)

        logger.info(f"Checkpoint {checkpoint_id} complete")

        return checkpoint_id
```

#### Change 2: Add Barrier Handling to AgentProcess
**File:** `sabot/agents/runtime.py`

**Update AgentProcess:**
```python
@dataclass
class AgentProcess:
    # ... existing fields ...

    barrier_queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    async def receive_barrier(self, checkpoint_id: int):
        """Receive checkpoint barrier."""
        logger.info(f"Agent {self.agent_id} received barrier {checkpoint_id}")
        await self.barrier_queue.put(checkpoint_id)

    async def _processing_loop(self):
        """Main message processing loop with checkpoint handling."""
        logger.info(f"Agent {self.agent_id} processing loop started")

        try:
            while True:
                # Check for barriers (non-blocking)
                try:
                    checkpoint_id = self.barrier_queue.get_nowait()
                    await self._handle_barrier(checkpoint_id)
                except asyncio.QueueEmpty:
                    pass

                # Process next message
                try:
                    message = await asyncio.wait_for(
                        self.consumer.get_next(),
                        timeout=0.1
                    )
                    await self._process_message(message)
                except asyncio.TimeoutError:
                    continue

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Processing loop error: {e}")
            self.state = AgentState.FAILED
            raise

    async def _handle_barrier(self, checkpoint_id: int):
        """Handle checkpoint barrier."""
        logger.info(f"Agent {self.agent_id} handling barrier {checkpoint_id}")

        # Snapshot state
        if self.state_backend:
            snapshot = await self.state_backend.snapshot()
            # Store snapshot (simplified)
            logger.info(f"Agent {self.agent_id} state snapshot for checkpoint {checkpoint_id}")

        # Acknowledge barrier
        # (would notify coordinator in real implementation)
```

### Verification

#### Unit Test
**File:** `tests/unit/test_checkpoint_coordination.py` (new file)

```python
import pytest
import asyncio
from sabot.agents.runtime import AgentRuntime, AgentProcess
from sabot import App


@pytest.mark.asyncio
async def test_checkpoint_triggered():
    """Test checkpoint can be triggered."""
    app = App("test-app")
    runtime = AgentRuntime(app)

    # Register a test agent
    async def test_agent(ctx):
        pass

    await runtime.register_agent(
        agent_id="test",
        agent_func=test_agent,
        topic="test"
    )

    # Trigger checkpoint
    checkpoint_id = await runtime.trigger_checkpoint()

    assert checkpoint_id > 0


@pytest.mark.asyncio
async def test_agent_receives_barrier():
    """Test agent receives checkpoint barrier."""
    async def test_agent(ctx):
        pass

    agent = AgentProcess(
        agent_id="test",
        agent_func=test_agent,
        topic="test",
        broker="memory://"
    )

    # Send barrier
    await agent.receive_barrier(1)

    # Verify barrier in queue
    checkpoint_id = await agent.barrier_queue.get()
    assert checkpoint_id == 1
```

---

## Step 2.6: Test Checkpoint Integration

### Test Implementation
**File:** `tests/integration/test_checkpoint_e2e.py` (new file)

```python
"""End-to-end checkpoint integration tests."""
import pytest
import asyncio
from sabot import App
from sabot.state import MemoryBackend, BackendConfig


@pytest.mark.integration
@pytest.mark.asyncio
async def test_checkpoint_end_to_end():
    """Test complete checkpoint flow."""
    backend = MemoryBackend(BackendConfig(backend_type="memory"))
    app = App("test-app", broker="memory://")
    app.state_backend = backend

    @app.agent(topic="test-topic")
    async def stateful_agent(ctx):
        count = await ctx.state().get("count") or 0
        await ctx.state().put("count", count + 1)

    # Start app
    # Process messages
    # Trigger checkpoint
    # Verify state snapshot created
    # Verify checkpoint completes


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_agents_checkpoint():
    """Test checkpoint with multiple agents."""
    app = App("test-app", broker="memory://")

    @app.agent(topic="agent1")
    async def agent1(ctx):
        pass

    @app.agent(topic="agent2")
    async def agent2(ctx):
        pass

    # Start both agents
    # Trigger checkpoint
    # Verify all agents participate
```

### Verification
```bash
pytest tests/integration/test_checkpoint_e2e.py -v
```

---

## Completion Criteria

### Step 2.1 Complete When:
- [ ] AgentProcess has Kafka consumer integration
- [ ] Agent processes messages from Kafka
- [ ] Processing loop handles errors
- [ ] Statistics tracked (messages, errors)

### Step 2.2 Complete When:
- [ ] Integration test for Kafka messages passes
- [ ] JSON deserialization tested
- [ ] Error handling tested

### Step 2.3 Complete When:
- [ ] AgentProcess has state backend
- [ ] AgentContext provides state access
- [ ] State operations work in agent functions

### Step 2.4 Complete When:
- [ ] Agent state persistence tested
- [ ] Multiple agents can share state
- [ ] State survives across messages

### Step 2.5 Complete When:
- [ ] AgentRuntime has checkpoint coordinator
- [ ] Periodic checkpoints triggered
- [ ] Agents receive barriers
- [ ] Barriers handled in processing loop

### Step 2.6 Complete When:
- [ ] End-to-end checkpoint test passes
- [ ] Multiple agents checkpoint together
- [ ] State snapshots created

### Phase 2 Complete When:
- [ ] All steps 2.1-2.6 complete
- [ ] Agents consume from Kafka
- [ ] Agents use state backends
- [ ] Checkpoints coordinated across agents
- [ ] Integration tests pass
- [ ] Can run fraud demo with real agent runtime

---

**Next:** See PHASE3_TECHNICAL_SPEC.md for Test Coverage Improvement
