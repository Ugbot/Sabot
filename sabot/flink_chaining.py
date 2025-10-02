# -*- coding: utf-8 -*-
"""Flink-style chaining API for distributed stream processing."""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic, Union, Awaitable
from dataclasses import dataclass
from collections import defaultdict

from .distributed_agents import DistributedAgent, DistributedAgentManager, AgentSpec
from .distributed_coordinator import DistributedCoordinator
from .channel_manager import ChannelManager
from .dbos_parallel_controller import DBOSParallelController

logger = logging.getLogger(__name__)

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

@dataclass
class WindowSpec:
    """Window specification for windowed operations."""
    window_type: str  # tumbling, sliding, session
    size: float  # window size in seconds
    slide: Optional[float] = None  # slide interval for sliding windows
    gap: Optional[float] = None  # gap for session windows

@dataclass
class KeyedStream(Generic[K, V]):
    """A keyed stream with key-value pairs."""
    key_func: Callable[[Any], K]
    value_func: Optional[Callable[[Any], V]] = None

class DistributedStream(Generic[T]):
    """Flink-style distributed stream with chaining operations."""

    def __init__(self,
                 agent_manager: DistributedAgentManager,
                 name: str,
                 input_channel_name: Optional[str] = None):
        self.agent_manager = agent_manager
        self.name = name
        self.input_channel_name = input_channel_name
        self.chain: List[DistributedAgent] = []
        self._keyed: bool = False
        self._key_selector: Optional[Callable] = None

    async def start(self) -> None:
        """Start the stream processing chain."""
        logger.info(f"Starting distributed stream {self.name}")

        # Connect agents in chain
        for i in range(len(self.chain) - 1):
            upstream = self.chain[i]
            downstream = self.chain[i + 1]
            await self.agent_manager.connect_agents(upstream.spec.name, downstream.spec.name)

        logger.info(f"Stream {self.name} chain connected: {[a.spec.name for a in self.chain]}")

    async def send(self, data: T) -> None:
        """Send data into the stream."""
        if self.chain:
            await self.chain[0].send(data)

    def map(self, func: Callable[[T], Any]) -> 'DistributedStream':
        """Map operation: transform each element."""
        agent_name = f"{self.name}_map_{len(self.chain)}"

        async def map_agent(event):
            return func(event)

        self._add_agent(agent_name, map_agent)
        return self

    def filter(self, func: Callable[[T], bool]) -> 'DistributedStream':
        """Filter operation: keep only elements where func returns True."""
        agent_name = f"{self.name}_filter_{len(self.chain)}"

        async def filter_agent(event):
            if func(event):
                return event
            return None  # Filtered out

        self._add_agent(agent_name, filter_agent)
        return self

    def flat_map(self, func: Callable[[T], List[Any]]) -> 'DistributedStream':
        """Flat map operation: transform each element into multiple elements."""
        agent_name = f"{self.name}_flatmap_{len(self.chain)}"

        async def flatmap_agent(event):
            results = func(event)
            # Return list to be flattened by downstream processing
            return results

        self._add_agent(agent_name, flatmap_agent)
        return self

    def key_by(self, key_func: Callable[[T], Any]) -> 'DistributedStream':
        """Key by operation: assign keys to elements for grouping."""
        agent_name = f"{self.name}_keyby_{len(self.chain)}"

        async def keyby_agent(event):
            key = key_func(event)
            return {"key": key, "value": event}

        self._add_agent(agent_name, keyby_agent)
        self._keyed = True
        self._key_selector = key_func
        return self

    def group_by(self, key_func: Callable[[T], Any]) -> 'DistributedStream':
        """Group by operation (alias for key_by)."""
        return self.key_by(key_func)

    def window(self, window_spec: WindowSpec) -> 'WindowedStream':
        """Window operation: create windowed stream."""
        return WindowedStream(self, window_spec)

    def reduce(self, func: Callable[[Any, Any], Any],
               initial_value: Any = None) -> 'DistributedStream':
        """Reduce operation: combine elements."""
        agent_name = f"{self.name}_reduce_{len(self.chain)}"

        state = {"value": initial_value}

        async def reduce_agent(event):
            if state["value"] is None:
                state["value"] = event
            else:
                state["value"] = func(state["value"], event)
            return state["value"]

        self._add_agent(agent_name, reduce_agent)
        return self

    async def union(self, *other_streams: 'DistributedStream') -> 'DistributedStream':
        """Union operation: merge multiple streams."""
        agent_name = f"{self.name}_union_{len(self.chain)}"

        async def union_agent(event):
            # In real implementation, this would merge from multiple inputs
            return event

        self._add_agent(agent_name, union_agent)

        # Connect other streams to this union agent
        for stream in other_streams:
            if stream.chain:
                await self.agent_manager.connect_agents(
                    stream.chain[-1].spec.name, agent_name
                )

        return self

    def split(self, *predicates: Callable[[T], bool]) -> List['DistributedStream']:
        """Split operation: split stream based on predicates."""
        streams = []
        for i, predicate in enumerate(predicates):
            split_stream = DistributedStream(self.agent_manager, f"{self.name}_split_{i}")
            split_stream.filter(predicate)._inherit_chain_from(self)
            streams.append(split_stream)

        return streams

    def sink(self, sink_func: Callable[[Any], Awaitable[None]]) -> 'DistributedStream':
        """Add sink operation: consume stream elements."""
        agent_name = f"{self.name}_sink_{len(self.chain)}"

        async def sink_agent(event):
            await sink_func(event)
            return event  # Pass through for chaining

        self._add_agent(agent_name, sink_agent)
        return self

    def print(self) -> 'DistributedStream':
        """Print sink for debugging."""
        return self.sink(lambda x: print(f"Stream {self.name}: {x}"))

    async def to_list(self) -> List[Any]:
        """Collect stream results into a list."""
        results = []

        async def collect(event):
            results.append(event)

        await self.sink(collect).start()

        # In real implementation, would wait for stream completion
        await asyncio.sleep(0.1)  # Brief wait for processing

        return results

    def _add_agent(self, name: str, func: Callable, concurrency: int = 1) -> None:
        """Add an agent to the processing chain."""
        # Create agent spec with DBOS-controlled concurrency
        spec = AgentSpec(
            name=name,
            func=func,
            concurrency=concurrency,
            channel_backend=self.agent_manager.channel_manager._get_default_backend()
        )

        # Create and add agent
        agent = DistributedAgent(
            spec,
            self.agent_manager.coordinator,
            self.agent_manager.channel_manager,
            self.agent_manager.dbos_controller
        )

        self.chain.append(agent)

    def _inherit_chain_from(self, other_stream: 'DistributedStream') -> None:
        """Inherit processing chain from another stream."""
        self.chain = other_stream.chain.copy()

class WindowedStream(Generic[T]):
    """Windowed stream for windowed operations."""

    def __init__(self, source_stream: DistributedStream, window_spec: WindowSpec):
        self.source_stream = source_stream
        self.window_spec = window_spec
        self.agent_manager = source_stream.agent_manager

    def reduce(self, func: Callable[[Any, Any], Any],
               initial_value: Any = None) -> DistributedStream:
        """Windowed reduce operation."""
        agent_name = f"{self.source_stream.name}_winreduce_{len(self.source_stream.chain)}"

        async def windowed_reduce_agent(event):
            # In real implementation, this would handle windowing logic
            # For now, just apply reduce within window context
            return func(initial_value, event) if initial_value is not None else event

        self.source_stream._add_agent(agent_name, windowed_reduce_agent)
        return self.source_stream

    def aggregate(self, func: Callable[[List[Any]], Any]) -> DistributedStream:
        """Windowed aggregate operation."""
        agent_name = f"{self.source_stream.name}_winagg_{len(self.source_stream.chain)}"

        async def windowed_agg_agent(event):
            # In real implementation, this would collect window elements
            # For now, just wrap in list
            return func([event])

        self.source_stream._add_agent(agent_name, windowed_agg_agent)
        return self.source_stream

    def count(self) -> DistributedStream:
        """Count elements in window."""
        return self.aggregate(lambda elements: len(elements))

    def sum(self, field: Optional[str] = None) -> DistributedStream:
        """Sum elements in window."""
        if field:
            return self.aggregate(lambda elements: sum(getattr(e, field, 0) for e in elements))
        else:
            return self.aggregate(lambda elements: sum(elements))

    def max(self, field: Optional[str] = None) -> DistributedStream:
        """Max element in window."""
        if field:
            return self.aggregate(lambda elements: max(getattr(e, field, 0) for e in elements))
        else:
            return self.aggregate(lambda elements: max(elements))

    def min(self, field: Optional[str] = None) -> DistributedStream:
        """Min element in window."""
        if field:
            return self.aggregate(lambda elements: min(getattr(e, field, field, 0) for e in elements))
        else:
            return self.aggregate(lambda elements: min(elements))

    def avg(self, field: Optional[str] = None) -> DistributedStream:
        """Average elements in window."""
        if field:
            return self.aggregate(lambda elements: sum(getattr(e, field, 0) for e in elements) / len(elements))
        else:
            return self.aggregate(lambda elements: sum(elements) / len(elements))

class StreamBuilder:
    """Builder for creating distributed streams with Flink-style API."""

    def __init__(self, agent_manager: DistributedAgentManager):
        self.agent_manager = agent_manager

    def stream(self, name: str, input_channel: Optional[str] = None) -> DistributedStream:
        """Create a new distributed stream."""
        return DistributedStream(self.agent_manager, name, input_channel)

    def from_collection(self, collection: List[T], name: str = "collection") -> DistributedStream[T]:
        """Create stream from a collection."""
        stream = self.stream(f"{name}_source")

        async def source_agent():
            # In real implementation, this would stream the collection
            for item in collection:
                await stream.send(item)
                await asyncio.sleep(0.001)  # Simulate streaming

        # Add source agent
        stream._add_agent(f"{name}_source_agent", source_agent)
        return stream

    def from_channel(self, channel_name: str, name: str = "channel") -> DistributedStream:
        """Create stream from a channel."""
        return self.stream(name, channel_name)

# Convenience functions

def create_stream_builder(agent_manager: DistributedAgentManager) -> StreamBuilder:
    """Create a stream builder."""
    return StreamBuilder(agent_manager)

async def demo_flink_chaining():
    """Demonstrate Flink-style chaining."""
    print("🧱 Flink-Style Chaining Demo")
    print("=" * 30)

    # Mock components (in real usage, these would be properly initialized)
    coordinator = None  # Would be DistributedCoordinator()
    channel_manager = None  # Would be ChannelManager()
    dbos_controller = None  # Would be DBOSParallelController()

    print("⚠️  This is a demonstration of the API structure.")
    print("   Full implementation requires initialized components.")

    # Show API usage
    print("\n📝 Flink-Style Chaining API:")
    print("""
# Create stream builder
builder = create_stream_builder(agent_manager)

# Create and chain operations
result = (builder.stream("sensor_data")
    .map(lambda x: x * 2)                    # Double values
    .filter(lambda x: x > 10)                # Keep > 10
    .key_by(lambda x: x % 3)                 # Group by modulo 3
    .window(WindowSpec("tumbling", 60.0))   # 60-second tumbling windows
    .sum()                                   # Sum values in each window
    .sink(lambda x: print(f"Result: {x}"))   # Print results
)

# Start processing
await result.start()

# Send data
await result.send(5)   # -> filtered out (5 * 2 = 10, not > 10)
await result.send(6)   # -> 12, key=0, window sum
await result.send(8)   # -> 16, key=1, window sum
""")

    print("\n🔗 Available Operations:")
    operations = [
        "map(func) - Transform each element",
        "filter(func) - Keep elements where func is True",
        "flat_map(func) - Transform to multiple elements",
        "key_by(func) - Assign keys for grouping",
        "group_by(func) - Alias for key_by",
        "window(spec) - Create windowed operations",
        "reduce(func) - Combine elements",
        "union(*streams) - Merge multiple streams",
        "split(*predicates) - Split stream conditionally",
        "sink(func) - Consume elements",
        "print() - Debug print sink"
    ]

    for op in operations:
        print(f"  • {op}")

    print("\n🏗️  Architecture:")
    print("  Each operation creates a DistributedAgent")
    print("  Agents are chained via channels")
    print("  DBOS controls deployment and scaling")
    print("  Fault tolerance via supervisor patterns")

    return True

if __name__ == "__main__":
    asyncio.run(demo_flink_chaining())
