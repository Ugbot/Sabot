"""
Agent-to-Agent Streaming Tests

Tests for streaming between Sabot agents (actors) using lock-free shuffle transport.
Agents represent stateful stream processing units that communicate via shuffle.

Topology examples:
- Agent A -> Shuffle -> Agent B (1:1 streaming)
- Agent A -> Shuffle -> [Agent B, Agent C] (1:N fan-out)
- [Agent A, Agent B] -> Shuffle -> Agent C (N:1 fan-in)
- Multi-stage agent pipelines
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa

from sabot._cython.shuffle.atomic_partition_store import AtomicPartitionStore
from sabot._cython.shuffle.lock_free_queue import SPSCRingBuffer, MPSCQueue


# ============================================================================
# Simulated Agent Framework
# ============================================================================

class StreamingAgent:
    """
    Simulated Sabot streaming agent (actor).

    Represents a stateful processing unit that:
    - Consumes input streams
    - Maintains state
    - Produces output streams
    - Communicates via shuffle
    """

    def __init__(self, agent_id, agent_type="generic"):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.state = {}  # Agent-local state
        self.output_queue = SPSCRingBuffer(capacity=64)  # Output buffer
        self.running = False

    def process(self, input_batch):
        """
        Process input batch and produce output.

        Override in subclasses for specific logic.
        """
        return input_batch  # Pass-through by default

    def emit(self, shuffle_hash, partition_id, batch):
        """Emit output batch to shuffle queue (lock-free)."""
        success = self.output_queue.push(shuffle_hash, partition_id, batch)
        return success

    def receive_from_queue(self):
        """Receive batch from output queue."""
        return self.output_queue.pop()


class MapAgent(StreamingAgent):
    """Agent that applies a map transformation."""

    def __init__(self, agent_id, map_func):
        super().__init__(agent_id, agent_type="map")
        self.map_func = map_func

    def process(self, input_batch):
        """Apply map function to batch."""
        return self.map_func(input_batch)


class FilterAgent(StreamingAgent):
    """Agent that filters records."""

    def __init__(self, agent_id, filter_func):
        super().__init__(agent_id, agent_type="filter")
        self.filter_func = filter_func

    def process(self, input_batch):
        """Apply filter to batch."""
        mask = self.filter_func(input_batch)
        return input_batch.filter(mask)


class AggregateAgent(StreamingAgent):
    """Agent that performs stateful aggregation."""

    def __init__(self, agent_id, key_column, agg_column):
        super().__init__(agent_id, agent_type="aggregate")
        self.key_column = key_column
        self.agg_column = agg_column

    def process(self, input_batch):
        """Aggregate input batch and update state."""
        keys = input_batch.column(self.key_column)
        values = input_batch.column(self.agg_column)

        # Update aggregation state
        for i in range(input_batch.num_rows):
            key = keys[i].as_py()
            value = values[i].as_py()

            if key not in self.state:
                self.state[key] = {'count': 0, 'sum': 0.0}

            self.state[key]['count'] += 1
            self.state[key]['sum'] += value

        # Return batch as-is (state is maintained internally)
        return input_batch

    def get_results(self):
        """Get aggregation results from state."""
        results = []
        for key, agg in self.state.items():
            results.append({
                'key': key,
                'count': agg['count'],
                'sum': agg['sum'],
                'avg': agg['sum'] / agg['count'] if agg['count'] > 0 else 0.0
            })
        return results


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def event_schema():
    """Schema for event stream."""
    return pa.schema([
        ('event_id', pa.int64()),
        ('user_id', pa.int32()),
        ('timestamp', pa.int64()),
        ('event_type', pa.string()),
        ('value', pa.float64()),
    ])


@pytest.fixture
def create_event_batch():
    """Factory for creating event batches."""
    def _create(start_id, num_events=100):
        return pa.RecordBatch.from_arrays([
            pa.array(list(range(start_id, start_id + num_events)), type=pa.int64()),
            pa.array([i % 10 for i in range(num_events)], type=pa.int32()),  # 10 users
            pa.array([int(time.time() * 1000) + i for i in range(num_events)], type=pa.int64()),
            pa.array(['click', 'view', 'purchase'][i % 3] for i in range(num_events), type=pa.string()),
            pa.array([float(i) * 1.5 for i in range(num_events)], type=pa.float64()),
        ], schema=event_schema())
    return _create


# ============================================================================
# Agent-to-Agent Streaming Tests
# ============================================================================

class TestAgentStreaming:
    """Test streaming between agents."""

    def test_single_agent_to_agent(self, event_schema, create_event_batch):
        """
        Test: Agent A -> Agent B (1:1 streaming)

        Agent A (source) -> Shuffle Queue -> Agent B (sink)
        """
        # Create agents
        agent_a = StreamingAgent(agent_id="A")
        agent_b = StreamingAgent(agent_id="B")

        # Agent A produces batch
        input_batch = create_event_batch(start_id=0, num_events=100)

        processed_batch = agent_a.process(input_batch)

        # Agent A emits to shuffle
        success = agent_a.emit(shuffle_hash=1, partition_id=0, batch=processed_batch)
        assert success

        # Agent B receives from shuffle
        result = agent_a.receive_from_queue()
        assert result is not None

        shuffle_hash, partition_id, received_batch = result

        # Agent B processes received batch
        output_batch = agent_b.process(received_batch)

        assert output_batch.num_rows == 100
        print(f"Agent A -> Agent B: Streamed {output_batch.num_rows} events")

    def test_map_agent_pipeline(self, event_schema, create_event_batch):
        """
        Test: Source -> MapAgent -> Sink

        MapAgent doubles all values.
        """
        # Create map agent (double values)
        def double_values(batch):
            value_col = batch.column('value')
            doubled = pa.compute.multiply(value_col, pa.scalar(2.0))
            return batch.set_column(4, 'value', doubled)

        map_agent = MapAgent(agent_id="mapper_1", map_func=double_values)

        # Process batch
        input_batch = create_event_batch(start_id=0, num_events=50)
        output_batch = map_agent.process(input_batch)

        # Verify transformation
        original_sum = pa.compute.sum(input_batch.column('value')).as_py()
        transformed_sum = pa.compute.sum(output_batch.column('value')).as_py()

        assert abs(transformed_sum - original_sum * 2.0) < 0.01
        print(f"MapAgent: {original_sum:.1f} -> {transformed_sum:.1f} (doubled)")

    def test_filter_agent_pipeline(self, event_schema, create_event_batch):
        """
        Test: Source -> FilterAgent -> Sink

        FilterAgent keeps only 'purchase' events.
        """
        # Create filter agent
        def is_purchase(batch):
            event_type_col = batch.column('event_type')
            return pa.compute.equal(event_type_col, pa.scalar('purchase'))

        filter_agent = FilterAgent(agent_id="filter_1", filter_func=is_purchase)

        # Process batch
        input_batch = create_event_batch(start_id=0, num_events=150)
        output_batch = filter_agent.process(input_batch)

        # Verify filtering (every 3rd event is 'purchase')
        expected_purchases = 150 // 3
        assert output_batch.num_rows == expected_purchases

        # Verify all remaining events are 'purchase'
        event_types = output_batch.column('event_type')
        for i in range(output_batch.num_rows):
            assert event_types[i].as_py() == 'purchase'

        print(f"FilterAgent: {input_batch.num_rows} -> {output_batch.num_rows} events (purchases only)")

    def test_aggregate_agent_stateful(self, event_schema, create_event_batch):
        """
        Test: Source -> AggregateAgent (stateful)

        AggregateAgent maintains running aggregation per user.
        """
        agg_agent = AggregateAgent(agent_id="agg_1", key_column='user_id', agg_column='value')

        # Process multiple batches (stateful aggregation)
        for batch_id in range(3):
            input_batch = create_event_batch(start_id=batch_id * 100, num_events=100)
            agg_agent.process(input_batch)

        # Get aggregation results
        results = agg_agent.get_results()

        print(f"\nAggregateAgent results ({len(results)} users):")
        for result in sorted(results, key=lambda x: x['key'])[:5]:
            print(f"  User {result['key']}: count={result['count']}, sum={result['sum']:.1f}, avg={result['avg']:.2f}")

        # Verify we have 10 users (0-9)
        assert len(results) == 10

        # Verify total count
        total_count = sum(r['count'] for r in results)
        assert total_count == 300  # 3 batches * 100 events


# ============================================================================
# Multi-Agent Topologies
# ============================================================================

class TestMultiAgentTopologies:
    """Test complex multi-agent streaming topologies."""

    def test_fan_out_topology(self, event_schema, create_event_batch):
        """
        Test: Agent A -> [Agent B, Agent C] (1:N fan-out)

        Source agent broadcasts to multiple downstream agents.
        """
        source_agent = StreamingAgent(agent_id="source")
        agent_b = StreamingAgent(agent_id="B")
        agent_c = StreamingAgent(agent_id="C")

        # Source produces batch
        input_batch = create_event_batch(start_id=0, num_events=100)

        # Fan-out to both agents (different partitions)
        source_agent.emit(shuffle_hash=1, partition_id=0, batch=input_batch)  # To B
        source_agent.emit(shuffle_hash=2, partition_id=0, batch=input_batch)  # To C

        # Agent B receives
        result_b = source_agent.receive_from_queue()
        assert result_b is not None
        _, _, batch_b = result_b

        # Agent C receives
        result_c = source_agent.receive_from_queue()
        assert result_c is not None
        _, _, batch_c = result_c

        # Both agents received the batch
        assert batch_b.num_rows == 100
        assert batch_c.num_rows == 100

        print(f"Fan-out: Source -> [Agent B ({batch_b.num_rows} rows), Agent C ({batch_c.num_rows} rows)]")

    def test_fan_in_topology(self, event_schema, create_event_batch):
        """
        Test: [Agent A, Agent B] -> Agent C (N:1 fan-in)

        Multiple agents send to single aggregator via MPSC queue.
        """
        agent_a = StreamingAgent(agent_id="A")
        agent_b = StreamingAgent(agent_id="B")

        # Use MPSC queue for fan-in
        fanin_queue = MPSCQueue(capacity=32)

        # Agent A produces and sends
        batch_a = create_event_batch(start_id=0, num_events=50)
        fanin_queue.push_mpsc(1, 0, batch_a)

        # Agent B produces and sends
        batch_b = create_event_batch(start_id=100, num_events=75)
        fanin_queue.push_mpsc(2, 0, batch_b)

        # Agent C receives from both (fan-in)
        total_received = 0
        batches_received = []

        for _ in range(2):
            result = fanin_queue.pop_mpsc()
            if result:
                _, _, batch = result
                total_received += batch.num_rows
                batches_received.append(batch)

        assert total_received == 125  # 50 + 75
        assert len(batches_received) == 2

        print(f"Fan-in: [Agent A (50 rows), Agent B (75 rows)] -> Agent C ({total_received} total rows)")

    def test_multi_stage_pipeline(self, event_schema, create_event_batch):
        """
        Test: Agent A -> Agent B -> Agent C (3-stage pipeline)

        Filter -> Map -> Aggregate pipeline.
        """
        # Stage 1: Filter agent
        def is_high_value(batch):
            value_col = batch.column('value')
            return pa.compute.greater(value_col, pa.scalar(100.0))

        filter_agent = FilterAgent(agent_id="filter", filter_func=is_high_value)

        # Stage 2: Map agent (scale values)
        def scale_values(batch):
            value_col = batch.column('value')
            scaled = pa.compute.multiply(value_col, pa.scalar(0.01))  # Scale down
            return batch.set_column(4, 'value', scaled)

        map_agent = MapAgent(agent_id="mapper", map_func=scale_values)

        # Stage 3: Aggregate agent
        agg_agent = AggregateAgent(agent_id="aggregator", key_column='user_id', agg_column='value')

        # Process through pipeline
        input_batch = create_event_batch(start_id=0, num_events=200)

        # Stage 1: Filter
        filtered = filter_agent.process(input_batch)
        print(f"Stage 1 (Filter): {input_batch.num_rows} -> {filtered.num_rows} rows")

        # Stage 2: Map
        mapped = map_agent.process(filtered)
        original_max = pa.compute.max(filtered.column('value')).as_py()
        mapped_max = pa.compute.max(mapped.column('value')).as_py()
        print(f"Stage 2 (Map): max value {original_max:.1f} -> {mapped_max:.2f}")

        # Stage 3: Aggregate
        agg_agent.process(mapped)
        results = agg_agent.get_results()
        print(f"Stage 3 (Aggregate): {len(results)} users aggregated")

        for result in results[:3]:
            print(f"  User {result['key']}: avg={result['avg']:.4f}")

    def test_shuffle_by_key_topology(self, event_schema, create_event_batch):
        """
        Test: [Agent A, Agent B] -> Shuffle by user_id -> [Agent C, Agent D]

        Demonstrates key-based partitioning across agents.
        """
        # Upstream agents produce data
        batch_a = create_event_batch(start_id=0, num_events=100)
        batch_b = create_event_batch(start_id=200, num_events=100)

        # Use atomic store for shuffle
        store = AtomicPartitionStore(size=256)

        # Partition by user_id (hash partitioning)
        num_partitions = 4

        def partition_and_store(batch, shuffle_id):
            """Partition batch by user_id and store."""
            user_ids = batch.column('user_id')

            partition_rows = {i: [] for i in range(num_partitions)}

            for row_idx in range(batch.num_rows):
                user_id = user_ids[row_idx].as_py()
                partition_id = user_id % num_partitions
                partition_rows[partition_id].append(row_idx)

            # Create and store partition batches
            for partition_id, row_indices in partition_rows.items():
                if row_indices:
                    indices = pa.array(row_indices, type=pa.int32())
                    partition_batch = batch.take(indices)

                    shuffle_hash = hash(f"shuffle_{shuffle_id}")
                    store.insert(shuffle_hash, partition_id, partition_batch)

        # Partition data from both agents
        partition_and_store(batch_a, "A")
        partition_and_store(batch_b, "B")

        # Downstream agents fetch their partitions
        # Agent C gets partitions 0,1 and Agent D gets partitions 2,3
        agent_c_rows = 0
        agent_d_rows = 0

        # Agent C fetches
        for partition_id in [0, 1]:
            for shuffle_id in ["A", "B"]:
                shuffle_hash = hash(f"shuffle_{shuffle_id}")
                batch = store.get(shuffle_hash, partition_id)
                if batch:
                    agent_c_rows += batch.num_rows

        # Agent D fetches
        for partition_id in [2, 3]:
            for shuffle_id in ["A", "B"]:
                shuffle_hash = hash(f"shuffle_{shuffle_id}")
                batch = store.get(shuffle_hash, partition_id)
                if batch:
                    agent_d_rows += batch.num_rows

        total_rows = agent_c_rows + agent_d_rows
        print(f"\nShuffle by key topology:")
        print(f"  Agent C received: {agent_c_rows} rows (partitions 0,1)")
        print(f"  Agent D received: {agent_d_rows} rows (partitions 2,3)")
        print(f"  Total: {total_rows} rows")

        assert total_rows == 200  # All data received


# ============================================================================
# Agent Coordination Tests
# ============================================================================

class TestAgentCoordination:
    """Test agent coordination and synchronization."""

    def test_barrier_coordination(self, event_schema, create_event_batch):
        """
        Test: Multiple agents coordinate via barriers.

        All agents must complete before next stage proceeds.
        """
        num_agents = 4
        barrier = threading.Barrier(num_agents)
        results = []

        def agent_task(agent_id):
            """Simulated agent processing."""
            # Create and process batch
            batch = create_event_batch(start_id=agent_id * 100, num_events=50)

            # Simulate processing
            time.sleep(0.01 * agent_id)  # Variable processing time

            # Wait at barrier (coordinate)
            barrier.wait()

            # All agents synchronized - continue
            results.append({
                'agent_id': agent_id,
                'rows_processed': batch.num_rows
            })

        # Run agents concurrently
        threads = []
        for agent_id in range(num_agents):
            thread = threading.Thread(target=agent_task, args=(agent_id,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all agents completed
        assert len(results) == num_agents
        total_rows = sum(r['rows_processed'] for r in results)
        assert total_rows == num_agents * 50

        print(f"Barrier coordination: {num_agents} agents synchronized, {total_rows} total rows processed")

    def test_concurrent_agent_execution(self, event_schema, create_event_batch):
        """Test many agents executing concurrently."""
        num_agents = 16
        store = AtomicPartitionStore(size=2048)

        def agent_worker(agent_id):
            """Agent processes and stores results."""
            batch = create_event_batch(start_id=agent_id * 1000, num_events=100)

            # Simulate some processing
            value_col = batch.column('value')
            processed = pa.compute.multiply(value_col, pa.scalar(1.1))
            output_batch = batch.set_column(4, 'value', processed)

            # Store result (lock-free)
            shuffle_hash = agent_id
            store.insert(shuffle_hash, 0, output_batch)

            return agent_id

        # Execute agents concurrently
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=num_agents) as executor:
            futures = [executor.submit(agent_worker, i) for i in range(num_agents)]
            completed_agents = [f.result() for f in futures]

        elapsed = time.perf_counter() - start

        # Verify all agents completed
        assert len(completed_agents) == num_agents

        # Verify all results stored
        for agent_id in range(num_agents):
            batch = store.get(agent_id, 0)
            assert batch is not None
            assert batch.num_rows == 100

        throughput = num_agents / elapsed
        print(f"\nConcurrent execution: {num_agents} agents in {elapsed*1000:.1f}ms ({throughput:.0f} agents/sec)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
