# cython: language_level=3
"""Sabot agents - Cython implementation inspired by Faust agents."""

import asyncio
import time
from typing import Any, Dict, List, Optional, Union, Callable, AsyncGenerator
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from cpython.ref cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize

# Import Python modules dynamically
cdef object asyncio_module = None
cdef object logging_module = None

def _import_modules():
    """Import required modules at runtime."""
    global asyncio_module, logging_module
    if asyncio_module is None:
        import asyncio
        import logging
        asyncio_module = asyncio
        logging_module = logging


cdef class FastArrowAgent:
    """Fast Arrow agent inspired by Faust agents - supports both per-record and per-batch processing."""

    cdef:
        str _name
        object _func  # Agent function (async generator)
        object _app
        str _topic_name
        int _concurrency
        bint _isolated_partitions
        list _sinks
        dict _agent_stats
        object _supervisor
        bint _running
        object _stream_source

    def __cinit__(self, str name, object func, object app, str topic_name,
                  int concurrency=1, bint isolated_partitions=False):
        self._name = name
        self._func = func
        self._app = app
        self._topic_name = topic_name
        self._concurrency = concurrency
        self._isolated_partitions = isolated_partitions
        self._sinks = []
        self._running = False
        self._stream_source = None

        self._agent_stats = {
            'messages_processed': 0,
            'batches_processed': 0,
            'errors': 0,
            'start_time': 0.0,
            'total_processing_time': 0.0
        }

    cpdef str get_name(self):
        """Get agent name."""
        return self._name

    cpdef void add_sink(self, object sink):
        """Add a sink (like Faust)."""
        self._sinks.append(sink)

    cpdef void remove_sink(self, object sink):
        """Remove a sink."""
        if sink in self._sinks:
            self._sinks.remove(sink)

    cpdef list get_sinks(self):
        """Get all sinks."""
        return self._sinks[:]

    cpdef void start(self):
        """Start the agent."""
        if self._running:
            return

        _import_modules()
        self._running = True
        self._agent_stats['start_time'] = time.time()

        # Initialize stream source (would connect to Kafka, etc.)
        self._initialize_stream_source()

        logging_module.info(f"Started Arrow agent {self._name} with concurrency {self._concurrency}")

    cpdef void stop(self):
        """Stop the agent."""
        if not self._running:
            return

        self._running = False

        # Clean up stream source
        if self._stream_source:
            asyncio_module.run_until_complete(self._stream_source.close())

        logging_module.info(f"Stopped Arrow agent {self._name}")

    cdef void _initialize_stream_source(self):
        """Initialize the stream source for this agent."""
        # This would connect to Kafka, Arrow Flight, etc.
        # For now, create a mock stream source
        self._stream_source = MockStreamSource(self._topic_name)

    async def process_stream(self):
        """Main stream processing loop (Faust agent pattern)."""
        if not self._running:
            return

        cdef double batch_start
        cdef double batch_time

        try:
            async for batch in self._stream_source:
                self._agent_stats['batches_processed'] += 1

                batch_start = time.time()

                # Call the agent function (async generator pattern from Faust)
                async for result in self._func(batch):
                    self._agent_stats['messages_processed'] += 1

                    # Send result to sinks (Faust pattern)
                    await self._send_to_sinks(result)

                batch_time = time.time() - batch_start
                self._agent_stats['total_processing_time'] += batch_time

        except Exception as e:
            self._agent_stats['errors'] += 1
            logging_module.error(f"Agent {self._name} processing error: {e}")
            raise

    async def _send_to_sinks(self, object result):
        """Send result to all sinks (Faust pattern)."""
        for sink in self._sinks:
            try:
                if asyncio_module.iscoroutinefunction(sink):
                    await sink(result)
                elif callable(sink):
                    # Assume it's a sync callable, run in thread pool
                    loop = asyncio_module.get_event_loop()
                    await loop.run_in_executor(None, sink, result)
                elif hasattr(sink, 'send'):
                    # Another agent or topic
                    await sink.send(result)
                elif isinstance(sink, str):
                    # Topic name - would send to Kafka
                    await self._send_to_topic(sink, result)
                else:
                    logging_module.warning(f"Unknown sink type: {type(sink)}")
            except Exception as e:
                logging_module.error(f"Error sending to sink {sink}: {e}")

    async def _send_to_topic(self, str topic_name, object result):
        """Send result to a topic."""
        # This would integrate with Kafka producer
        logging_module.debug(f"Would send {result} to topic {topic_name}")

    cpdef dict get_stats(self):
        """Get agent statistics."""
        stats = dict(self._agent_stats)
        if stats['start_time'] > 0:
            stats['uptime'] = time.time() - stats['start_time']
            if stats['messages_processed'] > 0:
                stats['avg_processing_time'] = stats['total_processing_time'] / stats['messages_processed']
        return stats

    cpdef bint is_running(self):
        """Check if agent is running."""
        return self._running


cdef class FastArrowAgentManager:
    """Agent manager with supervision (inspired by Faust)."""

    cdef:
        object _app
        dict _agents
        dict _supervisors
        dict _manager_stats

    def __cinit__(self, object app):
        self._app = app
        self._agents = {}
        self._supervisors = {}
        self._manager_stats = {
            'agents_created': 0,
            'agents_running': 0,
            'total_messages_processed': 0,
            'total_errors': 0
        }

    cpdef str add_agent(self, str name, object func, str topic_name,
                       int concurrency=1, bint isolated_partitions=False):
        """Add an agent with supervision."""
        agent = FastArrowAgent(name, func, self._app, topic_name,
                              concurrency, isolated_partitions)

        # Create supervisor (inspired by Faust)
        supervisor = CrashingSupervisor(agent, max_restarts=100.0, over=1.0)

        self._agents[name] = agent
        self._supervisors[name] = supervisor
        self._manager_stats['agents_created'] += 1

        return name

    cpdef void start_agent(self, str name):
        """Start an agent."""
        if name in self._agents:
            agent = self._agents[name]
            supervisor = self._supervisors[name]

            agent.start()

            # Start supervision
            asyncio_module.create_task(self._supervise_agent(name))

            self._manager_stats['agents_running'] += 1

    cpdef void stop_agent(self, str name):
        """Stop an agent."""
        if name in self._agents:
            self._agents[name].stop()
            self._manager_stats['agents_running'] -= 1

    async def _supervise_agent(self, str name):
        """Supervise an agent (Faust-inspired)."""
        agent = self._agents[name]
        supervisor = self._supervisors[name]

        while agent.is_running():
            try:
                # Run the agent
                await agent.process_stream()
            except Exception as e:
                logging_module.error(f"Agent {name} crashed: {e}")

                # Supervisor logic
                if supervisor.should_restart():
                    logging_module.info(f"Restarting agent {name}")
                    agent.start()
                    await asyncio_module.sleep(1.0)  # Backoff
                else:
                    logging_module.error(f"Agent {name} exceeded max restarts")
                    break

    cpdef void start_all_agents(self):
        """Start all agents."""
        for name in self._agents.keys():
            self.start_agent(name)

    cpdef void stop_all_agents(self):
        """Stop all agents."""
        for name in self._agents.keys():
            self.stop_agent(name)

    cpdef dict get_agent_stats(self, str name=None):
        """Get agent statistics."""
        if name:
            return self._agents.get(name, {}).get_stats() if name in self._agents else {}
        else:
            return {name: agent.get_stats() for name, agent in self._agents.items()}

    cpdef dict get_manager_stats(self):
        """Get manager statistics."""
        stats = dict(self._manager_stats)

        # Aggregate stats from all agents
        total_messages = 0
        total_errors = 0

        for agent in self._agents.values():
            agent_stats = agent.get_stats()
            total_messages += agent_stats.get('messages_processed', 0)
            total_errors += agent_stats.get('errors', 0)

        stats['total_messages_processed'] = total_messages
        stats['total_errors'] = total_errors

        return stats


cdef class CrashingSupervisor:
    """Supervisor strategy for agent fault tolerance (Faust-inspired)."""

    cdef:
        object _service
        double _max_restarts
        double _over_time
        int _restarts
        double _last_restart

    def __cinit__(self, object service, double max_restarts=100.0, double over_time=1.0):
        self._service = service
        self._max_restarts = max_restarts
        self._over_time = over_time
        self._restarts = 0
        self._last_restart = 0.0

    cpdef bint should_restart(self):
        """Check if service should be restarted."""
        cdef double current_time = time.time()

        # Reset counter if outside time window
        if current_time - self._last_restart > self._over_time:
            self._restarts = 0

        self._restarts += 1
        self._last_restart = current_time

        return self._restarts <= self._max_restarts


cdef class MockStreamSource:
    """Mock stream source for testing (would be replaced with real Kafka/Arrow Flight sources)."""

    cdef:
        str _topic_name
        int _batch_count

    def __cinit__(self, str topic_name):
        self._topic_name = topic_name
        self._batch_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        """Generate mock Arrow batches."""
        if self._batch_count >= 10:  # Stop after 10 batches
            raise StopAsyncIteration

        self._batch_count += 1

        # Create mock Arrow batch
        try:
            import pyarrow as pa

            data = {
                'id': list(range(self._batch_count * 10, (self._batch_count + 1) * 10)),
                'value': [i * 1.5 for i in range(self._batch_count * 10, (self._batch_count + 1) * 10)],
                'topic': [self._topic_name] * 10
            }

            table = pa.table(data)
            batch = pa.RecordBatch.from_struct_array(table.to_struct_array())

            # Simulate async delay
            await asyncio_module.sleep(0.1)

            return batch

        except ImportError:
            # Fallback if Arrow not available
            await asyncio_module.sleep(0.1)
            return []

    async def close(self):
        """Close the stream source."""
        pass


# Global instances
cdef FastArrowAgentManager _agent_manager = None

# Convenience functions
cpdef FastArrowAgent create_agent(str name, object func, object app, str topic_name,
                                 int concurrency=1, bint isolated_partitions=False):
    """Create a new Arrow agent."""
    return FastArrowAgent(name, func, app, topic_name, concurrency, isolated_partitions)

cpdef FastArrowAgentManager get_agent_manager(object app):
    """Get or create agent manager for app."""
    global _agent_manager
    if _agent_manager is None:
        _agent_manager = FastArrowAgentManager(app)
    return _agent_manager

cpdef void register_agent(object app, str name, object func, str topic_name,
                         int concurrency=1):
    """Register an agent with the manager."""
    manager = get_agent_manager(app)
    manager.add_agent(name, func, topic_name, concurrency)

cpdef void start_agents(object app):
    """Start all registered agents."""
    manager = get_agent_manager(app)
    manager.start_all_agents()

cpdef dict get_agent_stats(object app, str agent_name=None):
    """Get agent statistics."""
    manager = get_agent_manager(app)
    return manager.get_agent_stats(agent_name)
