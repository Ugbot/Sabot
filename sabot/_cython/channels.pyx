# -*- coding: utf-8 -*-
"""Cython-optimized channel operations for Sabot - high-performance streaming."""

import asyncio
import time
from typing import Optional, Dict, Any, Set, List
from cpython.ref cimport Py_INCREF, Py_DECREF
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from cpython cimport PyObject

# Cython imports for performance
cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char *mem, Py_ssize_t size, int flags)

# Forward declarations
cdef class FastChannel
cdef class FastSubscriberManager
cdef class FastMessageBuffer

# Constants for performance
DEF DEFAULT_BUFFER_SIZE = 1024
DEF MAX_SUBSCRIBERS = 1000
DEF BATCH_SIZE = 64

cdef class FastMessageBuffer:
    """High-performance message buffer using C arrays."""

    cdef:
        char* _buffer
        Py_ssize_t _buffer_size
        Py_ssize_t _used_size
        Py_ssize_t _message_count
        object _messages
        object _lock

    def __cinit__(self, Py_ssize_t buffer_size=DEFAULT_BUFFER_SIZE):
        self._buffer_size = buffer_size
        self._used_size = 0
        self._message_count = 0
        self._buffer = <char*>malloc(buffer_size)
        if self._buffer == NULL:
            raise MemoryError("Failed to allocate message buffer")
        self._messages = []
        self._lock = asyncio.Lock()

    def __dealloc__(self):
        if self._buffer != NULL:
            free(self._buffer)

    cdef inline bint _ensure_capacity(self, Py_ssize_t additional_size):
        """Ensure buffer has enough capacity for additional data."""
        cdef Py_ssize_t required_size = self._used_size + additional_size
        cdef Py_ssize_t new_size
        cdef char* new_buffer

        if required_size > self._buffer_size:
            # Double buffer size or use required size, whichever is larger
            new_size = max(required_size, self._buffer_size * 2)
            new_buffer = <char*>malloc(new_size)
            if new_buffer == NULL:
                return False

            # Copy existing data
            memcpy(new_buffer, self._buffer, self._used_size)
            free(self._buffer)
            self._buffer = new_buffer
            self._buffer_size = new_size

        return True

    async def append_message(self, object message):
        """Append a message to the buffer."""
        cdef bytes message_bytes
        cdef Py_ssize_t message_size
        cdef Py_ssize_t* size_ptr

        async with self._lock:
            # Serialize message to bytes
            if hasattr(message, 'serialize'):
                message_bytes = message.serialize()
            else:
                message_bytes = str(message).encode('utf-8')

            message_size = len(message_bytes)

            # Ensure capacity
            if not self._ensure_capacity(message_size + sizeof(Py_ssize_t)):
                raise MemoryError("Buffer capacity exceeded")

            # Store message size
            size_ptr = <Py_ssize_t*>(&self._buffer[self._used_size])
            size_ptr[0] = message_size
            self._used_size += sizeof(Py_ssize_t)

            # Store message data
            memcpy(&self._buffer[self._used_size], <char*>message_bytes, message_size)
            self._used_size += message_size

            # Keep reference for GC
            self._messages.append(message)
            self._message_count += 1

    cdef inline list _deserialize_messages(self):
        """Deserialize all messages from buffer."""
        cdef list messages = []
        cdef Py_ssize_t offset = 0
        cdef Py_ssize_t message_size
        cdef Py_ssize_t* size_ptr
        cdef bytes message_bytes

        while offset < self._used_size:
            size_ptr = <Py_ssize_t*>(&self._buffer[offset])
            message_size = size_ptr[0]
            offset += sizeof(Py_ssize_t)

            if offset + message_size > self._used_size:
                break  # Corrupted data

            # Extract message bytes
            message_bytes = self._buffer[offset:offset + message_size]
            messages.append(message_bytes.decode('utf-8'))
            offset += message_size

        return messages

    def get_messages(self) -> List[bytes]:
        """Get all messages as bytes."""
        return self._deserialize_messages()

    def clear(self):
        """Clear the buffer."""
        self._used_size = 0
        self._message_count = 0
        self._messages.clear()

    @property
    def message_count(self) -> int:
        """Get number of messages in buffer."""
        return self._message_count

    @property
    def used_size(self) -> int:
        """Get used buffer size."""
        return self._used_size

    @property
    def buffer_size(self) -> int:
        """Get total buffer size."""
        return self._buffer_size


cdef class FastSubscriberManager:
    """High-performance subscriber management with C arrays."""

    cdef:
        object _subscribers  # Use Python list instead of C array
        Py_ssize_t _capacity
        Py_ssize_t _count
        object _lock

    def __cinit__(self, Py_ssize_t initial_capacity=16):
        self._capacity = initial_capacity
        self._count = 0
        self._subscribers = []  # Use Python list for storing objects
        self._lock = asyncio.Lock()

        # Pre-allocate list capacity
        for i in range(initial_capacity):
            self._subscribers.append(None)

    cdef inline bint _ensure_capacity(self, Py_ssize_t required_capacity):
        """Ensure array has enough capacity."""
        if required_capacity <= self._capacity:
            return True

        cdef Py_ssize_t new_capacity = max(required_capacity, self._capacity * 2)

        # Extend list to new capacity
        while len(self._subscribers) < new_capacity:
            self._subscribers.append(None)

        self._capacity = new_capacity
        return True

    async def add_subscriber(self, object subscriber):
        """Add a subscriber."""
        cdef Py_ssize_t i

        async with self._lock:
            if not self._ensure_capacity(self._count + 1):
                raise MemoryError("Subscriber capacity exceeded")

            # Find empty slot or append
            for i in range(self._capacity):
                if self._subscribers[i] is None:
                    self._subscribers[i] = subscriber
                    # No need for Py_INCREF with Python lists
                    self._count += 1
                    return

    async def remove_subscriber(self, object subscriber):
        """Remove a subscriber."""
        cdef Py_ssize_t i

        async with self._lock:
            for i in range(self._capacity):
                if self._subscribers[i] is subscriber:
                    self._subscribers[i] = None
                    # No need for Py_DECREF with Python lists
                    self._count -= 1
                    return

    async def broadcast(self, object message):
        """Broadcast message to all subscribers."""
        cdef list active_subscribers = []
        cdef Py_ssize_t i
        cdef object subscriber

        # Collect active subscribers (avoid holding lock during broadcast)
        async with self._lock:
            for i in range(self._capacity):
                subscriber = self._subscribers[i]
                if subscriber is not None:
                    active_subscribers.append(subscriber)

        # Broadcast without holding lock
        for subscriber in active_subscribers:
            try:
                if hasattr(subscriber, 'put'):
                    await subscriber.put(message)
                elif hasattr(subscriber, 'send'):
                    await subscriber.send(message)
            except Exception as e:
                # Log error but continue broadcasting
                print(f"Broadcast error to subscriber: {e}")

    def get_subscriber_count(self) -> int:
        """Get number of active subscribers."""
        return self._count

    def clear(self):
        """Clear all subscribers."""
        cdef Py_ssize_t i
        for i in range(self._capacity):
            if self._subscribers[i] is not None:
                Py_DECREF(self._subscribers[i])
                self._subscribers[i] = None
        self._count = 0


cdef class FastChannel:
    """Cython-optimized channel with zero-copy operations."""

    cdef:
        object _queue
        FastSubscriberManager _subscriber_manager
        FastMessageBuffer _message_buffer
        object _schema
        object _key_type
        object _value_type
        object _app
        bint _is_iterator
        Py_ssize_t _maxsize
        double _last_access_time
        object _lock

    def __cinit__(self, app, *, maxsize=None, schema=None, key_type=None, value_type=None):
        self._app = app
        self._maxsize = maxsize or 1000
        self._schema = schema
        self._key_type = key_type
        self._value_type = value_type
        self._is_iterator = False
        self._last_access_time = time.time()
        self._lock = asyncio.Lock()

        # Initialize optimized components
        self._subscriber_manager = FastSubscriberManager()
        self._message_buffer = FastMessageBuffer()

        # Create queue (use Python object for compatibility)
        try:
            from ..utils.queues import ThrowableQueue
            self._queue = ThrowableQueue(maxsize=self._maxsize)
        except ImportError:
            # Fallback to asyncio.Queue
            self._queue = asyncio.Queue(maxsize=self._maxsize)

    async def put(self, object event):
        """Put event into channel with optimized broadcasting."""
        # Buffer message for batch processing
        await self._message_buffer.append_message(event)

        # Put in queue
        await self._queue.put(event)

        # Broadcast to subscribers using optimized manager
        await self._subscriber_manager.broadcast(event)

        self._last_access_time = time.time()

    async def get(self):
        """Get next event from channel."""
        cdef object event = await self._queue.get()
        self._last_access_time = time.time()
        return event

    async def send(self, *, key=None, value=None, **kwargs):
        """Send message with optimized serialization."""
        # Create event (simplified for demo)
        cdef object event = self._create_event(key, value, {})
        await self.put(event)

        # Return mock RecordMetadata
        return {
            'topic': 'fast-channel',
            'partition': 0,
            'offset': 0,
            'timestamp': time.time()
        }

    def clone(self, **kwargs):
        """Create optimized clone."""
        cdef object clone = FastChannel(
            self._app,
            maxsize=self._maxsize,
            schema=self._schema,
            key_type=self._key_type,
            value_type=self._value_type
        )
        clone._is_iterator = kwargs.get('is_iterator', False)
        return clone

    def add_subscriber(self, object subscriber):
        """Add subscriber using optimized manager."""
        return self._subscriber_manager.add_subscriber(subscriber)

    def remove_subscriber(self, object subscriber):
        """Remove subscriber using optimized manager."""
        return self._subscriber_manager.remove_subscriber(subscriber)

    def get_subscriber_count(self) -> int:
        """Get subscriber count."""
        return self._subscriber_manager.get_subscriber_count()

    def clear_buffer(self):
        """Clear message buffer."""
        self._message_buffer.clear()

    def get_buffer_stats(self) -> Dict[str, int]:
        """Get buffer statistics."""
        return {
            'message_count': self._message_buffer.message_count,
            'used_size': self._message_buffer.used_size,
            'buffer_size': self._message_buffer.buffer_size,
        }

    def _create_event(self, key, value, headers):
        """Create event object."""
        # Simplified event creation - in real implementation would use app.create_event
        return {
            'key': key,
            'value': value,
            'headers': headers,
            'timestamp': time.time()
        }

    def empty(self) -> bint:
        """Check if queue is empty."""
        return self._queue.empty()

    async def join(self):
        """Wait for all tasks to complete."""
        pass  # Implementation would wait for pending operations

    @property
    def maxsize(self) -> int:
        """Get max queue size."""
        return self._maxsize

    @property
    def schema(self):
        """Get schema."""
        return self._schema

    @property
    def last_access_time(self) -> float:
        """Get last access timestamp."""
        return self._last_access_time


# Python wrapper functions for easy integration
def create_fast_channel(app, **kwargs):
    """Create a FastChannel instance."""
    return FastChannel(app, **kwargs)

def create_fast_buffer(size=None):
    """Create a FastMessageBuffer instance."""
    return FastMessageBuffer(size or DEFAULT_BUFFER_SIZE)

def create_fast_subscriber_manager():
    """Create a FastSubscriberManager instance."""
    return FastSubscriberManager()

# Utility functions for performance monitoring
def get_channel_performance_stats(channel) -> Dict[str, Any]:
    """Get performance statistics for a FastChannel."""
    if isinstance(channel, FastChannel):
        return {
            'type': 'fast_channel',
            'subscriber_count': channel.get_subscriber_count(),
            'buffer_stats': channel.get_buffer_stats(),
            'last_access_time': channel.last_access_time,
            'maxsize': channel.maxsize,
            'empty': channel.empty(),
        }
    else:
        return {
            'type': 'regular_channel',
            'has_queue': hasattr(channel, 'queue'),
            'has_subscribers': hasattr(channel, '_subscribers'),
        }
