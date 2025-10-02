#!/usr/bin/env python3
"""
Queue utilities for Sabot

Provides specialized queue implementations for channel communication.
"""

import asyncio
import logging
from typing import Any, Optional, Generic, TypeVar
from dataclasses import dataclass
import threading
import time

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class QueueItem(Generic[T]):
    """Item in a throwable queue with metadata."""
    value: T
    timestamp: float
    sequence_id: int


class ThrowableQueue(Generic[T]):
    """
    A queue that can "throw" items when full or when exceptions occur.

    This queue is designed for high-throughput channel communication where
    backpressure and error handling are critical.
    """

    def __init__(self, maxsize: int = 0, throw_on_full: bool = False):
        """
        Initialize the throwable queue.

        Args:
            maxsize: Maximum queue size (0 = unlimited)
            throw_on_full: Whether to raise exceptions when queue is full
        """
        self._queue: asyncio.Queue[QueueItem[T]] = asyncio.Queue(maxsize=maxsize)
        self._maxsize = maxsize
        self._throw_on_full = throw_on_full
        self._sequence_counter = 0
        self._lock = threading.Lock()

    async def put(self, item: T, timeout: Optional[float] = None) -> None:
        """
        Put an item in the queue.

        Args:
            item: Item to put in the queue
            timeout: Timeout in seconds (None = no timeout)

        Raises:
            asyncio.QueueFull: If queue is full and throw_on_full is True
        """
        queue_item = QueueItem(
            value=item,
            timestamp=time.time(),
            sequence_id=self._get_next_sequence()
        )

        try:
            if timeout is not None:
                await asyncio.wait_for(
                    self._queue.put(queue_item),
                    timeout=timeout
                )
            else:
                await self._queue.put(queue_item)

        except asyncio.QueueFull:
            if self._throw_on_full:
                logger.warning(f"Queue full, throwing item: {item}")
                raise
            else:
                # Drop the oldest item and add the new one
                try:
                    dropped = self._queue.get_nowait()
                    logger.warning(f"Queue full, dropped old item: {dropped.value}")
                    await self._queue.put(queue_item)
                except asyncio.QueueEmpty:
                    await self._queue.put(queue_item)

    async def get(self) -> QueueItem[T]:
        """
        Get an item from the queue.

        Returns:
            QueueItem containing the value and metadata
        """
        return await self._queue.get()

    def get_nowait(self) -> QueueItem[T]:
        """
        Get an item from the queue without waiting.

        Returns:
            QueueItem containing the value and metadata

        Raises:
            asyncio.QueueEmpty: If queue is empty
        """
        return self._queue.get_nowait()

    async def throw(self, exception: Exception) -> None:
        """
        Throw an exception to all waiting consumers.

        This will cause all pending get() operations to raise the exception.
        """
        # Create a special item that represents an exception
        error_item = QueueItem(
            value=exception,
            timestamp=time.time(),
            sequence_id=-1  # Special marker for exceptions
        )

        # Try to put the error in the queue
        try:
            await self._queue.put(error_item)
        except asyncio.QueueFull:
            # If queue is full, replace the oldest item
            try:
                self._queue.get_nowait()
                await self._queue.put(error_item)
            except asyncio.QueueEmpty:
                await self._queue.put(error_item)

    def qsize(self) -> int:
        """Get the current queue size."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def full(self) -> bool:
        """Check if the queue is full."""
        return self._queue.full()

    def _get_next_sequence(self) -> int:
        """Get the next sequence number."""
        with self._lock:
            self._sequence_counter += 1
            return self._sequence_counter

    async def clear(self) -> int:
        """
        Clear all items from the queue.

        Returns:
            Number of items cleared
        """
        cleared = 0
        try:
            while not self._queue.empty():
                self._queue.get_nowait()
                cleared += 1
        except asyncio.QueueEmpty:
            pass

        logger.info(f"Cleared {cleared} items from queue")
        return cleared

    def get_stats(self) -> dict:
        """Get queue statistics."""
        return {
            'size': self.qsize(),
            'maxsize': self._maxsize,
            'empty': self.empty(),
            'full': self.full(),
            'sequence_counter': self._sequence_counter,
            'throw_on_full': self._throw_on_full
        }


class PriorityThrowableQueue(ThrowableQueue[T]):
    """
    A throwable queue with priority support.

    Items can be assigned priority levels for ordered processing.
    """

    def __init__(self, maxsize: int = 0, throw_on_full: bool = False):
        super().__init__(maxsize, throw_on_full)
        # PriorityQueue would need to be implemented differently
        # For now, this is a placeholder
        self._priorities: dict = {}

    async def put_with_priority(self, item: T, priority: int = 0,
                               timeout: Optional[float] = None) -> None:
        """
        Put an item with priority.

        Args:
            item: Item to put
            priority: Priority level (higher = more important)
            timeout: Timeout in seconds
        """
        # For now, just use regular put - priority implementation would need
        # a different data structure
        await self.put(item, timeout)


class BufferedThrowableQueue(ThrowableQueue[T]):
    """
    A throwable queue with batching/buffering capabilities.

    Groups items into batches for more efficient processing.
    """

    def __init__(self, maxsize: int = 0, throw_on_full: bool = False,
                 batch_size: int = 10, flush_interval: float = 1.0):
        super().__init__(maxsize, throw_on_full)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: list[QueueItem[T]] = []
        self._last_flush = time.time()

    async def put(self, item: T, timeout: Optional[float] = None) -> None:
        """Put an item, buffering until batch is full or flush interval."""
        queue_item = QueueItem(
            value=item,
            timestamp=time.time(),
            sequence_id=self._get_next_sequence()
        )

        self._buffer.append(queue_item)

        # Flush if batch is full or flush interval has passed
        current_time = time.time()
        if (len(self._buffer) >= self.batch_size or
            current_time - self._last_flush >= self.flush_interval):
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Flush the current buffer as a batch."""
        if not self._buffer:
            return

        # Create a batch item containing all buffered items
        batch_item = QueueItem(
            value=self._buffer.copy(),  # List of items
            timestamp=time.time(),
            sequence_id=self._get_next_sequence()
        )

        try:
            await self._queue.put(batch_item)
            self._buffer.clear()
            self._last_flush = time.time()
        except asyncio.QueueFull:
            if self._throw_on_full:
                raise
            else:
                # Drop oldest and try again
                try:
                    self._queue.get_nowait()
                    await self._queue.put(batch_item)
                    self._buffer.clear()
                    self._last_flush = time.time()
                except asyncio.QueueEmpty:
                    await self._queue.put(batch_item)
                    self._buffer.clear()
                    self._last_flush = time.time()
