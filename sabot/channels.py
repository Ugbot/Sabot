# -*- coding: utf-8 -*-
"""Channel system for Sabot - Arrow-native data streaming with Faust-inspired architecture."""

import asyncio
import typing
from abc import ABC, abstractmethod
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Dict, Generic, Mapping,
    Optional, Set, Tuple, TypeVar, Union, no_type_check
)
from weakref import WeakSet

from .types import (
    AppT, ChannelT, CodecArg, EventT, FutureMessage, HeadersArg,
    K, Message, MessageSentCallback, ModelArg, OpenHeadersArg,
    PendingMessage, RecordMetadata, SchemaT, StreamT, TP, V,
    prepare_headers, _PendingMessage_to_Message
)
from .utils.queues import ThrowableQueue

__all__ = ["Channel", "ChannelT", "SerializedChannel"]

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)


class Channel(ChannelT[T]):
    """Arrow-native channel for high-performance streaming.

    Channels provide the data source for streams in Sabot's agent architecture.
    They support Arrow RecordBatch buffering, subscriber patterns, and can be
    backed by various transports (memory, Kafka, Redis, etc.).

    Key features:
    - Arrow RecordBatch buffering for zero-copy operations
    - Subscriber pattern for multi-consumer scenarios
    - Async iteration with iterator isolation
    - Configurable queue sizes with flow control
    - Schema-based serialization/deserialization
    - Error handling for decode failures
    """

    app: AppT
    schema: SchemaT
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    is_iterator: bool
    maxsize: Optional[int]
    active_partitions: Optional[Set[TP]]

    def __init__(
        self,
        app: AppT,
        *,
        schema: Optional[SchemaT] = None,
        key_type: ModelArg = None,
        value_type: ModelArg = None,
        is_iterator: bool = False,
        queue: Optional[ThrowableQueue] = None,
        maxsize: Optional[int] = None,
        root: Optional[ChannelT] = None,
        active_partitions: Optional[Set[TP]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.app = app
        self.loop = loop or asyncio.get_event_loop()
        self.is_iterator = is_iterator
        self.maxsize = maxsize
        self._queue = queue
        self._root = root or self  # type: ignore
        self.active_partitions = active_partitions
        self._subscribers: WeakSet[Channel] = WeakSet()

        # Arrow-specific initialization
        self._arrow_schema = None
        self._record_batch_builder = None

        # Schema setup
        if schema is None:
            self.schema = self._get_default_schema(key_type, value_type)
        else:
            self.schema = schema
            self.schema.update(key_type=key_type, value_type=value_type)

        self.key_type = self.schema.key_type
        self.value_type = self.schema.value_type

        # Compile deliver method for performance
        self.deliver = self._compile_deliver()

    def _get_default_schema(
        self, key_type: ModelArg = None, value_type: ModelArg = None
    ) -> SchemaT:
        """Create default schema for channel."""
        return self.app.conf.Schema(
            key_type=key_type,
            value_type=value_type,
        )

    @property
    def queue(self) -> ThrowableQueue:
        """Return the underlying queue/buffer backing this channel."""
        if self._queue is None:
            # Initialize queue with proper size
            maxsize = self.maxsize
            if maxsize is None:
                maxsize = self.app.conf.stream_buffer_maxsize

            # Use Sabot's FlowControlQueue for better performance
            self._queue = self.app.FlowControlQueue(
                maxsize=maxsize,
                clear_on_resume=True,
            )
        return self._queue

    def clone(
        self, *, is_iterator: Optional[bool] = None, **kwargs: Any
    ) -> ChannelT[T]:
        """Create clone of this channel.

        Arguments:
            is_iterator: Set to True if this is now a channel
                that is being iterated over.
            **kwargs: Any keyword arguments passed will override
                arguments from __init__.
        """
        is_it = is_iterator if is_iterator is not None else self.is_iterator
        subchannel = self._clone(is_iterator=is_it, **kwargs)

        if is_it:
            # Add to subscribers for broadcasting
            (self._root or self)._subscribers.add(subchannel)  # type: ignore

        # Ensure queue is initialized
        subchannel.queue  # Access property to initialize
        return subchannel

    def clone_using_queue(self, queue: asyncio.Queue) -> ChannelT[T]:
        """Create clone of this channel using specific queue instance."""
        return self.clone(queue=queue, is_iterator=True)

    def _clone(self, **kwargs: Any) -> ChannelT[T]:
        """Internal clone implementation."""
        return type(self)(**{**self._clone_args(), **kwargs})

    def _clone_args(self) -> Mapping[str, Any]:
        """Return arguments needed to clone this channel."""
        return {
            "app": self.app,
            "loop": self.loop,
            "schema": self.schema,
            "key_type": self.key_type,
            "value_type": self.value_type,
            "maxsize": self.maxsize,
            "root": self._root or self,
            "queue": None,  # Always create new queue for clones
            "active_partitions": self.active_partitions,
        }

    def stream(self, **kwargs: Any) -> Any:
        """Create stream reading from this channel."""
        return self.app.stream(self, **kwargs)

    def get_topic_name(self) -> str:
        """Get the topic name, or raise if this is not a named channel."""
        raise NotImplementedError("Channels are unnamed topics - use Topic class")

    async def send(
        self,
        *,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]:
        """Send message to channel."""
        return await self._send_now(
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            schema=schema,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    def send_soon(
        self,
        *,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
        eager_partitioning: bool = False,
    ) -> FutureMessage:
        """Send message by adding to buffer (not supported for in-memory channels)."""
        raise NotImplementedError("Use send() for in-memory channels")

    def as_future_message(
        self,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        eager_partitioning: bool = False,
    ) -> FutureMessage:
        """Create promise that message will be transmitted."""
        open_headers = self.prepare_headers(headers)
        final_key, open_headers = self.prepare_key(
            key, key_serializer, schema, open_headers
        )
        final_value, open_headers = self.prepare_value(
            value, value_serializer, schema, open_headers
        )

        return FutureMessage(
            PendingMessage(
                self,
                final_key,
                final_value,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                partition=partition,
                timestamp=timestamp,
                headers=open_headers,
                callback=callback,
                topic=None,
                offset=None,
                generation_id=self.app.consumer_generation_id,
            ),
        )

    def prepare_headers(self, headers: Optional[HeadersArg]) -> OpenHeadersArg:
        """Prepare headers passed before publishing."""
        if headers is not None:
            return prepare_headers(headers)
        return {}

    async def _send_now(
        self,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
    ) -> Awaitable[RecordMetadata]:
        """Send message to channel immediately."""
        return await self.publish_message(
            self.as_future_message(
                key=key,
                value=value,
                partition=partition,
                timestamp=timestamp,
                headers=headers,
                schema=schema,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                callback=callback,
            )
        )

    async def publish_message(
        self, fut: FutureMessage, wait: bool = True
    ) -> Awaitable[RecordMetadata]:
        """Publish message to channel."""
        event = self._future_message_to_event(fut)
        await self.put(event)

        # Create mock RecordMetadata for in-memory channels
        tp = TP("<anon>", fut.message.partition or -1)
        return RecordMetadata(
            topic=tp.topic,
            partition=tp.partition,
            topic_partition=tp,
            offset=-1,
            timestamp=fut.message.timestamp,
            timestamp_type=1,
        )

    def _future_message_to_event(self, fut: FutureMessage) -> EventT:
        """Convert FutureMessage to Event."""
        return self._create_event(
            fut.message.key,
            fut.message.value,
            fut.message.headers,
            message=_PendingMessage_to_Message(fut.message),
        )

    async def put(self, value: EventT[T_contra]) -> None:
        """Put event onto this channel and broadcast to subscribers."""
        # Put on own queue
        await self.queue.put(value)

        # Broadcast to all subscribers
        for subscriber in (self._root or self)._subscribers:
            if subscriber is not self:  # Don't double-send to self
                await subscriber.queue.put(value)

    async def get(self, *, timeout: Optional[float] = None) -> EventT[T]:
        """Get the next Event received on this channel."""
        if timeout is not None:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        return await self.queue.get()

    def empty(self) -> bool:
        """Return True if the queue is empty."""
        return self.queue.empty()

    async def maybe_declare(self) -> None:
        """Declare/create this channel, but only if it doesn't exist."""
        pass  # In-memory channels don't need declaration

    async def declare(self) -> None:
        """Declare/create this channel."""
        pass  # In-memory channels don't need declaration

    def prepare_key(
        self,
        key: K,
        key_serializer: CodecArg,
        schema: Optional[SchemaT] = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Prepare key before it is sent to this channel."""
        return key, headers or {}

    def prepare_value(
        self,
        value: V,
        value_serializer: CodecArg,
        schema: Optional[SchemaT] = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Prepare value before it is sent to this channel."""
        return value, headers or {}

    async def decode(self, message: Message, *, propagate: bool = False) -> EventT[T]:
        """Decode Message into Event."""
        return self._create_event(
            message.key, message.value, message.headers, message=message
        )

    async def deliver(self, message: Message) -> None:
        """Deliver message to queue from consumer (compiled method)."""
        pass  # This is replaced by _compile_deliver

    def _compile_deliver(self) -> Callable[[Message], Awaitable[None]]:
        """Compile optimized deliver method."""
        async def deliver(message: Message) -> None:
            event = await self.decode(message)
            await self.queue.put(event)

        return deliver

    def _create_event(
        self, key: K, value: V, headers: Optional[HeadersArg], message: Message
    ) -> EventT[T]:
        """Create Event from decoded message components."""
        return self.app.create_event(key, value, headers, message)

    async def on_key_decode_error(self, exc: Exception, message: Message) -> None:
        """Handle key decode errors."""
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_value_decode_error(self, exc: Exception, message: Message) -> None:
        """Handle value decode errors."""
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_decode_error(self, exc: Exception, message: Message) -> None:
        """Handle decode errors - override for custom error handling."""
        await self.app.log.error(
            "Channel decode error",
            exc=exc,
            message=message,
            channel=str(self)
        )

    def on_stop_iteration(self) -> None:
        """Called when iteration over this channel stops."""
        pass

    def derive(self, **kwargs: Any) -> ChannelT[T]:
        """Derive new channel from this channel with different configuration."""
        return self.clone(**kwargs)

    def __aiter__(self) -> ChannelT[T]:
        """Async iterator entry point."""
        return self if self.is_iterator else self.clone(is_iterator=True)

    async def __anext__(self) -> EventT[T]:
        """Async iterator next method."""
        if not self.is_iterator:
            raise RuntimeError("Need to call channel.__aiter__() first")
        return await self.queue.get()

    async def throw(self, exc: BaseException) -> None:
        """Throw exception to be received by channel subscribers."""
        self.queue._throw(exc)

    def _throw(self, exc: BaseException) -> None:
        """Non-async version of throw."""
        self.queue._throw(exc)

    def __repr__(self) -> str:
        """String representation of channel."""
        s = f"<{self.label}@{id(self):#x}"
        if self.active_partitions is not None and self.active_partitions:
            active = "{" + ", ".join(
                sorted(f"{tp.topic}:{tp.partition}" for tp in self.active_partitions)
            ) + "}"
            s += f" active={active}"
        s += ">"
        return s

    def __str__(self) -> str:
        """Short string representation."""
        return "<ANON>"

    @property
    def subscriber_count(self) -> int:
        """Return number of active subscribers to this channel."""
        return len((self._root or self)._subscribers)

    @property
    def label(self) -> str:
        """Short textual description of channel."""
        sym = "(*)" if self.is_iterator else ""
        return f"{sym}{type(self).__name__}: {self}"


class SerializedChannel(Channel[T]):
    """Channel with serialization support for transport-backed channels."""

    def __init__(
        self,
        app: AppT,
        *,
        schema: Optional[SchemaT] = None,
        key_type: ModelArg = None,
        value_type: ModelArg = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        allow_empty: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        self.app = app  # Set early for schema creation

        # Handle schema with serialization settings
        if schema is not None:
            self._contribute_to_schema(
                schema,
                key_type=key_type,
                value_type=value_type,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                allow_empty=allow_empty,
            )
        else:
            schema = self._get_default_schema(
                key_type, value_type, key_serializer, value_serializer, allow_empty
            )

        # Initialize parent with schema
        super().__init__(
            app,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs,
        )

        # Store serializer settings
        self.key_serializer = self.schema.key_serializer
        self.value_serializer = self.schema.value_serializer
        self.allow_empty = self.schema.allow_empty

    def _contribute_to_schema(
        self,
        schema: SchemaT,
        *,
        key_type: ModelArg = None,
        value_type: ModelArg = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        allow_empty: Optional[bool] = None,
    ) -> None:
        """Update schema with serialization settings."""
        schema.update(
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            allow_empty=allow_empty,
        )

    def _get_default_schema(
        self,
        key_type: ModelArg = None,
        value_type: ModelArg = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        allow_empty: Optional[bool] = None,
    ) -> SchemaT:
        """Create default schema with serialization."""
        return self.app.conf.Schema(
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            allow_empty=allow_empty,
        )

    def _clone_args(self) -> Mapping[str, Any]:
        """Clone arguments including serialization settings."""
        return {
            **super()._clone_args(),
            "key_serializer": self.key_serializer,
            "value_serializer": self.value_serializer,
        }

    def prepare_key(
        self,
        key: K,
        key_serializer: CodecArg,
        schema: Optional[SchemaT] = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Serialize key for transport."""
        if key is not None:
            schema = schema or self.schema
            assert schema is not None
            return schema.dumps_key(
                self.app, key, serializer=key_serializer, headers=headers or {}
            )
        return None, headers or {}

    def prepare_value(
        self,
        value: V,
        value_serializer: CodecArg,
        schema: Optional[SchemaT] = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Serialize value for transport."""
        schema = schema or self.schema
        assert schema is not None
        return schema.dumps_value(
            self.app, value, serializer=value_serializer, headers=headers or {}
        )
