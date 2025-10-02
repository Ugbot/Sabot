# -*- coding: utf-8 -*-
"""Channel type definitions for Sabot."""

import abc
import asyncio
import typing
from typing import Any, AsyncIterator, Awaitable, Callable, Generic, Optional, Set, TypeVar

from ..utils.queues import ThrowableQueue

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
    from .events import EventT as _EventT
    from .models import ModelArg as _ModelArg
    from .serializers import SchemaT as _SchemaT
    from .streams import StreamT as _StreamT
else:
    class _AppT: ...
    class _EventT(Generic[TypeVar("_T")]): ...
    class _ModelArg: ...
    class _SchemaT: ...
    class _StreamT: ...

from .codecs import CodecArg
from .core import HeadersArg, K, V
from .tuples import TP, FutureMessage, Message, MessageSentCallback, RecordMetadata

_T = TypeVar("_T")
_T_contra = TypeVar("_T_contra", contravariant=True)


class ChannelT(AsyncIterator[_EventT[_T]]):
    """Abstract channel interface for Sabot streaming."""

    app: _AppT
    schema: _SchemaT
    key_type: Optional[_ModelArg]
    value_type: Optional[_ModelArg]
    maxsize: Optional[int]
    active_partitions: Optional[Set[TP]]

    @abc.abstractmethod
    def __init__(
        self,
        app: _AppT,
        *,
        schema: Optional[_SchemaT] = None,
        key_type: _ModelArg = None,
        value_type: _ModelArg = None,
        is_iterator: bool = False,
        queue: Optional[ThrowableQueue] = None,
        maxsize: Optional[int] = None,
        root: "ChannelT" = None,
        active_partitions: Optional[Set[TP]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None: ...

    @abc.abstractmethod
    def clone(
        self, *, is_iterator: Optional[bool] = None, **kwargs: Any
    ) -> "ChannelT[_T]": ...

    @abc.abstractmethod
    def clone_using_queue(self, queue: asyncio.Queue) -> "ChannelT[_T]": ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> "_StreamT[_T]": ...

    @abc.abstractmethod
    def get_topic_name(self) -> str: ...

    @abc.abstractmethod
    async def send(
        self,
        *,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]: ...

    @abc.abstractmethod
    def send_soon(
        self,
        *,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
        eager_partitioning: bool = False,
    ) -> FutureMessage: ...

    @abc.abstractmethod
    def as_future_message(
        self,
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        eager_partitioning: bool = False,
    ) -> FutureMessage: ...

    @abc.abstractmethod
    async def publish_message(
        self, fut: FutureMessage, wait: bool = True
    ) -> Awaitable[RecordMetadata]: ...

    async def maybe_declare(self) -> None: ...

    @abc.abstractmethod
    async def declare(self) -> None: ...

    @abc.abstractmethod
    def prepare_key(
        self, key: K, key_serializer: CodecArg, schema: Optional[_SchemaT] = None
    ) -> Any: ...

    @abc.abstractmethod
    def prepare_value(
        self, value: V, value_serializer: CodecArg, schema: Optional[_SchemaT] = None
    ) -> Any: ...

    @abc.abstractmethod
    async def decode(
        self, message: Message, *, propagate: bool = False
    ) -> _EventT[_T]: ...

    @abc.abstractmethod
    async def deliver(self, message: Message) -> None: ...

    @abc.abstractmethod
    async def put(self, value: _EventT[_T_contra]) -> None: ...

    @abc.abstractmethod
    async def get(self, *, timeout: Optional[float] = None) -> _EventT[_T]: ...

    @abc.abstractmethod
    def empty(self) -> bool: ...

    @abc.abstractmethod
    async def on_key_decode_error(self, exc: Exception, message: Message) -> None: ...

    @abc.abstractmethod
    async def on_value_decode_error(self, exc: Exception, message: Message) -> None: ...

    @abc.abstractmethod
    async def on_decode_error(self, exc: Exception, message: Message) -> None: ...

    @abc.abstractmethod
    def on_stop_iteration(self) -> None: ...

    @abc.abstractmethod
    def __aiter__(self) -> "ChannelT": ...

    @abc.abstractmethod
    async def __anext__(self) -> Awaitable[_EventT[_T]]: ...

    @abc.abstractmethod
    async def throw(self, exc: BaseException) -> None: ...

    @abc.abstractmethod
    def _throw(self, exc: BaseException) -> None: ...

    @abc.abstractmethod
    def derive(self, **kwargs: Any) -> "ChannelT": ...

    @property
    @abc.abstractmethod
    def subscriber_count(self) -> int: ...

    @property
    @abc.abstractmethod
    def queue(self) -> ThrowableQueue: ...
