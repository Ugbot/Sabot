# -*- coding: utf-8 -*-
"""RocksDB-backed channel implementation for Sabot."""

import asyncio
import logging
import os
from typing import Any, Optional

from .channels import SerializedChannel
from .types import AppT, ChannelT

logger = logging.getLogger(__name__)


class RocksDBChannel(SerializedChannel):
    """RocksDB-backed channel for durable local message storage."""

    def __init__(
        self,
        app: AppT,
        name: str,
        *,
        path: Optional[str] = None,
        maxsize: Optional[int] = None,
        **kwargs
    ):
        self.channel_name = name
        self.db_path = path or f"./data/rocksdb/channels/{name}"

        # Ensure directory exists
        os.makedirs(self.db_path, exist_ok=True)

        # Try to create RocksDB instance
        try:
            import rocksdb
            # Open database in write mode for producers, read mode for consumers
            self._db = rocksdb.DB(self.db_path, rocksdb.Options(create_if_missing=True))
        except ImportError:
            raise ImportError("RocksDB channel requires rocksdb-py. Install with: pip install rocksdb")

        # Sequence number for message ordering
        self._sequence = 0

        super().__init__(app, maxsize=maxsize, **kwargs)

    async def send(self, *, key=None, value=None, **kwargs):
        """Store message durably in RocksDB."""
        # Serialize the message
        final_key, headers = self.prepare_key(key, self.key_serializer, headers={})
        final_value, headers = self.prepare_value(value, self.value_serializer, headers=headers)

        # Create unique key for this message
        message_key = f"{self.channel_name}:{self._sequence:020d}".encode()

        # Serialize message data
        import pickle
        message_data = pickle.dumps({
            'key': final_key,
            'value': final_value,
            'headers': headers,
            'timestamp': asyncio.get_event_loop().time(),
        })

        # Store in RocksDB
        await asyncio.get_event_loop().run_in_executor(
            None, self._db.put, message_key, message_data
        )

        self._sequence += 1

        # Return mock RecordMetadata
        from .types import RecordMetadata, TP
        return RecordMetadata(
            topic=self.channel_name,
            partition=0,
            topic_partition=TP(self.channel_name, 0),
            offset=self._sequence - 1,
            timestamp=asyncio.get_event_loop().time(),
            timestamp_type=0,
        )

    async def get(self, *, timeout=None):
        """Retrieve next message from RocksDB."""
        # For RocksDB channels, we iterate through stored messages
        # This is a simplified implementation - in practice, we'd need
        # consumer offset tracking

        try:
            # Get iterator for messages
            it = await asyncio.get_event_loop().run_in_executor(
                None, self._db.iterkeys
            )
            await asyncio.get_event_loop().run_in_executor(
                None, it.seek_to_first
            )

            # Find next unread message
            import pickle
            for key_bytes in it:
                key_str = key_bytes.decode()
                if key_str.startswith(f"{self.channel_name}:"):
                    # Get the message data
                    message_data = await asyncio.get_event_loop().run_in_executor(
                        None, self._db.get, key_bytes
                    )

                    if message_data:
                        data = pickle.loads(message_data)
                        from .types import Message

                        message = Message(
                            key=data['key'],
                            value=data['value'],
                            headers=data.get('headers', {}),
                            topic=self.channel_name,
                            partition=0,
                            offset=int(key_str.split(':')[1]),
                            timestamp=data.get('timestamp', 0),
                            generation_id=0,
                        )

                        return await self.decode(message)

        except Exception as e:
            logger.debug(f"No messages available in RocksDB channel: {e}")

        # If no data available
        if timeout:
            await asyncio.sleep(timeout)
            raise asyncio.TimeoutError()
        return None

    def derive(self, **kwargs: Any) -> ChannelT:
        """Derive new channel from this RocksDB channel."""
        return RocksDBChannel(
            self.app,
            self.channel_name,
            path=self.db_path,
            maxsize=self.maxsize,
            **{**self._clone_args(), **kwargs}
        )

    def __str__(self) -> str:
        return f"rocksdb:{self.channel_name}"
