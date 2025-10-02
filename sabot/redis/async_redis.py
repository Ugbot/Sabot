#!/usr/bin/env python3
"""
Python wrapper for the FastRedis Cython extension.
Provides a high-level interface for Redis operations with threading support.
"""

import asyncio
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable
import logging

# Optional uvloop integration for better async performance
try:
    import uvloop
    _UVLOOP_AVAILABLE = True
except ImportError:
    uvloop = None
    _UVLOOP_AVAILABLE = False

# Import the Cython extension (will be available after building)
try:
    from fast_redis import FastRedisClient, ThreadedStreamManager, RedisError, ConnectionError
except ImportError:
    print("Warning: fast_redis extension not built. Run 'python setup.py build_ext --inplace' first.")
    FastRedisClient = None
    ThreadedStreamManager = None
    RedisError = Exception
    ConnectionError = Exception

logger = logging.getLogger(__name__)


class RedisConnectionPool:
    """Connection pool manager for Redis connections"""

    def __init__(self, host: str = "localhost", port: int = 6379,
                 max_connections: int = 10, max_workers: int = 4):
        if FastRedisClient is None:
            raise ImportError("FastRedis extension not available. Build it first.")

        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.max_workers = max_workers
        self._clients: List[FastRedisClient] = []
        self._lock = threading.Lock()

    def get_client(self) -> FastRedisClient:
        """Get a Redis client from the pool"""
        with self._lock:
            if not self._clients:
                client = FastRedisClient(self.host, self.port,
                                       self.max_connections, self.max_workers)
                self._clients.append(client)
            return self._clients[0]  # For now, just return the first client

    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            for client in self._clients:
                # Clients are automatically closed in their __dealloc__ method
                pass
            self._clients.clear()


class HighPerformanceRedis:
    """
    High-performance Redis client with C-level threading support.
    Wraps the Cython FastRedisClient for optimal performance.
    """

    def __init__(self, host: str = "localhost", port: int = 6379,
                 max_connections: int = 10, max_workers: int = 4, use_uvloop: bool = None):
        self.pool = RedisConnectionPool(host, port, max_connections, max_workers)
        self.client = self.pool.get_client()
        # C-level threading is now handled by the FastRedisClient

        # uvloop configuration
        if use_uvloop is None:
            use_uvloop = _UVLOOP_AVAILABLE
        self.use_uvloop = use_uvloop and _UVLOOP_AVAILABLE

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close all connections"""
        self.pool.close_all()
        # C-level thread pools are automatically cleaned up by the client

    # Synchronous operations
    def set(self, key: str, value: str) -> str:
        """Set a key-value pair"""
        return self.client.set(key, value)

    def get(self, key: str) -> Optional[str]:
        """Get the value of a key"""
        return self.client.get(key)

    def delete(self, key: str) -> int:
        """Delete a key"""
        return self.client.delete(key)

    def publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel"""
        return self.client.publish(channel, message)

    # Stream operations
    def xadd(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        """Add a message to a stream"""
        return self.client.xadd(stream, data, message_id)

    def xread(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        """Read from streams"""
        return self.client.xread(streams, count, block)

    # Asynchronous operations
    async def set_async(self, key: str, value: str) -> str:
        """Asynchronously set a key-value pair"""
        return await self.client.execute_async('set', key, value)

    async def get_async(self, key: str) -> Optional[str]:
        """Asynchronously get the value of a key"""
        return await self.client.execute_async('get', key)

    async def xadd_async(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        """Asynchronously add a message to a stream"""
        return await self.client.execute_async('xadd', stream, data, message_id)

    async def xread_async(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        """Asynchronously read from streams"""
        return await self.client.execute_async('xread', streams, count, block)

    # C-level threaded operations (fire-and-forget)
    def set_threaded(self, key: str, value: str) -> None:
        """Set a key-value pair using C-level threading (fire-and-forget)"""
        self.client.execute_threaded('set', key, value)

    def get_threaded(self, key: str) -> None:
        """Get the value of a key using C-level threading (fire-and-forget)"""
        self.client.execute_threaded('get', key)

    def xadd_threaded(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> None:
        """Add a message to a stream using C-level threading (fire-and-forget)"""
        self.client.execute_threaded('xadd', stream, data, message_id)

    # Keyspace notifications and watch operations
    def watch(self, keys: Union[str, List[str]]) -> str:
        """Watch keys for changes (optimistic locking)"""
        return self.client.watch(keys)

    def unwatch(self) -> str:
        """Unwatch all keys"""
        return self.client.unwatch()

    def enable_keyspace_notifications(self, events: str = 'AKE') -> str:
        """Enable keyspace notifications"""
        return self.client.enable_keyspace_notifications(events)

    def publish(self, channel: str, message: str) -> int:
        """Publish message to channel"""
        return self.client.publish(channel, message)

    def subscribe(self, channels: Union[str, List[str]]) -> Any:
        """Subscribe to channels"""
        return self.client.subscribe(channels)

    def psubscribe(self, patterns: Union[str, List[str]]) -> Any:
        """Subscribe to patterns"""
        return self.client.psubscribe(patterns)

    def pubsub_channels(self, pattern: Optional[str] = None) -> List[str]:
        """List active pub/sub channels"""
        return self.client.pubsub_channels(pattern)

    def pubsub_numsub(self, channels: Optional[Union[str, List[str]]] = None) -> Dict[str, int]:
        """Get number of subscribers for channels"""
        return self.client.pubsub_numsub(channels)

    # Enhanced stream operations
    def xrange(self, stream: str, start: str = '-', end: str = '+', count: Optional[int] = None) -> List[tuple]:
        """Read stream entries in range"""
        return self.client.xrange(stream, start, end, count)

    def xrevrange(self, stream: str, end: str = '+', start: str = '-', count: Optional[int] = None) -> List[tuple]:
        """Read stream entries in reverse range"""
        return self.client.xrevrange(stream, end, start, count)

    def xtrim(self, stream: str, maxlen: Optional[int] = None, minid: Optional[str] = None, limit: Optional[int] = None) -> int:
        """Trim stream to max length or min ID"""
        return self.client.xtrim(stream, maxlen, minid, limit)

    def xdel(self, stream: str, message_ids: Union[str, List[str]]) -> int:
        """Delete messages from stream"""
        return self.client.xdel(stream, message_ids)

    def xlen(self, stream: str) -> int:
        """Get stream length"""
        return self.client.xlen(stream)

    def xinfo_stream(self, stream: str) -> Dict[str, Any]:
        """Get stream information"""
        return self.client.xinfo_stream(stream)

    def xinfo_groups(self, stream: str) -> List[Dict[str, Any]]:
        """Get consumer groups for stream"""
        return self.client.xinfo_groups(stream)

    def xinfo_consumers(self, stream: str, group: str) -> List[Dict[str, Any]]:
        """Get consumers in a group"""
        return self.client.xinfo_consumers(stream, group)

    def xgroup_create(self, stream: str, group: str, id: str = '$', mkstream: bool = False) -> str:
        """Create a consumer group"""
        return self.client.xgroup_create(stream, group, id, mkstream)

    def xgroup_destroy(self, stream: str, group: str) -> int:
        """Destroy a consumer group"""
        return self.client.xgroup_destroy(stream, group)

    def xreadgroup(self, group: str, consumer: str, streams: Dict[str, str], count: Optional[int] = None,
                   block: Optional[int] = None, noack: bool = False) -> List[tuple]:
        """Read from stream as consumer group"""
        return self.client.xreadgroup(group, consumer, streams, count, block, noack)

    def xack(self, stream: str, group: str, message_ids: Union[str, List[str]]) -> int:
        """Acknowledge messages in consumer group"""
        return self.client.xack(stream, group, message_ids)

    def xpending(self, stream: str, group: str, start: str = '-', end: str = '+',
                 count: Optional[int] = None, consumer: Optional[str] = None) -> List[Any]:
        """Get pending messages in consumer group"""
        return self.client.xpending(stream, group, start, end, count, consumer)

    def xclaim(self, stream: str, group: str, consumer: str, min_idle_time: int,
               message_ids: Union[str, List[str]], idle: Optional[int] = None,
               time: Optional[int] = None, retrycount: Optional[int] = None, force: bool = False) -> List[tuple]:
        """Claim pending messages"""
        return self.client.xclaim(stream, group, consumer, min_idle_time, message_ids, idle, time, retrycount, force)

    # List operations for worker queues
    def lpush(self, key: str, values: Union[str, List[str]]) -> int:
        """Push values to head of list"""
        return self.client.lpush(key, values)

    def rpush(self, key: str, values: Union[str, List[str]]) -> int:
        """Push values to tail of list"""
        return self.client.rpush(key, values)

    def lpop(self, key: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """Pop values from head of list"""
        return self.client.lpop(key, count)

    def rpop(self, key: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """Pop values from tail of list"""
        return self.client.rpop(key, count)

    def blpop(self, keys: Union[str, List[str]], timeout: int = 0) -> Optional[List[str]]:
        """Blocking left pop"""
        return self.client.blpop(keys, timeout)

    def brpop(self, keys: Union[str, List[str]], timeout: int = 0) -> Optional[List[str]]:
        """Blocking right pop"""
        return self.client.brpop(keys, timeout)

    def llen(self, key: str) -> int:
        """Get list length"""
        return self.client.llen(key)

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        """Get range of list elements"""
        return self.client.lrange(key, start, end)

    def rpoplpush(self, source: str, destination: str) -> str:
        """Pop from source and push to destination"""
        return self.client.rpoplpush(source, destination)

    def brpoplpush(self, source: str, destination: str, timeout: int = 0) -> str:
        """Blocking pop from source and push to destination"""
        return self.client.brpoplpush(source, destination, timeout)

    # Hash operations for distributed objects
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value"""
        return self.client.hget(key, field)

    def hset(self, key: str, field: str, value: str) -> int:
        """Set hash field value"""
        return self.client.hset(key, field, value)

    def hmget(self, key: str, fields: Union[str, List[str]]) -> List[Optional[str]]:
        """Get multiple hash fields"""
        return self.client.hmget(key, fields)

    def hmset(self, key: str, field_value_dict: Dict[str, str]) -> str:
        """Set multiple hash fields"""
        return self.client.hmset(key, field_value_dict)

    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields and values"""
        return self.client.hgetall(key)

    def hkeys(self, key: str) -> List[str]:
        """Get all hash field names"""
        return self.client.hkeys(key)

    def hvals(self, key: str) -> List[str]:
        """Get all hash values"""
        return self.client.hvals(key)

    def hlen(self, key: str) -> int:
        """Get number of hash fields"""
        return self.client.hlen(key)

    def hdel(self, key: str, fields: Union[str, List[str]]) -> int:
        """Delete hash fields"""
        return self.client.hdel(key, fields)

    def hexists(self, key: str, field: str) -> int:
        """Check if hash field exists"""
        return self.client.hexists(key, field)

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """Increment hash field by integer"""
        return self.client.hincrby(key, field, amount)

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0) -> float:
        """Increment hash field by float"""
        return self.client.hincrbyfloat(key, field, amount)

    # Lua script evaluation
    def eval(self, script: str, keys: Optional[List[str]] = None,
             args: Optional[List[str]] = None) -> Any:
        """Execute Lua script"""
        return self.client.eval(script, keys, args)

    def evalsha(self, sha: str, keys: Optional[List[str]] = None,
                args: Optional[List[str]] = None) -> Any:
        """Execute Lua script by SHA"""
        return self.client.evalsha(sha, keys, args)

    def script_load(self, script: str) -> str:
        """Load Lua script into Redis"""
        return self.client.script_load(script)

    def script_kill(self) -> str:
        """Kill running Lua script"""
        return self.client.script_kill()

    def script_flush(self) -> str:
        """Flush all Lua scripts"""
        return self.client.script_flush()

    # Set operations
    def sadd(self, key: str, members: Union[str, List[str]]) -> int:
        """Add members to set"""
        return self.client.sadd(key, members)

    def srem(self, key: str, members: Union[str, List[str]]) -> int:
        """Remove members from set"""
        return self.client.srem(key, members)

    def smembers(self, key: str) -> List[str]:
        """Get all set members"""
        return self.client.smembers(key)

    def scard(self, key: str) -> int:
        """Get set cardinality"""
        return self.client.scard(key)

    def sismember(self, key: str, member: str) -> int:
        """Check if member is in set"""
        return self.client.sismember(key, member)

    def spop(self, key: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """Remove and return random members"""
        return self.client.spop(key, count)

    def srandmember(self, key: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """Get random members without removing"""
        return self.client.srandmember(key, count)

    # Sorted set operations
    def zadd(self, key: str, score_member_dict: Dict[str, Union[int, float]]) -> int:
        """Add members to sorted set"""
        return self.client.zadd(key, score_member_dict)

    def zrem(self, key: str, members: Union[str, List[str]]) -> int:
        """Remove members from sorted set"""
        return self.client.zrem(key, members)

    def zscore(self, key: str, member: str) -> Optional[float]:
        """Get score of member in sorted set"""
        return self.client.zscore(key, member)

    def zrank(self, key: str, member: str) -> Optional[int]:
        """Get rank of member in sorted set"""
        return self.client.zrank(key, member)

    def zrevrank(self, key: str, member: str) -> Optional[int]:
        """Get reverse rank of member in sorted set"""
        return self.client.zrevrank(key, member)

    def zrange(self, key: str, start: int, end: int, withscores: bool = False) -> Union[List[str], List[tuple]]:
        """Get range of members from sorted set"""
        return self.client.zrange(key, start, end, withscores)

    def zrevrange(self, key: str, start: int, end: int, withscores: bool = False) -> Union[List[str], List[tuple]]:
        """Get reverse range of members from sorted set"""
        return self.client.zrevrange(key, start, end, withscores)

    def zcard(self, key: str) -> int:
        """Get sorted set cardinality"""
        return self.client.zcard(key)

    def zcount(self, key: str, min_score: Union[int, float], max_score: Union[int, float]) -> int:
        """Count members in score range"""
        return self.client.zcount(key, min_score, max_score)

    def zincrby(self, key: str, increment: Union[int, float], member: str) -> float:
        """Increment score of member in sorted set"""
        return self.client.zincrby(key, increment, member)

    # Enhanced Lua script management
    def script_exists(self, shas: Union[str, List[str]]) -> List[int]:
        """Check if scripts exist in cache"""
        return self.client.script_exists(shas)

    def script_debug(self, mode: str) -> str:
        """Set script debug mode (YES/NO/SYNC)"""
        return self.client.script_debug(mode)

    def load_script(self, script: str) -> str:
        """Load script and return SHA (with error handling)"""
        return self.client.load_script(script)

    def execute_script(self, script: str, keys: Optional[List[str]] = None,
                      args: Optional[List[str]] = None, use_cache: bool = True) -> Any:
        """Execute script with automatic caching and error handling"""
        return self.client.execute_script(script, keys, args, use_cache)

    # Redis Cluster commands
    def cluster_slots(self) -> List[List[Any]]:
        """Get cluster slots information"""
        return self.client.cluster_slots()

    def cluster_nodes(self) -> str:
        """Get cluster nodes information"""
        return self.client.cluster_nodes()

    def cluster_meet(self, ip: str, port: int) -> str:
        """Add node to cluster"""
        return self.client.cluster_meet(ip, port)

    def cluster_forget(self, node_id: str) -> str:
        """Remove node from cluster"""
        return self.client.cluster_forget(node_id)

    def cluster_replicate(self, node_id: str) -> str:
        """Configure node as replica"""
        return self.client.cluster_replicate(node_id)

    def cluster_failover(self, force: bool = False) -> str:
        """Force failover"""
        return self.client.cluster_failover(force)

    def cluster_info(self) -> str:
        """Get cluster information"""
        return self.client.cluster_info()

    def cluster_keyslot(self, key: str) -> int:
        """Get hash slot for key"""
        return self.client.cluster_keyslot(key)

    def cluster_countkeysinslot(self, slot: int) -> int:
        """Count keys in hash slot"""
        return self.client.cluster_countkeysinslot(slot)

    def cluster_getkeysinslot(self, slot: int, count: int) -> List[str]:
        """Get keys in hash slot"""
        return self.client.cluster_getkeysinslot(slot, count)

    def cluster_savaconfig(self) -> str:
        """Save cluster configuration"""
        return self.client.cluster_savaconfig()

    def cluster_loadconfig(self) -> str:
        """Load cluster configuration"""
        return self.client.cluster_loadconfig()

    def cluster_reset(self, hard: bool = False) -> str:
        """Reset cluster node"""
        return self.client.cluster_reset(hard)

    def cluster_flushslots(self) -> str:
        """Flush hash slots"""
        return self.client.cluster_flushslots()

    # Sentinel commands
    def sentinel_masters(self) -> List[Dict[str, Any]]:
        """Get list of monitored masters"""
        return self.client.sentinel_masters()

    def sentinel_master(self, master_name: str) -> Dict[str, Any]:
        """Get master information"""
        return self.client.sentinel_master(master_name)

    def sentinel_slaves(self, master_name: str) -> List[Dict[str, Any]]:
        """Get slave information"""
        return self.client.sentinel_slaves(master_name)

    def sentinel_sentinels(self, master_name: str) -> List[Dict[str, Any]]:
        """Get sentinel information"""
        return self.client.sentinel_sentinels(master_name)

    def sentinel_get_master_addr_by_name(self, master_name: str) -> List[str]:
        """Get master address"""
        return self.client.sentinel_get_master_addr_by_name(master_name)

    def sentinel_reset(self, pattern: str) -> int:
        """Reset masters matching pattern"""
        return self.client.sentinel_reset(pattern)

    def sentinel_failover(self, master_name: str) -> str:
        """Force failover"""
        return self.client.sentinel_failover(master_name)

    def sentinel_monitor(self, name: str, ip: str, port: int, quorum: int) -> str:
        """Monitor a new master"""
        return self.client.sentinel_monitor(name, ip, port, quorum)

    def sentinel_remove(self, name: str) -> str:
        """Remove master monitoring"""
        return self.client.sentinel_remove(name)

    def sentinel_set(self, master_name: str, option: str, value: str) -> str:
        """Set sentinel configuration"""
        return self.client.sentinel_set(master_name, option, value)


class ThreadedStreamConsumer:
    """
    Threaded stream consumer for high-throughput stream processing.
    Uses the Cython ThreadedStreamManager for optimal performance.
    """

    def __init__(self, redis_client: HighPerformanceRedis, max_workers: int = 4):
        if ThreadedStreamManager is None:
            raise ImportError("ThreadedStreamManager not available. Build the extension first.")

        self.redis = redis_client
        self.stream_manager = ThreadedStreamManager(redis_client.client, max_workers)
        self.callbacks: Dict[str, Callable] = {}
        self.running = False

    def subscribe(self, stream: str, start_id: str = "0"):
        """Subscribe to a stream starting from a specific message ID"""
        self.stream_manager.subscribe_to_streams({stream: start_id})

    def set_callback(self, stream: str, callback: Callable[[str, str, Dict], None]):
        """Set a callback function for processing messages from a stream"""
        self.callbacks[stream] = callback

    def _message_handler(self, stream: str, message_id: str, data: Dict):
        """Internal message handler that routes to user callbacks"""
        if stream in self.callbacks:
            try:
                self.callbacks[stream](stream, message_id, data)
            except Exception as e:
                logger.error(f"Error in callback for stream {stream}: {e}")
        else:
            logger.warning(f"No callback registered for stream {stream}")

    def start_consuming(self):
        """Start consuming messages from subscribed streams"""
        self.running = True
        self.stream_manager.start_consuming(self._message_handler)
        logger.info("Started threaded stream consumption")

    def stop_consuming(self):
        """Stop consuming messages"""
        self.running = False
        self.stream_manager.stop_consuming()
        logger.info("Stopped threaded stream consumption")

    def is_running(self) -> bool:
        """Check if the consumer is running"""
        return self.running


class KeyspaceWatcher:
    """
    Convenience class for Redis keyspace notifications and watches.
    Handles key events, optimistic locking, and notifications.
    """

    def __init__(self, redis_client: HighPerformanceRedis):
        self.redis = redis_client
        self.watched_keys: Set[str] = set()
        self.key_callbacks: Dict[str, Callable] = {}
        self.event_callbacks: Dict[str, Callable] = {}

    def watch_keys(self, keys: Union[str, List[str]]) -> bool:
        """Watch keys for optimistic locking"""
        if isinstance(keys, str):
            keys = [keys]

        try:
            result = self.redis.watch(keys)
            self.watched_keys.update(keys)
            return result == 'OK'
        except Exception:
            return False

    def unwatch_keys(self) -> bool:
        """Unwatch all keys"""
        try:
            result = self.redis.unwatch()
            self.watched_keys.clear()
            return result == 'OK'
        except Exception:
            return False

    def enable_notifications(self, events: str = 'AKE') -> bool:
        """Enable keyspace notifications"""
        try:
            result = self.redis.enable_keyspace_notifications(events)
            return result == 'OK'
        except Exception:
            return False

    def on_key_event(self, key_pattern: str, callback: Callable[[str, str], None]):
        """Register callback for key events"""
        self.key_callbacks[key_pattern] = callback

    def on_event_type(self, event_type: str, callback: Callable[[str, str], None]):
        """Register callback for specific event types (set, del, expire, etc.)"""
        self.event_callbacks[event_type] = callback

    def start_watching(self) -> 'KeyspaceNotificationConsumer':
        """Start watching for keyspace events"""
        consumer = KeyspaceNotificationConsumer(self.redis, self)
        consumer.start()
        return consumer


class KeyspaceNotificationConsumer:
    """
    Consumer for Redis keyspace notifications.
    Handles pub/sub subscriptions to keyspace channels.
    """

    def __init__(self, redis: HighPerformanceRedis, watcher: KeyspaceWatcher):
        self.redis = redis
        self.watcher = watcher
        self.consumer = None
        self.running = False

    def _keyspace_callback(self, channel: str, message: str):
        """Handle keyspace notification messages"""
        # Parse keyspace channel format: __keyspace@0__:keyname
        if channel.startswith('__keyspace@'):
            key = channel.split(':', 2)[-1]
            event = message

            # Call event-specific callbacks
            if event in self.watcher.event_callbacks:
                self.watcher.event_callbacks[event](key, event)

            # Call key-specific callbacks
            for pattern, callback in self.watcher.key_callbacks.items():
                if self._matches_pattern(key, pattern):
                    callback(key, event)

    def _keyevent_callback(self, channel: str, message: str):
        """Handle keyevent notification messages"""
        # Parse keyevent channel format: __keyevent@0__:event:keyname
        if channel.startswith('__keyevent@'):
            parts = channel.split(':')
            if len(parts) >= 3:
                event = parts[-2]
                key = parts[-1]

                # Call event-specific callbacks
                if event in self.watcher.event_callbacks:
                    self.watcher.event_callbacks[event](key, event)

                # Call key-specific callbacks
                for pattern, callback in self.watcher.key_callbacks.items():
                    if self._matches_pattern(key, pattern):
                        callback(key, event)

    def _matches_pattern(self, key: str, pattern: str) -> bool:
        """Simple pattern matching for key patterns"""
        if '*' in pattern:
            import fnmatch
            return fnmatch.fnmatch(key, pattern)
        return key == pattern

    def start(self):
        """Start consuming keyspace notifications"""
        if self.consumer is None:
            if CPubSubConsumer is None:
                raise ImportError("CPubSubConsumer not available. Build the extension first.")

            self.consumer = CPubSubConsumer(self.redis.client, max_workers=2)
            self.consumer.set_callback(self._keyspace_callback)
            self.consumer.set_pattern_callback(self._keyevent_callback)

            # Subscribe to keyspace and keyevent channels
            keyspace_patterns = ['__keyspace@*__:*']
            keyevent_patterns = ['__keyevent@*__:*']

            self.consumer.psubscribe(keyspace_patterns)
            self.consumer.psubscribe(keyevent_patterns)
            self.consumer.start_listening()

        self.running = True

    def stop(self):
        """Stop consuming notifications"""
        self.running = False
        if self.consumer:
            self.consumer.stop_listening()
            self.consumer = None


class OptimisticLock:
    """
    Convenience class for optimistic locking using Redis WATCH/MULTI/EXEC.
    """

    def __init__(self, redis_client: HighPerformanceRedis):
        self.redis = redis_client

    def execute_transaction(self, keys_to_watch: List[str],
                          transaction_func: Callable) -> Any:
        """
        Execute a transaction with optimistic locking.

        Args:
            keys_to_watch: Keys to watch for changes
            transaction_func: Function that performs operations within MULTI/EXEC

        Returns:
            Result of EXEC, or None if transaction failed
        """
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Watch keys
                self.redis.watch(keys_to_watch)

                # Start transaction
                # Note: MULTI/EXEC not fully implemented in this version
                # This is a placeholder for the concept

                # Execute user function
                result = transaction_func()

                # In a full implementation, you would:
                # 1. Call MULTI
                # 2. Execute transaction_func
                # 3. Call EXEC
                # 4. Handle watch failures

                return result

            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise e
                # Unwatch and retry
                self.redis.unwatch()

        return None


class StreamConsumerGroup:
    """
    High-level Redis Streams consumer group with automatic message handling,
    acknowledgments, and dead letter queues.
    """

    def __init__(self, redis: HighPerformanceRedis, stream: str, group: str, consumer: str,
                 dead_letter_queue: Optional[str] = None, max_retries: int = 3):
        self.redis = redis
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.dead_letter_queue = dead_letter_queue or f"{stream}:dead"
        self.max_retries = max_retries
        self.message_handlers: Dict[str, Callable] = {}
        self.running = False

    def create_group(self, start_id: str = '$', mkstream: bool = True) -> bool:
        """Create the consumer group"""
        try:
            result = self.redis.xgroup_create(self.stream, self.group, start_id, mkstream)
            return result == 'OK'
        except Exception as e:
            logger.error(f"Failed to create consumer group: {e}")
            return False

    def register_handler(self, message_type: str, handler: Callable[[Dict[str, Any], str], bool]):
        """
        Register a message handler function.
        Handler should return True if message processed successfully, False otherwise.
        """
        self.message_handlers[message_type] = handler

    def process_messages(self, count: int = 10, block: int = 5000) -> int:
        """Process pending messages from the stream"""
        streams = {self.stream: '>'}  # '>' means only new messages
        messages = self.redis.xreadgroup(self.group, self.consumer, streams, count, block)

        processed = 0
        successful_ids = []
        failed_messages = []

        for stream, msg_id, data in messages:
            if self._process_message(msg_id, data):
                successful_ids.append(msg_id)
                processed += 1
            else:
                failed_messages.append((msg_id, data))

        # Acknowledge successful messages
        if successful_ids:
            self.redis.xack(self.stream, self.group, successful_ids)

        # Handle failed messages
        for msg_id, data in failed_messages:
            self._handle_failed_message(msg_id, data)

        return processed

    def _process_message(self, msg_id: str, data: Dict[str, Any]) -> bool:
        """Process a single message"""
        message_type = data.get('type', 'default')

        if message_type in self.message_handlers:
            try:
                return self.message_handlers[message_type](data, msg_id)
            except Exception as e:
                logger.error(f"Error processing message {msg_id}: {e}")
                return False
        else:
            logger.warning(f"No handler for message type: {message_type}")
            return False

    def _handle_failed_message(self, msg_id: str, data: Dict[str, Any]):
        """Handle a failed message with retry logic"""
        retry_count = data.get('_retry_count', 0)

        if retry_count < self.max_retries:
            # Increment retry count and re-queue
            data['_retry_count'] = retry_count + 1
            self.redis.xadd(self.stream, data)
            logger.info(f"Re-queued message {msg_id}, attempt {retry_count + 1}")
        else:
            # Move to dead letter queue
            data['_original_msg_id'] = msg_id
            data['_failed_at'] = str(time.time())
            self.redis.xadd(self.dead_letter_queue, data)
            logger.warning(f"Message {msg_id} moved to dead letter queue after {retry_count} retries")

        # Acknowledge the failed message
        self.redis.xack(self.stream, self.group, msg_id)

    def get_pending_info(self) -> Dict[str, Any]:
        """Get information about pending messages"""
        return {
            'pending_count': len(self.redis.xpending(self.stream, self.group)),
            'group_info': self.redis.xinfo_groups(self.stream),
            'consumer_info': self.redis.xinfo_consumers(self.stream, self.group)
        }

    def claim_stale_messages(self, min_idle_time: int = 60000) -> int:
        """Claim messages that have been idle too long"""
        pending = self.redis.xpending(self.stream, self.group, count=100)
        if not pending:
            return 0

        stale_ids = []
        for entry in pending:
            if len(entry) >= 4 and entry[3] > min_idle_time:  # idle time
                stale_ids.append(entry[0])  # message id

        if stale_ids:
            claimed = self.redis.xclaim(self.stream, self.group, self.consumer, min_idle_time, stale_ids)
            return len(claimed)

        return 0


class WorkerQueue:
    """
    Redis-based worker queue using lists or streams.
    Supports job queuing, processing, and monitoring.
    """

    def __init__(self, redis: HighPerformanceRedis, queue_name: str,
                 use_streams: bool = False, dead_letter_queue: Optional[str] = None):
        self.redis = redis
        self.queue_name = queue_name
        self.processing_queue = f"{queue_name}:processing"
        self.dead_letter_queue = dead_letter_queue or f"{queue_name}:dead"
        self.use_streams = use_streams
        self.job_handlers: Dict[str, Callable] = {}

    def enqueue_job(self, job_type: str, data: Dict[str, Any], priority: str = 'normal') -> str:
        """Enqueue a job for processing"""
        job = {
            'type': job_type,
            'data': data,
            'priority': priority,
            'created_at': str(time.time()),
            'id': str(time.time())  # Simple ID generation
        }

        if self.use_streams:
            return self.redis.xadd(self.queue_name, job)
        else:
            # Use lists with priority support
            queue_key = f"{self.queue_name}:{priority}"
            return self.redis.lpush(queue_key, json.dumps(job))

    def dequeue_job(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """Dequeue a job for processing"""
        if self.use_streams:
            # For streams, we would use consumer groups
            # This is a simplified version
            streams = {self.queue_name: '0'}
            messages = self.redis.xread(streams, count=1, block=timeout*1000)
            if messages:
                stream, msg_id, data = messages[0]
                return {'id': msg_id, **data}
        else:
            # Priority queue using BRPOP
            priority_queues = [
                f"{self.queue_name}:high",
                f"{self.queue_name}:normal",
                f"{self.queue_name}:low"
            ]

            result = self.redis.brpop(priority_queues, timeout)
            if result:
                queue_name, job_data = result
                job = json.loads(job_data)

                # Move to processing queue
                self.redis.lpush(self.processing_queue, job_data)
                return job

        return None

    def complete_job(self, job_id: str) -> bool:
        """Mark a job as completed"""
        # Remove from processing queue
        # This is simplified - in practice you'd need to track which job
        return True

    def fail_job(self, job_id: str, error: str) -> bool:
        """Mark a job as failed"""
        # Move from processing queue to dead letter queue
        # This is simplified
        return True

    def register_job_handler(self, job_type: str, handler: Callable[[Dict[str, Any]], bool]):
        """Register a handler for a specific job type"""
        self.job_handlers[job_type] = handler

    def process_jobs(self, worker_id: str, max_jobs: int = 10) -> int:
        """Process jobs from the queue"""
        processed = 0

        for _ in range(max_jobs):
            job = self.dequeue_job(timeout=1)
            if not job:
                break

            job_type = job.get('type')
            if job_type in self.job_handlers:
                try:
                    if self.job_handlers[job_type](job):
                        self.complete_job(job['id'])
                        processed += 1
                    else:
                        self.fail_job(job['id'], "Handler returned False")
                except Exception as e:
                    logger.error(f"Job processing failed: {e}")
                    self.fail_job(job['id'], str(e))
            else:
                logger.warning(f"No handler for job type: {job_type}")
                self.fail_job(job['id'], f"No handler for {job_type}")

        return processed

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        stats = {
            'queue_length': self.redis.llen(self.queue_name) if not self.use_streams else self.redis.xlen(self.queue_name),
            'processing_length': self.redis.llen(self.processing_queue),
            'dead_letter_length': self.redis.llen(self.dead_letter_queue),
        }

        if self.use_streams:
            stats.update({
                'stream_info': self.redis.xinfo_stream(self.queue_name),
                'groups': self.redis.xinfo_groups(self.queue_name)
            })

        return stats


class PubSubHub:
    """
    Advanced pub/sub hub with pattern matching, message routing,
    and automatic reconnection.
    """

    def __init__(self, redis: HighPerformanceRedis):
        self.redis = redis
        self.subscriptions: Dict[str, Callable] = {}
        self.pattern_subscriptions: Dict[str, Callable] = {}
        self.consumer = None
        self.running = False

    def subscribe(self, channels: Union[str, List[str]], callback: Optional[Callable] = None):
        """Subscribe to channels"""
        if isinstance(channels, str):
            channels = [channels]

        if callback:
            for channel in channels:
                self.subscriptions[channel] = callback

        if self.consumer:
            self.consumer.subscribe(channels, callback)

    def psubscribe(self, patterns: Union[str, List[str]], callback: Optional[Callable] = None):
        """Subscribe to patterns"""
        if isinstance(patterns, str):
            patterns = [patterns]

        if callback:
            for pattern in patterns:
                self.pattern_subscriptions[pattern] = callback

        if self.consumer:
            self.consumer.psubscribe(patterns, callback)

    def publish(self, channel: str, message: Any, message_type: str = 'default') -> int:
        """Publish a message to a channel"""
        data = {
            'type': message_type,
            'data': message,
            'timestamp': str(time.time())
        }

        return self.redis.publish(channel, json.dumps(data))

    def start(self):
        """Start the pub/sub consumer"""
        if self.consumer is None and CPubSubConsumer is not None:
            self.consumer = CPubSubConsumer(self.redis.client, max_workers=2)

            # Set up callbacks
            def channel_callback(channel, message):
                if channel in self.subscriptions:
                    try:
                        data = json.loads(message)
                        self.subscriptions[channel](channel, data)
                    except:
                        self.subscriptions[channel](channel, message)

            def pattern_callback(pattern, message):
                if pattern in self.pattern_subscriptions:
                    try:
                        data = json.loads(message)
                        self.pattern_subscriptions[pattern](pattern, data)
                    except:
                        self.pattern_subscriptions[pattern](pattern, message)

            self.consumer.set_callback(channel_callback)
            self.consumer.set_pattern_callback(pattern_callback)

            # Subscribe to existing subscriptions
            if self.subscriptions:
                self.consumer.subscribe(list(self.subscriptions.keys()))
            if self.pattern_subscriptions:
                self.consumer.psubscribe(list(self.pattern_subscriptions.keys()))

            self.consumer.start_listening()

        self.running = True

    def stop(self):
        """Stop the pub/sub consumer"""
        self.running = False
        if self.consumer:
            self.consumer.stop_listening()
            self.consumer = None

    def get_channel_info(self) -> Dict[str, Any]:
        """Get information about active channels"""
        return {
            'channels': self.redis.pubsub_channels(),
            'subscriptions': list(self.subscriptions.keys()),
            'pattern_subscriptions': list(self.pattern_subscriptions.keys())
        }


# Import json for serialization in worker queues
import json


# Global uvloop utilities
def enable_uvloop():
    """
    Enable uvloop for better async performance.
    Must be called before creating any async tasks.
    """
    if not _UVLOOP_AVAILABLE:
        raise ImportError("uvloop is not installed. Install with: pip install uvloop")

    if not asyncio.AbstractEventLoopPolicy._loop_factory:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return True
    return False


def is_uvloop_enabled():
    """Check if uvloop is currently enabled"""
    try:
        return isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy)
    except:
        return False


def create_uvloop_event_loop():
    """Create a uvloop event loop"""
    if not _UVLOOP_AVAILABLE:
        raise ImportError("uvloop is not installed. Install with: pip install uvloop")

    return uvloop.new_event_loop()


class HighPerformanceAsyncRedis:
    """
    High-performance async Redis client using uvloop for maximum performance.
    Provides async versions of all Redis operations.
    """

    def __init__(self, host: str = "localhost", port: int = 6379,
                 max_connections: int = 10, use_uvloop: bool = None):
        # uvloop configuration
        if use_uvloop is None:
            use_uvloop = _UVLOOP_AVAILABLE
        self.use_uvloop = use_uvloop and _UVLOOP_AVAILABLE

        # Sync client for operations that need it
        self.sync_client = HighPerformanceRedis(host, port, max_connections, 4, use_uvloop=False)

        # Enable uvloop if requested and available
        if self.use_uvloop:
            enable_uvloop()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close all connections"""
        self.sync_client.close()

    # Async versions of all Redis operations using thread pools
    async def set(self, key: str, value: str) -> str:
        """Async SET operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.set, key, value)

    async def get(self, key: str) -> Optional[str]:
        """Async GET operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.get, key)

    async def delete(self, key: str) -> int:
        """Async DELETE operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.delete, key)

    async def incr(self, key: str) -> int:
        """Async INCR operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.sync_client.eval(
            "return redis.call('INCR', KEYS[1])", [key], []
        ))

    async def publish(self, channel: str, message: str) -> int:
        """Async PUBLISH operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.publish, channel, message)

    async def subscribe(self, channels: Union[str, List[str]]) -> Any:
        """Async SUBSCRIBE operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.subscribe, channels)

    async def xadd(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        """Async XADD operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.xadd, stream, data, message_id)

    async def xread(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        """Async XREAD operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.xread, streams, count, block)

    async def lpush(self, key: str, values: Union[str, List[str]]) -> int:
        """Async LPUSH operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.lpush, key, values)

    async def rpop(self, key: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """Async RPOP operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.rpop, key, count)

    async def blpop(self, keys: Union[str, List[str]], timeout: int = 0) -> Optional[List[str]]:
        """Async BLPOP operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sync_client.blpop, keys, timeout)

    async def execute_script(self, script: str, keys: Optional[List[str]] = None,
                           args: Optional[List[str]] = None, use_cache: bool = True) -> Any:
        """Execute Lua script asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.sync_client.execute_script(script, keys, args, use_cache)
        )

    async def pipeline_execute(self, commands: List[tuple]) -> List[Any]:
        """Execute multiple commands in a pipeline asynchronously"""
        # Simplified pipeline implementation
        results = []
        for cmd in commands:
            method_name, *args = cmd
            if hasattr(self, method_name):
                result = await getattr(self, method_name)(*args)
                results.append(result)
            else:
                # Fallback to sync client
                result = await asyncio.get_event_loop().run_in_executor(
                    None, getattr(self.sync_client, method_name), *args
                )
                results.append(result)
        return results


# Pre-defined Lua Scripts Library
class RedisScripts:
    """
    Collection of useful Lua scripts for common Redis patterns.
    These scripts provide atomic operations and improved performance.
    """

    # Rate Limiting Scripts
    RATE_LIMIT_SLIDING_WINDOW = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local current_time = tonumber(ARGV[3])

    -- Remove old entries outside the window
    redis.call('ZREMRANGEBYSCORE', key, 0, current_time - window)

    -- Count current requests in window
    local current_count = redis.call('ZCARD', key)

    if current_count < limit then
        -- Add current request
        redis.call('ZADD', key, current_time, current_time)
        -- Set expiration for cleanup
        redis.call('EXPIRE', key, window)
        return 1
    else
        return 0
    end
    """

    RATE_LIMIT_TOKEN_BUCKET = """
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_rate = tonumber(ARGV[2])
    local tokens_requested = tonumber(ARGV[3])
    local current_time = tonumber(ARGV[4])

    local data = redis.call('HMGET', key, 'tokens', 'last_refill')
    local current_tokens = tonumber(data[1] or capacity)
    local last_refill = tonumber(data[2] or current_time)

    -- Calculate tokens to add since last refill
    local time_passed = current_time - last_refill
    local tokens_to_add = math.floor(time_passed * refill_rate)
    current_tokens = math.min(capacity, current_tokens + tokens_to_add)

    if current_tokens >= tokens_requested then
        -- Consume tokens
        current_tokens = current_tokens - tokens_requested
        redis.call('HMSET', key, 'tokens', current_tokens, 'last_refill', current_time)
        redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) * 2)
        return 1
    else
        return 0
    end
    """

    # Distributed Lock Scripts
    DISTRIBUTED_LOCK_ACQUIRE = """
    local lock_key = KEYS[1]
    local lock_value = ARGV[1]
    local ttl = tonumber(ARGV[2])

    if redis.call('SET', lock_key, lock_value, 'NX', 'PX', ttl) then
        return 1
    else
        return 0
    end
    """

    DISTRIBUTED_LOCK_RELEASE = """
    local lock_key = KEYS[1]
    local lock_value = ARGV[1]

    if redis.call('GET', lock_key) == lock_value then
        redis.call('DEL', lock_key)
        return 1
    else
        return 0
    end
    """

    # Atomic Counter with Bounds
    BOUNDED_COUNTER_INCR = """
    local key = KEYS[1]
    local increment = tonumber(ARGV[1])
    local min_val = tonumber(ARGV[2] or '-inf')
    local max_val = tonumber(ARGV[3] or 'inf')

    local current = tonumber(redis.call('GET', key) or '0')
    local new_value = current + increment

    -- Check bounds
    if min_val ~= '-inf' and new_value < min_val then
        new_value = min_val
    elseif max_val ~= 'inf' and new_value > max_val then
        new_value = max_val
    end

    redis.call('SET', key, new_value)
    return new_value
    """

    # Queue Management Scripts
    QUEUE_PUSH_WITH_PRIORITY = """
    local queue_key = KEYS[1]
    local priority = ARGV[1]
    local item = ARGV[2]

    -- Use priority as score in sorted set
    redis.call('ZADD', queue_key, priority, item)
    return redis.call('ZCARD', queue_key)
    """

    QUEUE_POP_BY_PRIORITY = """
    local queue_key = KEYS[1]
    local count = tonumber(ARGV[1] or 1)

    -- Get highest priority items (lowest score)
    local items = redis.call('ZRANGE', queue_key, 0, count - 1)
    if #items > 0 then
        redis.call('ZREM', queue_key, unpack(items))
    end
    return items
    """

    # Cache Management Scripts
    CACHE_GET_WITH_BACKOFF = """
    local cache_key = KEYS[1]
    local data = redis.call('GET', cache_key)

    if data then
        -- Cache hit - update access time for LRU
        redis.call('ZADD', KEYS[2], redis.call('TIME')[1], cache_key)
        return data
    else
        -- Cache miss - increment miss counter
        redis.call('INCR', KEYS[3])
        return nil
    end
    """

    CACHE_SET_WITH_EVICTION = """
    local cache_key = KEYS[1]
    local value = ARGV[1]
    local ttl = tonumber(ARGV[2])
    local max_size = tonumber(ARGV[3] or 1000)
    local access_times_key = KEYS[2]

    -- Check current cache size
    local current_size = redis.call('ZCARD', access_times_key)

    if current_size >= max_size then
        -- Evict oldest accessed items (LRU)
        local to_evict = redis.call('ZRANGE', access_times_key, 0, current_size - max_size)
        for i, key in ipairs(to_evict) do
            redis.call('DEL', key)
        end
        redis.call('ZREMRANGEBYRANK', access_times_key, 0, current_size - max_size)
    end

    -- Set cache value
    redis.call('SETEX', cache_key, ttl, value)
    redis.call('ZADD', access_times_key, redis.call('TIME')[1], cache_key)

    return 1
    """

    # Batch Operations
    BATCH_SET_WITH_TTL = """
    local ttl = tonumber(ARGV[#ARGV])
    for i = 1, #KEYS do
        redis.call('SETEX', KEYS[i], ttl, ARGV[i])
    end
    return #KEYS
    """

    BATCH_DELETE_IF_EQUALS = """
    local deleted = 0
    for i = 1, #KEYS do
        if redis.call('GET', KEYS[i]) == ARGV[i] then
            redis.call('DEL', KEYS[i])
            deleted = deleted + 1
        end
    end
    return deleted
    """

    # Atomic Set Operations
    SET_ADD_MULTIPLE = """
    local key = KEYS[1]
    local added = 0
    for i = 1, #ARGV do
        if redis.call('SADD', key, ARGV[i]) == 1 then
            added = added + 1
        end
    end
    return added
    """

    SET_REMOVE_MULTIPLE_IF_EXISTS = """
    local key = KEYS[1]
    local removed = 0
    for i = 1, #ARGV do
        if redis.call('SISMEMBER', key, ARGV[i]) == 1 then
            redis.call('SREM', key, ARGV[i])
            removed = removed + 1
        end
    end
    return removed
    """

    # Bloom Filter Operations (Simplified)
    BLOOM_ADD = """
    local key = KEYS[1]
    local item = ARGV[1]
    local hash_count = tonumber(ARGV[2] or 3)
    local filter_size = tonumber(ARGV[3] or 1000)

    -- Simple hash functions (in production, use better hashing)
    local added = 0
    for i = 1, hash_count do
        local hash = redis.call('MURMURHASH', item .. tostring(i)) % filter_size
        if redis.call('SETBIT', key, hash, 1) == 0 then
            added = 1
        end
    end

    return added
    """

    BLOOM_CHECK = """
    local key = KEYS[1]
    local item = ARGV[1]
    local hash_count = tonumber(ARGV[2] or 3)
    local filter_size = tonumber(ARGV[3] or 1000)

    for i = 1, hash_count do
        local hash = redis.call('MURMURHASH', item .. tostring(i)) % filter_size
        if redis.call('GETBIT', key, hash) == 0 then
            return 0  -- Definitely not in set
        end
    end

    return 1  -- Possibly in set
    """

    # Semaphore Operations
    SEMAPHORE_ACQUIRE = """
    local sem_key = KEYS[1]
    local permits = tonumber(ARGV[1])
    local timeout = tonumber(ARGV[2] or 0)
    local current_time = redis.call('TIME')[1]

    local current_permits = tonumber(redis.call('GET', sem_key) or permits)

    if current_permits > 0 then
        redis.call('SET', sem_key, current_permits - 1)
        return 1
    elseif timeout > 0 then
        -- Wait for permit (simplified)
        local wait_key = sem_key .. ':wait'
        redis.call('ZADD', wait_key, current_time + timeout, current_time)
        return 0
    else
        return 0
    end
    """

    SEMAPHORE_RELEASE = """
    local sem_key = KEYS[1]
    local permits = tonumber(ARGV[1])

    local current_permits = tonumber(redis.call('GET', sem_key) or 0)
    local new_permits = math.min(permits, current_permits + 1)

    redis.call('SET', sem_key, new_permits)
    return new_permits
    """

    # Time Series Operations
    TIME_SERIES_ADD = """
    local key = KEYS[1]
    local timestamp = ARGV[1]
    local value = ARGV[2]
    local retention = ARGV[3]

    redis.call('ZADD', key, timestamp, value)
    if retention then
        redis.call('ZREMRANGEBYSCORE', key, 0, timestamp - retention)
    end

    return redis.call('ZCARD', key)
    """

    TIME_SERIES_RANGE = """
    local key = KEYS[1]
    local start_time = ARGV[1]
    local end_time = ARGV[2]
    local limit = ARGV[3]

    return redis.call('ZRANGEBYSCORE', key, start_time, end_time, 'LIMIT', 0, limit or -1)
    """

    # Atomic Configuration Updates
    ATOMIC_CONFIG_UPDATE = """
    local config_key = KEYS[1]
    local updates = {}

    -- Parse updates from ARGV
    for i = 1, #ARGV, 2 do
        local field = ARGV[i]
        local value = ARGV[i + 1]
        updates[field] = value
    end

    -- Get current config
    local current = redis.call('HGETALL', config_key)
    local changed = {}

    -- Apply updates and track changes
    for field, new_value in pairs(updates) do
        local current_value = redis.call('HGET', config_key, field)
        if current_value ~= new_value then
            redis.call('HSET', config_key, field, new_value)
            changed[field] = {old = current_value, new = new_value}
        end
    end

    return changed
    """

    # Leader Election
    LEADER_HEARTBEAT = """
    local leader_key = KEYS[1]
    local candidate_id = ARGV[1]
    local ttl = tonumber(ARGV[2])

    local current_leader = redis.call('GET', leader_key)

    if current_leader == candidate_id or current_leader == nil then
        redis.call('SETEX', leader_key, ttl, candidate_id)
        return 1
    else
        return 0
    end
    """

    LEADER_CHECK = """
    local leader_key = KEYS[1]
    local candidate_id = ARGV[1]

    local current_leader = redis.call('GET', leader_key)
    return current_leader == candidate_id and 1 or 0
    """


class LuaScriptManager:
    """
    Advanced Lua script management with caching, versioning, and debugging.
    """

    def __init__(self, redis: HighPerformanceRedis, namespace: str = "scripts"):
        self.redis = redis
        self.namespace = namespace
        self.script_cache = {}


class ScriptHelper:
    """
    High-level helper class for using pre-defined Lua scripts.
    Provides convenient methods for common Redis patterns.
    """

    def __init__(self, redis_client: HighPerformanceRedis):
        self.redis = redis_client
        self.script_manager = LuaScriptManager(redis_client)
        self._load_builtin_scripts()

    def _load_builtin_scripts(self):
        """Load all built-in scripts into the script manager"""
        scripts = RedisScripts()
        script_mapping = {
            'rate_limit_sliding_window': scripts.RATE_LIMIT_SLIDING_WINDOW,
            'rate_limit_token_bucket': scripts.RATE_LIMIT_TOKEN_BUCKET,
            'distributed_lock_acquire': scripts.DISTRIBUTED_LOCK_ACQUIRE,
            'distributed_lock_release': scripts.DISTRIBUTED_LOCK_RELEASE,
            'bounded_counter_incr': scripts.BOUNDED_COUNTER_INCR,
            'queue_push_priority': scripts.QUEUE_PUSH_WITH_PRIORITY,
            'queue_pop_priority': scripts.QUEUE_POP_BY_PRIORITY,
            'cache_get_lru': scripts.CACHE_GET_WITH_BACKOFF,
            'cache_set_eviction': scripts.CACHE_SET_WITH_EVICTION,
            'batch_set_ttl': scripts.BATCH_SET_WITH_TTL,
            'batch_delete_equals': scripts.BATCH_DELETE_IF_EQUALS,
            'set_add_multiple': scripts.SET_ADD_MULTIPLE,
            'set_remove_multiple': scripts.SET_REMOVE_MULTIPLE_IF_EXISTS,
            'bloom_add': scripts.BLOOM_ADD,
            'bloom_check': scripts.BLOOM_CHECK,
            'semaphore_acquire': scripts.SEMAPHORE_ACQUIRE,
            'semaphore_release': scripts.SEMAPHORE_RELEASE,
            'time_series_add': scripts.TIME_SERIES_ADD,
            'time_series_range': scripts.TIME_SERIES_RANGE,
            'config_update_atomic': scripts.ATOMIC_CONFIG_UPDATE,
            'leader_heartbeat': scripts.LEADER_HEARTBEAT,
            'leader_check': scripts.LEADER_CHECK,
        }

        for name, script in script_mapping.items():
            self.script_manager.register_script(name, script)

    # Rate Limiting Methods
    def rate_limit_sliding_window(self, key: str, limit: int, window_seconds: int) -> bool:
        """
        Rate limiting using sliding window algorithm.
        Returns True if request is allowed, False if rate limited.
        """
        current_time = int(time.time() * 1000)  # milliseconds
        result = self.script_manager.execute_script(
            'rate_limit_sliding_window',
            keys=[key],
            args=[limit, window_seconds, current_time]
        )
        return bool(result)

    def rate_limit_token_bucket(self, key: str, capacity: int, refill_rate: float,
                              tokens_requested: int = 1) -> bool:
        """
        Rate limiting using token bucket algorithm.
        Returns True if tokens are available, False otherwise.
        """
        current_time = int(time.time())
        result = self.script_manager.execute_script(
            'rate_limit_token_bucket',
            keys=[key],
            args=[capacity, refill_rate, tokens_requested, current_time]
        )
        return bool(result)

    # Distributed Lock Methods
    def acquire_lock(self, lock_key: str, lock_value: str, ttl_ms: int) -> bool:
        """
        Acquire a distributed lock with automatic expiration.
        Returns True if lock was acquired, False otherwise.
        """
        result = self.script_manager.execute_script(
            'distributed_lock_acquire',
            keys=[lock_key],
            args=[lock_value, ttl_ms]
        )
        return bool(result)

    def release_lock(self, lock_key: str, lock_value: str) -> bool:
        """
        Release a distributed lock if we own it.
        Returns True if lock was released, False otherwise.
        """
        result = self.script_manager.execute_script(
            'distributed_lock_release',
            keys=[lock_key],
            args=[lock_value]
        )
        return bool(result)

    # Bounded Counter Methods
    def bounded_increment(self, key: str, increment: int = 1,
                         min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
        """
        Increment a counter with bounds checking.
        Returns the new value after increment (clamped to bounds).
        """
        min_str = str(min_val) if min_val is not None else '-inf'
        max_str = str(max_val) if max_val is not None else 'inf'
        result = self.script_manager.execute_script(
            'bounded_counter_incr',
            keys=[key],
            args=[increment, min_str, max_str]
        )
        return int(result)

    # Priority Queue Methods
    def push_priority_queue(self, queue_key: str, priority: int, item: str) -> int:
        """
        Push an item to a priority queue.
        Returns the new size of the queue.
        """
        result = self.script_manager.execute_script(
            'queue_push_priority',
            keys=[queue_key],
            args=[priority, item]
        )
        return int(result)

    def pop_priority_queue(self, queue_key: str, count: int = 1) -> List[str]:
        """
        Pop highest priority items from queue (lowest priority number = highest priority).
        Returns list of popped items.
        """
        result = self.script_manager.execute_script(
            'queue_pop_priority',
            keys=[queue_key],
            args=[count]
        )
        return result if isinstance(result, list) else []

    # Cache Management Methods
    def cache_get_lru(self, cache_key: str, access_times_key: str, miss_counter_key: str) -> Optional[str]:
        """
        Get from cache with LRU tracking and miss counting.
        Returns cached value or None if not found.
        """
        result = self.script_manager.execute_script(
            'cache_get_lru',
            keys=[cache_key, access_times_key, miss_counter_key],
            args=[]
        )
        return result

    def cache_set_with_eviction(self, cache_key: str, value: str, ttl_seconds: int,
                               access_times_key: str, max_size: int = 1000) -> bool:
        """
        Set cache value with LRU eviction.
        Returns True on success.
        """
        result = self.script_manager.execute_script(
            'cache_set_eviction',
            keys=[cache_key, access_times_key],
            args=[value, ttl_seconds, max_size]
        )
        return bool(result)

    # Batch Operations
    def batch_set_with_ttl(self, key_value_pairs: Dict[str, str], ttl_seconds: int) -> int:
        """
        Set multiple keys with the same TTL atomically.
        Returns number of keys set.
        """
        keys = list(key_value_pairs.keys())
        values = [key_value_pairs[k] for k in keys]
        values.append(str(ttl_seconds))

        result = self.script_manager.execute_script(
            'batch_set_ttl',
            keys=keys,
            args=values
        )
        return int(result)

    def batch_delete_if_equals(self, key_value_pairs: Dict[str, str]) -> int:
        """
        Delete multiple keys only if their values match.
        Returns number of keys deleted.
        """
        keys = list(key_value_pairs.keys())
        values = [key_value_pairs[k] for k in keys]

        result = self.script_manager.execute_script(
            'batch_delete_equals',
            keys=keys,
            args=values
        )
        return int(result)

    # Set Operations
    def set_add_multiple(self, set_key: str, items: List[str]) -> int:
        """
        Add multiple items to a set atomically.
        Returns number of new items added.
        """
        result = self.script_manager.execute_script(
            'set_add_multiple',
            keys=[set_key],
            args=items
        )
        return int(result)

    def set_remove_multiple(self, set_key: str, items: List[str]) -> int:
        """
        Remove multiple items from a set (only if they exist).
        Returns number of items removed.
        """
        result = self.script_manager.execute_script(
            'set_remove_multiple',
            keys=[set_key],
            args=items
        )
        return int(result)

    # Bloom Filter Methods
    def bloom_add(self, filter_key: str, item: str, hash_count: int = 3, filter_size: int = 10000) -> bool:
        """
        Add item to bloom filter.
        Returns True if item was newly added, False if already existed.
        """
        result = self.script_manager.execute_script(
            'bloom_add',
            keys=[filter_key],
            args=[item, hash_count, filter_size]
        )
        return bool(result)

    def bloom_check(self, filter_key: str, item: str, hash_count: int = 3, filter_size: int = 10000) -> bool:
        """
        Check if item is possibly in bloom filter.
        Returns True if possibly present, False if definitely not present.
        """
        result = self.script_manager.execute_script(
            'bloom_check',
            keys=[filter_key],
            args=[item, hash_count, filter_size]
        )
        return bool(result)

    # Semaphore Methods
    def semaphore_acquire(self, sem_key: str, permits: int = 1, timeout_seconds: int = 0) -> bool:
        """
        Acquire a permit from semaphore.
        Returns True if permit acquired, False otherwise.
        """
        result = self.script_manager.execute_script(
            'semaphore_acquire',
            keys=[sem_key],
            args=[permits, timeout_seconds]
        )
        return bool(result)

    def semaphore_release(self, sem_key: str, max_permits: int) -> int:
        """
        Release a permit back to semaphore.
        Returns new permit count.
        """
        result = self.script_manager.execute_script(
            'semaphore_release',
            keys=[sem_key],
            args=[max_permits]
        )
        return int(result)

    # Time Series Methods
    def time_series_add(self, series_key: str, timestamp: int, value: str,
                       retention_seconds: Optional[int] = None) -> int:
        """
        Add a data point to time series.
        Returns new size of series.
        """
        args = [timestamp, value]
        if retention_seconds is not None:
            args.append(retention_seconds)

        result = self.script_manager.execute_script(
            'time_series_add',
            keys=[series_key],
            args=args
        )
        return int(result)

    def time_series_range(self, series_key: str, start_time: int, end_time: int,
                         limit: Optional[int] = None) -> List[str]:
        """
        Get time series data within time range.
        Returns list of values.
        """
        args = [start_time, end_time]
        if limit is not None:
            args.append(limit)

        result = self.script_manager.execute_script(
            'time_series_range',
            keys=[series_key],
            args=args
        )
        return result if isinstance(result, list) else []

    # Configuration Methods
    def config_update_atomic(self, config_key: str, updates: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """
        Atomically update configuration fields.
        Returns dict of changed fields with old/new values.
        """
        args = []
        for field, value in updates.items():
            args.extend([field, value])

        result = self.script_manager.execute_script(
            'config_update_atomic',
            keys=[config_key],
            args=args
        )
        return result if isinstance(result, dict) else {}

    # Leader Election Methods
    def leader_heartbeat(self, leader_key: str, candidate_id: str, ttl_seconds: int) -> bool:
        """
        Send heartbeat to maintain leadership.
        Returns True if leadership maintained/extended.
        """
        result = self.script_manager.execute_script(
            'leader_heartbeat',
            keys=[leader_key],
            args=[candidate_id, ttl_seconds]
        )
        return bool(result)

    def check_leader(self, leader_key: str, candidate_id: str) -> bool:
        """
        Check if we are still the leader.
        Returns True if we are the current leader.
        """
        result = self.script_manager.execute_script(
            'leader_check',
            keys=[leader_key],
            args=[candidate_id]
        )
        return bool(result)


class WebSessionManager:
    """
    Web session management with Redis backend.
    Provides session storage, management, and lifecycle operations.
    """

    def __init__(self, redis_client: HighPerformanceRedis, namespace: str = "sessions",
                 default_ttl: int = 1800, cleanup_interval: int = 300):
        self.redis = redis_client
        self.namespace = namespace
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval
        self.script_manager = LuaScriptManager(redis_client)

        # Register session management scripts
        self._register_session_scripts()

    def _register_session_scripts(self):
        """Register Lua scripts for session management"""
        session_create_script = """
        local session_key = KEYS[1]
        local session_data = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local created_at = tonumber(ARGV[3])

        -- Create session with metadata
        redis.call('HMSET', session_key,
                   'data', session_data,
                   'created_at', created_at,
                   'last_accessed', created_at,
                   'access_count', 1)

        -- Set TTL
        redis.call('EXPIRE', session_key, ttl)

        return 1
        """

        session_access_script = """
        local session_key = KEYS[1]
        local ttl = tonumber(ARGV[1])
        local current_time = tonumber(ARGV[2])

        -- Check if session exists
        if redis.call('EXISTS', session_key) == 0 then
            return nil
        end

        -- Update access metadata
        redis.call('HINCRBY', session_key, 'access_count', 1)
        redis.call('HSET', session_key, 'last_accessed', current_time)

        -- Refresh TTL
        redis.call('EXPIRE', session_key, ttl)

        -- Return session data
        return redis.call('HGET', session_key, 'data')
        """

        session_update_script = """
        local session_key = KEYS[1]
        local updates = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local current_time = tonumber(ARGV[3])

        -- Update session data
        redis.call('HSET', session_key, 'data', updates)
        redis.call('HSET', session_key, 'last_accessed', current_time)

        -- Refresh TTL
        redis.call('EXPIRE', session_key, ttl)

        return 1
        """

        self.script_manager.register_script('session_create', session_create_script)
        self.script_manager.register_script('session_access', session_access_script)
        self.script_manager.register_script('session_update', session_update_script)

    def create_session(self, session_id: str, initial_data: Dict[str, Any],
                      ttl_seconds: Optional[int] = None) -> bool:
        """
        Create a new web session.

        Args:
            session_id: Unique session identifier
            initial_data: Initial session data
            ttl_seconds: Session TTL (uses default if None)

        Returns:
            True if session created successfully
        """
        session_key = f"{self.namespace}:{session_id}"
        ttl = ttl_seconds or self.default_ttl
        data_json = json.dumps(initial_data)
        created_at = int(time.time())

        result = self.script_manager.execute_script(
            'session_create',
            keys=[session_key],
            args=[data_json, ttl, created_at]
        )

        return bool(result)

    def get_session(self, session_id: str, extend_ttl: bool = True) -> Optional[Dict[str, Any]]:
        """
        Retrieve session data and optionally extend TTL.

        Args:
            session_id: Session identifier
            extend_ttl: Whether to extend session TTL on access

        Returns:
            Session data dictionary or None if session doesn't exist
        """
        session_key = f"{self.namespace}:{session_id}"
        ttl = self.default_ttl if extend_ttl else 0
        current_time = int(time.time())

        result = self.script_manager.execute_script(
            'session_access',
            keys=[session_key],
            args=[ttl, current_time]
        )

        if result:
            return json.loads(result)
        return None

    def update_session(self, session_id: str, data: Dict[str, Any],
                      ttl_seconds: Optional[int] = None) -> bool:
        """
        Update session data.

        Args:
            session_id: Session identifier
            data: New session data
            ttl_seconds: New TTL (uses default if None)

        Returns:
            True if session updated successfully
        """
        session_key = f"{self.namespace}:{session_id}"
        ttl = ttl_seconds or self.default_ttl
        data_json = json.dumps(data)
        current_time = int(time.time())

        result = self.script_manager.execute_script(
            'session_update',
            keys=[session_key],
            args=[data_json, ttl, current_time]
        )

        return bool(result)

    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session.

        Args:
            session_id: Session identifier

        Returns:
            True if session was deleted
        """
        session_key = f"{self.namespace}:{session_id}"
        return self.redis.delete(session_key)

    def session_exists(self, session_id: str) -> bool:
        """
        Check if a session exists.

        Args:
            session_id: Session identifier

        Returns:
            True if session exists
        """
        session_key = f"{self.namespace}:{session_id}"
        return self.redis.exists(session_key)

    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed session information including metadata.

        Args:
            session_id: Session identifier

        Returns:
            Session info dictionary or None if session doesn't exist
        """
        session_key = f"{self.namespace}:{session_id}"

        # Get all session data
        session_data = self.redis.hgetall(session_key)
        if not session_data:
            return None

        # Parse and return structured info
        return {
            'session_id': session_id,
            'data': json.loads(session_data.get('data', '{}')),
            'created_at': int(session_data.get('created_at', 0)),
            'last_accessed': int(session_data.get('last_accessed', 0)),
            'access_count': int(session_data.get('access_count', 0)),
            'ttl_remaining': self.redis.ttl(session_key)
        }

    def cleanup_expired_sessions(self) -> int:
        """
        Cleanup expired sessions (Redis handles this automatically with TTL,
        but this method can be used for manual cleanup or statistics).

        Returns:
            Number of active sessions (expired ones are auto-cleaned by Redis)
        """
        # Get all session keys (this is a simplified version)
        # In production, you'd want to use SCAN for large datasets
        pattern = f"{self.namespace}:*"
        session_keys = self.redis.keys(pattern)
        return len(session_keys)

    def get_session_stats(self) -> Dict[str, Any]:
        """
        Get session management statistics.

        Returns:
            Statistics dictionary
        """
        pattern = f"{self.namespace}:*"
        session_keys = self.redis.keys(pattern)

        total_sessions = len(session_keys)
        active_sessions = sum(1 for key in session_keys if self.redis.ttl(key) > 0)

        return {
            'total_sessions': total_sessions,
            'active_sessions': active_sessions,
            'expired_sessions': total_sessions - active_sessions,
            'namespace': self.namespace,
            'default_ttl': self.default_ttl
        }


class XATransactionManager:
    """
    XA Transaction Manager for distributed transactions.
    Provides two-phase commit protocol support.
    """

    def __init__(self, redis_client: HighPerformanceRedis, transaction_timeout: int = 300):
        self.redis = redis_client
        self.transaction_timeout = transaction_timeout
        self.script_manager = LuaScriptManager(redis_client)
        self._register_xa_scripts()

    def _register_xa_scripts(self):
        """Register XA transaction scripts"""
        xa_prepare_script = """
        local tx_key = KEYS[1]
        local participant_key = KEYS[2]
        local operations = ARGV[1]
        local tx_id = ARGV[2]
        local prepared_at = tonumber(ARGV[3])

        -- Store transaction operations
        redis.call('HMSET', tx_key,
                   'status', 'prepared',
                   'operations', operations,
                   'prepared_at', prepared_at,
                   'tx_id', tx_id)

        -- Add to prepared transactions set
        redis.call('SADD', 'xa:prepared', tx_key)

        -- Set expiration
        redis.call('EXPIRE', tx_key, 3600)  -- 1 hour timeout

        return 1
        """

        xa_commit_script = """
        local tx_key = KEYS[1]
        local participant_key = KEYS[2]
        local committed_at = tonumber(ARGV[1])

        -- Check transaction status
        local status = redis.call('HGET', tx_key, 'status')
        if status ~= 'prepared' then
            return {error='Transaction not in prepared state', status=status}
        end

        -- Get operations
        local operations = redis.call('HGET', tx_key, 'operations')
        if not operations then
            return {error='No operations found for transaction'}
        end

        -- Execute operations (simplified - in real XA, this would be more complex)
        -- For this demo, we'll just mark as committed
        redis.call('HMSET', tx_key,
                   'status', 'committed',
                   'committed_at', committed_at)

        -- Remove from prepared set
        redis.call('SREM', 'xa:prepared', tx_key)

        -- Add to committed set
        redis.call('SADD', 'xa:committed', tx_key)

        return {status='committed', operations=operations}
        """

        xa_rollback_script = """
        local tx_key = KEYS[1]
        local participant_key = KEYS[2]
        local rolled_back_at = tonumber(ARGV[1])

        -- Mark as rolled back
        redis.call('HMSET', tx_key,
                   'status', 'rolled_back',
                   'rolled_back_at', rolled_back_at)

        -- Remove from prepared set
        redis.call('SREM', 'xa:prepared', tx_key)

        -- Add to rolled back set
        redis.call('SADD', 'xa:rolled_back', tx_key)

        return {status='rolled_back'}
        """

        self.script_manager.register_script('xa_prepare', xa_prepare_script)
        self.script_manager.register_script('xa_commit', xa_commit_script)
        self.script_manager.register_script('xa_rollback', xa_rollback_script)

    def begin_transaction(self, tx_id: str) -> str:
        """
        Begin a new XA transaction.

        Args:
            tx_id: Unique transaction identifier

        Returns:
            Transaction ID
        """
        # Create transaction key
        tx_key = f"xa:tx:{tx_id}"

        # Initialize transaction
        self.redis.hset(tx_key, {
            'status': 'active',
            'started_at': int(time.time()),
            'tx_id': tx_id
        })

        self.redis.expire(tx_key, self.transaction_timeout)

        return tx_id

    def prepare_transaction(self, tx_id: str, operations: List[Dict[str, Any]]) -> bool:
        """
        Prepare a transaction for commit (Phase 1 of XA).

        Args:
            tx_id: Transaction identifier
            operations: List of operations to prepare

        Returns:
            True if prepared successfully
        """
        tx_key = f"xa:tx:{tx_id}"
        participant_key = f"xa:participant:{tx_id}"

        # Check transaction status
        status = self.redis.hget(tx_key, 'status')
        if status != 'active':
            raise ValueError(f"Transaction {tx_id} is not in active state (status: {status})")

        # Serialize operations
        operations_json = json.dumps(operations)
        prepared_at = int(time.time())

        result = self.script_manager.execute_script(
            'xa_prepare',
            keys=[tx_key, participant_key],
            args=[operations_json, tx_id, prepared_at]
        )

        return bool(result)

    def commit_transaction(self, tx_id: str) -> Dict[str, Any]:
        """
        Commit a prepared transaction (Phase 2 of XA).

        Args:
            tx_id: Transaction identifier

        Returns:
            Commit result information
        """
        tx_key = f"xa:tx:{tx_id}"
        participant_key = f"xa:participant:{tx_id}"
        committed_at = int(time.time())

        result = self.script_manager.execute_script(
            'xa_commit',
            keys=[tx_key, participant_key],
            args=[committed_at]
        )

        if isinstance(result, dict) and 'error' in result:
            raise RuntimeError(f"Commit failed: {result['error']}")

        return result or {}

    def rollback_transaction(self, tx_id: str) -> Dict[str, Any]:
        """
        Rollback a transaction.

        Args:
            tx_id: Transaction identifier

        Returns:
            Rollback result information
        """
        tx_key = f"xa:tx:{tx_id}"
        participant_key = f"xa:participant:{tx_id}"
        rolled_back_at = int(time.time())

        result = self.script_manager.execute_script(
            'xa_rollback',
            keys=[tx_key, participant_key],
            args=[rolled_back_at]
        )

        return result or {}

    def get_transaction_status(self, tx_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction status and information.

        Args:
            tx_id: Transaction identifier

        Returns:
            Transaction information or None if not found
        """
        tx_key = f"xa:tx:{tx_id}"
        tx_data = self.redis.hgetall(tx_key)

        if not tx_data:
            return None

        return {
            'tx_id': tx_data.get('tx_id'),
            'status': tx_data.get('status'),
            'started_at': int(tx_data.get('started_at', 0)),
            'prepared_at': int(tx_data.get('prepared_at', 0)) if tx_data.get('prepared_at') else None,
            'committed_at': int(tx_data.get('committed_at', 0)) if tx_data.get('committed_at') else None,
            'rolled_back_at': int(tx_data.get('rolled_back_at', 0)) if tx_data.get('rolled_back_at') else None,
            'operations': json.loads(tx_data.get('operations', '[]')) if tx_data.get('operations') else []
        }

    def list_prepared_transactions(self) -> List[str]:
        """
        List all prepared transactions.

        Returns:
            List of transaction IDs in prepared state
        """
        prepared_keys = self.redis.smembers('xa:prepared')
        return [key.split(':')[-1] for key in prepared_keys]

    def recover_transactions(self) -> Dict[str, List[str]]:
        """
        Recover transactions that need completion after failure.

        Returns:
            Dictionary with transaction IDs by recovery action needed
        """
        prepared = self.redis.smembers('xa:prepared')
        committed = self.redis.smembers('xa:committed')
        rolled_back = self.redis.smembers('xa:rolled_back')

        return {
            'prepared': [key.split(':')[-1] for key in prepared],
            'committed': [key.split(':')[-1] for key in committed],
            'rolled_back': [key.split(':')[-1] for key in rolled_back]
        }


class ObservabilityManager:
    """
    Observability and monitoring for FastRedis operations.
    Provides metrics, health checks, and performance monitoring.
    """

    def __init__(self, redis_client: HighPerformanceRedis, metrics_prefix: str = "fastredis"):
        self.redis = redis_client
        self.metrics_prefix = metrics_prefix
        self.start_time = time.time()

    def record_operation(self, operation: str, duration_ms: float, success: bool = True,
                        metadata: Optional[Dict[str, Any]] = None):
        """
        Record operation metrics.

        Args:
            operation: Operation name (e.g., 'get', 'set', 'lua_script')
            duration_ms: Operation duration in milliseconds
            success: Whether operation was successful
            metadata: Additional operation metadata
        """
        current_time = int(time.time())

        # Record operation count
        op_count_key = f"{self.metrics_prefix}:operations:{operation}:count"
        self.redis.incr(op_count_key)

        # Record success/failure
        status = 'success' if success else 'failure'
        status_key = f"{self.metrics_prefix}:operations:{operation}:{status}"
        self.redis.incr(status_key)

        # Record duration histogram (simplified)
        duration_key = f"{self.metrics_prefix}:operations:{operation}:duration_ms"
        self.redis.lpush(duration_key, duration_ms)
        self.redis.ltrim(duration_key, 0, 999)  # Keep last 1000 durations

        # Record latency percentiles (simplified - just min/max/avg)
        stats_key = f"{self.metrics_prefix}:operations:{operation}:stats"

        # Get current stats
        current_stats = self.redis.hgetall(stats_key) or {}

        min_duration = min(float(current_stats.get('min_duration', duration_ms)), duration_ms)
        max_duration = max(float(current_stats.get('max_duration', duration_ms)), duration_ms)

        # Calculate new average (simplified)
        total_ops = int(current_stats.get('total_operations', 0)) + 1
        current_avg = float(current_stats.get('avg_duration', 0))
        new_avg = ((current_avg * (total_ops - 1)) + duration_ms) / total_ops

        # Update stats
        self.redis.hset(stats_key, {
            'min_duration': min_duration,
            'max_duration': max_duration,
            'avg_duration': new_avg,
            'total_operations': total_ops,
            'last_updated': current_time
        })

        # Set expiration (7 days)
        self.redis.expire(stats_key, 604800)
        self.redis.expire(op_count_key, 604800)
        self.redis.expire(status_key, 604800)
        self.redis.expire(duration_key, 604800)

    def get_operation_metrics(self, operation: str) -> Dict[str, Any]:
        """
        Get metrics for a specific operation.

        Args:
            operation: Operation name

        Returns:
            Metrics dictionary
        """
        base_key = f"{self.metrics_prefix}:operations:{operation}"

        # Get basic counts
        total_count = self.redis.get(f"{base_key}:count") or 0
        success_count = self.redis.get(f"{base_key}:success") or 0
        failure_count = self.redis.get(f"{base_key}:failure") or 0

        # Get stats
        stats = self.redis.hgetall(f"{base_key}:stats") or {}

        # Calculate success rate
        total = int(total_count)
        success_rate = (int(success_count) / total * 100) if total > 0 else 0

        return {
            'operation': operation,
            'total_operations': total,
            'successful_operations': int(success_count),
            'failed_operations': int(failure_count),
            'success_rate_percent': round(success_rate, 2),
            'min_duration_ms': float(stats.get('min_duration', 0)),
            'max_duration_ms': float(stats.get('max_duration', 0)),
            'avg_duration_ms': float(stats.get('avg_duration', 0)),
            'last_updated': int(stats.get('last_updated', 0))
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get all available metrics.

        Returns:
            Comprehensive metrics dictionary
        """
        # Get all operation keys
        pattern = f"{self.metrics_prefix}:operations:*:count"
        operation_keys = self.redis.keys(pattern)

        operations = {}
        for key in operation_keys:
            # Extract operation name from key
            parts = key.split(':')
            if len(parts) >= 4:
                operation = parts[3]  # operations:{operation}:count
                operations[operation] = self.get_operation_metrics(operation)

        # System metrics
        uptime_seconds = time.time() - self.start_time

        # Redis connection info
        try:
            info = self.redis.info('server')
            redis_version = info.get('redis_version', 'unknown')
            redis_uptime = info.get('uptime_in_seconds', 0)
        except:
            redis_version = 'unknown'
            redis_uptime = 0

        return {
            'system': {
                'uptime_seconds': int(uptime_seconds),
                'redis_version': redis_version,
                'redis_uptime_seconds': int(redis_uptime),
                'metrics_prefix': self.metrics_prefix
            },
            'operations': operations
        }

    def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check.

        Returns:
            Health check results
        """
        health_status = {'status': 'healthy', 'checks': {}}

        try:
            # Redis connectivity check
            pong = self.redis.ping()
            health_status['checks']['redis_connection'] = {
                'status': 'healthy' if pong else 'unhealthy',
                'response': pong
            }

            # Memory usage check
            memory_info = self.redis.info('memory')
            used_memory = int(memory_info.get('used_memory', 0))
            max_memory = int(memory_info.get('maxmemory', 0) or 0)

            memory_status = 'healthy'
            if max_memory > 0 and used_memory > max_memory * 0.9:  # >90% memory usage
                memory_status = 'warning'
            elif max_memory > 0 and used_memory > max_memory * 0.95:  # >95% memory usage
                memory_status = 'critical'

            health_status['checks']['memory_usage'] = {
                'status': memory_status,
                'used_memory_bytes': used_memory,
                'max_memory_bytes': max_memory,
                'usage_percent': round(used_memory / max_memory * 100, 2) if max_memory > 0 else 0
            }

            # Connection count check
            clients_info = self.redis.info('clients')
            connected_clients = int(clients_info.get('connected_clients', 0))
            max_clients = int(clients_info.get('maxclients', 10000))

            client_status = 'healthy'
            if connected_clients > max_clients * 0.8:  # >80% of max connections
                client_status = 'warning'

            health_status['checks']['client_connections'] = {
                'status': client_status,
                'connected_clients': connected_clients,
                'max_clients': max_clients,
                'usage_percent': round(connected_clients / max_clients * 100, 2)
            }

        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['error'] = str(e)

        # Overall status
        if any(check.get('status') in ['warning', 'critical', 'unhealthy']
               for check in health_status['checks'].values()):
            if any(check.get('status') in ['critical', 'unhealthy']
                   for check in health_status['checks'].values()):
                health_status['status'] = 'unhealthy'
            else:
                health_status['status'] = 'warning'

        return health_status

    def reset_metrics(self, operation: Optional[str] = None):
        """
        Reset metrics for all operations or a specific operation.

        Args:
            operation: Specific operation to reset (None for all)
        """
        if operation:
            # Reset specific operation metrics
            pattern = f"{self.metrics_prefix}:operations:{operation}:*"
            keys_to_delete = self.redis.keys(pattern)
            if keys_to_delete:
                self.redis.delete(*keys_to_delete)
        else:
            # Reset all metrics
            pattern = f"{self.metrics_prefix}:*"
            keys_to_delete = self.redis.keys(pattern)
            if keys_to_delete:
                self.redis.delete(*keys_to_delete)


class SecurityManager:
    """
    Security features for FastRedis including password encryption.
    """

    def __init__(self, redis_client: HighPerformanceRedis, encryption_key: Optional[str] = None):
        self.redis = redis_client
        self.encryption_key = encryption_key or "default_fastredis_key_change_in_production"
        self.script_manager = LuaScriptManager(redis_client)

    def encrypt_password(self, password: str) -> str:
        """
        Encrypt a password using simple encryption (in production, use proper encryption).

        Args:
            password: Plain text password

        Returns:
            Encrypted password
        """
        # This is a simplified example - use proper encryption in production
        import base64
        import hashlib

        # Create a simple hash-based encryption
        key_hash = hashlib.sha256(self.encryption_key.encode()).digest()
        password_bytes = password.encode()

        # XOR encryption (very basic - use proper encryption in production!)
        encrypted = bytes(a ^ b for a, b in zip(password_bytes, key_hash[:len(password_bytes)]))

        return base64.b64encode(encrypted).decode()

    def decrypt_password(self, encrypted_password: str) -> str:
        """
        Decrypt a password.

        Args:
            encrypted_password: Encrypted password

        Returns:
            Plain text password
        """
        import base64
        import hashlib

        # Reverse the encryption process
        key_hash = hashlib.sha256(self.encryption_key.encode()).digest()
        encrypted_bytes = base64.b64decode(encrypted_password)

        # XOR decryption
        decrypted = bytes(a ^ b for a, b in zip(encrypted_bytes, key_hash[:len(encrypted_bytes)]))

        return decrypted.decode()

    def store_encrypted_password(self, service: str, username: str, password: str):
        """
        Store an encrypted password for a service.

        Args:
            service: Service name
            username: Username
            password: Plain text password
        """
        encrypted = self.encrypt_password(password)
        key = f"security:passwords:{service}:{username}"

        self.redis.hset(key, {
            'encrypted_password': encrypted,
            'created_at': int(time.time()),
            'updated_at': int(time.time())
        })

    def get_decrypted_password(self, service: str, username: str) -> Optional[str]:
        """
        Retrieve and decrypt a password for a service.

        Args:
            service: Service name
            username: Username

        Returns:
            Plain text password or None if not found
        """
        key = f"security:passwords:{service}:{username}"
        data = self.redis.hgetall(key)

        if data and 'encrypted_password' in data:
            return self.decrypt_password(data['encrypted_password'])

        return None

    def validate_access(self, resource: str, user: str, permission: str) -> bool:
        """
        Validate if a user has permission for a resource.

        Args:
            resource: Resource identifier
            user: User identifier
            permission: Required permission (read/write/admin)

        Returns:
            True if access is granted
        """
        # Check user permissions
        user_perms_key = f"security:permissions:{user}"
        user_permissions = self.redis.smembers(user_perms_key)

        # Check resource ACL
        resource_acl_key = f"security:acl:{resource}"
        resource_permissions = self.redis.hgetall(resource_acl_key)

        # Check if user has required permission
        if permission in user_permissions:
            return True

        # Check resource-specific permissions
        allowed_users = resource_permissions.get(permission, '').split(',')
        if user in allowed_users:
            return True

        return False

    def audit_log(self, action: str, user: str, resource: str, success: bool,
                  details: Optional[Dict[str, Any]] = None):
        """
        Log security-related actions for audit purposes.

        Args:
            action: Action performed
            user: User who performed the action
            resource: Resource affected
            success: Whether action was successful
            details: Additional audit details
        """
        audit_entry = {
            'timestamp': int(time.time()),
            'action': action,
            'user': user,
            'resource': resource,
            'success': success,
            'details': json.dumps(details or {}),
            'ip_address': 'unknown',  # Would be populated in real implementation
            'user_agent': 'unknown'   # Would be populated in real implementation
        }

        # Store in audit log
        audit_key = f"security:audit:{int(time.time() // 86400)}"  # Daily log
        self.redis.lpush(audit_key, json.dumps(audit_entry))

        # Keep only last 1000 entries per day
        self.redis.ltrim(audit_key, 0, 999)

        # Set expiration (90 days)
        self.redis.expire(audit_key, 7776000)


class LuaScriptManager:
    """
    Advanced Lua script management with caching, versioning, and debugging.
    """

    def __init__(self, redis: HighPerformanceRedis, namespace: str = "scripts"):
        self.redis = redis
        self.namespace = namespace
        self.loaded_scripts = {}  # script_name -> sha
        self.script_versions = {}  # script_name -> version
        self.script_sources = {}   # script_name -> source_code

    def register_script(self, name: str, script: str, version: str = "1.0.0") -> str:
        """Register a script with versioning"""
        # Check if script already exists with same version
        existing_version = self.redis.hget(f"{self.namespace}:versions", name)
        if existing_version == version:
            # Script already loaded, get SHA
            sha = self.redis.hget(f"{self.namespace}:shas", name)
            if sha:
                self.loaded_scripts[name] = sha
                self.script_versions[name] = version
                self.script_sources[name] = script
                return sha

        # Load new script
        try:
            sha = self.redis.script_load(script)

            # Store metadata
            self.redis.hset(f"{self.namespace}:shas", name, sha)
            self.redis.hset(f"{self.namespace}:versions", name, version)
            self.redis.hset(f"{self.namespace}:sources", name, script)

            self.loaded_scripts[name] = sha
            self.script_versions[name] = version
            self.script_sources[name] = script

            return sha
        except Exception as e:
            raise Exception(f"Failed to register script {name}: {e}")

    def execute_script(self, name: str, keys: Optional[List[str]] = None,
                      args: Optional[List[str]] = None) -> Any:
        """Execute a registered script"""
        if name not in self.loaded_scripts:
            raise Exception(f"Script {name} not registered")

        sha = self.loaded_scripts[name]
        return self.redis.evalsha(sha, keys, args)

    def get_script_info(self, name: str) -> Dict[str, Any]:
        """Get information about a registered script"""
        if name not in self.loaded_scripts:
            return {}

        return {
            'name': name,
            'sha': self.loaded_scripts[name],
            'version': self.script_versions.get(name),
            'source_length': len(self.script_sources.get(name, "")),
            'exists_in_cache': self.redis.script_exists([self.loaded_scripts[name]])[0] == 1
        }

    def list_scripts(self) -> Dict[str, Dict[str, Any]]:
        """List all registered scripts"""
        scripts = {}
        for name in self.loaded_scripts.keys():
            scripts[name] = self.get_script_info(name)
        return scripts

    def reload_all_scripts(self) -> Dict[str, str]:
        """Reload all registered scripts (useful after Redis restart)"""
        results = {}
        for name, source in self.script_sources.items():
            try:
                new_sha = self.redis.script_load(source)
                self.loaded_scripts[name] = new_sha
                self.redis.hset(f"{self.namespace}:shas", name, new_sha)
                results[name] = f"Reloaded: {new_sha}"
            except Exception as e:
                results[name] = f"Failed: {e}"

        return results

    def debug_script(self, script: str, keys: Optional[List[str]] = None,
                    args: Optional[List[str]] = None) -> Any:
        """Debug a script execution"""
        # Enable debug mode
        self.redis.script_debug('YES')

        try:
            return self.redis.execute_script(script, keys, args, use_cache=False)
        finally:
            # Disable debug mode
            self.redis.script_debug('NO')

    def validate_script(self, script: str) -> Dict[str, Any]:
        """Validate script syntax and check for issues"""
        try:
            # Try to load the script
            sha = self.redis.script_load(script)

            # Check if it's already cached
            exists = self.redis.script_exists([sha])[0]

            # Try to execute with dummy data to check for runtime errors
            test_result = None
            try:
                test_result = self.redis.evalsha(sha, [], [])
            except Exception as e:
                runtime_error = str(e)
            else:
                runtime_error = None

            return {
                'valid': True,
                'sha': sha,
                'cached': bool(exists),
                'runtime_error': runtime_error,
                'length': len(script)
            }

        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'length': len(script)
            }

    def cleanup_scripts(self, confirm: bool = False) -> Dict[str, Any]:
        """Clean up script cache and metadata"""
        if not confirm:
            return {
                'error': 'Confirmation required. Set confirm=True to proceed.',
                'scripts_to_remove': list(self.loaded_scripts.keys())
            }

        # Flush script cache
        self.redis.script_flush()

        # Clear local cache
        self.loaded_scripts.clear()
        self.script_versions.clear()
        self.script_sources.clear()

        # Remove metadata from Redis
        self.redis.delete(f"{self.namespace}:shas")
        self.redis.delete(f"{self.namespace}:versions")
        self.redis.delete(f"{self.namespace}:sources")

        return {
            'message': 'All scripts cleaned up',
            'flushed_cache': True,
            'cleared_metadata': True
        }


class RedisClusterManager:
    """
    Redis Cluster management and monitoring utilities.
    """

    def __init__(self, redis: HighPerformanceRedis):
        self.redis = redis

    def get_cluster_topology(self) -> Dict[str, Any]:
        """Get complete cluster topology information"""
        try:
            slots = self.redis.cluster_slots()
            nodes = self.redis.cluster_nodes()
            info = self.redis.cluster_info()

            # Parse nodes information
            node_list = []
            if nodes:
                for line in nodes.strip().split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 8:
                            node_info = {
                                'id': parts[0],
                                'endpoint': parts[1],
                                'flags': parts[2],
                                'master': parts[3] if parts[3] != '-' else None,
                                'ping_sent': parts[4],
                                'pong_recv': parts[5],
                                'config_epoch': parts[6],
                                'link_state': parts[7],
                            }
                            node_list.append(node_info)

            return {
                'slots': slots,
                'nodes': node_list,
                'info': self._parse_cluster_info(info),
                'healthy': self._check_cluster_health(node_list, slots)
            }

        except Exception as e:
            return {
                'error': str(e),
                'available': False
            }

    def _parse_cluster_info(self, info_str: str) -> Dict[str, Any]:
        """Parse CLUSTER INFO output"""
        info = {}
        if info_str:
            for line in info_str.strip().split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    info[key.strip()] = value.strip()
        return info

    def _check_cluster_health(self, nodes: List[Dict], slots: List) -> bool:
        """Check if cluster is healthy"""
        if not nodes or not slots:
            return False

        # Check if we have at least one master
        masters = [n for n in nodes if 'master' in n.get('flags', '').lower()]
        if not masters:
            return False

        # Check if slots are covered
        covered_slots = set()
        for slot_range in slots:
            if len(slot_range) >= 3:
                start_slot, end_slot = slot_range[0], slot_range[1]
                for slot in range(start_slot, end_slot + 1):
                    covered_slots.add(slot)

        # Redis cluster has 16384 slots
        return len(covered_slots) == 16384

    def get_slot_distribution(self) -> Dict[str, Any]:
        """Get slot distribution across cluster nodes"""
        slots = self.redis.cluster_slots()
        distribution = {}

        for slot_range in slots:
            if len(slot_range) >= 3:
                start_slot, end_slot = slot_range[0], slot_range[1]
                master_node = slot_range[2][0] if len(slot_range) > 2 and slot_range[2] else 'unknown'

                if master_node not in distribution:
                    distribution[master_node] = {
                        'slot_ranges': [],
                        'total_slots': 0
                    }

                slot_count = end_slot - start_slot + 1
                distribution[master_node]['slot_ranges'].append((start_slot, end_slot))
                distribution[master_node]['total_slots'] += slot_count

        return distribution

    def find_node_for_key(self, key: str) -> Optional[Dict[str, Any]]:
        """Find which cluster node handles a given key"""
        try:
            slot = self.redis.cluster_keyslot(key)
            slots = self.redis.cluster_slots()

            for slot_range in slots:
                if len(slot_range) >= 3:
                    start_slot, end_slot = slot_range[0], slot_range[1]
                    if start_slot <= slot <= end_slot:
                        master_info = slot_range[2][0] if len(slot_range) > 2 and slot_range[2] else None
                        if master_info:
                            return {
                                'slot': slot,
                                'node': master_info,
                                'slot_range': (start_slot, end_slot)
                            }

        except Exception:
            pass

        return None

    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get comprehensive cluster statistics"""
        try:
            topology = self.get_cluster_topology()
            distribution = self.get_slot_distribution()

            stats = {
                'topology': topology,
                'slot_distribution': distribution,
                'total_nodes': len(topology.get('nodes', [])),
                'total_masters': len([n for n in topology.get('nodes', [])
                                    if 'master' in n.get('flags', '').lower()]),
                'cluster_state': topology.get('info', {}).get('cluster_state'),
                'slots_assigned': topology.get('info', {}).get('cluster_slots_assigned', '0'),
                'slots_ok': topology.get('info', {}).get('cluster_slots_ok', '0'),
            }

            return stats

        except Exception as e:
            return {
                'error': str(e),
                'available': False
            }

    def rebalance_slots(self, target_node: str, num_slots: int = 100) -> Dict[str, Any]:
        """Rebalance slots to target node (basic implementation)"""
        # Note: This is a simplified version. Real rebalancing requires
        # careful coordination across the cluster
        try:
            current_slots = self.get_slot_distribution()

            # Find source nodes with excess slots
            source_candidates = []
            for node, info in current_slots.items():
                if node != target_node and info['total_slots'] > 0:
                    source_candidates.append((node, info['total_slots']))

            if not source_candidates:
                return {'error': 'No source nodes with slots found'}

            # This would require actual slot migration commands
            # For now, just return analysis
            return {
                'target_node': target_node,
                'requested_slots': num_slots,
                'source_candidates': source_candidates,
                'note': 'Actual rebalancing requires cluster administration commands'
            }

        except Exception as e:
            return {'error': str(e)}

    def monitor_cluster_health(self, interval: int = 30) -> None:
        """Monitor cluster health continuously"""
        print("Starting cluster health monitoring (Ctrl+C to stop)...")

        try:
            while True:
                stats = self.get_cluster_stats()

                if stats.get('available', False):
                    state = stats.get('cluster_state', 'unknown')
                    nodes = stats.get('total_nodes', 0)
                    masters = stats.get('total_masters', 0)

                    status = "✓" if state == 'ok' else "✗"
                    print(f"{status} Cluster: {state}, Nodes: {nodes}, Masters: {masters}")
                else:
                    print("✗ Cluster not available")

                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nCluster monitoring stopped.")


class RedisSentinelManager:
    """
    Redis Sentinel management and monitoring utilities.
    """

    def __init__(self, redis: HighPerformanceRedis):
        self.redis = redis

    def get_sentinel_overview(self) -> Dict[str, Any]:
        """Get overview of all monitored Redis instances"""
        try:
            masters = self.redis.sentinel_masters()

            overview = {
                'masters': [],
                'total_masters': len(masters) if masters else 0
            }

            if masters:
                for master in masters:
                    if isinstance(master, dict):
                        master_name = master.get('name', 'unknown')
                        master_info = self.redis.sentinel_master(master_name)

                        slaves = self.redis.sentinel_slaves(master_name)
                        sentinels = self.redis.sentinel_sentinels(master_name)

                        overview['masters'].append({
                            'name': master_name,
                            'info': master_info,
                            'slaves_count': len(slaves) if slaves else 0,
                            'sentinels_count': len(sentinels) if sentinels else 0,
                            'slaves': slaves,
                            'sentinels': sentinels
                        })

            return overview

        except Exception as e:
            return {
                'error': str(e),
                'available': False
            }

    def failover_master(self, master_name: str) -> Dict[str, Any]:
        """Trigger manual failover for a master"""
        try:
            result = self.redis.sentinel_failover(master_name)
            return {
                'success': True,
                'result': result,
                'master': master_name
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'master': master_name
            }

    def get_master_address(self, master_name: str) -> Optional[Tuple[str, int]]:
        """Get current master address for a named instance"""
        try:
            addr = self.redis.sentinel_get_master_addr_by_name(master_name)
            if addr and len(addr) >= 2:
                return (addr[0], int(addr[1]))
        except Exception:
            pass
        return None

    def monitor_master(self, name: str, host: str, port: int, quorum: int = 2) -> Dict[str, Any]:
        """Configure Sentinel to monitor a new Redis master"""
        try:
            result = self.redis.sentinel_monitor(name, host, port, quorum)
            return {
                'success': True,
                'result': result,
                'master': name,
                'address': f"{host}:{port}",
                'quorum': quorum
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def update_sentinel_config(self, master_name: str, configs: Dict[str, str]) -> Dict[str, Any]:
        """Update Sentinel configuration for a master"""
        results = {}

        for option, value in configs.items():
            try:
                result = self.redis.sentinel_set(master_name, option, value)
                results[option] = {
                    'success': True,
                    'result': result
                }
            except Exception as e:
                results[option] = {
                    'success': False,
                    'error': str(e)
                }

        return {
            'master': master_name,
            'configs_updated': results
        }

    def get_failover_status(self, master_name: str) -> Dict[str, Any]:
        """Get detailed failover status for a master"""
        try:
            master_info = self.redis.sentinel_master(master_name)
            slaves = self.redis.sentinel_slaves(master_name)

            # Analyze slave status
            slave_status = []
            if slaves:
                for slave in slaves:
                    if isinstance(slave, dict):
                        slave_status.append({
                            'address': f"{slave.get('ip', 'unknown')}:{slave.get('port', 'unknown')}",
                            'status': 'online' if slave.get('flags', '').find('s_down') == -1 else 'down',
                            'lag': slave.get('slave_repl_offset', 0)
                        })

            return {
                'master': master_name,
                'master_info': master_info,
                'slaves': slave_status,
                'quorum': master_info.get('quorum', 'unknown') if isinstance(master_info, dict) else 'unknown',
                'failover_state': master_info.get('master_failover_state', 'unknown') if isinstance(master_info, dict) else 'unknown'
            }

        except Exception as e:
            return {
                'error': str(e),
                'master': master_name
            }


# Distributed Locks and Synchronizers
class DistributedLock:
    """
    Distributed lock using Redis for synchronization across multiple processes/nodes.
    Similar to Redisson's distributed locks.
    """

    def __init__(self, redis: HighPerformanceRedis, name: str, lease_time: int = 30000):
        self.redis = redis
        self.name = f"lock:{name}"
        self.lease_time = lease_time
        self.lock_value = None

    def try_lock(self, wait_time: int = 0, lease_time: Optional[int] = None) -> bool:
        """Try to acquire the lock"""
        if lease_time is None:
            lease_time = self.lease_time

        lock_value = str(time.time()) + ":" + str(threading.get_ident())

        # Try to acquire lock
        result = self.redis.set(self.name, lock_value, px=lease_time, nx=True)
        if result == 'OK':
            self.lock_value = lock_value
            return True

        # If wait_time > 0, wait and retry
        if wait_time > 0:
            start_time = time.time()
            while time.time() - start_time < wait_time / 1000.0:
                time.sleep(0.01)  # Small sleep to avoid busy waiting
                result = self.redis.set(self.name, lock_value, px=lease_time, nx=True)
                if result == 'OK':
                    self.lock_value = lock_value
                    return True

        return False

    def lock(self, lease_time: Optional[int] = None):
        """Acquire the lock (blocking)"""
        while not self.try_lock(100, lease_time):  # Wait 100ms and retry
            pass

    def unlock(self) -> bool:
        """Release the lock"""
        if self.lock_value is None:
            return False

        # Use Lua script for atomic unlock
        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """

        try:
            result = self.redis.eval(script, keys=[self.name], args=[self.lock_value])
            if result == 1:
                self.lock_value = None
                return True
        except Exception:
            pass

        return False

    def is_locked(self) -> bool:
        """Check if lock is held"""
        return self.redis.exists(self.name) == 1

    def force_unlock(self) -> bool:
        """Force unlock (dangerous - use with caution)"""
        result = self.redis.delete(self.name)
        self.lock_value = None
        return result == 1


class ReadWriteLock:
    """
    Distributed read-write lock using Redis.
    Multiple readers can hold the lock simultaneously, but only one writer.
    """

    def __init__(self, redis: HighPerformanceRedis, name: str, lease_time: int = 30000):
        self.redis = redis
        self.name = f"rwlock:{name}"
        self.lease_time = lease_time
        self.read_lock_count = 0
        self.write_lock_value = None

    def read_lock(self) -> bool:
        """Acquire read lock"""
        # Increment read lock count atomically
        script = """
        local count = redis.call('incr', KEYS[1])
        if count == 1 then
            redis.call('pexpire', KEYS[1], ARGV[1])
        end
        return count
        """

        try:
            result = self.redis.eval(script, keys=[f"{self.name}:readers"],
                                   args=[self.lease_time])
            self.read_lock_count = result
            return True
        except Exception:
            return False

    def read_unlock(self) -> bool:
        """Release read lock"""
        if self.read_lock_count == 0:
            return False

        script = """
        local count = redis.call('decr', KEYS[1])
        if count == 0 then
            redis.call('del', KEYS[1])
        else
            redis.call('pexpire', KEYS[1], ARGV[1])
        end
        return count
        """

        try:
            result = self.redis.eval(script, keys=[f"{self.name}:readers"],
                                   args=[self.lease_time])
            self.read_lock_count = result
            return True
        except Exception:
            return False

    def write_lock(self) -> bool:
        """Acquire write lock"""
        lock_value = str(time.time()) + ":" + str(threading.get_ident())
        result = self.redis.set(f"{self.name}:writer", lock_value,
                              px=self.lease_time, nx=True)
        if result == 'OK':
            self.write_lock_value = lock_value
            return True
        return False

    def write_unlock(self) -> bool:
        """Release write lock"""
        if self.write_lock_value is None:
            return False

        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """

        try:
            result = self.redis.eval(script, keys=[f"{self.name}:writer"],
                                   args=[self.write_lock_value])
            if result == 1:
                self.write_lock_value = None
                return True
        except Exception:
            pass

        return False


class Semaphore:
    """
    Distributed semaphore using Redis.
    Limits the number of concurrent operations.
    """

    def __init__(self, redis: HighPerformanceRedis, name: str, permits: int = 1):
        self.redis = redis
        self.name = f"semaphore:{name}"
        self.permits = permits

    def acquire(self) -> bool:
        """Acquire a permit"""
        current = self.redis.incr(f"{self.name}:counter")
        if current <= self.permits:
            return True
        else:
            # Too many permits, decrement back
            self.redis.decr(f"{self.name}:counter")
            return False

    def release(self) -> bool:
        """Release a permit"""
        current = self.redis.decr(f"{self.name}:counter")
        return current >= 0

    def available_permits(self) -> int:
        """Get number of available permits"""
        used = int(self.redis.get(f"{self.name}:counter") or 0)
        return max(0, self.permits - used)


class DistributedCounter:
    """
    Distributed atomic counter using Redis.
    """

    def __init__(self, redis: HighPerformanceRedis, name: str, initial_value: int = 0):
        self.redis = redis
        self.name = f"counter:{name}"
        if not self.redis.exists(self.name):
            self.redis.set(self.name, initial_value)

    def get(self) -> int:
        """Get current value"""
        return int(self.redis.get(self.name) or 0)

    def set(self, value: int):
        """Set value"""
        self.redis.set(self.name, value)

    def increment(self) -> int:
        """Increment and return new value"""
        return self.redis.incr(self.name)

    def decrement(self) -> int:
        """Decrement and return new value"""
        return self.redis.decr(self.name)

    def add(self, delta: int) -> int:
        """Add delta and return new value"""
        return self.redis.incrby(self.name, delta)


class LocalCache:
    """
    Local cache with Redis backing (similar to Redisson's local cache).
    Implements TTL, size limits, and automatic sync with Redis.
    """

    def __init__(self, redis: HighPerformanceRedis, namespace: str = "",
                 max_size: int = 1000, ttl: int = 300, sync_interval: int = 30):
        self.redis = redis
        self.namespace = namespace
        self.max_size = max_size
        self.ttl = ttl
        self.sync_interval = sync_interval
        self.cache = {}  # key -> (value, timestamp)
        self.last_sync = time.time()
        self.lock = threading.Lock()

    def _make_key(self, key: str) -> str:
        """Make Redis key with namespace"""
        return f"{self.namespace}:{key}" if self.namespace else key

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache, checking Redis if not found locally"""
        with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return value
                else:
                    # Expired, remove from cache
                    del self.cache[key]

            # Check Redis
            redis_value = self.redis.get(self._make_key(key))
            if redis_value is not None:
                self.cache[key] = (redis_value, time.time())
                return redis_value

        return None

    def put(self, key: str, value: Any):
        """Put value in cache and Redis"""
        redis_key = self._make_key(key)

        with self.lock:
            # Update local cache
            self.cache[key] = (value, time.time())

            # Evict if cache is full
            if len(self.cache) > self.max_size:
                # Remove oldest entries
                sorted_items = sorted(self.cache.items(),
                                    key=lambda x: x[1][1])  # Sort by timestamp
                for old_key, _ in sorted_items[:len(self.cache) - self.max_size]:
                    del self.cache[old_key]

            # Update Redis
            self.redis.set(redis_key, value, ex=self.ttl)

    def invalidate(self, key: str):
        """Remove from cache and Redis"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]

        self.redis.delete(self._make_key(key))

    def clear(self):
        """Clear local cache and Redis keys"""
        with self.lock:
            self.cache.clear()

        # Note: Clearing all Redis keys would require a pattern delete
        # For now, just clear local cache

    def sync(self):
        """Sync with Redis (invalidate expired entries)"""
        with self.lock:
            current_time = time.time()
            expired_keys = []

            for key, (value, timestamp) in self.cache.items():
                if current_time - timestamp >= self.ttl:
                    expired_keys.append(key)

            for key in expired_keys:
                del self.cache[key]

            self.last_sync = current_time


class ReliableQueue:
    """
    Reliable queue with message visibility timeout, delivery attempts limit,
    and dead letter queue (similar to Redisson's Reliable Queue).
    """

    def __init__(self, redis: HighPerformanceRedis, name: str,
                 visibility_timeout: int = 30000, max_attempts: int = 3,
                 dead_letter_queue: Optional[str] = None):
        self.redis = redis
        self.name = name
        self.processing_queue = f"{name}:processing"
        self.dead_letter_queue = dead_letter_queue or f"{name}:dead"
        self.visibility_timeout = visibility_timeout
        self.max_attempts = max_attempts

    def enqueue(self, message: Any, delay: int = 0, priority: str = 'normal') -> str:
        """Enqueue a message with optional delay and priority"""
        message_id = str(time.time()) + ":" + str(threading.get_ident())

        job = {
            'id': message_id,
            'data': message,
            'priority': priority,
            'created_at': time.time(),
            'attempts': 0,
            'next_visible': time.time() + (delay / 1000.0) if delay > 0 else time.time()
        }

        # Use appropriate queue based on priority
        queue_key = f"{self.name}:{priority}"
        self.redis.lpush(queue_key, json.dumps(job))

        return message_id

    def dequeue(self) -> Optional[Dict[str, Any]]:
        """Dequeue a message with visibility timeout"""
        # Try priorities in order: high, normal, low
        priority_queues = [
            f"{self.name}:high",
            f"{self.name}:normal",
            f"{self.name}:low"
        ]

        for queue_key in priority_queues:
            # Try to get a message from this priority queue
            result = self.redis.brpoplpush(queue_key, self.processing_queue, timeout=1)
            if result:
                try:
                    job = json.loads(result)

                    # Check if message is ready to be processed
                    if time.time() >= job['next_visible']:
                        # Update visibility timeout and increment attempts
                        job['attempts'] += 1
                        job['next_visible'] = time.time() + (self.visibility_timeout / 1000.0)

                        # Update in processing queue
                        self.redis.lset(self.processing_queue,
                                      self._find_job_index(job['id']), json.dumps(job))

                        return job
                    else:
                        # Not ready, put back in original queue
                        self.redis.lpush(queue_key, result)
                        self.redis.lrem(self.processing_queue, 1, result)

                except (json.JSONDecodeError, KeyError):
                    # Invalid message, move to dead letter queue
                    self.redis.lpush(self.dead_letter_queue, result)
                    self.redis.lrem(self.processing_queue, 1, result)

        return None

    def _find_job_index(self, job_id: str) -> int:
        """Find job index in processing queue (simplified)"""
        # In a real implementation, you'd maintain an index or use Redis search
        return 0

    def acknowledge(self, job_id: str) -> bool:
        """Acknowledge successful processing"""
        # Remove from processing queue
        # In a real implementation, you'd find and remove the specific job
        return True

    def nack(self, job_id: str, delay: int = 0) -> bool:
        """Negative acknowledge - re-queue with delay"""
        # Find job in processing queue and move back to main queue with delay
        # In a real implementation, you'd implement proper job tracking
        return True

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'pending_high': self.redis.llen(f"{self.name}:high"),
            'pending_normal': self.redis.llen(f"{self.name}:normal"),
            'pending_low': self.redis.llen(f"{self.name}:low"),
            'processing': self.redis.llen(self.processing_queue),
            'dead_letter': self.redis.llen(self.dead_letter_queue),
        }


class LiveObject:
    """
    Live object service - automatically syncs object changes with Redis.
    Similar to Redisson's Live Object service.
    """

    def __init__(self, redis: HighPerformanceRedis, cls: type, key: str):
        self.redis = redis
        self.cls = cls
        self.key = key
        self._obj = None
        self._dirty = set()

    def get(self) -> Any:
        """Get the live object"""
        if self._obj is None:
            # Load from Redis
            data = self.redis.hgetall(self.key)
            if data:
                # Convert string values back to appropriate types
                converted_data = {}
                for k, v in data.items():
                    try:
                        converted_data[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        converted_data[k] = v

                self._obj = self.cls(**converted_data)
            else:
                self._obj = self.cls()

        return self._obj

    def save(self):
        """Save object to Redis"""
        if self._obj is None:
            return

        data = {}
        for attr in dir(self._obj):
            if not attr.startswith('_'):
                value = getattr(self._obj, attr)
                if not callable(value):
                    data[attr] = json.dumps(value)

        self.redis.hmset(self.key, data)
        self._dirty.clear()

    def delete(self):
        """Delete object from Redis"""
        self.redis.delete(self.key)
        self._obj = None
        self._dirty.clear()


# Utility functions for building and testing
def build_extension():
    """Build the Cython extension"""
    import subprocess
    import sys

    print("Building FastRedis extension...")
    try:
        result = subprocess.run([
            sys.executable, "setup.py", "build_ext", "--inplace"
        ], cwd=".", capture_output=True, text=True)

        if result.returncode == 0:
            print("Extension built successfully!")
            return True
        else:
            print("Build failed:")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"Build error: {e}")
        return False


def install_hiredis():
    """Install hiredis library if not available"""
    import subprocess
    import sys

    print("Installing hiredis library...")
    try:
        # Try different package managers
        for cmd in [
            ["brew", "install", "hiredis"],  # macOS
            ["apt-get", "update", "&&", "apt-get", "install", "-y", "libhiredis-dev"],  # Ubuntu/Debian
            ["yum", "install", "-y", "hiredis-devel"],  # CentOS/RHEL
            ["dnf", "install", "-y", "hiredis-devel"],  # Fedora
        ]:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    print("hiredis installed successfully!")
                    return True
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        print("Could not install hiredis automatically. Please install it manually:")
        print("  macOS: brew install hiredis")
        print("  Ubuntu/Debian: sudo apt-get install libhiredis-dev")
        print("  CentOS/RHEL: sudo yum install hiredis-devel")
        print("  Fedora: sudo dnf install hiredis-devel")
        return False
    except Exception as e:
        print(f"Installation error: {e}")
        return False


if __name__ == "__main__":
    # Example usage and build script
    import argparse

    parser = argparse.ArgumentParser(description="FastRedis - High-performance Redis client")
    parser.add_argument("--build", action="store_true", help="Build the Cython extension")
    parser.add_argument("--install-deps", action="store_true", help="Install hiredis dependency")
    parser.add_argument("--test", action="store_true", help="Run a basic test")

    args = parser.parse_args()

    if args.install_deps:
        install_hiredis()

    if args.build:
        build_extension()

    if args.test:
        try:
            # Simple test
            with HighPerformanceRedis() as redis:
                redis.set("test_key", "test_value")
                value = redis.get("test_key")
                print(f"Test result: {value}")
                assert value == "test_value"
                print("Basic test passed!")
        except Exception as e:
            print(f"Test failed: {e}")
