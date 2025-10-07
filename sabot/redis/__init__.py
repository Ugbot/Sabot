"""
Redis client for Sabot - High-performance Redis client using Cython.

This package provides a Redis client with threading support,
built using Cython and the hiredis C library for optimal performance.

Includes PostgreSQL read-through caching via Redis modules.
"""

import os

try:
    from .redis_client import RedisClient, StreamManager, RedisError, ConnectionError
    from .pg_cache import (
        PostgreSQLConnection, ReadThroughCache, PGCacheManager,
        load_pgcache_module, check_pgcache_module_loaded, get_pgcache_module_info
    )
    from .async_redis import (
        AsyncRedis,
        StreamConsumer,
        KeyspaceWatcher,
        KeyspaceNotificationConsumer,
        OptimisticLock,
        StreamConsumerGroup,
        WorkerQueue,
        PubSubHub,
        DistributedLock,
        ReadWriteLock,
        Semaphore,
        DistributedCounter,
        LocalCache,
        LiveObject,
        ReliableQueue,
        LuaScriptManager,
        RedisClusterManager,
        RedisSentinelManager,
        RedisScripts,
        ScriptHelper,
        WebSessionManager,
        XATransactionManager,
        ObservabilityManager,
        SecurityManager,
        enable_uvloop,
        is_uvloop_enabled,
        create_uvloop_event_loop,
        RedisPluginManager,
    )

    # Backwards compatibility aliases (legacy names)
    CyRedisClient = RedisClient
    ThreadedStreamManager = StreamManager
    ThreadedStreamConsumer = StreamConsumer
    HighPerformanceRedis = AsyncRedis
    HighPerformanceAsyncRedis = AsyncRedis

except ImportError as e:
    raise ImportError(
        "Redis extension not built. Please install with: pip install -e ."
    ) from e


def get_pgcache_module_path():
    """
    Get the path to the built pgcache Redis module.

    Returns:
        str: Path to pgcache.so, or None if not found
    """
    package_dir = os.path.dirname(__file__)
    module_path = os.path.join(package_dir, 'pgcache', 'pgcache.so')

    if os.path.exists(module_path):
        return module_path

    # Try to find it in the installed package
    try:
        import sabot.redis.pgcache
        module_path = os.path.join(os.path.dirname(sabot.redis.pgcache.__file__), 'pgcache.so')
        if os.path.exists(module_path):
            return module_path
    except ImportError:
        pass

    return None

__version__ = "0.1.0"
__all__ = [
    # Core clients
    "RedisClient",
    "AsyncRedis",
    "StreamManager",
    "RedisError",
    "ConnectionError",

    # Stream and pub/sub
    "StreamConsumer",
    "KeyspaceWatcher",
    "KeyspaceNotificationConsumer",
    "StreamConsumerGroup",
    "WorkerQueue",
    "PubSubHub",

    # Distributed coordination
    "OptimisticLock",
    "DistributedLock",
    "ReadWriteLock",
    "Semaphore",
    "DistributedCounter",

    # Caching and storage
    "LocalCache",
    "LiveObject",
    "ReliableQueue",

    # Lua and clustering
    "LuaScriptManager",
    "RedisClusterManager",
    "RedisSentinelManager",
    "RedisScripts",
    "ScriptHelper",

    # Web and transactions
    "WebSessionManager",
    "XATransactionManager",

    # Observability and security
    "ObservabilityManager",
    "SecurityManager",

    # PostgreSQL integration
    "PostgreSQLConnection",
    "ReadThroughCache",
    "PGCacheManager",
    "load_pgcache_module",
    "check_pgcache_module_loaded",
    "get_pgcache_module_info",

    # Event loop
    "enable_uvloop",
    "is_uvloop_enabled",
    "create_uvloop_event_loop",

    # Plugin system
    "RedisPluginManager",
    "get_pgcache_module_path",

    # Backwards compatibility (legacy names)
    "CyRedisClient",
    "ThreadedStreamManager",
    "ThreadedStreamConsumer",
    "HighPerformanceRedis",
    "HighPerformanceAsyncRedis",

    "__version__",
]
