"""
FastRedis PostgreSQL Read-Through Cache

Provides a high-performance read-through caching layer that integrates
PostgreSQL with Redis for optimal data access patterns.
"""

import json
import time
import asyncio
import os
from typing import Dict, List, Optional, Any, Union, Callable
from contextlib import asynccontextmanager
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor

from .rpc import RedisPluginManager
from . import get_pgcache_module_path


class PostgreSQLConnection:
    """PostgreSQL connection wrapper with connection pooling"""

    def __init__(self, host: str = "localhost", port: int = 5432,
                 database: str = "postgres", user: str = "postgres",
                 password: str = "", min_connections: int = 1,
                 max_connections: int = 10):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_connections = min_connections
        self.max_connections = max_connections

        # Connection pool
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_connections,
            maxconn=max_connections,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def get_connection(self):
        """Get a connection from the pool"""
        return self._pool.getconn()

    def return_connection(self, conn):
        """Return a connection to the pool"""
        self._pool.putconn(conn)

    def close_all(self):
        """Close all connections in the pool"""
        self._pool.closeall()


class ReadThroughCache:
    """
    Read-through cache that automatically fetches from PostgreSQL
    when cache misses occur, with Redis pubsub for real-time updates.
    """

    def __init__(self, redis_client, pg_connection: PostgreSQLConnection,
                 cache_prefix: str = "pg_cache:", default_ttl: int = 3600,
                 enable_pubsub: bool = True, pubsub_channel: str = "pg_cache_events"):
        self.redis = redis_client
        self.pg = pg_connection
        self.cache_prefix = cache_prefix
        self.default_ttl = default_ttl
        self.enable_pubsub = enable_pubsub
        self.pubsub_channel = pubsub_channel

        # Table metadata cache
        self.table_metadata = {}

        # Plugin manager for Redis modules
        self.plugin_mgr = RedisPluginManager(redis_client)

        # Executor for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _build_cache_key(self, table: str, primary_key: Dict[str, Any]) -> str:
        """Build a cache key from table name and primary key"""
        key_parts = [self.cache_prefix, table]

        # Sort primary key for consistent ordering
        for k, v in sorted(primary_key.items()):
            key_parts.extend([str(k), str(v)])

        return ":".join(key_parts)

    def _extract_primary_key(self, table: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract primary key from row data"""
        if table not in self.table_metadata:
            self._load_table_metadata(table)

        pk_columns = self.table_metadata[table]['primary_key']
        return {col: row_data[col] for col in pk_columns if col in row_data}

    def _load_table_metadata(self, table: str):
        """Load table metadata including primary key information"""
        conn = self.pg.get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Get primary key columns
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                """, (table,))

                pk_columns = [row['attname'] for row in cursor.fetchall()]

                # Get all columns
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, (table,))

                columns = cursor.fetchall()

                self.table_metadata[table] = {
                    'primary_key': pk_columns,
                    'columns': columns
                }

        finally:
            self.pg.return_connection(conn)

    def _execute_pg_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a PostgreSQL query and return results"""
        conn = self.pg.get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                else:
                    return []
        finally:
            self.pg.return_connection(conn)

    def _publish_event(self, event_type: str, table: str, data: Dict[str, Any]):
        """Publish cache event to Redis pubsub"""
        if not self.enable_pubsub:
            return

        event = {
            'type': event_type,
            'table': table,
            'data': data,
            'timestamp': time.time()
        }

        self.redis.publish(self.pubsub_channel, json.dumps(event))

    def get(self, table: str, primary_key: Dict[str, Any],
            ttl: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get data from cache or PostgreSQL.

        Args:
            table: Table name
            primary_key: Primary key values
            ttl: Cache TTL override

        Returns:
            Row data or None if not found
        """
        cache_key = self._build_cache_key(table, primary_key)

        # Try cache first
        cached_data = self.redis.get(cache_key)
        if cached_data:
            # Cache hit
            self._publish_event('cache_hit', table, {'key': cache_key})
            return json.loads(cached_data)

        # Cache miss - query PostgreSQL
        pk_columns = list(primary_key.keys())
        pk_values = list(primary_key.values())

        placeholders = ', '.join(['%s'] * len(pk_columns))
        query = f"SELECT * FROM {table} WHERE ({', '.join(pk_columns)}) = ({placeholders})"

        results = self._execute_pg_query(query, tuple(pk_values))

        if results:
            # Found in database - cache it
            row_data = results[0]
            cache_ttl = ttl or self.default_ttl
            self.redis.setex(cache_key, cache_ttl, json.dumps(row_data))

            self._publish_event('cache_miss', table, {'key': cache_key, 'cached': True})

            return row_data

        # Not found anywhere
        self._publish_event('cache_miss', table, {'key': cache_key, 'cached': False})
        return None

    def set(self, table: str, primary_key: Dict[str, Any], data: Dict[str, Any],
            ttl: Optional[int] = None):
        """
        Set data in cache (typically called after database updates).

        Args:
            table: Table name
            primary_key: Primary key values
            data: Row data
            ttl: Cache TTL override
        """
        cache_key = self._build_cache_key(table, primary_key)
        cache_ttl = ttl or self.default_ttl

        self.redis.setex(cache_key, cache_ttl, json.dumps(data))
        self._publish_event('cache_set', table, {'key': cache_key, 'ttl': cache_ttl})

    def invalidate(self, table: str, primary_key: Dict[str, Any]):
        """
        Invalidate cache entry.

        Args:
            table: Table name
            primary_key: Primary key values
        """
        cache_key = self._build_cache_key(table, primary_key)
        self.redis.delete(cache_key)
        self._publish_event('cache_invalidate', table, {'key': cache_key})

    def invalidate_table(self, table: str):
        """
        Invalidate all cache entries for a table.

        Args:
            table: Table name
        """
        pattern = f"{self.cache_prefix}{table}:*"
        keys = self.redis.keys(pattern)

        if keys:
            self.redis.delete(*keys)

        self._publish_event('table_invalidate', table, {'keys_deleted': len(keys) if keys else 0})

    def get_multi(self, table: str, primary_keys: List[Dict[str, Any]],
                  ttl: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get multiple rows from cache/PostgreSQL.

        Args:
            table: Table name
            primary_keys: List of primary key dictionaries
            ttl: Cache TTL override

        Returns:
            Dictionary mapping cache keys to row data
        """
        results = {}

        # Build cache keys
        cache_keys = []
        key_to_pk = {}
        for pk in primary_keys:
            cache_key = self._build_cache_key(table, pk)
            cache_keys.append(cache_key)
            key_to_pk[cache_key] = pk

        # Multi-get from cache
        cached_data = self.redis.mget(cache_keys)

        # Process results and identify misses
        misses = []
        for i, data in enumerate(cached_data):
            cache_key = cache_keys[i]
            pk = key_to_pk[cache_key]

            if data:
                # Cache hit
                results[cache_key] = json.loads(data)
            else:
                # Cache miss
                misses.append(pk)

        # Handle cache misses
        if misses:
            # Build batch query for PostgreSQL
            pk_columns = list(misses[0].keys())  # Assume same structure
            value_placeholders = []

            all_values = []
            for pk in misses:
                placeholders = ', '.join(['%s'] * len(pk_columns))
                value_placeholders.append(f"({placeholders})")
                all_values.extend(pk.values())

            query = f"""
                SELECT * FROM {table}
                WHERE ({', '.join(pk_columns)}) IN ({', '.join(value_placeholders)})
            """

            pg_results = self._execute_pg_query(query, tuple(all_values))

            # Index results by primary key for fast lookup
            pk_to_row = {}
            for row in pg_results:
                pk = self._extract_primary_key(table, row)
                pk_tuple = tuple(sorted(pk.items()))
                pk_to_row[pk_tuple] = row

            # Cache misses and build results
            cache_ttl = ttl or self.default_ttl
            for pk in misses:
                cache_key = self._build_cache_key(table, pk)
                pk_tuple = tuple(sorted(pk.items()))

                if pk_tuple in pk_to_row:
                    row_data = pk_to_row[pk_tuple]
                    self.redis.setex(cache_key, cache_ttl, json.dumps(row_data))
                    results[cache_key] = row_data

        return results

    def preload_table(self, table: str, where_clause: str = "",
                      params: tuple = None, ttl: Optional[int] = None):
        """
        Preload table data into cache.

        Args:
            table: Table name
            where_clause: Optional WHERE clause
            params: Query parameters
            ttl: Cache TTL override
        """
        query = f"SELECT * FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        results = self._execute_pg_query(query, params)
        cache_ttl = ttl or self.default_ttl

        cached_count = 0
        for row in results:
            pk = self._extract_primary_key(table, row)
            if pk:  # Only cache rows with primary keys
                cache_key = self._build_cache_key(table, pk)
                self.redis.setex(cache_key, cache_ttl, json.dumps(row))
                cached_count += 1

        self._publish_event('table_preload', table,
                          {'rows_cached': cached_count, 'where_clause': where_clause})

        return cached_count

    async def get_async(self, table: str, primary_key: Dict[str, Any],
                       ttl: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Async version of get() method.

        Args:
            table: Table name
            primary_key: Primary key values
            ttl: Cache TTL override

        Returns:
            Row data or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, self.get, table, primary_key, ttl
        )

    async def get_multi_async(self, table: str, primary_keys: List[Dict[str, Any]],
                            ttl: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """
        Async version of get_multi() method.

        Args:
            table: Table name
            primary_keys: List of primary key dictionaries
            ttl: Cache TTL override

        Returns:
            Dictionary mapping cache keys to row data
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, self.get_multi, table, primary_keys, ttl
        )

    @asynccontextmanager
    async def subscribe_events(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Subscribe to cache events via Redis pubsub.

        Args:
            callback: Function to call when events are received

        Usage:
            async with cache.subscribe_events(handle_event):
                # Events will be processed here
                await asyncio.sleep(60)
        """
        if not self.enable_pubsub:
            yield
            return

        # Create pubsub connection
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.pubsub_channel)

        def event_handler():
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        event_data = json.loads(message['data'])
                        callback(event_data)
                    except json.JSONDecodeError:
                        continue

        # Run handler in thread
        import threading
        handler_thread = threading.Thread(target=event_handler, daemon=True)
        handler_thread.start()

        try:
            yield
        finally:
            pubsub.unsubscribe()
            pubsub.close()

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Cache performance statistics
        """
        # Get Redis info
        redis_info = self.redis.info('memory')

        # Count cache keys
        cache_keys = self.redis.keys(f"{self.cache_prefix}*")
        total_cached = len(cache_keys) if cache_keys else 0

        # Get hit/miss stats (simplified)
        stats = {
            'total_cached_items': total_cached,
            'redis_memory_used': redis_info.get('used_memory_human', 'unknown'),
            'cache_prefix': self.cache_prefix,
            'pubsub_enabled': self.enable_pubsub,
            'pubsub_channel': self.pubsub_channel if self.enable_pubsub else None
        }

        return stats

    def setup_table_triggers(self, table: str):
        """
        Setup PostgreSQL triggers for automatic cache invalidation.
        Requires the pg_redis_cache PostgreSQL extension.

        Args:
            table: Table name
        """
        # This would use the PostgreSQL extension functions
        setup_query = f"SELECT pg_redis_cache.setup_table_cache('{table}', ARRAY['id']);"
        self._execute_pg_query(setup_query)

    def remove_table_triggers(self, table: str):
        """
        Remove PostgreSQL triggers for cache invalidation.

        Args:
            table: Table name
        """
        cleanup_query = f"SELECT pg_redis_cache.remove_table_cache('{table}');"
        self._execute_pg_query(cleanup_query)

    def close(self):
        """Close all connections"""
        self.pg.close_all()
        self.executor.shutdown(wait=True)


class PGCacheManager:
    """
    High-level manager for PostgreSQL read-through caching with Redis.
    Provides automatic cache management and monitoring.
    """

    def __init__(self, redis_client, pg_config: Dict[str, Any],
                 cache_config: Optional[Dict[str, Any]] = None):
        self.redis = redis_client

        # PostgreSQL connection
        self.pg_conn = PostgreSQLConnection(**pg_config)

        # Cache configuration
        cache_config = cache_config or {}
        self.cache = ReadThroughCache(self.redis, self.pg_conn, **cache_config)

        # Monitoring
        self.monitoring_enabled = cache_config.get('monitoring', True)
        if self.monitoring_enabled:
            from .monitoring import ObservabilityManager
            self.monitor = ObservabilityManager(redis_client, "pg_cache")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get(self, table: str, **primary_key) -> Optional[Dict[str, Any]]:
        """
        Get a row by primary key.

        Args:
            table: Table name
            **primary_key: Primary key column-value pairs

        Returns:
            Row data or None
        """
        if self.monitoring_enabled:
            start_time = time.time()
            result = self.cache.get(table, primary_key)
            duration = time.time() - start_time

            success = result is not None
            self.monitor.record_operation(f"pg_cache.get.{table}", duration * 1000, success)

            return result

        return self.cache.get(table, primary_key)

    def get_multi(self, table: str, primary_keys: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get multiple rows.

        Args:
            table: Table name
            primary_keys: List of primary key dictionaries

        Returns:
            Dictionary of cache_key -> row_data
        """
        if self.monitoring_enabled:
            start_time = time.time()
            result = self.cache.get_multi(table, primary_keys)
            duration = time.time() - start_time

            success = len(result) > 0
            self.monitor.record_operation(f"pg_cache.get_multi.{table}", duration * 1000, success)

            return result

        return self.cache.get_multi(table, primary_keys)

    async def get_async(self, table: str, **primary_key) -> Optional[Dict[str, Any]]:
        """Async version of get()"""
        if self.monitoring_enabled:
            start_time = time.time()
            result = await self.cache.get_async(table, primary_key)
            duration = time.time() - start_time

            success = result is not None
            self.monitor.record_operation(f"pg_cache.get_async.{table}", duration * 1000, success)

            return result

        return await self.cache.get_async(table, primary_key)

    def setup_auto_invalidation(self, table: str):
        """
        Setup automatic cache invalidation for a table.

        Args:
            table: Table name
        """
        self.cache.setup_table_triggers(table)

    def preload_table(self, table: str, where_clause: str = "", **filters):
        """
        Preload table data into cache.

        Args:
            table: Table name
            where_clause: WHERE clause for selective loading
            **filters: Additional filters
        """
        return self.cache.preload_table(table, where_clause)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache and system statistics"""
        stats = self.cache.get_cache_stats()

        if self.monitoring_enabled:
            monitor_stats = self.monitor.get_all_metrics()
            stats['monitoring'] = monitor_stats

        return stats

    def close(self):
        """Close all connections"""
        self.cache.close()


# Redis Module Utilities

def load_pgcache_module(redis_client, pg_config: Dict[str, Any]) -> bool:
    """
    Load the pgcache Redis module into a running Redis instance.

    Args:
        redis_client: FastRedis client instance
        pg_config: PostgreSQL configuration dictionary with keys:
            - host: PostgreSQL host
            - port: PostgreSQL port
            - database: PostgreSQL database
            - user: PostgreSQL user
            - password: PostgreSQL password

    Returns:
        bool: True if module loaded successfully

    Example:
        >>> from fast_redis import HighPerformanceRedis, load_pgcache_module
        >>> redis = HighPerformanceRedis()
        >>> pg_config = {
        ...     'host': 'localhost',
        ...     'database': 'myapp',
        ...     'user': 'postgres',
        ...     'password': 'secret'
        ... }
        >>> load_pgcache_module(redis, pg_config)
        True
    """
    module_path = get_pgcache_module_path()
    if not module_path:
        raise FileNotFoundError("pgcache Redis module not found. Make sure FastRedis is properly installed.")

    # Build module arguments
    args = [
        "pg_host", pg_config.get('host', 'localhost'),
        "pg_port", str(pg_config.get('port', 5432)),
        "pg_database", pg_config.get('database', 'postgres'),
        "pg_user", pg_config.get('user', 'postgres'),
        "pg_password", pg_config.get('password', ''),
    ]

    try:
        # Load the module
        result = redis_client.call_method('MODULE', 'LOAD', module_path, *args)
        return result == 'OK'
    except Exception as e:
        raise RuntimeError(f"Failed to load pgcache module: {e}")


def check_pgcache_module_loaded(redis_client) -> bool:
    """
    Check if the pgcache Redis module is loaded.

    Args:
        redis_client: FastRedis client instance

    Returns:
        bool: True if module is loaded
    """
    try:
        modules = redis_client.call_method('MODULE', 'LIST')
        for module_info in modules:
            if module_info.get('name') == 'pgcache':
                return True
        return False
    except Exception:
        return False


def get_pgcache_module_info(redis_client) -> Optional[Dict[str, Any]]:
    """
    Get information about the loaded pgcache module.

    Args:
        redis_client: FastRedis client instance

    Returns:
        dict: Module information or None if not loaded
    """
    try:
        modules = redis_client.call_method('MODULE', 'LIST')
        for module_info in modules:
            if module_info.get('name') == 'pgcache':
                return module_info
        return None
    except Exception:
        return None
