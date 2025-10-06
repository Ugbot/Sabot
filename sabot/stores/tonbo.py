# -*- coding: utf-8 -*-
"""Tonbo store backend for Sabot tables."""

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator, Union, AsyncIterator, Tuple

from .base import StoreBackend, StoreBackendConfig, Transaction

# Tonbo imports - will be available when installed
try:
    from tonbo import TonboDB, DbOption, Column, DataType, Record, Bound
    TONBO_AVAILABLE = True
except ImportError:
    TONBO_AVAILABLE = False
    TonboDB = None
    DbOption = None
    Column = None
    DataType = None
    Record = None
    Bound = None

# Cython Tonbo wrapper - provides high-performance operations
try:
    from .._cython.tonbo_wrapper import FastTonboBackend, TonboCythonWrapper
    CYTHON_TONBO_AVAILABLE = True
except ImportError:
    CYTHON_TONBO_AVAILABLE = False
    FastTonboBackend = None
    TonboCythonWrapper = None

# Arrow-integrated Tonbo store - provides columnar operations
try:
    from .._cython.tonbo_arrow import ArrowTonboStore, ArrowTonboIntegration
    ARROW_TONBO_AVAILABLE = True
except ImportError:
    ARROW_TONBO_AVAILABLE = False
    ArrowTonboStore = None
    ArrowTonboIntegration = None


class TonboTransaction(Transaction):
    """Tonbo transaction implementation using Tonbo's native transactions."""

    def __init__(self, backend: 'TonboBackend', tonbo_txn):
        self.backend = backend
        self.tonbo_txn = tonbo_txn  # Native Tonbo transaction

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return None

        try:
            result = await self.tonbo_txn.get(key)
            return result
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction get error: {e}")
            return None

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return

        try:
            # Convert value to Tonbo record format
            tonbo_record = self.backend._value_to_tonbo_record(key, value)
            await self.tonbo_txn.insert(tonbo_record)
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction set error: {e}")
            raise

    async def delete(self, key: Any) -> bool:
        """Delete within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return False

        try:
            # For Tonbo, we need to remove by primary key
            # This assumes the key is the primary key
            await self.tonbo_txn.remove(key)
            return True
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction delete error: {e}")
            return False


class TonboBackend(StoreBackend):
    """Tonbo store backend using the embedded Arrow database.

    High-performance, persistent storage with LSM tree and Arrow integration.
    Uses zero-copy operations and pushdown predicates.

    Automatically uses Cython optimizations when available for maximum performance.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.db_path = Path(config.path) if config.path else Path("./sabot_tonbo_db")
        self.db_path.mkdir(parents=True, exist_ok=True)
        self._db = None  # Will hold the Tonbo database instance
        self._schema = None  # Dynamic schema for key-value storage
        self._cython_backend = None  # High-performance Cython backend
        self._arrow_store = None  # Arrow-integrated store for columnar operations
        self._use_cython = CYTHON_TONBO_AVAILABLE  # Use Cython by default if available
        self._use_arrow = ARROW_TONBO_AVAILABLE and TONBO_AVAILABLE  # Only use Arrow if Tonbo is available
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(__name__)

        # Fallback storage for when Tonbo is not available
        self._fallback_store = None
        self._fallback_file = None

    async def start(self) -> None:
        """Initialize the Tonbo backend with optimal performance settings."""
        if not CYTHON_TONBO_AVAILABLE and not TONBO_AVAILABLE and not ARROW_TONBO_AVAILABLE:
            raise RuntimeError(
                "Tonbo backend requires Tonbo Cython FFI or Python bindings. "
                "Build with: python setup.py build_ext --inplace"
            )

        try:
            # Initialize Arrow store first (provides columnar capabilities)
            if self._use_arrow and ARROW_TONBO_AVAILABLE:
                self._arrow_store = ArrowTonboStore(str(self.db_path))
                await self._arrow_store.initialize()
                self.logger.info(f"Tonbo Arrow backend initialized at {self.db_path}")

            # Use Cython FFI backend for maximum performance (synchronous, no async needed)
            if self._use_cython and CYTHON_TONBO_AVAILABLE:
                self._cython_backend = FastTonboBackend(str(self.db_path))
                # FFI backend is synchronous, call initialize() directly
                self._cython_backend.initialize()
                self.logger.info(f"Tonbo Cython FFI backend initialized at {self.db_path}")

            else:
                # No Tonbo backends available, use file-based fallback
                self.logger.warning("Tonbo not available, falling back to file-based storage")
                self._fallback_store = {}
                self._fallback_file = self.db_path / "tonbo_fallback.db"
                await self._load_fallback_data()
                self.logger.info(f"Tonbo fallback file-based backend initialized at {self.db_path}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Tonbo backend: {e}")
            raise RuntimeError(f"Tonbo backend initialization failed: {e}")

    async def _load_fallback_data(self) -> None:
        """Load data from fallback file storage."""
        if self._fallback_file and self._fallback_file.exists():
            try:
                import pickle
                with open(self._fallback_file, 'rb') as f:
                    self._fallback_store = pickle.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load fallback data: {e}")
                self._fallback_store = {}
        else:
            self._fallback_store = {}

    async def _save_fallback_data(self) -> None:
        """Save data to fallback file storage."""
        if self._fallback_file and self._fallback_store is not None:
            try:
                import pickle
                with open(self._fallback_file, 'wb') as f:
                    pickle.dump(self._fallback_store, f)
            except Exception as e:
                self.logger.error(f"Failed to save fallback data: {e}")

    async def stop(self) -> None:
        """Clean up the Tonbo backend and Arrow resources."""
        async with self._lock:
            # Close Arrow store first
            if self._arrow_store:
                try:
                    await self._arrow_store.close()
                except Exception as e:
                    self.logger.error(f"Error closing Arrow store: {e}")
                self._arrow_store = None

            # Close Cython FFI backend (synchronous)
            if self._cython_backend:
                try:
                    self._cython_backend.close()
                except Exception as e:
                    self.logger.error(f"Error closing Cython backend: {e}")
                self._cython_backend = None

            # Close main database
            if self._db:
                # TODO: Close Tonbo database when API is available
                # await self._db.close()
                pass

            # Save fallback data
            if self._fallback_store is not None:
                await self._save_fallback_data()

    def _key_to_string(self, key: Any) -> str:
        """Convert key to string format for Tonbo."""
        if isinstance(key, str):
            return key
        elif isinstance(key, (int, float)):
            return str(key)
        else:
            # For complex keys, use repr
            return repr(key)

    def _value_to_bytes(self, value: Any) -> bytes:
        """Convert value to bytes for storage."""
        import pickle
        return pickle.dumps(value)

    def _bytes_to_value(self, data: bytes) -> Any:
        """Convert bytes back to value."""
        import pickle
        return pickle.loads(data)

    def _value_to_tonbo_record(self, key: Any, value: Any) -> Any:
        """Convert key-value pair to Tonbo record format."""
        if not self._schema:
            raise RuntimeError("Schema not initialized")

        key_str = self._key_to_string(key)
        value_bytes = self._value_to_bytes(value)

        # Create record instance
        return self._schema(key=key_str, value=value_bytes)

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key using Tonbo's efficient lookup."""
        # Use Cython FFI backend for maximum performance (synchronous call)
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                value_bytes = self._cython_backend.fast_get(key_str)
                return self._bytes_to_value(value_bytes) if value_bytes else None
            except Exception as e:
                self.logger.error(f"Tonbo Cython get error for key {key}: {e}")
                return None

        # Fallback to Python implementation
        if self._db:
            async with self._lock:
                try:
                    key_str = self._key_to_string(key)
                    result = await self._db.get(key_str)

                    if result and result.get('value'):
                        return self._bytes_to_value(result['value'])
                    return None

                except Exception as e:
                    self.logger.error(f"Tonbo get error for key {key}: {e}")
                    return None

        # Use fallback storage
        elif self._fallback_store is not None:
            async with self._lock:
                key_str = self._key_to_string(key)
                value_bytes = self._fallback_store.get(key_str)
                return self._bytes_to_value(value_bytes) if value_bytes else None

        return None

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key using Tonbo's insert operation."""
        # Use Cython FFI backend for maximum performance (synchronous call)
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                value_bytes = self._value_to_bytes(value)
                self._cython_backend.fast_insert(key_str, value_bytes)
                return
            except Exception as e:
                self.logger.error(f"Tonbo Cython set error for key {key}: {e}")
                raise

        # Fallback to Python implementation
        if self._db:
            async with self._lock:
                try:
                    tonbo_record = self._value_to_tonbo_record(key, value)
                    await self._db.insert(tonbo_record)
                except Exception as e:
                    self.logger.error(f"Tonbo set error for key {key}: {e}")
                    raise

        # Use fallback storage
        elif self._fallback_store is not None:
            async with self._lock:
                key_str = self._key_to_string(key)
                value_bytes = self._value_to_bytes(value)
                self._fallback_store[key_str] = value_bytes

    async def delete(self, key: Any) -> bool:
        """Delete a value by key using Tonbo's remove operation."""
        # Use Cython FFI backend for maximum performance (synchronous call)
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                return self._cython_backend.fast_delete(key_str)
            except Exception as e:
                self.logger.error(f"Tonbo Cython delete error for key {key}: {e}")
                return False

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return False

        async with self._lock:
            try:
                key_str = self._key_to_string(key)
                await self._db.remove(key_str)
                return True
            except Exception as e:
                self.logger.error(f"Tonbo delete error for key {key}: {e}")
                return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        return await self.get(key) is not None

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        if self._fallback_store is not None:
            async with self._lock:
                keys = list(self._fallback_store.keys())
                if prefix:
                    keys = [k for k in keys if str(k).startswith(prefix)]
                return keys

        elif self._db:
            async with self._lock:
                try:
                    # TODO: Implement Tonbo scan operation
                    # keys = await self._db.scan_keys()
                    keys = list(self._db.keys())  # Placeholder

                    if prefix:
                        keys = [k for k in keys if str(k).startswith(prefix)]

                    return keys
                except Exception:
                    return []

        return []

    async def values(self) -> List[Any]:
        """Get all values."""
        if not self._db:
            return []

        async with self._lock:
            try:
                # TODO: Implement efficient values scan
                keys = await self.keys()
                values = []
                for key in keys:
                    value = await self.get(key)
                    if value is not None:
                        values.append(value)
                return values
            except Exception:
                return []

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        if not self._db:
            return []

        async with self._lock:
            try:
                # TODO: Implement efficient items scan with projection
                keys = await self.keys(prefix)
                items = []
                for key in keys:
                    value = await self.get(key)
                    if value is not None:
                        items.append((key, value))
                return items
            except Exception:
                return []

    async def clear(self) -> None:
        """Clear all data using Cython backend for maximum performance."""
        async with self._lock:
            try:
                # Use Cython backend if available for better performance
                if self._cython_backend:
                    # For now, recreate the database to clear it
                    # In a real implementation, Tonbo would have a clear method
                    await self._cython_backend.close()
                    import shutil
                    if self.db_path.exists():
                        shutil.rmtree(self.db_path)
                    self.db_path.mkdir(parents=True, exist_ok=True)
                    await self._cython_backend.initialize()
                    self.logger.info("Tonbo database cleared and reinitialized")
                elif self._arrow_store:
                    await self._arrow_store.clear()
                elif self._db:
                    # Fallback: clear what we can
                    # Note: Real Tonbo clear would be more efficient
                    pass
                elif self._fallback_store is not None:
                    # Clear fallback storage
                    async with self._lock:
                        self._fallback_store.clear()
                        # Remove the fallback file
                        if self._fallback_file and self._fallback_file.exists():
                            self._fallback_file.unlink()
            except Exception as e:
                self.logger.error(f"Failed to clear Tonbo database: {e}")
                raise

    async def size(self) -> int:
        """Get number of items stored."""
        if self._fallback_store is not None:
            async with self._lock:
                return len(self._fallback_store)
        else:
            # Use the keys method for other backends
            keys = await self.keys()
            return len(keys)

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys with Tonbo's efficient pushdown capabilities."""
        # Use Cython backend for maximum performance if available
        if self._cython_backend:
            try:
                start_str = self._key_to_string(start_key) if start_key else None
                end_str = self._key_to_string(end_key) if end_key else None

                results = []
                async for key, value_bytes, timestamp in self._cython_backend.fast_scan_range(
                    start_str, end_str, limit or 1000
                ):
                    value = self._bytes_to_value(value_bytes)

                    # Apply prefix filter if specified
                    if prefix and not str(key).startswith(prefix):
                        continue

                    results.append((key, value))

                return iter(results)

            except Exception as e:
                self.logger.error(f"Tonbo Cython scan error: {e}")
                return iter([])

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return iter([])

        async with self._lock:
            try:
                # Convert keys to Tonbo format
                lower_bound = None
                if start_key is not None:
                    start_str = self._key_to_string(start_key)
                    lower_bound = Bound.Included(start_str)

                upper_bound = None
                if end_key is not None:
                    end_str = self._key_to_string(end_key)
                    upper_bound = Bound.Excluded(end_str)

                # Use Tonbo's efficient scan with bounds and limit
                scan_stream = await self._db.scan(
                    lower_bound,
                    upper_bound,
                    limit=limit,
                    projection=["key", "value"]  # Only fetch what we need
                )

                results = []
                async for record in scan_stream:
                    if record and 'key' in record and 'value' in record:
                        key = record['key']
                        value = self._bytes_to_value(record['value'])

                        # Apply prefix filter if specified
                        if prefix and not str(key).startswith(prefix):
                            continue

                        results.append((key, value))

                return iter(results)

            except Exception as e:
                self.logger.error(f"Tonbo scan error: {e}")
                return iter([])

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch using Cython backend."""
        if not items:
            return

        async with self._lock:
            try:
                # Use Cython backend with individual inserts (FFI doesn't have batch yet)
                if self._cython_backend:
                    for key, value in items.items():
                        key_str = self._key_to_string(key)
                        value_bytes = self._value_to_bytes(value)
                        self._cython_backend.fast_insert(key_str, value_bytes)
                    self.logger.debug(f"Batch inserted {len(items)} items")

                elif self._arrow_store:
                    await self._arrow_store.batch_insert(items)

                else:
                    # Fallback: individual inserts
                    for key, value in items.items():
                        await self.set(key, value)

            except Exception as e:
                self.logger.error(f"Batch set failed: {e}")
                raise

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch using Cython backend."""
        if not keys:
            return 0

        async with self._lock:
            try:
                # Use Cython FFI backend with individual deletes
                if self._cython_backend:
                    deleted_count = 0
                    for key in keys:
                        key_str = self._key_to_string(key)
                        if self._cython_backend.fast_exists(key_str):
                            if self._cython_backend.fast_delete(key_str):
                                deleted_count += 1

                    self.logger.debug(f"Batch deleted {deleted_count} items")
                    return deleted_count

                elif self._arrow_store:
                    return await self._arrow_store.batch_delete(keys)

                else:
                    # Fallback: individual deletes
                    deleted = 0
                    for key in keys:
                        try:
                            await self.delete(key)
                            deleted += 1
                        except KeyError:
                            pass
                    return deleted

            except Exception as e:
                self.logger.error(f"Batch delete failed: {e}")
                return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive backend statistics including Arrow metrics."""
        size = await self.size()

        # Get Arrow statistics if available
        arrow_stats = await self.get_arrow_stats()

        # Get LSM statistics from Cython backend if available
        lsm_stats = {}
        if self._cython_backend:
            try:
                # In a real implementation, these would come from Tonbo's statistics API
                lsm_stats = {
                    'lsm_tree_levels': 1,  # Placeholder - would be retrieved from Tonbo
                    'total_sst_files': 1,  # Placeholder - would be retrieved from Tonbo
                    'memory_usage': 1024 * 1024,  # Placeholder - 1MB estimate
                    'compaction_backlog': 0,  # Placeholder - no backlog
                }
            except Exception as e:
                self.logger.debug(f"Could not get LSM stats: {e}")

        stats = {
            'backend_type': 'tonbo',
            'size': size,
            'db_path': str(self.db_path),
            'cython_enabled': self._use_cython and CYTHON_TONBO_AVAILABLE,
            'arrow_enabled': self._use_arrow and ARROW_TONBO_AVAILABLE,
            'lsm_tree_levels': lsm_stats.get('lsm_tree_levels', 0),
            'total_sst_files': lsm_stats.get('total_sst_files', 0),
            'memory_usage': lsm_stats.get('memory_usage', 0),
            'compaction_backlog': lsm_stats.get('compaction_backlog', 0),
        }

        # Merge Arrow statistics
        if arrow_stats:
            stats.update(arrow_stats)

        return stats

    async def backup(self, path: Path) -> None:
        """Create a backup of the Tonbo database."""
        async with self._lock:
            try:
                # Use Cython backend for backup if available
                if self._cython_backend:
                    # For now, implement backup as directory copy
                    # In real Tonbo, this would be a native backup operation
                    import shutil
                    if path.exists():
                        shutil.rmtree(path)
                    shutil.copytree(self.db_path, path)
                    self.logger.info(f"Tonbo database backed up to {path}")

                elif self._arrow_store:
                    await self._arrow_store.backup(path)

                else:
                    raise NotImplementedError("Backup requires Cython or Arrow backend")

            except Exception as e:
                self.logger.error(f"Tonbo backup failed: {e}")
                raise

    async def restore(self, path: Path) -> None:
        """Restore from a Tonbo database backup."""
        if not path.exists():
            raise FileNotFoundError(f"Backup path does not exist: {path}")

        async with self._lock:
            try:
                # Stop current backend
                await self.stop()

                # Replace database directory with backup
                import shutil
                if self.db_path.exists():
                    shutil.rmtree(self.db_path)
                shutil.copytree(path, self.db_path)

                # Restart with restored data
                await self.start()
                self.logger.info(f"Tonbo database restored from {path}")

            except Exception as e:
                self.logger.error(f"Tonbo restore failed: {e}")
                # Try to restart even if restore failed
                try:
                    await self.start()
                except Exception:
                    pass
                raise

    async def values(self) -> Iterator[Any]:
        """Iterate over all values (like dict.values())."""
        async for key, value in self.items():
            yield value

    async def items(self) -> Iterator[Tuple[Any, Any]]:
        """Iterate over all key-value pairs (like dict.items())."""
        # Use scan to get all items
        async for key, value in self.scan():
            yield (key, value)

    async def begin_transaction(self) -> Transaction:
        """Begin a transaction using Tonbo's native transaction support."""
        if not TONBO_AVAILABLE or not self._db:
            raise RuntimeError("Tonbo backend not available")

        try:
            # Start a Tonbo transaction
            tonbo_txn = await self._db.transaction()
            return TonboTransaction(self, tonbo_txn)
        except Exception as e:
            self.logger.error(f"Failed to begin Tonbo transaction: {e}")
            raise

    async def commit_transaction(self, transaction: TonboTransaction) -> None:
        """Commit a Tonbo transaction."""
        if not TONBO_AVAILABLE or not hasattr(transaction, 'tonbo_txn'):
            return

        try:
            await transaction.tonbo_txn.commit()
        except Exception as e:
            self.logger.error(f"Failed to commit Tonbo transaction: {e}")
            raise

    async def rollback_transaction(self, transaction: TonboTransaction) -> None:
        """Rollback a Tonbo transaction."""
        # Tonbo handles rollback automatically if commit fails
        # No explicit rollback needed
        pass

    # Arrow-integrated methods for columnar operations

    async def arrow_scan_to_table(self, start_key: Optional[str] = None,
                                end_key: Optional[str] = None,
                                limit: int = 10000) -> Optional[object]:
        """
        Scan data and return as Arrow table for columnar processing.

        Args:
            start_key: Start key for range scan
            end_key: End key for range scan
            limit: Maximum number of rows to return

        Returns:
            PyArrow Table or None if Arrow not available
        """
        if self._arrow_store:
            return await self._arrow_store.arrow_scan_to_table(start_key, end_key, limit)
        return None

    async def arrow_execute_query(self, query_type: str, **params) -> Optional[object]:
        """
        Execute Arrow-native queries directly on stored data.

        Args:
            query_type: Type of query ('filter', 'aggregate', 'sort', 'project', 'join')
            **params: Query-specific parameters

        Returns:
            PyArrow Table result or None
        """
        if self._arrow_store:
            return await self._arrow_store.execute_arrow_query(query_type, params)
        return None

    async def arrow_filter(self, condition: object) -> Optional[object]:
        """
        Filter data using Arrow compute expressions.

        Args:
            condition: PyArrow compute expression

        Returns:
            Filtered PyArrow Table
        """
        return await self.arrow_execute_query("filter", condition=condition)

    async def arrow_aggregate(self, group_by: List[str],
                            aggregations: Dict[str, str]) -> Optional[object]:
        """
        Perform aggregation using Arrow compute.

        Args:
            group_by: Columns to group by
            aggregations: Aggregation functions by column

        Returns:
            Aggregated PyArrow Table
        """
        return await self.arrow_execute_query("aggregate",
                                            group_by=group_by,
                                            aggregations=aggregations)

    async def arrow_join(self, other_table: object,
                        join_keys: List[str] = None,
                        join_type: str = "inner") -> Optional[object]:
        """
        Join with another Arrow table.

        Args:
            other_table: PyArrow Table to join with
            join_keys: Keys to join on
            join_type: Type of join ('inner', 'left', 'right', 'outer', 'asof')

        Returns:
            Joined PyArrow Table
        """
        return await self.arrow_execute_query("join",
                                            other_table=other_table,
                                            join_keys=join_keys or ["key"],
                                            join_type=join_type)

    async def arrow_streaming_aggregate(self, aggregation_type: str,
                                      group_by_cols: List[str],
                                      **params) -> AsyncIterator[object]:
        """
        Perform streaming aggregation operations.

        Args:
            aggregation_type: Type of aggregation ('groupby_count', 'time_window', 'rolling')
            group_by_cols: Columns to group by
            **params: Additional parameters for the aggregation

        Yields:
            PyArrow Tables with aggregation results
        """
        if self._arrow_store:
            async for result in self._arrow_store.arrow_streaming_aggregate(
                aggregation_type, group_by_cols, params
            ):
                yield result

    async def arrow_export_dataset(self, output_path: str,
                                 partitioning: Dict = None) -> bool:
        """
        Export data as Arrow Dataset for external processing.

        Args:
            output_path: Path to export dataset to
            partitioning: Partitioning configuration

        Returns:
            True if successful, False otherwise
        """
        if self._arrow_store:
            return await self._arrow_store.arrow_export_to_dataset(output_path, partitioning)
        return False

    async def get_arrow_stats(self) -> Dict[str, Any]:
        """
        Get Arrow-specific performance statistics.

        Returns:
            Dictionary with Arrow performance metrics
        """
        if self._arrow_store:
            return await self._arrow_store.get_arrow_stats()

        return {
            'arrow_available': ARROW_TONBO_AVAILABLE,
            'tonbo_available': TONBO_AVAILABLE,
            'cython_available': CYTHON_TONBO_AVAILABLE,
            'arrow_integration': False
        }
