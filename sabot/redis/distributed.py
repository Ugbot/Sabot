"""
FastRedis Distributed Systems Components

Distributed locks, counters, semaphores, and other distributed primitives.
"""

import time
from typing import Optional, Any, Dict, List
from contextlib import contextmanager


class DistributedLock:
    """
    Distributed lock implementation using Redis.
    Provides exclusive access to shared resources across multiple processes/instances.
    """

    def __init__(self, redis_client, lock_key: str, lock_value: Optional[str] = None,
                 ttl_ms: int = 30000, retry_delay: float = 0.1, max_retries: int = 50):
        self.redis = redis_client
        self.lock_key = lock_key
        self.lock_value = lock_value or f"lock:{id(self)}:{time.time()}"
        self.ttl_ms = ttl_ms
        self.retry_delay = retry_delay
        self.max_retries = max_retries

    @contextmanager
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None):
        """
        Context manager for acquiring and releasing locks.

        Args:
            blocking: Whether to block until lock is acquired
            timeout: Maximum time to wait for lock (None = no timeout)
        """
        acquired = self.try_acquire(blocking=blocking, timeout=timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release()

    def try_acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Try to acquire the lock.

        Args:
            blocking: Whether to block until lock is acquired
            timeout: Maximum time to wait for lock

        Returns:
            True if lock acquired, False otherwise
        """
        start_time = time.time()
        timeout = timeout or (self.max_retries * self.retry_delay)

        while True:
            # Try to set the lock
            result = self.redis.set(self.lock_key, self.lock_value, nx=True, px=self.ttl_ms)

            if result:
                return True

            # Check if we've timed out
            if not blocking or (time.time() - start_time) >= timeout:
                return False

            # Wait before retrying
            time.sleep(self.retry_delay)

    def release(self) -> bool:
        """
        Release the lock if we own it.

        Returns:
            True if lock was released, False otherwise
        """
        # Use Lua script to ensure atomic check-and-delete
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script, keys=[self.lock_key], args=[self.lock_value])
        return bool(result)

    def extend(self, ttl_ms: Optional[int] = None) -> bool:
        """
        Extend the lock TTL if we own it.

        Args:
            ttl_ms: New TTL in milliseconds (uses current TTL if None)

        Returns:
            True if extended successfully
        """
        ttl_ms = ttl_ms or self.ttl_ms

        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script, keys=[self.lock_key], args=[self.lock_value, ttl_ms])
        return bool(result)

    def is_locked(self) -> bool:
        """
        Check if the lock is currently held.

        Returns:
            True if lock is held by anyone
        """
        return self.redis.exists(self.lock_key)

    def get_lock_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the current lock holder.

        Returns:
            Lock information or None if not locked
        """
        lock_value = self.redis.get(self.lock_key)
        if not lock_value:
            return None

        ttl = self.redis.pttl(self.lock_key)

        return {
            'owner': lock_value.decode() if isinstance(lock_value, bytes) else lock_value,
            'ttl_ms': ttl,
            'is_owned_by_me': lock_value == self.lock_value
        }


class ReadWriteLock:
    """
    Distributed read-write lock allowing multiple readers or single writer.
    """

    def __init__(self, redis_client, lock_key: str, lock_value: Optional[str] = None,
                 ttl_ms: int = 30000):
        self.redis = redis_client
        self.base_key = lock_key
        self.lock_value = lock_value or f"rwlock:{id(self)}:{time.time()}"
        self.ttl_ms = ttl_ms

        # Separate keys for read and write locks
        self.write_lock_key = f"{lock_key}:write"
        self.read_count_key = f"{lock_key}:readers"

    @contextmanager
    def read_lock(self):
        """Context manager for read locks"""
        self.acquire_read_lock()
        try:
            yield
        finally:
            self.release_read_lock()

    @contextmanager
    def write_lock(self):
        """Context manager for write locks"""
        self.acquire_write_lock()
        try:
            yield
        finally:
            self.release_write_lock()

    def acquire_read_lock(self) -> bool:
        """
        Acquire a read lock.

        Returns:
            True if lock acquired
        """
        # Check if write lock exists
        if self.redis.exists(self.write_lock_key):
            return False

        # Increment reader count atomically
        script = """
        if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('INCR', KEYS[2])
            redis.call('EXPIRE', KEYS[2], ARGV[1])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script,
                               keys=[self.write_lock_key, self.read_count_key],
                               args=[self.ttl_ms // 1000])
        return bool(result)

    def release_read_lock(self) -> bool:
        """
        Release a read lock.

        Returns:
            True if lock released
        """
        script = """
        local count = redis.call('DECR', KEYS[1])
        if count <= 0 then
            redis.call('DEL', KEYS[1])
        end
        return 1
        """

        result = self.redis.eval(script, keys=[self.read_count_key], args=[])
        return bool(result)

    def acquire_write_lock(self) -> bool:
        """
        Acquire a write lock.

        Returns:
            True if lock acquired
        """
        # Check if any readers or another writer
        if self.redis.exists(self.write_lock_key) or self.redis.exists(self.read_count_key):
            return False

        # Acquire write lock
        result = self.redis.set(self.write_lock_key, self.lock_value, nx=True, px=self.ttl_ms)
        return bool(result)

    def release_write_lock(self) -> bool:
        """
        Release a write lock.

        Returns:
            True if lock released
        """
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script, keys=[self.write_lock_key], args=[self.lock_value])
        return bool(result)


class Semaphore:
    """
    Distributed semaphore for controlling access to limited resources.
    """

    def __init__(self, redis_client, semaphore_key: str, max_permits: int = 1,
                 lock_value: Optional[str] = None, ttl_ms: int = 30000):
        self.redis = redis_client
        self.semaphore_key = semaphore_key
        self.max_permits = max_permits
        self.lock_value = lock_value or f"semaphore:{id(self)}:{time.time()}"
        self.ttl_ms = ttl_ms

    @contextmanager
    def acquire(self, permits: int = 1, blocking: bool = True, timeout: Optional[float] = None):
        """
        Context manager for acquiring semaphore permits.

        Args:
            permits: Number of permits to acquire
            blocking: Whether to block until permits available
            timeout: Maximum time to wait
        """
        acquired = self.try_acquire(permits, blocking=blocking, timeout=timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(permits)

    def try_acquire(self, permits: int = 1, blocking: bool = True,
                   timeout: Optional[float] = None) -> bool:
        """
        Try to acquire semaphore permits.

        Args:
            permits: Number of permits to acquire
            blocking: Whether to block until permits available
            timeout: Maximum time to wait

        Returns:
            True if permits acquired
        """
        start_time = time.time()
        timeout = timeout or 30.0

        while True:
            # Try to acquire permits atomically
            script = """
            local current = tonumber(redis.call('GET', KEYS[1]) or ARGV[2])
            local requested = tonumber(ARGV[1])

            if current >= requested then
                redis.call('SET', KEYS[1], current - requested)
                redis.call('EXPIRE', KEYS[1], ARGV[3])
                return 1
            else
                return 0
            end
            """

            result = self.redis.eval(script,
                                   keys=[self.semaphore_key],
                                   args=[permits, self.max_permits, self.ttl_ms // 1000])

            if result:
                return True

            if not blocking or (time.time() - start_time) >= timeout:
                return False

            time.sleep(0.1)

    def release(self, permits: int = 1) -> bool:
        """
        Release semaphore permits.

        Args:
            permits: Number of permits to release

        Returns:
            True if permits released
        """
        script = """
        local current = tonumber(redis.call('GET', KEYS[1]) or '0')
        local released = tonumber(ARGV[1])
        local max_permits = tonumber(ARGV[2])

        local new_count = math.min(max_permits, current + released)
        redis.call('SET', KEYS[1], new_count)
        redis.call('EXPIRE', KEYS[1], ARGV[3])

        return new_count
        """

        result = self.redis.eval(script,
                               keys=[self.semaphore_key],
                               args=[permits, self.max_permits, self.ttl_ms // 1000])
        return result is not None

    def available_permits(self) -> int:
        """
        Get number of available permits.

        Returns:
            Number of available permits
        """
        current = self.redis.get(self.semaphore_key)
        if current is None:
            return self.max_permits

        current = int(current)
        return max(0, self.max_permits - current)

    def get_semaphore_info(self) -> Dict[str, Any]:
        """
        Get semaphore information.

        Returns:
            Semaphore status information
        """
        current = self.redis.get(self.semaphore_key)
        current_permits = int(current) if current else 0
        available = max(0, self.max_permits - current_permits)

        return {
            'max_permits': self.max_permits,
            'current_permits_used': current_permits,
            'available_permits': available,
            'utilization_percent': (current_permits / self.max_permits * 100) if self.max_permits > 0 else 0
        }


class DistributedCounter:
    """
    Distributed atomic counter with bounds and statistics.
    """

    def __init__(self, redis_client, counter_key: str, initial_value: int = 0,
                 min_value: Optional[int] = None, max_value: Optional[int] = None):
        self.redis = redis_client
        self.counter_key = counter_key
        self.min_value = min_value
        self.max_value = max_value

        # Initialize if doesn't exist
        if not self.redis.exists(counter_key):
            self.redis.set(counter_key, initial_value)

    def increment(self, amount: int = 1) -> int:
        """
        Increment counter by specified amount.

        Args:
            amount: Amount to increment (can be negative)

        Returns:
            New counter value after increment
        """
        if self.min_value is not None or self.max_value is not None:
            # Use Lua script for bounds checking
            script = """
            local key = KEYS[1]
            local increment = tonumber(ARGV[1])
            local min_val = ARGV[2]
            local max_val = ARGV[3]

            local current = tonumber(redis.call('GET', key) or '0')
            local new_value = current + increment

            -- Apply bounds
            if min_val ~= 'none' then
                min_val = tonumber(min_val)
                if new_value < min_val then
                    new_value = min_val
                end
            end

            if max_val ~= 'none' then
                max_val = tonumber(max_val)
                if new_value > max_val then
                    new_value = max_val
                end
            end

            redis.call('SET', key, new_value)
            return new_value
            """

            min_str = str(self.min_value) if self.min_value is not None else 'none'
            max_str = str(self.max_value) if self.max_value is not None else 'none'

            result = self.redis.eval(script, keys=[self.counter_key],
                                   args=[amount, min_str, max_str])
            return int(result)
        else:
            # Simple increment without bounds
            return self.redis.incrby(self.counter_key, amount)

    def get(self) -> int:
        """
        Get current counter value.

        Returns:
            Current counter value
        """
        value = self.redis.get(self.counter_key)
        return int(value) if value else 0

    def set(self, value: int) -> bool:
        """
        Set counter to specific value.

        Args:
            value: New counter value

        Returns:
            True if set successfully
        """
        # Apply bounds if set
        if self.min_value is not None and value < self.min_value:
            value = self.min_value
        if self.max_value is not None and value > self.max_value:
            value = self.max_value

        return self.redis.set(self.counter_key, value)

    def reset(self) -> bool:
        """
        Reset counter to 0.

        Returns:
            True if reset successfully
        """
        return self.set(0)

    def get_info(self) -> Dict[str, Any]:
        """
        Get counter information and statistics.

        Returns:
            Counter information
        """
        value = self.get()

        return {
            'value': value,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'at_min': self.min_value is not None and value <= self.min_value,
            'at_max': self.max_value is not None and value >= self.max_value,
            'in_bounds': (self.min_value is None or value >= self.min_value) and
                        (self.max_value is None or value <= self.max_value)
        }


class LeaderElection:
    """
    Distributed leader election using Redis.
    """

    def __init__(self, redis_client, election_key: str, candidate_id: str,
                 ttl_seconds: int = 30):
        self.redis = redis_client
        self.election_key = election_key
        self.candidate_id = candidate_id
        self.ttl_seconds = ttl_seconds

    def try_become_leader(self) -> bool:
        """
        Try to become the leader.

        Returns:
            True if became leader, False otherwise
        """
        result = self.redis.set(self.election_key, self.candidate_id,
                              nx=True, ex=self.ttl_seconds)
        return bool(result)

    def is_leader(self) -> bool:
        """
        Check if current instance is the leader.

        Returns:
            True if this instance is the leader
        """
        current_leader = self.redis.get(self.election_key)
        return current_leader == self.candidate_id

    def renew_leadership(self) -> bool:
        """
        Renew leadership if currently leader.

        Returns:
            True if leadership renewed
        """
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script, keys=[self.election_key],
                               args=[self.candidate_id, self.ttl_seconds])
        return bool(result)

    def get_current_leader(self) -> Optional[str]:
        """
        Get the current leader.

        Returns:
            Current leader ID or None
        """
        leader = self.redis.get(self.election_key)
        return leader if leader else None

    def resign_leadership(self) -> bool:
        """
        Resign from leadership if currently leader.

        Returns:
            True if resigned successfully
        """
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        else
            return 0
        end
        """

        result = self.redis.eval(script, keys=[self.election_key],
                               args=[self.candidate_id])
        return bool(result)

