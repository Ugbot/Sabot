# cython: language_level=3
# distutils: language=c

__version__ = "0.1.0"

import asyncio
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable
from libc.stdlib cimport malloc, free, calloc, realloc
from libc.string cimport memcpy, strlen, strcpy
from libc.stdio cimport printf
from cpython.ref cimport Py_INCREF, Py_DECREF, Py_XDECREF
from cpython.exc cimport PyErr_CheckSignals
from cpython cimport PyObject, PyThreadState_SetAsyncExc
from posix.unistd cimport usleep
from posix.pthread cimport pthread_t, pthread_create, pthread_join, pthread_detach, pthread_self
from posix.pthread cimport pthread_mutex_t, pthread_mutex_init, pthread_mutex_destroy
from posix.pthread cimport pthread_mutex_lock, pthread_mutex_unlock
from posix.pthread cimport pthread_cond_t, pthread_cond_init, pthread_cond_destroy
from posix.pthread cimport pthread_cond_wait, pthread_cond_signal, pthread_cond_broadcast

# sys/time.h for timeval (needed by hiredis)
cdef extern from "sys/time.h":
    struct timeval:
        long tv_sec
        long tv_usec

# Hiredis includes
cdef extern from "hiredis/hiredis.h":
    ctypedef struct redisContext:
        int err
        char errstr[128]
        int fd
        int flags
        char *obuf
        redisReader *reader

    ctypedef struct redisReply:
        int type
        long long integer
        double dval
        size_t len
        char *str
        size_t elements
        redisReply **element

    redisContext *redisConnect(const char *ip, int port)
    redisContext *redisConnectWithTimeout(const char *ip, int port, const timeval tv)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)

    redisReply *redisCommand(redisContext *c, const char *format, ...)
    void freeReplyObject(redisReply *reply)

cdef extern from "hiredis/read.h":
    ctypedef struct redisReader:
        pass

    redisReader *redisReaderCreate()
    void redisReaderFree(redisReader *reader)
    int redisReaderFeed(redisReader *reader, const char *buf, size_t len)
    int redisReaderGetReply(redisReader *reader, void **reply)

# C-level task queue structures
ctypedef struct Task:
    PyObject* func      # Function to execute
    PyObject* args      # Arguments tuple
    PyObject* kwargs    # Keyword arguments dict
    PyObject* result    # Result of execution
    PyObject* exception # Exception if any
    int completed       # Completion flag
    Task* next          # Next task in queue

ctypedef struct TaskQueue:
    Task* head          # Queue head
    Task* tail          # Queue tail
    int size            # Current queue size
    int max_size        # Maximum queue size
    pthread_mutex_t lock
    pthread_cond_t cond

ctypedef struct ThreadPool:
    pthread_t* threads          # Array of thread IDs
    int num_threads             # Number of threads
    TaskQueue* queue           # Task queue
    int shutdown               # Shutdown flag
    pthread_mutex_t shutdown_lock

# C-level queue operations
cdef TaskQueue* task_queue_create(int max_size):
    """Create a new task queue"""
    cdef TaskQueue* queue = <TaskQueue*>malloc(sizeof(TaskQueue))
    if queue == NULL:
        return NULL

    queue.head = NULL
    queue.tail = NULL
    queue.size = 0
    queue.max_size = max_size

    pthread_mutex_init(&queue.lock, NULL)
    pthread_cond_init(&queue.cond, NULL)

    return queue

cdef void task_queue_destroy(TaskQueue* queue):
    """Destroy a task queue"""
    if queue == NULL:
        return

    # Free all remaining tasks
    cdef Task* task = queue.head
    cdef Task* next_task
    while task != NULL:
        next_task = task.next
        Py_XDECREF(task.func)
        Py_XDECREF(task.args)
        Py_XDECREF(task.kwargs)
        Py_XDECREF(task.result)
        Py_XDECREF(task.exception)
        free(task)
        task = next_task

    pthread_mutex_destroy(&queue.lock)
    pthread_cond_destroy(&queue.cond)
    free(queue)

cdef int task_queue_push(TaskQueue* queue, PyObject* func, PyObject* args, PyObject* kwargs):
    """Push a task onto the queue"""
    if queue == NULL:
        return -1

    pthread_mutex_lock(&queue.lock)

    # Check if queue is full
    if queue.max_size > 0 and queue.size >= queue.max_size:
        pthread_mutex_unlock(&queue.lock)
        return -1

    # Create new task
    cdef Task* task = <Task*>malloc(sizeof(Task))
    if task == NULL:
        pthread_mutex_unlock(&queue.lock)
        return -1

    Py_INCREF(func)
    Py_INCREF(args)
    if kwargs != NULL:
        Py_INCREF(kwargs)

    task.func = func
    task.args = args
    task.kwargs = kwargs
    task.result = NULL
    task.exception = NULL
    task.completed = 0
    task.next = NULL

    # Add to queue
    if queue.tail == NULL:
        queue.head = task
        queue.tail = task
    else:
        queue.tail.next = task
        queue.tail = task

    queue.size += 1

    # Signal waiting threads
    pthread_cond_signal(&queue.cond)
    pthread_mutex_unlock(&queue.lock)

    return 0

cdef Task* task_queue_pop(TaskQueue* queue):
    """Pop a task from the queue (blocking)"""
    if queue == NULL:
        return NULL

    pthread_mutex_lock(&queue.lock)

    while queue.size == 0 and queue.head == NULL:
        pthread_cond_wait(&queue.cond, &queue.lock)

    if queue.head == NULL:
        pthread_mutex_unlock(&queue.lock)
        return NULL

    cdef Task* task = queue.head
    queue.head = task.next
    if queue.head == NULL:
        queue.tail = NULL
    queue.size -= 1

    pthread_mutex_unlock(&queue.lock)
    return task

cdef void task_destroy(Task* task):
    """Destroy a task"""
    if task == NULL:
        return

    Py_XDECREF(task.func)
    Py_XDECREF(task.args)
    Py_XDECREF(task.kwargs)
    Py_XDECREF(task.result)
    Py_XDECREF(task.exception)
    free(task)

# Thread pool worker function
cdef void* thread_worker(void* arg):
    """Worker thread function"""
    cdef ThreadPool* pool = <ThreadPool*>arg
    cdef Task* task

    while True:
        # Check shutdown flag
        pthread_mutex_lock(&pool.shutdown_lock)
        if pool.shutdown:
            pthread_mutex_unlock(&pool.shutdown_lock)
            break
        pthread_mutex_unlock(&pool.shutdown_lock)

        # Get task from queue
        task = task_queue_pop(pool.queue)
        if task == NULL:
            continue

        # Execute task
        try:
            # Call Python function
            cdef PyObject* result
            if task.kwargs != NULL and task.kwargs != Py_None:
                result = PyObject_Call(task.func, task.args, task.kwargs)
            else:
                result = PyObject_CallObject(task.func, task.args)

            if result != NULL:
                task.result = result
            else:
                # Exception occurred
                cdef PyObject* exc_type
                cdef PyObject* exc_value
                cdef PyObject* exc_traceback
                PyErr_Fetch(&exc_type, &exc_value, &exc_traceback)
                task.exception = exc_value
                Py_XDECREF(exc_type)
                Py_XDECREF(exc_traceback)
        except:
            cdef PyObject* exc_type
            cdef PyObject* exc_value
            cdef PyObject* exc_traceback
            PyErr_Fetch(&exc_type, &exc_value, &exc_traceback)
            task.exception = exc_value
            Py_XDECREF(exc_type)
            Py_XDECREF(exc_traceback)

        task.completed = 1

    return NULL

cdef ThreadPool* thread_pool_create(int num_threads, int queue_size):
    """Create a thread pool"""
    cdef ThreadPool* pool = <ThreadPool*>malloc(sizeof(ThreadPool))
    if pool == NULL:
        return NULL

    pool.num_threads = num_threads
    pool.shutdown = 0
    pool.queue = task_queue_create(queue_size)

    if pool.queue == NULL:
        free(pool)
        return NULL

    pthread_mutex_init(&pool.shutdown_lock, NULL)

    # Allocate thread array
    pool.threads = <pthread_t*>malloc(sizeof(pthread_t) * num_threads)
    if pool.threads == NULL:
        task_queue_destroy(pool.queue)
        free(pool)
        return NULL

    # Start worker threads
    cdef int i
    for i in range(num_threads):
        if pthread_create(&pool.threads[i], NULL, thread_worker, <void*>pool) != 0:
            # Failed to create thread, cleanup and return NULL
            pool.num_threads = i  # Only count successfully created threads
            thread_pool_destroy(pool)
            return NULL

    return pool

cdef void thread_pool_destroy(ThreadPool* pool):
    """Destroy a thread pool"""
    if pool == NULL:
        return

    # Signal shutdown
    pthread_mutex_lock(&pool.shutdown_lock)
    pool.shutdown = 1
    pthread_mutex_unlock(&pool.shutdown_lock)

    # Wake up all waiting threads
    pthread_cond_broadcast(&pool.queue.cond)

    # Wait for all threads to finish
    cdef int i
    for i in range(pool.num_threads):
        pthread_join(pool.threads[i], NULL)

    # Cleanup
    if pool.threads != NULL:
        free(pool.threads)
    if pool.queue != NULL:
        task_queue_destroy(pool.queue)
    pthread_mutex_destroy(&pool.shutdown_lock)
    free(pool)

cdef int thread_pool_submit(ThreadPool* pool, PyObject* func, PyObject* args, PyObject* kwargs=NULL):
    """Submit a task to the thread pool"""
    if pool == NULL or pool.queue == NULL:
        return -1

    return task_queue_push(pool.queue, func, args, kwargs)

# Python wrapper classes for C-level queues and threading

cdef class CThreadPool:
    """C-level thread pool wrapper"""
    cdef ThreadPool* pool

    def __cinit__(self, int num_threads, int queue_size=0):
        self.pool = thread_pool_create(num_threads, queue_size)
        if self.pool == NULL:
            raise MemoryError("Failed to create thread pool")

    def __dealloc__(self):
        if self.pool != NULL:
            thread_pool_destroy(self.pool)

    def submit(self, func, args=None, kwargs=None):
        """Submit a task to the thread pool"""
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        cdef PyObject* py_func = <PyObject*>func
        cdef PyObject* py_args = <PyObject*>args
        cdef PyObject* py_kwargs = <PyObject*>kwargs

        if thread_pool_submit(self.pool, py_func, py_args, py_kwargs) != 0:
            raise RuntimeError("Failed to submit task to thread pool")

    def shutdown(self):
        """Shutdown the thread pool"""
        if self.pool != NULL:
            thread_pool_destroy(self.pool)
            self.pool = NULL

# Thread-safe connection pool
cdef class ConnectionPool:
    cdef:
        list connections
        int max_connections
        object lock
        str host
        int port
        double timeout

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, double timeout=5.0):
        self.connections = []
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.timeout = timeout

    cdef redisContext* _get_connection(self):
        with self.lock:
            if self.connections:
                return self.connections.pop()
            elif len(self.connections) < self.max_connections:
                cdef redisContext* ctx
                cdef timeval tv
                tv.tv_sec = <long>self.timeout
                tv.tv_usec = <long>((self.timeout - tv.tv_sec) * 1000000)
                ctx = redisConnectWithTimeout(self.host.encode('utf-8'), self.port, tv)
                if ctx == NULL or ctx.err:
                    if ctx:
                        redisFree(ctx)
                    raise ConnectionError(f"Failed to connect to Redis: {ctx.errstr.decode('utf-8') if ctx else 'Unknown error'}")
                return ctx
        return NULL

    cdef void _return_connection(self, redisContext* ctx):
        with self.lock:
            if len(self.connections) < self.max_connections:
                self.connections.append(ctx)
            else:
                redisFree(ctx)

    def close(self):
        with self.lock:
            for ctx in self.connections:
                if ctx:
                    redisFree(ctx)
            self.connections.clear()

# High-performance Redis client
cdef class FastRedisClient:
    cdef:
        ConnectionPool pool
        CThreadPool thread_pool
        dict stream_offsets
        object offset_lock

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, int max_workers=4):
        self.pool = ConnectionPool(host, port, max_connections)
        self.thread_pool = CThreadPool(max_workers, 0)  # 0 = unlimited queue
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()
        self._script_cache = {}  # Cache for script SHA hashes

    def __dealloc__(self):
        if self.thread_pool:
            self.thread_pool.shutdown()
        if self.pool:
            self.pool.close()

    # Additional high-performance operations

    def mget(self, keys):
        """Get multiple keys in a single operation"""
        cdef list cmd_args = ['MGET']
        cmd_args.extend(keys)
        return self._execute_command(cmd_args)

    def mset(self, key_value_dict):
        """Set multiple keys in a single operation"""
        cdef list cmd_args = ['MSET']
        for key, value in key_value_dict.items():
            cmd_args.extend([key, str(value)])
        return self._execute_command(cmd_args)

    def incr(self, key):
        """Increment a key by 1"""
        return self._execute_command(['INCR', key])

    def incrby(self, key, amount):
        """Increment a key by specified amount"""
        return self._execute_command(['INCRBY', key, str(amount)])

    def decr(self, key):
        """Decrement a key by 1"""
        return self._execute_command(['DECR', key])

    def decrby(self, key, amount):
        """Decrement a key by specified amount"""
        return self._execute_command(['DECRBY', key, str(amount)])

    def exists(self, keys):
        """Check if keys exist"""
        cdef list cmd_args = ['EXISTS']
        if isinstance(keys, list):
            cmd_args.extend(keys)
        else:
            cmd_args.append(keys)
        return self._execute_command(cmd_args)

    def expire(self, key, seconds):
        """Set key expiration in seconds"""
        return self._execute_command(['EXPIRE', key, str(seconds)])

    def ttl(self, key):
        """Get time-to-live for a key"""
        return self._execute_command(['TTL', key])

    def persist(self, key):
        """Remove expiration from a key"""
        return self._execute_command(['PERSIST', key])

    def keys(self, pattern):
        """Find keys matching a pattern"""
        return self._execute_command(['KEYS', pattern])

    def dbsize(self):
        """Get the number of keys in the database"""
        return self._execute_command(['DBSIZE'])

    def flushdb(self, async=False):
        """Remove all keys from the current database"""
        if async:
            return self._execute_command(['FLUSHDB', 'ASYNC'])
        else:
            return self._execute_command(['FLUSHDB'])

    def ping(self):
        """Ping the Redis server"""
        return self._execute_command(['PING'])

    def echo(self, message):
        """Echo a message"""
        return self._execute_command(['ECHO', message])

    def select(self, db):
        """Select database"""
        return self._execute_command(['SELECT', str(db)])

    def info(self, section=None):
        """Get server information"""
        if section:
            return self._execute_command(['INFO', section])
        else:
            return self._execute_command(['INFO'])

    cdef object _execute_command(self, list args):
        cdef redisContext* ctx = self.pool._get_connection()
        if ctx == NULL:
            raise ConnectionError("No available connections")

        try:
            # Build command string
            cdef str cmd = "*%d\r\n" % len(args)
            for arg in args:
                if isinstance(arg, str):
                    arg_bytes = arg.encode('utf-8')
                elif isinstance(arg, bytes):
                    arg_bytes = arg
                else:
                    arg_bytes = str(arg).encode('utf-8')
                cmd += "$%d\r\n%s\r\n" % (len(arg_bytes), arg_bytes.decode('utf-8'))

            cdef bytes cmd_bytes = cmd.encode('utf-8')
            cdef redisReply* reply = redisCommand(ctx, cmd_bytes)
            if reply == NULL:
                raise ConnectionError(f"Redis command failed: {ctx.errstr.decode('utf-8')}")

            try:
                result = self._parse_reply(reply)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            self.pool._return_connection(ctx)

    cdef object _parse_reply(self, redisReply* reply):
        if reply.type == 1:  # REDIS_REPLY_STRING
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == 2:  # REDIS_REPLY_ARRAY
            return [self._parse_reply(reply.element[i]) for i in range(reply.elements)]
        elif reply.type == 3:  # REDIS_REPLY_INTEGER
            return reply.integer
        elif reply.type == 4:  # REDIS_REPLY_NIL
            return None
        elif reply.type == 5:  # REDIS_REPLY_STATUS
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == 6:  # REDIS_REPLY_ERROR
            raise RedisError(reply.str.decode('utf-8') if reply.str else "Unknown error")
        else:
            return None

    # High-level API methods
    def set(self, str key, str value):
        return self._execute_command(['SET', key, value])

    def get(self, str key):
        return self._execute_command(['GET', key])

    def delete(self, str key):
        return self._execute_command(['DEL', key])

    def publish(self, str channel, str message):
        return self._execute_command(['PUBLISH', channel, message])

    def xadd(self, str stream, dict data, str message_id='*'):
        args = ['XADD', stream, message_id]
        for k, v in data.items():
            args.extend([k, str(v)])
        return self._execute_command(args)

    def xread(self, dict streams, int count=10, int block=1000):
        args = ['XREAD', 'COUNT', str(count), 'BLOCK', str(block), 'STREAMS']
        args.extend(streams.keys())
        args.extend(streams.values())
        result = self._execute_command(args)
        if result is None:
            return []
        return self._parse_xread_result(result)

    # Keyspace notifications and watch operations
    def watch(self, keys):
        """Watch keys for changes (optimistic locking)"""
        if isinstance(keys, str):
            keys = [keys]
        args = ['WATCH'] + keys
        return self._execute_command(args)

    def unwatch(self):
        """Unwatch all keys"""
        return self._execute_command(['UNWATCH'])

    def config_set(self, parameter, value):
        """Set Redis configuration"""
        return self._execute_command(['CONFIG', 'SET', parameter, str(value)])

    def config_get(self, parameter):
        """Get Redis configuration"""
        return self._execute_command(['CONFIG', 'GET', parameter])

    def enable_keyspace_notifications(self, events='AKE'):
        """
        Enable keyspace notifications
        Events: A=Alias, E=Expired, e=Evicted, K=Keyspace, g=Generic, etc.
        """
        return self.config_set('notify-keyspace-events', events)

    def psubscribe(self, patterns):
        """Subscribe to pub/sub patterns"""
        if isinstance(patterns, str):
            patterns = [patterns]
        args = ['PSUBSCRIBE'] + patterns
        return self._execute_command(args)

    def punsubscribe(self, patterns=None):
        """Unsubscribe from pub/sub patterns"""
        args = ['PUNSUBSCRIBE']
        if patterns:
            if isinstance(patterns, str):
                patterns = [patterns]
            args.extend(patterns)
        return self._execute_command(args)

    def subscribe(self, channels):
        """Subscribe to pub/sub channels"""
        if isinstance(channels, str):
            channels = [channels]
        args = ['SUBSCRIBE'] + channels
        return self._execute_command(args)

    def unsubscribe(self, channels=None):
        """Unsubscribe from pub/sub channels"""
        args = ['UNSUBSCRIBE']
        if channels:
            if isinstance(channels, str):
                channels = [channels]
            args.extend(channels)
        return self._execute_command(args)

    def publish(self, channel, message):
        """Publish message to channel"""
        return self._execute_command(['PUBLISH', channel, message])

    def pubsub_channels(self, pattern=None):
        """List active pub/sub channels"""
        args = ['PUBSUB', 'CHANNELS']
        if pattern:
            args.append(pattern)
        return self._execute_command(args)

    def pubsub_numsub(self, channels=None):
        """Get number of subscribers for channels"""
        args = ['PUBSUB', 'NUMSUB']
        if channels:
            if isinstance(channels, str):
                channels = [channels]
            args.extend(channels)
        return self._execute_command(args)

    # Enhanced stream operations
    def xrange(self, stream, start='-', end='+', count=None):
        """Read stream entries in range"""
        args = ['XRANGE', stream, start, end]
        if count is not None:
            args.extend(['COUNT', str(count)])
        return self._execute_command(args)

    def xrevrange(self, stream, end='+', start='-', count=None):
        """Read stream entries in reverse range"""
        args = ['XREVRANGE', stream, end, start]
        if count is not None:
            args.extend(['COUNT', str(count)])
        return self._execute_command(args)

    def xtrim(self, stream, maxlen=None, minid=None, limit=None):
        """Trim stream to max length or min ID"""
        args = ['XTRIM', stream]
        if maxlen is not None:
            args.extend(['MAXLEN', str(maxlen)])
        elif minid is not None:
            args.extend(['MINID', str(minid)])
        if limit is not None:
            args.extend(['LIMIT', str(limit)])
        return self._execute_command(args)

    def xdel(self, stream, message_ids):
        """Delete messages from stream"""
        if isinstance(message_ids, str):
            message_ids = [message_ids]
        args = ['XDEL', stream] + message_ids
        return self._execute_command(args)

    def xlen(self, stream):
        """Get stream length"""
        return self._execute_command(['XLEN', stream])

    def xinfo_stream(self, stream):
        """Get stream information"""
        return self._execute_command(['XINFO', 'STREAM', stream])

    def xinfo_groups(self, stream):
        """Get consumer groups for stream"""
        return self._execute_command(['XINFO', 'GROUPS', stream])

    def xinfo_consumers(self, stream, group):
        """Get consumers in a group"""
        return self._execute_command(['XINFO', 'CONSUMERS', stream, group])

    def xgroup_create(self, stream, group, id='$', mkstream=False):
        """Create a consumer group"""
        args = ['XGROUP', 'CREATE', stream, group, id]
        if mkstream:
            args.append('MKSTREAM')
        return self._execute_command(args)

    def xgroup_destroy(self, stream, group):
        """Destroy a consumer group"""
        return self._execute_command(['XGROUP', 'DESTROY', stream, group])

    def xgroup_delconsumer(self, stream, group, consumer):
        """Delete a consumer from group"""
        return self._execute_command(['XGROUP', 'DELCONSUMER', stream, group, consumer])

    def xreadgroup(self, group, consumer, streams, count=None, block=None, noack=False):
        """Read from stream as consumer group"""
        args = ['XREADGROUP', 'GROUP', group, consumer]
        if count is not None:
            args.extend(['COUNT', str(count)])
        if block is not None:
            args.extend(['BLOCK', str(block)])
        if noack:
            args.append('NOACK')
        args.append('STREAMS')
        args.extend(streams.keys())
        args.extend(streams.values())
        result = self._execute_command(args)
        if result is None:
            return []
        return self._parse_xread_result(result)

    def xack(self, stream, group, message_ids):
        """Acknowledge messages in consumer group"""
        if isinstance(message_ids, str):
            message_ids = [message_ids]
        args = ['XACK', stream, group] + message_ids
        return self._execute_command(args)

    def xpending(self, stream, group, start='-', end='+', count=None, consumer=None):
        """Get pending messages in consumer group"""
        args = ['XPENDING', stream, group, start, end]
        if count is not None:
            args.extend([str(count)])
        if consumer is not None:
            args.extend([consumer])
        return self._execute_command(args)

    def xclaim(self, stream, group, consumer, min_idle_time, message_ids, idle=None, time=None, retrycount=None, force=False):
        """Claim pending messages"""
        args = ['XCLAIM', stream, group, consumer, str(min_idle_time)]
        if isinstance(message_ids, str):
            message_ids = [message_ids]
        args.extend(message_ids)
        if idle is not None:
            args.extend(['IDLE', str(idle)])
        if time is not None:
            args.extend(['TIME', str(time)])
        if retrycount is not None:
            args.extend(['RETRYCOUNT', str(retrycount)])
        if force:
            args.append('FORCE')
        return self._execute_command(args)

    # List operations for worker queues
    def lpush(self, key, values):
        """Push values to head of list"""
        if isinstance(values, str):
            values = [values]
        elif not isinstance(values, list):
            values = [values]
        args = ['LPUSH', key] + [str(v) for v in values]
        return self._execute_command(args)

    def rpush(self, key, values):
        """Push values to tail of list"""
        if isinstance(values, str):
            values = [values]
        elif not isinstance(values, list):
            values = [values]
        args = ['RPUSH', key] + [str(v) for v in values]
        return self._execute_command(args)

    def lpop(self, key, count=None):
        """Pop values from head of list"""
        args = ['LPOP', key]
        if count is not None:
            args.extend([str(count)])
        return self._execute_command(args)

    def rpop(self, key, count=None):
        """Pop values from tail of list"""
        args = ['RPOP', key]
        if count is not None:
            args.extend([str(count)])
        return self._execute_command(args)

    def blpop(self, keys, timeout=0):
        """Blocking left pop"""
        if isinstance(keys, str):
            keys = [keys]
        args = ['BLPOP'] + keys + [str(timeout)]
        return self._execute_command(args)

    def brpop(self, keys, timeout=0):
        """Blocking right pop"""
        if isinstance(keys, str):
            keys = [keys]
        args = ['BRPOP'] + keys + [str(timeout)]
        return self._execute_command(args)

    def llen(self, key):
        """Get list length"""
        return self._execute_command(['LLEN', key])

    def lrange(self, key, start, end):
        """Get range of list elements"""
        return self._execute_command(['LRANGE', key, str(start), str(end)])

    def ltrim(self, key, start, end):
        """Trim list to range"""
        return self._execute_command(['LTRIM', key, str(start), str(end)])

    def lindex(self, key, index):
        """Get element at index"""
        return self._execute_command(['LINDEX', key, str(index)])

    def lset(self, key, index, value):
        """Set element at index"""
        return self._execute_command(['LSET', key, str(index), str(value)])

    def lrem(self, key, count, value):
        """Remove elements equal to value"""
        return self._execute_command(['LREM', key, str(count), str(value)])

    def rpoplpush(self, source, destination):
        """Pop from source and push to destination"""
        return self._execute_command(['RPOPLPUSH', source, destination])

    def brpoplpush(self, source, destination, timeout=0):
        """Blocking pop from source and push to destination"""
        return self._execute_command(['BRPOPLPUSH', source, destination, str(timeout)])

    # Hash operations for distributed objects
    def hget(self, key, field):
        """Get hash field value"""
        return self._execute_command(['HGET', key, field])

    def hset(self, key, field, value):
        """Set hash field value"""
        return self._execute_command(['HSET', key, field, str(value)])

    def hmget(self, key, fields):
        """Get multiple hash fields"""
        if isinstance(fields, str):
            fields = [fields]
        return self._execute_command(['HMGET', key] + fields)

    def hmset(self, key, field_value_dict):
        """Set multiple hash fields"""
        args = ['HMSET', key]
        for field, value in field_value_dict.items():
            args.extend([field, str(value)])
        return self._execute_command(args)

    def hgetall(self, key):
        """Get all hash fields and values"""
        return self._execute_command(['HGETALL', key])

    def hkeys(self, key):
        """Get all hash field names"""
        return self._execute_command(['HKEYS', key])

    def hvals(self, key):
        """Get all hash values"""
        return self._execute_command(['HVALS', key])

    def hlen(self, key):
        """Get number of hash fields"""
        return self._execute_command(['HLEN', key])

    def hdel(self, key, fields):
        """Delete hash fields"""
        if isinstance(fields, str):
            fields = [fields]
        return self._execute_command(['HDEL', key] + fields)

    def hexists(self, key, field):
        """Check if hash field exists"""
        return self._execute_command(['HEXISTS', key, field])

    def hincrby(self, key, field, amount=1):
        """Increment hash field by integer"""
        return self._execute_command(['HINCRBY', key, field, str(amount)])

    def hincrbyfloat(self, key, field, amount=1.0):
        """Increment hash field by float"""
        return self._execute_command(['HINCRBYFLOAT', key, field, str(amount)])

    # Lua script evaluation
    def eval(self, script, keys=None, args=None):
        """Execute Lua script"""
        if keys is None:
            keys = []
        if args is None:
            args = []

        command_args = ['EVAL', script, str(len(keys))] + keys + args
        return self._execute_command(command_args)

    def evalsha(self, sha, keys=None, args=None):
        """Execute Lua script by SHA"""
        if keys is None:
            keys = []
        if args is None:
            args = []

        command_args = ['EVALSHA', sha, str(len(keys))] + keys + args
        return self._execute_command(command_args)

    def script_load(self, script):
        """Load Lua script into Redis"""
        return self._execute_command(['SCRIPT', 'LOAD', script])

    def script_kill(self):
        """Kill running Lua script"""
        return self._execute_command(['SCRIPT', 'KILL'])

    def script_flush(self):
        """Flush all Lua scripts"""
        return self._execute_command(['SCRIPT', 'FLUSH'])

    def script_exists(self, shas):
        """Check if scripts exist in cache"""
        if isinstance(shas, str):
            shas = [shas]
        args = ['SCRIPT', 'EXISTS'] + shas
        return self._execute_command(args)

    def script_debug(self, mode):
        """Set script debug mode (YES/NO/SYNC)"""
        return self._execute_command(['SCRIPT', 'DEBUG', mode])

    # Enhanced Lua script management utilities
    def load_script(self, script):
        """Load script and return SHA (with error handling)"""
        try:
            return self.script_load(script)
        except Exception as e:
            # Try to get more detailed error info
            if hasattr(e, 'args') and len(e.args) > 0:
                error_msg = str(e.args[0])
                if 'NOSCRIPT' in error_msg:
                    raise Exception(f"Script compilation failed: {error_msg}")
                elif 'BUSY' in error_msg:
                    raise Exception(f"Script engine busy: {error_msg}")
                else:
                    raise Exception(f"Script loading error: {error_msg}")
            raise

    def execute_script(self, script, keys=None, args=None, use_cache=True):
        """Execute script with automatic caching and error handling"""
        if keys is None:
            keys = []
        if args is None:
            args = []

        try:
            if use_cache:
                # Try to get cached SHA first
                sha = self.script_load(script)
                return self.evalsha(sha, keys, args)
            else:
                return self.eval(script, keys, args)
        except Exception as e:
            error_msg = str(e)
            if 'NOSCRIPT' in error_msg and use_cache:
                # Script not cached, load and retry
                sha = self.script_load(script)
                return self.evalsha(sha, keys, args)
            raise

    # High-performance batch operations
    def batch_operations(self, operations):
        """
        Execute multiple operations in batch for better performance.
        Operations is a list of (method_name, *args) tuples.
        """
        cdef list results = []
        cdef str method_name
        cdef list method_args
        cdef object result

        for operation in operations:
            method_name = operation[0]
            method_args = list(operation[1:])

            # Map method names to actual methods
            if method_name == 'set':
                result = self.set(*method_args)
            elif method_name == 'get':
                result = self.get(*method_args)
            elif method_name == 'delete':
                result = self.delete(*method_args)
            elif method_name == 'incr':
                result = self.incr(*method_args)
            elif method_name == 'mget':
                result = self.mget(*method_args)
            elif method_name == 'mset':
                result = self.mset(*method_args)
            else:
                # Fallback - could add more operations
                continue

            results.append(result)

        return results

    # AMQP-style routing operations optimized in Cython
    def amqp_route_message(self, exchange_name, exchange_type, routing_key, message_id, headers=None):
        """
        High-performance AMQP-style message routing using Lua scripts.
        Returns list of queue names to route message to.
        """
        cdef list bindings_key = [f"amqp:bindings:{exchange_name}"]
        cdef list args = [routing_key, message_id, exchange_name]

        if exchange_type == 'direct':
            # Direct routing - exact match
            script = """
            local bindings = redis.call('HGETALL', KEYS[1])
            local routing_key = ARGV[1]
            local results = {}

            for i = 1, #bindings, 2 do
                local binding_key = bindings[i]
                local queue_name = bindings[i + 1]

                local dot_pos = string.find(binding_key, '.', 1, true)
                if dot_pos then
                    local exch = string.sub(binding_key, 1, dot_pos - 1)
                    local key = string.sub(binding_key, dot_pos + 1)

                    if exch == ARGV[3] and key == routing_key then
                        results[queue_name] = true
                    end
                end
            end

            local final = {}
            for queue, _ in pairs(results) do
                table.insert(final, queue)
            end
            return final
            """
            return self.eval(script, bindings_key, args)

        elif exchange_type == 'topic':
            # Topic routing - pattern matching
            script = """
            local bindings = redis.call('HGETALL', KEYS[1])
            local routing_key = ARGV[1]
            local results = {}

            -- Split routing key
            local rk_parts = {}
            for part in string.gmatch(routing_key, "[^%.]+") do
                table.insert(rk_parts, part)
            end

            for i = 1, #bindings, 2 do
                local binding_key = bindings[i]
                local queue_name = bindings[i + 1]

                local dot_pos = string.find(binding_key, '.', 1, true)
                if dot_pos then
                    local exch = string.sub(binding_key, 1, dot_pos - 1)
                    local pattern = string.sub(binding_key, dot_pos + 1)

                    if exch == ARGV[3] and topic_match(pattern, routing_key) then
                        results[queue_name] = true
                    end
                end
            end

            local final = {}
            for queue, _ in pairs(results) do
                table.insert(final, queue)
            end
            return final
            """

            # Include topic matching helper
            topic_helper = """
            function topic_match(pattern, routing_key)
                local p_parts = {}
                for part in string.gmatch(pattern, "[^%.]+") do
                    table.insert(p_parts, part)
                end

                local rk_parts = {}
                for part in string.gmatch(routing_key, "[^%.]+") do
                    table.insert(rk_parts, part)
                end

                local p_idx = 1
                local rk_idx = 1

                while p_idx <= #p_parts and rk_idx <= #rk_parts do
                    local p_part = p_parts[p_idx]
                    local rk_part = rk_parts[rk_idx]

                    if p_part == '#' then
                        return true
                    elseif p_part == '*' then
                        p_idx = p_idx + 1
                        rk_idx = rk_idx + 1
                    elseif p_part == rk_part then
                        p_idx = p_idx + 1
                        rk_idx = rk_idx + 1
                    else
                        return false
                    end
                end

                return p_idx > #p_parts and rk_idx > #rk_parts
            end
            """

            full_script = topic_helper .. "\n\n" .. script
            return self.eval(full_script, bindings_key, args)

        elif exchange_type == 'fanout':
            # Fanout routing - all queues
            script = """
            local bindings = redis.call('HGETALL', KEYS[1])
            local results = {}

            for i = 1, #bindings, 2 do
                local binding_key = bindings[i]
                local queue_name = bindings[i + 1]

                local dot_pos = string.find(binding_key, '.', 1, true)
                if dot_pos then
                    local exch = string.sub(binding_key, 1, dot_pos - 1)
                    if exch == ARGV[1] then
                        results[queue_name] = true
                    end
                end
            end

            local final = {}
            for queue, _ in pairs(results) do
                table.insert(final, queue)
            end
            return final
            """
            return self.eval(script, [f"amqp:bindings:{exchange_name}"], [exchange_name])

        return []

    # Redis Module/Plugin commands
    def module_load(self, path, args=None):
        """Load a Redis module/plugin"""
        cmd_args = ['MODULE', 'LOAD', path]
        if args:
            cmd_args.extend(args)
        return self._execute_command(cmd_args)

    def module_unload(self, name):
        """Unload a Redis module/plugin"""
        return self._execute_command(['MODULE', 'UNLOAD', name])

    def module_list(self):
        """List loaded Redis modules/plugins"""
        return self._execute_command(['MODULE', 'LIST'])

    def module_info(self, name):
        """Get information about a loaded module/plugin"""
        return self._execute_command(['MODULE', 'INFO', name])

    # Redis Cluster commands
    def cluster_slots(self):
        """Get cluster slots information"""
        return self._execute_command(['CLUSTER', 'SLOTS'])

    def cluster_nodes(self):
        """Get cluster nodes information"""
        return self._execute_command(['CLUSTER', 'NODES'])

    def cluster_meet(self, ip, port):
        """Add node to cluster"""
        return self._execute_command(['CLUSTER', 'MEET', ip, str(port)])

    def cluster_forget(self, node_id):
        """Remove node from cluster"""
        return self._execute_command(['CLUSTER', 'FORGET', node_id])

    def cluster_replicate(self, node_id):
        """Configure node as replica"""
        return self._execute_command(['CLUSTER', 'REPLICATE', node_id])

    def cluster_failover(self, force=False):
        """Force failover"""
        args = ['CLUSTER', 'FAILOVER']
        if force:
            args.append('FORCE')
        return self._execute_command(args)

    def cluster_info(self):
        """Get cluster information"""
        return self._execute_command(['CLUSTER', 'INFO'])

    def cluster_keyslot(self, key):
        """Get hash slot for key"""
        return self._execute_command(['CLUSTER', 'KEYSLOT', key])

    def cluster_countkeysinslot(self, slot):
        """Count keys in hash slot"""
        return self._execute_command(['CLUSTER', 'COUNTKEYSINSLOT', str(slot)])

    def cluster_getkeysinslot(self, slot, count):
        """Get keys in hash slot"""
        return self._execute_command(['CLUSTER', 'GETKEYSINSLOT', str(slot), str(count)])

    def cluster_savaconfig(self):
        """Save cluster configuration"""
        return self._execute_command(['CLUSTER', 'SAVECONFIG'])

    def cluster_loadconfig(self):
        """Load cluster configuration"""
        return self._execute_command(['CLUSTER', 'LOADCONFIG'])

    def cluster_reset(self, hard=False):
        """Reset cluster node"""
        args = ['CLUSTER', 'RESET']
        if hard:
            args.append('HARD')
        else:
            args.append('SOFT')
        return self._execute_command(args)

    def cluster_flushslots(self):
        """Flush hash slots"""
        return self._execute_command(['CLUSTER', 'FLUSHSLOTS'])

    # Sentinel commands for Redis Sentinel management
    def sentinel_masters(self):
        """Get list of monitored masters"""
        return self._execute_command(['SENTINEL', 'MASTERS'])

    def sentinel_master(self, master_name):
        """Get master information"""
        return self._execute_command(['SENTINEL', 'MASTER', master_name])

    def sentinel_slaves(self, master_name):
        """Get slave information"""
        return self._execute_command(['SENTINEL', 'SLAVES', master_name])

    def sentinel_sentinels(self, master_name):
        """Get sentinel information"""
        return self._execute_command(['SENTINEL', 'SENTINELS', master_name])

    def sentinel_get_master_addr_by_name(self, master_name):
        """Get master address"""
        return self._execute_command(['SENTINEL', 'GET-MASTER-ADDR-BY-NAME', master_name])

    def sentinel_reset(self, pattern):
        """Reset masters matching pattern"""
        return self._execute_command(['SENTINEL', 'RESET', pattern])

    def sentinel_failover(self, master_name):
        """Force failover"""
        return self._execute_command(['SENTINEL', 'FAILOVER', master_name])

    def sentinel_monitor(self, name, ip, port, quorum):
        """Monitor a new master"""
        return self._execute_command(['SENTINEL', 'MONITOR', name, ip, str(port), str(quorum)])

    def sentinel_remove(self, name):
        """Remove master monitoring"""
        return self._execute_command(['SENTINEL', 'REMOVE', name])

    def sentinel_set(self, master_name, option, value):
        """Set sentinel configuration"""
        return self._execute_command(['SENTINEL', 'SET', master_name, option, value])

    # Set operations
    def sadd(self, key, members):
        """Add members to set"""
        if isinstance(members, str):
            members = [members]
        elif not isinstance(members, list):
            members = [members]
        args = ['SADD', key] + [str(m) for m in members]
        return self._execute_command(args)

    def srem(self, key, members):
        """Remove members from set"""
        if isinstance(members, str):
            members = [members]
        elif not isinstance(members, list):
            members = [members]
        args = ['SREM', key] + [str(m) for m in members]
        return self._execute_command(args)

    def smembers(self, key):
        """Get all set members"""
        return self._execute_command(['SMEMBERS', key])

    def scard(self, key):
        """Get set cardinality"""
        return self._execute_command(['SCARD', key])

    def sismember(self, key, member):
        """Check if member is in set"""
        return self._execute_command(['SISMEMBER', key, str(member)])

    def spop(self, key, count=None):
        """Remove and return random members"""
        args = ['SPOP', key]
        if count is not None:
            args.append(str(count))
        return self._execute_command(args)

    def srandmember(self, key, count=None):
        """Get random members without removing"""
        args = ['SRANDMEMBER', key]
        if count is not None:
            args.append(str(count))
        return self._execute_command(args)

    # Sorted set operations
    def zadd(self, key, score_member_dict):
        """Add members to sorted set"""
        args = ['ZADD', key]
        for member, score in score_member_dict.items():
            args.extend([str(score), str(member)])
        return self._execute_command(args)

    def zrem(self, key, members):
        """Remove members from sorted set"""
        if isinstance(members, str):
            members = [members]
        elif not isinstance(members, list):
            members = [members]
        args = ['ZREM', key] + [str(m) for m in members]
        return self._execute_command(args)

    def zscore(self, key, member):
        """Get score of member in sorted set"""
        return self._execute_command(['ZSCORE', key, str(member)])

    def zrank(self, key, member):
        """Get rank of member in sorted set"""
        return self._execute_command(['ZRANK', key, str(member)])

    def zrevrank(self, key, member):
        """Get reverse rank of member in sorted set"""
        return self._execute_command(['ZREVRANK', key, str(member)])

    def zrange(self, key, start, end, withscores=False):
        """Get range of members from sorted set"""
        args = ['ZRANGE', key, str(start), str(end)]
        if withscores:
            args.append('WITHSCORES')
        return self._execute_command(args)

    def zrevrange(self, key, start, end, withscores=False):
        """Get reverse range of members from sorted set"""
        args = ['ZREVRANGE', key, str(start), str(end)]
        if withscores:
            args.append('WITHSCORES')
        return self._execute_command(args)

    def zcard(self, key):
        """Get sorted set cardinality"""
        return self._execute_command(['ZCARD', key])

    def zcount(self, key, min_score, max_score):
        """Count members in score range"""
        return self._execute_command(['ZCOUNT', key, str(min_score), str(max_score)])

    def zincrby(self, key, increment, member):
        """Increment score of member in sorted set"""
        return self._execute_command(['ZINCRBY', key, str(increment), str(member)])

    cdef list _parse_xread_result(self, list result):
        parsed = []
        for stream_data in result:
            if len(stream_data) >= 2:
                stream_name = stream_data[0]
                messages = stream_data[1]
                for msg in messages:
                    if len(msg) >= 2:
                        msg_id = msg[0]
                        msg_data = {}
                        # Parse field-value pairs
                        for i in range(1, len(msg), 2):
                            if i + 1 < len(msg):
                                field = msg[i]
                                value = msg[i + 1]
                                msg_data[field] = value
                        parsed.append((stream_name, msg_id, msg_data))
        return parsed

    # C-level threading support
    def execute_threaded(self, str operation, *args):
        """Execute operation using C-level thread pool"""
        cdef object func = getattr(self, operation)
        cdef object py_args = args
        cdef object py_kwargs = {}

        # Submit to C thread pool
        self.thread_pool.submit(func, py_args, py_kwargs)

        # For now, return None since C thread pool doesn't return futures
        # In a more complete implementation, we'd implement C-level futures
        return None

    async def execute_async(self, str operation, *args):
        """Async execution (currently falls back to threaded execution)"""
        # For now, use threaded execution since C thread pool doesn't return awaitables
        # In a more complete implementation, we'd bridge C threads with asyncio
        return self.execute_threaded(operation, *args)

# C-level pub/sub consumer
cdef class CPubSubConsumer:
    """C-level pub/sub consumer for keyspace notifications"""
    cdef:
        FastRedisClient redis_client
        dict subscriptions
        object callback
        object pattern_callback
        object offset_lock
        int running
        CThreadPool thread_pool

    def __cinit__(self, FastRedisClient redis_client, int max_workers=4):
        self.redis_client = redis_client
        self.subscriptions = {}
        self.pattern_subscriptions = {}
        self.callback = None
        self.pattern_callback = None
        self.offset_lock = threading.Lock()
        self.running = 0
        self.thread_pool = CThreadPool(max_workers, 0)

    def subscribe(self, channels, callback=None):
        """Subscribe to channels"""
        if isinstance(channels, str):
            channels = [channels]

        with self.offset_lock:
            for channel in channels:
                self.subscriptions[channel] = callback or self.callback

        # Use a separate connection for pub/sub
        self.thread_pool.submit(self._pubsub_listener, (channels, False))

    def psubscribe(self, patterns, callback=None):
        """Subscribe to patterns"""
        if isinstance(patterns, str):
            patterns = [patterns]

        with self.offset_lock:
            for pattern in patterns:
                self.pattern_subscriptions[pattern] = callback or self.pattern_callback

        # Use a separate connection for pub/sub
        self.thread_pool.submit(self._pubsub_listener, (patterns, True))

    def set_callback(self, callback):
        """Set default message callback"""
        self.callback = callback

    def set_pattern_callback(self, callback):
        """Set pattern message callback"""
        self.pattern_callback = callback

    def _pubsub_listener(self, args):
        """Pub/sub listener loop"""
        channels, is_pattern = args
        cdef FastRedisClient client = FastRedisClient(
            self.redis_client.pool.host,
            self.redis_client.pool.port,
            self.redis_client.pool.max_connections,
            1  # Single worker for this connection
        )

        try:
            # Subscribe
            if is_pattern:
                client.psubscribe(channels)
            else:
                client.subscribe(channels)

            # Listen loop (simplified - in real implementation would need proper pub/sub parsing)
            while self.running:
                try:
                    # This is a simplified version - real pub/sub needs proper protocol handling
                    time.sleep(0.01)  # Small delay
                except:
                    break
        finally:
            client.pool.close()

    def start_listening(self):
        """Start listening for messages"""
        self.running = 1

    def stop_listening(self):
        """Stop listening"""
        self.running = 0
        if self.thread_pool:
            self.thread_pool.shutdown()

# C-level stream consumer
cdef class CStreamConsumer:
    """C-level stream consumer using pthreads"""
    cdef:
        FastRedisClient redis_client
        dict stream_offsets
        object callback
        object offset_lock
        int running
        CThreadPool thread_pool

    def __cinit__(self, FastRedisClient redis_client, int max_workers=4):
        self.redis_client = redis_client
        self.stream_offsets = {}
        self.callback = None
        self.offset_lock = threading.Lock()
        self.running = 0
        self.thread_pool = CThreadPool(max_workers, 0)

    def subscribe_to_streams(self, streams: Dict[str, str]):
        """Subscribe to multiple streams"""
        with self.offset_lock:
            self.stream_offsets.update(streams)

    def set_callback(self, callback: Callable[[str, str, Dict], None]):
        """Set the message callback function"""
        self.callback = callback

    def start_consuming(self):
        """Start consuming from streams using C threads"""
        if not self.callback or not self.stream_offsets:
            return

        self.running = 1

        # Submit consumer tasks to C thread pool
        for stream_name in self.stream_offsets.keys():
            self.thread_pool.submit(self._consume_stream_loop, (stream_name,))

    def stop_consuming(self):
        """Stop consuming"""
        self.running = 0
        # Shutdown thread pool will stop all threads
        if self.thread_pool:
            self.thread_pool.shutdown()

    def _consume_stream_loop(self, stream_name: str):
        """Consumer loop for a single stream"""
        while self.running:
            try:
                with self.offset_lock:
                    offset = self.stream_offsets.get(stream_name, '0')

                messages = self.redis_client.xread({stream_name: offset}, count=1, block=1000)
                for stream, msg_id, data in messages:
                    if self.callback:
                        self.callback(stream, msg_id, data)
                    with self.offset_lock:
                        self.stream_offsets[stream] = msg_id
            except Exception as e:
                print(f"Error consuming from {stream_name}: {e}")
                # Small delay on error
                time.sleep(0.1)

# Python wrapper for backward compatibility
class ThreadedStreamManager:
    """Python wrapper for C-level stream consumer"""
    def __init__(self, redis_client: FastRedisClient, max_workers: int = 4):
        self.consumer = CStreamConsumer(redis_client, max_workers)
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()
        self.running = False

    def subscribe_to_streams(self, streams: Dict[str, str]):
        """Subscribe to multiple streams"""
        self.consumer.subscribe_to_streams(streams)
        with self.offset_lock:
            self.stream_offsets.update(streams)

    def start_consuming(self, callback: Callable[[str, str, Dict], None]):
        """Start consuming from streams using C threads"""
        self.consumer.set_callback(callback)
        self.consumer.start_consuming()
        self.running = True

    def stop_consuming(self):
        """Stop consuming"""
        self.consumer.stop_consuming()
        self.running = False

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass
