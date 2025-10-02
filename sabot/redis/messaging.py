"""
FastRedis Messaging Components

Queues, streams, pubsub, and other messaging patterns.
"""

import json
import time
import threading
from typing import Dict, List, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor


class ReliableQueue:
    """
    Reliable queue with visibility timeouts, retries, and dead letter queues.
    """

    def __init__(self, redis_client, queue_name: str, visibility_timeout: int = 30,
                 max_retries: int = 3, dead_letter_queue: Optional[str] = None):
        self.redis = redis_client
        self.queue_name = queue_name
        self.visibility_timeout = visibility_timeout
        self.max_retries = max_retries
        self.dead_letter_queue = dead_letter_queue or f"{queue_name}:dead"

        # Queue keys
        self.pending_key = f"{queue_name}:pending"
        self.processing_key = f"{queue_name}:processing"
        self.failed_key = f"{queue_name}:failed"
        self.dead_key = self.dead_letter_queue

    def push(self, message: Any, priority: int = 0, delay: int = 0) -> str:
        """
        Push a message to the queue.

        Args:
            message: Message to enqueue
            priority: Message priority (lower = higher priority)
            delay: Delay before message becomes visible (seconds)

        Returns:
            Message ID
        """
        import uuid
        message_id = str(uuid.uuid4())

        message_data = {
            'id': message_id,
            'data': message,
            'priority': priority,
            'created_at': int(time.time()),
            'retry_count': 0,
            'delay_until': int(time.time()) + delay
        }

        if delay > 0:
            # Delayed message
            delayed_key = f"{self.queue_name}:delayed"
            self.redis.zadd(delayed_key, {json.dumps(message_data): message_data['delay_until']})
        else:
            # Immediate message
            self.redis.zadd(self.pending_key, {json.dumps(message_data): priority})

        return message_id

    def pop(self, count: int = 1) -> List[Dict[str, Any]]:
        """
        Pop messages from the queue.

        Args:
            count: Number of messages to pop

        Returns:
            List of message dictionaries
        """
        # First, move expired delayed messages to pending
        self._process_delayed_messages()

        # Get messages from pending queue
        messages_data = self.redis.zrange(self.pending_key, 0, count - 1)

        if not messages_data:
            return []

        messages = []
        processing_data = {}

        for msg_json in messages_data:
            try:
                message = json.loads(msg_json)

                # Remove from pending
                self.redis.zrem(self.pending_key, msg_json)

                # Add to processing with visibility timeout
                visibility_timeout = int(time.time()) + self.visibility_timeout
                processing_data[json.dumps(message)] = visibility_timeout

                messages.append(message)

            except json.JSONDecodeError:
                continue

        # Batch add to processing
        if processing_data:
            self.redis.zadd(self.processing_key, processing_data)

        return messages

    def acknowledge(self, message_id: str) -> bool:
        """
        Acknowledge successful processing of a message.

        Args:
            message_id: ID of processed message

        Returns:
            True if acknowledged successfully
        """
        # Find and remove message from processing queue
        processing_messages = self.redis.zrange(self.processing_key, 0, -1, withscores=True)

        for msg_json, score in processing_messages:
            try:
                message = json.loads(msg_json)
                if message['id'] == message_id:
                    self.redis.zrem(self.processing_key, msg_json)
                    return True
            except json.JSONDecodeError:
                continue

        return False

    def fail(self, message_id: str, error: Optional[str] = None) -> Dict[str, Any]:
        """
        Mark a message as failed, potentially retrying or moving to dead letter queue.

        Args:
            message_id: ID of failed message
            error: Error description

        Returns:
            Action taken (retry, dead_letter, etc.)
        """
        # Find message in processing queue
        processing_messages = self.redis.zrange(self.processing_key, 0, -1)

        for msg_json in processing_messages:
            try:
                message = json.loads(msg_json)

                if message['id'] == message_id:
                    self.redis.zrem(self.processing_key, msg_json)

                    message['retry_count'] += 1
                    message['last_error'] = error
                    message['failed_at'] = int(time.time())

                    if message['retry_count'] < self.max_retries:
                        # Retry with exponential backoff
                        backoff = 2 ** message['retry_count']
                        delay_until = int(time.time()) + backoff

                        delayed_key = f"{self.queue_name}:delayed"
                        self.redis.zadd(delayed_key, {json.dumps(message): delay_until})

                        return {'action': 'retry', 'retry_count': message['retry_count'], 'delay': backoff}
                    else:
                        # Move to dead letter queue
                        message['final_failure'] = True
                        self.redis.zadd(self.dead_key, {json.dumps(message): int(time.time())})

                        return {'action': 'dead_letter', 'retry_count': message['retry_count']}

            except json.JSONDecodeError:
                continue

        return {'action': 'not_found'}

    def _process_delayed_messages(self):
        """Move expired delayed messages to pending queue"""
        delayed_key = f"{self.queue_name}:delayed"
        current_time = int(time.time())

        # Get expired messages
        expired_messages = self.redis.zrangebyscore(delayed_key, 0, current_time)

        if expired_messages:
            # Remove from delayed and add to pending
            for msg_json in expired_messages:
                self.redis.zrem(delayed_key, msg_json)

                try:
                    message = json.loads(msg_json)
                    priority = message.get('priority', 0)
                    self.redis.zadd(self.pending_key, {msg_json: priority})
                except json.JSONDecodeError:
                    continue

    def get_queue_info(self) -> Dict[str, Any]:
        """
        Get queue statistics and information.

        Returns:
            Queue information dictionary
        """
        pending_count = self.redis.zcard(self.pending_key)
        processing_count = self.redis.zcard(self.processing_key)
        failed_count = self.redis.zcard(self.failed_key)
        dead_count = self.redis.zcard(self.dead_key)
        delayed_count = self.redis.zcard(f"{self.queue_name}:delayed")

        return {
            'queue_name': self.queue_name,
            'pending': pending_count,
            'processing': processing_count,
            'failed': failed_count,
            'dead_letter': dead_count,
            'delayed': delayed_count,
            'total': pending_count + processing_count + failed_count + dead_count + delayed_count,
            'visibility_timeout': self.visibility_timeout,
            'max_retries': self.max_retries
        }


class PubSubHub:
    """
    Advanced pubsub hub with pattern matching and message persistence.
    """

    def __init__(self, redis_client, hub_name: str = "pubsub"):
        self.redis = redis_client
        self.hub_name = hub_name
        self.channels_key = f"{hub_name}:channels"
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.pattern_subscriptions: Dict[str, List[Callable]] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def publish(self, channel: str, message: Any, persist: bool = False) -> int:
        """
        Publish a message to a channel.

        Args:
            channel: Channel name
            message: Message to publish
            persist: Whether to persist message

        Returns:
            Number of subscribers
        """
        message_data = {
            'channel': channel,
            'message': message,
            'timestamp': int(time.time()),
            'publisher': f"hub:{self.hub_name}"
        }

        # Publish to Redis pubsub
        json_message = json.dumps(message_data)
        subscribers = self.redis.publish(channel, json_message)

        # Persist message if requested
        if persist:
            history_key = f"{self.hub_name}:history:{channel}"
            self.redis.lpush(history_key, json_message)
            self.redis.ltrim(history_key, 0, 999)  # Keep last 1000 messages

        # Track active channels
        self.redis.sadd(self.channels_key, channel)
        self.redis.expire(self.channels_key, 3600)  # 1 hour

        return subscribers

    def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], None],
                  pattern: bool = False):
        """
        Subscribe to a channel or pattern.

        Args:
            channel: Channel name or pattern
            callback: Function to call when message received
            pattern: Whether this is a pattern subscription
        """
        if pattern:
            if channel not in self.pattern_subscriptions:
                self.pattern_subscriptions[channel] = []
            self.pattern_subscriptions[channel].append(callback)
        else:
            if channel not in self.subscriptions:
                self.subscriptions[channel] = []
            self.subscriptions[channel].append(callback)

    def unsubscribe(self, channel: str, callback: Optional[Callable] = None,
                   pattern: bool = False):
        """
        Unsubscribe from a channel or pattern.

        Args:
            channel: Channel name or pattern
            callback: Specific callback to remove (None for all)
            pattern: Whether this is a pattern subscription
        """
        target_dict = self.pattern_subscriptions if pattern else self.subscriptions

        if channel in target_dict:
            if callback is None:
                del target_dict[channel]
            else:
                target_dict[channel] = [cb for cb in target_dict[channel] if cb != callback]
                if not target_dict[channel]:
                    del target_dict[channel]

    def get_channel_history(self, channel: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent message history for a channel.

        Args:
            channel: Channel name
            limit: Maximum number of messages to return

        Returns:
            List of recent messages
        """
        history_key = f"{self.hub_name}:history:{channel}"
        messages = self.redis.lrange(history_key, 0, limit - 1)

        result = []
        for msg_json in messages:
            try:
                result.append(json.loads(msg_json))
            except json.JSONDecodeError:
                continue

        return result

    def get_active_channels(self) -> List[str]:
        """
        Get list of active channels.

        Returns:
            List of active channel names
        """
        return self.redis.smembers(self.channels_key)

    def get_channel_info(self, channel: str) -> Dict[str, Any]:
        """
        Get information about a channel.

        Args:
            channel: Channel name

        Returns:
            Channel information
        """
        history_key = f"{self.hub_name}:history:{channel}"
        message_count = self.redis.llen(history_key)

        # Get subscriber count (approximate)
        subscriber_count = 0
        if hasattr(self.redis, 'pubsub_numsub'):
            try:
                sub_info = self.redis.pubsub_numsub(channel)
                subscriber_count = sub_info.get(channel, 0)
            except:
                pass

        return {
            'channel': channel,
            'message_count': message_count,
            'subscriber_count': subscriber_count,
            'has_history': message_count > 0
        }


class StreamConsumerGroup:
    """
    Redis Streams consumer group with automatic message processing.
    """

    def __init__(self, redis_client, stream_name: str, group_name: str,
                 consumer_name: str, auto_ack: bool = False):
        self.redis = redis_client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.auto_ack = auto_ack

        # Create consumer group if it doesn't exist
        try:
            self.redis.xgroup_create(stream_name, group_name, '$', mkstream=True)
        except:
            # Group might already exist
            pass

    def read_messages(self, count: int = 10, block_ms: int = 5000) -> List[Dict[str, Any]]:
        """
        Read messages from the stream for this consumer.

        Args:
            count: Maximum number of messages to read
            block_ms: Block timeout in milliseconds

        Returns:
            List of message dictionaries
        """
        try:
            messages = self.redis.xreadgroup(
                self.group_name,
                self.consumer_name,
                {self.stream_name: '>'},
                count=count,
                block=block_ms
            )

            if not messages:
                return []

            # Parse messages
            result = []
            for stream, stream_messages in messages:
                for message_id, message_data in stream_messages:
                    result.append({
                        'id': message_id,
                        'data': message_data,
                        'stream': stream
                    })

            return result

        except Exception as e:
            print(f"Error reading messages: {e}")
            return []

    def acknowledge_messages(self, message_ids: List[str]) -> int:
        """
        Acknowledge processing of messages.

        Args:
            message_ids: List of message IDs to acknowledge

        Returns:
            Number of messages acknowledged
        """
        try:
            result = self.redis.xack(self.stream_name, self.group_name, *message_ids)
            return result
        except Exception as e:
            print(f"Error acknowledging messages: {e}")
            return 0

    def add_to_pending(self, message_id: str, consumer: Optional[str] = None) -> bool:
        """
        Add a message back to pending list for reprocessing.

        Args:
            message_id: Message ID to requeue
            consumer: Consumer to assign to (None = current consumer)

        Returns:
            True if successfully added to pending
        """
        consumer = consumer or self.consumer_name

        try:
            # This is a simplified version - in practice you'd use XPENDING/XCLAIM
            result = self.redis.xclaim(self.stream_name, self.group_name,
                                     consumer, 0, message_id)
            return len(result) > 0
        except Exception as e:
            print(f"Error adding to pending: {e}")
            return False

    def get_pending_info(self) -> Dict[str, Any]:
        """
        Get information about pending messages in the consumer group.

        Returns:
            Pending message information
        """
        try:
            pending_info = self.redis.xpending(self.stream_name, self.group_name)

            return {
                'total_pending': pending_info.get('pending', 0),
                'min_id': pending_info.get('min', '0'),
                'max_id': pending_info.get('max', '0'),
                'consumers': pending_info.get('consumers', 0)
            }
        except Exception as e:
            print(f"Error getting pending info: {e}")
            return {}

    def get_consumer_info(self) -> Dict[str, Any]:
        """
        Get information about consumers in this group.

        Returns:
            Consumer information
        """
        try:
            consumers = self.redis.xinfo_consumers(self.stream_name, self.group_name)

            result = {}
            for consumer in consumers:
                name = consumer['name']
                result[name] = {
                    'pending': consumer['pending'],
                    'idle': consumer['idle']
                }

            return result
        except Exception as e:
            print(f"Error getting consumer info: {e}")
            return {}


class WorkerQueue:
    """
    Priority-based worker queue with job scheduling and monitoring.
    """

    def __init__(self, redis_client, queue_name: str = "jobs"):
        self.redis = redis_client
        self.queue_name = queue_name
        self.jobs_key = f"{queue_name}:jobs"
        self.processing_key = f"{queue_name}:processing"
        self.completed_key = f"{queue_name}:completed"
        self.failed_key = f"{queue_name}:failed"

    def submit_job(self, job_type: str, payload: Dict[str, Any],
                   priority: int = 0, delay: int = 0) -> str:
        """
        Submit a job to the queue.

        Args:
            job_type: Type of job
            payload: Job data
            priority: Job priority (lower = higher priority)
            delay: Delay before job becomes available

        Returns:
            Job ID
        """
        import uuid
        job_id = str(uuid.uuid4())

        job_data = {
            'id': job_id,
            'type': job_type,
            'payload': payload,
            'priority': priority,
            'created_at': int(time.time()),
            'delay_until': int(time.time()) + delay,
            'status': 'queued'
        }

        json_data = json.dumps(job_data)

        if delay > 0:
            delayed_key = f"{self.queue_name}:delayed"
            self.redis.zadd(delayed_key, {json_data: job_data['delay_until']})
        else:
            self.redis.zadd(self.jobs_key, {json_data: priority})

        return job_id

    def get_job(self) -> Optional[Dict[str, Any]]:
        """
        Get next available job from queue.

        Returns:
            Job data or None if no jobs available
        """
        # Process delayed jobs
        self._process_delayed_jobs()

        # Get highest priority job
        job_data_list = self.redis.zrange(self.jobs_key, 0, 0)

        if not job_data_list:
            return None

        job_json = job_data_list[0]

        try:
            job = json.loads(job_json)

            # Remove from queue and add to processing
            self.redis.zrem(self.jobs_key, job_json)

            processing_data = job.copy()
            processing_data['started_at'] = int(time.time())
            processing_data['status'] = 'processing'

            self.redis.set(f"{self.processing_key}:{job['id']}",
                          json.dumps(processing_data), ex=3600)

            return job

        except json.JSONDecodeError:
            # Remove corrupted job
            self.redis.zrem(self.jobs_key, job_json)
            return None

    def complete_job(self, job_id: str, result: Optional[Any] = None) -> bool:
        """
        Mark a job as completed.

        Args:
            job_id: Job ID
            result: Job result

        Returns:
            True if completed successfully
        """
        processing_key = f"{self.processing_key}:{job_id}"

        job_data = self.redis.get(processing_key)
        if not job_data:
            return False

        try:
            job = json.loads(job_data)

            completed_data = job.copy()
            completed_data['completed_at'] = int(time.time())
            completed_data['result'] = result
            completed_data['status'] = 'completed'

            # Move to completed
            self.redis.set(f"{self.completed_key}:{job_id}",
                          json.dumps(completed_data), ex=86400)  # Keep for 24 hours

            # Remove from processing
            self.redis.delete(processing_key)

            return True

        except json.JSONDecodeError:
            return False

    def fail_job(self, job_id: str, error: str) -> bool:
        """
        Mark a job as failed.

        Args:
            job_id: Job ID
            error: Error message

        Returns:
            True if marked as failed
        """
        processing_key = f"{self.processing_key}:{job_id}"

        job_data = self.redis.get(processing_key)
        if not job_data:
            return False

        try:
            job = json.loads(job_data)

            failed_data = job.copy()
            failed_data['failed_at'] = int(time.time())
            failed_data['error'] = error
            failed_data['status'] = 'failed'

            # Move to failed
            self.redis.set(f"{self.failed_key}:{job_id}",
                          json.dumps(failed_data), ex=86400)

            # Remove from processing
            self.redis.delete(processing_key)

            return True

        except json.JSONDecodeError:
            return False

    def _process_delayed_jobs(self):
        """Move expired delayed jobs to main queue"""
        delayed_key = f"{self.queue_name}:delayed"
        current_time = int(time.time())

        expired_jobs = self.redis.zrangebyscore(delayed_key, 0, current_time)

        for job_json in expired_jobs:
            self.redis.zrem(delayed_key, job_json)

            try:
                job = json.loads(job_json)
                priority = job.get('priority', 0)
                self.redis.zadd(self.jobs_key, {job_json: priority})
            except json.JSONDecodeError:
                continue

    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.

        Returns:
            Queue statistics
        """
        queued = self.redis.zcard(self.jobs_key)
        processing = len(self.redis.keys(f"{self.processing_key}:*"))
        completed = len(self.redis.keys(f"{self.completed_key}:*"))
        failed = len(self.redis.keys(f"{self.failed_key}:*"))
        delayed = self.redis.zcard(f"{self.queue_name}:delayed")

        return {
            'queued': queued,
            'processing': processing,
            'completed': completed,
            'failed': failed,
            'delayed': delayed,
            'total': queued + processing + completed + failed + delayed
        }


class AMQPRouter:
    """
    AMQP-style message routing system with exchanges, bindings, and queues.
    Supports direct, topic, headers, and fanout exchange types.
    """

    def __init__(self, redis_client, namespace: str = "amqp"):
        self.redis = redis_client
        self.namespace = namespace

        # Key prefixes
        self.exchanges_key = f"{namespace}:exchanges"
        self.bindings_key = f"{namespace}:bindings"
        self.queues_key = f"{namespace}:queues"

        # Register routing scripts
        self._register_routing_scripts()

    def _register_routing_scripts(self):
        """Register Lua scripts for AMQP-style routing"""

        # Direct exchange routing (exact match)
        direct_route_script = """
        local exchange_key = KEYS[1]
        local routing_key = ARGV[1]
        local message_id = ARGV[2]

        -- Get bindings for this exchange
        local bindings_key = KEYS[2]
        local exchange_bindings = redis.call('HGETALL', bindings_key)

        local routed_queues = {}

        -- Find matching bindings (direct exchange: routing key must match exactly)
        for i = 1, #exchange_bindings, 2 do
            local binding_key = exchange_bindings[i]
            local queue_name = exchange_bindings[i + 1]

            -- Parse binding key (format: "exchange.routing_key")
            local dot_pos = string.find(binding_key, '.', 1, true)
            if dot_pos then
                local exch_name = string.sub(binding_key, 1, dot_pos - 1)
                local bind_routing_key = string.sub(binding_key, dot_pos + 1)

                if exch_name == ARGV[3] and bind_routing_key == routing_key then
                    routed_queues[queue_name] = true
                end
            end
        end

        -- Return list of queues to route to
        local result = {}
        for queue_name, _ in pairs(routed_queues) do
            table.insert(result, queue_name)
        end

        return result
        """

        # Topic exchange routing (pattern matching)
        topic_route_script = """
        local exchange_key = KEYS[1]
        local routing_key = ARGV[1]
        local message_id = ARGV[2]
        local exchange_name = ARGV[3]

        local bindings_key = KEYS[2]
        local exchange_bindings = redis.call('HGETALL', bindings_key)

        local routed_queues = {}

        -- Split routing key into parts
        local rk_parts = {}
        for part in string.gmatch(routing_key, "[^%.]+") do
            table.insert(rk_parts, part)
        end

        -- Check each binding pattern
        for i = 1, #exchange_bindings, 2 do
            local binding_key = exchange_bindings[i]
            local queue_name = exchange_bindings[i + 1]

            -- Parse binding key
            local dot_pos = string.find(binding_key, '.', 1, true)
            if dot_pos then
                local exch_name = string.sub(binding_key, 1, dot_pos - 1)
                local pattern = string.sub(binding_key, dot_pos + 1)

                if exch_name == exchange_name and topic_match(pattern, routing_key) then
                    routed_queues[queue_name] = true
                end
            end
        end

        local result = {}
        for queue_name, _ in pairs(routed_queues) do
            table.insert(result, queue_name)
        end

        return result
        """

        # Fanout exchange routing (deliver to all bound queues)
        fanout_route_script = """
        local bindings_key = KEYS[1]
        local exchange_name = ARGV[1]

        local exchange_bindings = redis.call('HGETALL', bindings_key)
        local routed_queues = {}

        -- All bindings for this exchange
        for i = 1, #exchange_bindings, 2 do
            local binding_key = exchange_bindings[i]
            local queue_name = exchange_bindings[i + 1]

            -- Parse binding key
            local dot_pos = string.find(binding_key, '.', 1, true)
            if dot_pos then
                local exch_name = string.sub(binding_key, 1, dot_pos - 1)
                if exch_name == exchange_name then
                    routed_queues[queue_name] = true
                end
            end
        end

        local result = {}
        for queue_name, _ in pairs(routed_queues) do
            table.insert(result, queue_name)
        end

        return result
        """

        # Helper function for topic matching (defined within script)
        topic_helper = """
        function topic_match(pattern, routing_key)
            -- Simple topic matching implementation
            -- Supports * (single word) and # (rest of key)

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
                    return true  -- # matches rest
                elseif p_part == '*' then
                    -- * matches single word
                    p_idx = p_idx + 1
                    rk_idx = rk_idx + 1
                elseif p_part == rk_part then
                    p_idx = p_idx + 1
                    rk_idx = rk_idx + 1
                else
                    return false
                end
            end

            -- Check if we've consumed all pattern parts
            return p_idx > #p_parts and rk_idx > #rk_parts
        end
        """

        # Combine topic script with helper
        full_topic_script = topic_helper .. "\n\n" .. topic_route_script

        self.redis.script_load(direct_route_script)  # We'll store SHA hashes
        self.redis.script_load(full_topic_script)
        self.redis.script_load(fanout_route_script)

        # Store script content for later use
        self._routing_scripts = {
            'direct': direct_route_script,
            'topic': full_topic_script,
            'fanout': fanout_route_script
        }

    def create_exchange(self, name: str, exchange_type: str) -> bool:
        """
        Create a message exchange.

        Args:
            name: Exchange name
            exchange_type: Type of exchange (direct, topic, headers, fanout)

        Returns:
            True if created successfully
        """
        if exchange_type not in ['direct', 'topic', 'headers', 'fanout']:
            raise ValueError(f"Unsupported exchange type: {exchange_type}")

        exchange_key = f"{self.exchanges_key}:{name}"
        exchange_data = {
            'name': name,
            'type': exchange_type,
            'created_at': int(time.time())
        }

        return self.redis.hset(exchange_key, exchange_data)

    def delete_exchange(self, name: str) -> bool:
        """
        Delete an exchange and its bindings.

        Args:
            name: Exchange name

        Returns:
            True if deleted successfully
        """
        exchange_key = f"{self.exchanges_key}:{name}"

        # Remove all bindings for this exchange
        bindings_pattern = f"{self.bindings_key}:{name}:*"
        binding_keys = self.redis.keys(bindings_pattern)

        if binding_keys:
            self.redis.delete(*binding_keys)

        # Remove exchange
        return self.redis.delete(exchange_key)

    def bind_queue(self, exchange_name: str, queue_name: str,
                   routing_key: str = "") -> bool:
        """
        Bind a queue to an exchange with a routing key.

        Args:
            exchange_name: Exchange name
            queue_name: Queue name
            routing_key: Routing key (used differently by exchange types)

        Returns:
            True if binding created successfully
        """
        # Verify exchange exists
        exchange_key = f"{self.exchanges_key}:{exchange_name}"
        if not self.redis.exists(exchange_key):
            raise ValueError(f"Exchange '{exchange_name}' does not exist")

        # Create binding
        binding_key = f"{self.bindings_key}:{exchange_name}"
        binding_id = f"{exchange_name}.{routing_key}"

        return self.redis.hset(binding_key, {binding_id: queue_name})

    def unbind_queue(self, exchange_name: str, queue_name: str,
                     routing_key: str = "") -> bool:
        """
        Remove a binding between exchange and queue.

        Args:
            exchange_name: Exchange name
            queue_name: Queue name
            routing_key: Routing key

        Returns:
            True if binding removed successfully
        """
        binding_key = f"{self.bindings_key}:{exchange_name}"
        binding_id = f"{exchange_name}.{routing_key}"

        return self.redis.hdel(binding_key, binding_id)

    def publish(self, exchange_name: str, routing_key: str, message: Any,
                headers: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Publish a message to an exchange.

        Args:
            exchange_name: Target exchange
            routing_key: Message routing key
            message: Message payload
            headers: Message headers (for headers exchange)

        Returns:
            Publishing result with routed queues
        """
        # Verify exchange exists
        exchange_key = f"{self.exchanges_key}:{exchange_name}"
        exchange_info = self.redis.hgetall(exchange_key)

        if not exchange_info:
            raise ValueError(f"Exchange '{exchange_name}' does not exist")

        exchange_type = exchange_info['type']

        # Create message
        import uuid
        message_id = str(uuid.uuid4())

        message_data = {
            'id': message_id,
            'exchange': exchange_name,
            'routing_key': routing_key,
            'payload': message,
            'headers': headers or {},
            'timestamp': int(time.time()),
            'publisher': f"amqp:{self.namespace}"
        }

        # Route message based on exchange type
        routed_queues = self._route_message(exchange_name, exchange_type,
                                          routing_key, message_id, headers)

        # Deliver to routed queues
        delivered_count = 0
        for queue_name in routed_queues:
            queue_key = f"{self.queues_key}:{queue_name}"
            json_message = json.dumps(message_data)

            # Add to queue (using priority 0 for now)
            self.redis.zadd(queue_key, {json_message: 0})
            delivered_count += 1

        return {
            'message_id': message_id,
            'routed_queues': routed_queues,
            'delivered_count': delivered_count,
            'exchange_type': exchange_type
        }

    def _route_message(self, exchange_name: str, exchange_type: str,
                      routing_key: str, message_id: str,
                      headers: Optional[Dict[str, Any]] = None) -> List[str]:
        """Route message based on exchange type"""

        # Try to use optimized Cython routing if available
        if hasattr(self.redis, 'amqp_route_message'):
            try:
                return self.redis.amqp_route_message(exchange_name, exchange_type,
                                                   routing_key, message_id, headers)
            except Exception:
                # Fall back to Python implementation
                pass

        # Fallback to Python implementation
        bindings_key = f"{self.bindings_key}:{exchange_name}"

        if exchange_type == 'direct':
            # Direct exchange: exact routing key match
            script_sha = self.redis.script_load(self._routing_scripts['direct'])
            result = self.redis.evalsha(script_sha, 2,
                                      self.exchanges_key, bindings_key,
                                      routing_key, message_id, exchange_name)

        elif exchange_type == 'topic':
            # Topic exchange: pattern matching
            script_sha = self.redis.script_load(self._routing_scripts['topic'])
            result = self.redis.evalsha(script_sha, 2,
                                      self.exchanges_key, bindings_key,
                                      routing_key, message_id, exchange_name)

        elif exchange_type == 'fanout':
            # Fanout exchange: deliver to all bound queues
            script_sha = self.redis.script_load(self._routing_scripts['fanout'])
            result = self.redis.evalsha(script_sha, 1, bindings_key, exchange_name)

        elif exchange_type == 'headers':
            # Headers exchange: match message headers (simplified)
            result = self._route_headers_exchange(exchange_name, headers or {})

        else:
            result = []

        return result if isinstance(result, list) else []

    def _route_headers_exchange(self, exchange_name: str,
                               message_headers: Dict[str, Any]) -> List[str]:
        """Route message for headers exchange"""
        bindings_key = f"{self.bindings_key}:{exchange_name}"
        exchange_bindings = self.redis.hgetall(bindings_key)

        routed_queues = []

        for binding_key, queue_name in exchange_bindings.items():
            # Parse binding key to get header requirements
            # This is a simplified implementation
            dot_pos = binding_key.find('.', 0)
            if dot_pos > 0:
                exch_name = binding_key[:dot_pos]
                header_pattern = binding_key[dot_pos + 1:]

                if exch_name == exchange_name:
                    # Simple header matching (could be enhanced)
                    if self._headers_match(header_pattern, message_headers):
                        routed_queues.append(queue_name)

        return routed_queues

    def _headers_match(self, pattern: str, headers: Dict[str, Any]) -> bool:
        """Simple header matching (can be enhanced)"""
        # This is a basic implementation - could be made more sophisticated
        try:
            pattern_dict = json.loads(pattern)
            for key, expected_value in pattern_dict.items():
                if headers.get(key) != expected_value:
                    return False
            return True
        except:
            return False

    def consume(self, queue_name: str, count: int = 1) -> List[Dict[str, Any]]:
        """
        Consume messages from a queue.

        Args:
            queue_name: Queue name
            count: Number of messages to consume

        Returns:
            List of consumed messages
        """
        queue_key = f"{self.queues_key}:{queue_name}"

        # Get messages from queue
        message_jsons = self.redis.zrange(queue_key, 0, count - 1)

        messages = []
        for msg_json in message_jsons:
            try:
                message = json.loads(msg_json)
                messages.append(message)

                # Remove from queue
                self.redis.zrem(queue_key, msg_json)

            except json.JSONDecodeError:
                # Remove corrupted message
                self.redis.zrem(queue_key, msg_json)

        return messages

    def acknowledge(self, queue_name: str, message_id: str) -> bool:
        """
        Acknowledge message processing (messages are removed when consumed).

        Args:
            queue_name: Queue name
            message_id: Message ID

        Returns:
            True (acknowledgment is implicit in consume)
        """
        # In this simplified implementation, acknowledgment is implicit
        # In a full AMQP implementation, you'd track unacked messages
        return True

    def get_exchange_info(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an exchange.

        Args:
            exchange_name: Exchange name

        Returns:
            Exchange information or None if not found
        """
        exchange_key = f"{self.exchanges_key}:{exchange_name}"
        exchange_data = self.redis.hgetall(exchange_key)

        if not exchange_data:
            return None

        # Get bindings
        bindings_key = f"{self.bindings_key}:{exchange_name}"
        bindings = self.redis.hlen(bindings_key)

        return {
            'name': exchange_data.get('name'),
            'type': exchange_data.get('type'),
            'bindings_count': bindings,
            'created_at': int(exchange_data.get('created_at', 0))
        }

    def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """
        Get information about a queue.

        Args:
            queue_name: Queue name

        Returns:
            Queue information
        """
        queue_key = f"{self.queues_key}:{queue_name}"
        message_count = self.redis.zcard(queue_key)

        return {
            'name': queue_name,
            'message_count': message_count,
            'namespace': self.namespace
        }

    def purge_queue(self, queue_name: str) -> int:
        """
        Remove all messages from a queue.

        Args:
            queue_name: Queue name

        Returns:
            Number of messages removed
        """
        queue_key = f"{self.queues_key}:{queue_name}"
        return self.redis.delete(queue_key)

    def purge_exchange(self, exchange_name: str) -> Dict[str, int]:
        """
        Remove all messages from queues bound to an exchange.

        Args:
            exchange_name: Exchange name

        Returns:
            Dictionary with purge results
        """
        bindings_key = f"{self.bindings_key}:{exchange_name}"
        bindings = self.redis.hgetall(bindings_key)

        results = {}
        for binding_key, queue_name in bindings.items():
            if queue_name not in results:
                purged = self.purge_queue(queue_name)
                results[queue_name] = purged

        return results
