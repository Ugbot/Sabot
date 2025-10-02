"""
FastRedis RPC (Remote Procedure Call) Layer

Provides distributed RPC capabilities over Redis with service discovery,
load balancing, and fault tolerance.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor, Future
import threading


class RPCError(Exception):
    """Base exception for RPC operations"""
    pass


class RPCServiceUnavailable(RPCError):
    """Raised when no service instances are available"""
    pass


class RPCTimeoutError(RPCError):
    """Raised when RPC call times out"""
    pass


class RPCRequest:
    """Represents an RPC request"""

    def __init__(self, service: str, method: str, args: Optional[List[Any]] = None,
                 kwargs: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None,
                 timeout: int = 30):
        self.service = service
        self.method = method
        self.args = args or []
        self.kwargs = kwargs or {}
        self.request_id = request_id or str(uuid.uuid4())
        self.timestamp = int(time.time())
        self.timeout = timeout

    def to_dict(self) -> Dict[str, Any]:
        """Convert request to dictionary for serialization"""
        return {
            'service': self.service,
            'method': self.method,
            'args': self.args,
            'kwargs': self.kwargs,
            'request_id': self.request_id,
            'timestamp': self.timestamp,
            'timeout': self.timeout
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RPCRequest':
        """Create request from dictionary"""
        request = cls(
            service=data['service'],
            method=data['method'],
            args=data.get('args', []),
            kwargs=data.get('kwargs', {}),
            request_id=data.get('request_id'),
            timeout=data.get('timeout', 30)
        )
        request.timestamp = data.get('timestamp', request.timestamp)
        return request


class RPCResponse:
    """Represents an RPC response"""

    def __init__(self, request_id: str, result: Optional[Any] = None,
                 error: Optional[str] = None, service_instance: Optional[str] = None):
        self.request_id = request_id
        self.result = result
        self.error = error
        self.service_instance = service_instance
        self.timestamp = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        """Convert response to dictionary for serialization"""
        return {
            'request_id': self.request_id,
            'result': self.result,
            'error': self.error,
            'service_instance': self.service_instance,
            'timestamp': self.timestamp
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RPCResponse':
        """Create response from dictionary"""
        response = cls(
            request_id=data['request_id'],
            result=data.get('result'),
            error=data.get('error'),
            service_instance=data.get('service_instance')
        )
        response.timestamp = data.get('timestamp', response.timestamp)
        return response

    @property
    def success(self) -> bool:
        """Check if response indicates success"""
        return self.error is None


class RPCServiceRegistry:
    """Service registry for RPC service discovery"""

    def __init__(self, redis_client, namespace: str = "rpc:services"):
        self.redis = redis_client
        self.namespace = namespace

    def register_service(self, service_name: str, instance_id: str,
                        metadata: Optional[Dict[str, Any]] = None,
                        ttl: int = 60) -> bool:
        """
        Register a service instance

        Args:
            service_name: Name of the service
            instance_id: Unique instance identifier
            metadata: Additional service metadata
            ttl: Time-to-live for registration

        Returns:
            True if registration successful
        """
        service_key = f"{self.namespace}:{service_name}:instances"
        instance_key = f"{self.namespace}:{service_name}:instance:{instance_id}"

        # Store instance metadata
        instance_data = {
            'instance_id': instance_id,
            'registered_at': int(time.time()),
            'ttl': ttl
        }
        if metadata:
            instance_data.update(metadata)

        # Add to service instances set
        self.redis.sadd(service_key, instance_id)

        # Store instance details
        self.redis.hset(instance_key, instance_data)

        # Set expiration
        self.redis.expire(service_key, ttl)
        self.redis.expire(instance_key, ttl)

        return True

    def unregister_service(self, service_name: str, instance_id: str) -> bool:
        """
        Unregister a service instance

        Args:
            service_name: Name of the service
            instance_id: Instance identifier

        Returns:
            True if unregistration successful
        """
        service_key = f"{self.namespace}:{service_name}:instances"
        instance_key = f"{self.namespace}:{service_name}:instance:{instance_id}"

        # Remove from service instances
        self.redis.srem(service_key, instance_id)

        # Remove instance details
        self.redis.delete(instance_key)

        return True

    def get_service_instances(self, service_name: str) -> List[Dict[str, Any]]:
        """
        Get all available instances for a service

        Args:
            service_name: Name of the service

        Returns:
            List of service instance information
        """
        service_key = f"{self.namespace}:{service_name}:instances"
        instance_ids = self.redis.smembers(service_key)

        instances = []
        for instance_id in instance_ids:
            instance_key = f"{self.namespace}:{service_name}:instance:{instance_id}"
            instance_data = self.redis.hgetall(instance_key)
            if instance_data:
                instances.append(instance_data)

        return instances

    def discover_service(self, service_name: str) -> Optional[Dict[str, Any]]:
        """
        Discover a service instance (load balancing)

        Args:
            service_name: Name of the service

        Returns:
            Service instance information or None if no instances available
        """
        instances = self.get_service_instances(service_name)
        if not instances:
            return None

        # Simple round-robin load balancing (in production, use more sophisticated algorithms)
        # For now, just return the first available instance
        return instances[0] if instances else None


class RPCServer:
    """RPC server for handling incoming RPC requests"""

    def __init__(self, redis_client, service_name: str, instance_id: str,
                 registry: Optional[RPCServiceRegistry] = None):
        self.redis = redis_client
        self.service_name = service_name
        self.instance_id = instance_id
        self.registry = registry or RPCServiceRegistry(redis_client)
        self.methods: Dict[str, Callable] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def register_method(self, method_name: str, handler: Callable):
        """
        Register an RPC method handler

        Args:
            method_name: Name of the method
            handler: Function to handle the method call
        """
        self.methods[method_name] = handler

    def start(self):
        """Start the RPC server"""
        if self.running:
            return

        # Register service
        self.registry.register_service(self.service_name, self.instance_id)

        self.running = True
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop the RPC server"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)

        # Unregister service
        self.registry.unregister_service(self.service_name, self.instance_id)

    def _run_server(self):
        """Main server loop"""
        request_queue = f"rpc:requests:{self.service_name}"
        response_queue = f"rpc:responses:{self.instance_id}"

        while self.running:
            try:
                # Wait for requests (blocking pop with timeout)
                request_data = self.redis.blpop(request_queue, timeout=1)
                if not request_data:
                    continue

                # Parse request
                try:
                    request_dict = json.loads(request_data[1])
                    request = RPCRequest.from_dict(request_dict)
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Invalid request format: {e}")
                    continue

                # Process request
                response = self._handle_request(request)

                # Send response
                response_data = json.dumps(response.to_dict())
                self.redis.lpush(response_queue, response_data)

            except Exception as e:
                print(f"RPC server error: {e}")
                time.sleep(1)  # Avoid tight loop on errors

    def _handle_request(self, request: RPCRequest) -> RPCResponse:
        """Handle an RPC request"""
        try:
            if request.method not in self.methods:
                return RPCResponse(
                    request_id=request.request_id,
                    error=f"Method '{request.method}' not found",
                    service_instance=self.instance_id
                )

            # Call the method
            method = self.methods[request.method]
            result = method(*request.args, **request.kwargs)

            return RPCResponse(
                request_id=request.request_id,
                result=result,
                service_instance=self.instance_id
            )

        except Exception as e:
            return RPCResponse(
                request_id=request.request_id,
                error=str(e),
                service_instance=self.instance_id
            )


class RPCClient:
    """RPC client for making remote procedure calls"""

    def __init__(self, redis_client, registry: Optional[RPCServiceRegistry] = None,
                 default_timeout: int = 30):
        self.redis = redis_client
        self.registry = registry or RPCServiceRegistry(redis_client)
        self.default_timeout = default_timeout
        self.response_queues: Dict[str, str] = {}

    def call(self, service: str, method: str, *args, timeout: Optional[int] = None,
             **kwargs) -> Any:
        """
        Make a synchronous RPC call

        Args:
            service: Service name
            method: Method name
            *args: Positional arguments
            timeout: Call timeout in seconds
            **kwargs: Keyword arguments

        Returns:
            Method result

        Raises:
            RPCServiceUnavailable: If no service instances available
            RPCTimeoutError: If call times out
            RPCError: For other RPC errors
        """
        future = self.call_async(service, method, *args, timeout=timeout, **kwargs)
        return future.result()

    def call_async(self, service: str, method: str, *args, timeout: Optional[int] = None,
                   **kwargs) -> Future:
        """
        Make an asynchronous RPC call

        Args:
            service: Service name
            method: Method name
            *args: Positional arguments
            timeout: Call timeout in seconds
            **kwargs: Keyword arguments

        Returns:
            Future object for the result
        """
        timeout = timeout or self.default_timeout

        # Discover service instance
        instance = self.registry.discover_service(service)
        if not instance:
            future = Future()
            future.set_exception(RPCServiceUnavailable(f"No instances available for service '{service}'"))
            return future

        instance_id = instance['instance_id']

        # Create request
        request = RPCRequest(service, method, args=list(args), kwargs=kwargs, timeout=timeout)

        # Send request
        request_queue = f"rpc:requests:{service}"
        request_data = json.dumps(request.to_dict())
        self.redis.lpush(request_queue, request_data)

        # Create future for response
        future = Future()
        response_queue = f"rpc:responses:{instance_id}"

        # Start response listener thread
        def wait_for_response():
            try:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    # Check for response
                    response_data = self.redis.blpop(response_queue, timeout=1)
                    if response_data:
                        try:
                            response_dict = json.loads(response_data[1])
                            response = RPCResponse.from_dict(response_dict)

                            if response.request_id == request.request_id:
                                if response.success:
                                    future.set_result(response.result)
                                else:
                                    future.set_exception(RPCError(response.error))
                                return
                        except (json.JSONDecodeError, KeyError) as e:
                            continue

                # Timeout
                future.set_exception(RPCTimeoutError(f"RPC call to {service}.{method} timed out"))

            except Exception as e:
                future.set_exception(RPCError(f"RPC communication error: {e}"))

        thread = threading.Thread(target=wait_for_response, daemon=True)
        thread.start()

        return future


class RPCService:
    """High-level RPC service decorator and utilities"""

    def __init__(self, redis_client, service_name: str, instance_id: Optional[str] = None):
        self.redis = redis_client
        self.service_name = service_name
        self.instance_id = instance_id or str(uuid.uuid4())
        self.server: Optional[RPCServer] = None
        self.client: Optional[RPCClient] = None

    def __call__(self, cls):
        """Decorator to turn a class into an RPC service"""
        # Create server and client instances
        self.server = RPCServer(self.redis, self.service_name, self.instance_id)
        self.client = RPCClient(self.redis)

        # Register all public methods
        for method_name in dir(cls):
            if not method_name.startswith('_'):
                method = getattr(cls, method_name)
                if callable(method):
                    self.server.register_method(method_name, method)

        # Add RPC client methods to the class
        cls.rpc_call = self.client.call
        cls.rpc_call_async = self.client.call_async

        return cls

    def start(self):
        """Start the RPC service"""
        if self.server:
            self.server.start()

    def stop(self):
        """Stop the RPC service"""
        if self.server:
            self.server.stop()


# Utility functions for creating RPC services
def rpc_service(service_name: str, redis_client=None, instance_id: Optional[str] = None):
    """
    Decorator to create an RPC service

    Usage:
        @rpc_service("my_service", redis_client)
        class MyService:
            def add(self, a, b):
                return a + b

        service = MyService()
        service.start()

        # Call remotely
        result = service.rpc_call("my_service", "add", 1, 2)
    """
    def decorator(cls):
        service = RPCService(redis_client, service_name, instance_id)
        return service(cls)
    return decorator


def create_rpc_client(redis_client, service_name: str) -> RPCClient:
    """Create an RPC client for a specific service"""
    client = RPCClient(redis_client)
    # Add convenience methods
    def call_method(method: str, *args, **kwargs):
        return client.call(service_name, method, *args, **kwargs)

    def call_method_async(method: str, *args, **kwargs):
        return client.call_async(service_name, method, *args, **kwargs)

    client.call_method = call_method
    client.call_method_async = call_method_async

    return client


class RedisPluginManager:
    """
    Redis plugin/module loading and management system.
    Provides dynamic loading, configuration, and monitoring of Redis modules.
    """

    def __init__(self, redis_client, plugin_dir: str = None, auto_discover: bool = True):
        self.redis = redis_client
        self.plugin_dir = plugin_dir or "/usr/lib/redis/modules"
        self.loaded_plugins = {}
        self.plugin_configs = {}
        self.auto_discover = auto_discover

        if auto_discover:
            self._discover_available_plugins()

    def _discover_available_plugins(self):
        """Discover available Redis plugins/modules"""
        try:
            # Check common plugin directories
            import os
            import glob

            plugin_paths = [
                "/usr/lib/redis/modules",
                "/opt/redis/modules",
                "/usr/local/lib/redis/modules",
                "./redis_modules",
                self.plugin_dir
            ]

            discovered_plugins = {}

            for path in plugin_paths:
                if os.path.exists(path):
                    # Look for .so files (Linux/Mac)
                    so_files = glob.glob(os.path.join(path, "*.so"))
                    for so_file in so_files:
                        plugin_name = os.path.splitext(os.path.basename(so_file))[0]
                        discovered_plugins[plugin_name] = {
                            'path': so_file,
                            'type': 'shared_library',
                            'platform': 'linux'
                        }

                    # Look for .dll files (Windows)
                    dll_files = glob.glob(os.path.join(path, "*.dll"))
                    for dll_file in dll_files:
                        plugin_name = os.path.splitext(os.path.basename(dll_file))[0]
                        discovered_plugins[plugin_name] = {
                            'path': dll_file,
                            'type': 'shared_library',
                            'platform': 'windows'
                        }

            self.available_plugins = discovered_plugins

        except Exception as e:
            print(f"Plugin discovery failed: {e}")
            self.available_plugins = {}

    def load_plugin(self, name: str, path: str = None, args: list = None) -> bool:
        """
        Load a Redis plugin/module.

        Args:
            name: Plugin name
            path: Path to plugin file (optional if auto-discovered)
            args: Arguments to pass to the plugin

        Returns:
            True if loaded successfully
        """
        try:
            # Find plugin path
            plugin_path = path
            if not plugin_path and name in self.available_plugins:
                plugin_path = self.available_plugins[name]['path']

            if not plugin_path:
                raise ValueError(f"Plugin '{name}' not found. Specify path or ensure plugin is in plugin directory.")

            # Load the module
            cmd_args = ['MODULE', 'LOAD', plugin_path]
            if args:
                cmd_args.extend(args)

            result = self.redis._execute_command(cmd_args)

            if result == 'OK':
                # Store plugin info
                self.loaded_plugins[name] = {
                    'path': plugin_path,
                    'loaded_at': int(time.time()),
                    'args': args or []
                }

                print(f"✓ Plugin '{name}' loaded successfully from {plugin_path}")
                return True
            else:
                print(f"✗ Failed to load plugin '{name}': {result}")
                return False

        except Exception as e:
            print(f"✗ Error loading plugin '{name}': {e}")
            return False

    def unload_plugin(self, name: str) -> bool:
        """
        Unload a Redis plugin/module.

        Args:
            name: Plugin name

        Returns:
            True if unloaded successfully
        """
        try:
            result = self.redis._execute_command(['MODULE', 'UNLOAD', name])

            if result == 'OK':
                if name in self.loaded_plugins:
                    del self.loaded_plugins[name]
                print(f"✓ Plugin '{name}' unloaded successfully")
                return True
            else:
                print(f"✗ Failed to unload plugin '{name}': {result}")
                return False

        except Exception as e:
            print(f"✗ Error unloading plugin '{name}': {e}")
            return False

    def list_loaded_plugins(self) -> list:
        """
        List all loaded plugins/modules.

        Returns:
            List of loaded plugin information
        """
        try:
            modules_info = self.redis._execute_command(['MODULE', 'LIST'])
            return modules_info
        except Exception as e:
            print(f"Error listing modules: {e}")
            return []

    def get_plugin_info(self, name: str) -> dict:
        """
        Get information about a loaded plugin.

        Args:
            name: Plugin name

        Returns:
            Plugin information dictionary
        """
        try:
            # Get module info from Redis
            redis_info = self.redis._execute_command(['MODULE', 'INFO', name])

            # Combine with our stored info
            plugin_info = self.loaded_plugins.get(name, {})

            if redis_info:
                plugin_info.update({
                    'redis_info': redis_info,
                    'status': 'loaded'
                })
            else:
                plugin_info['status'] = 'not_found'

            return plugin_info

        except Exception as e:
            return {
                'name': name,
                'status': 'error',
                'error': str(e)
            }

    def list_available_plugins(self) -> dict:
        """
        List all available (discovered) plugins.

        Returns:
            Dictionary of available plugins
        """
        return self.available_plugins.copy()

    def load_builtin_plugins(self):
        """Load commonly used built-in plugins if available"""
        common_plugins = {
            'redisjson': 'RedisJSON for JSON operations',
            'redisearch': 'RediSearch for full-text search',
            'redistimeseries': 'RedisTimeSeries for time series data',
            'redisgraph': 'RedisGraph for graph database operations',
            'redisbloom': 'RedisBloom for Bloom filters',
            'redisgears': 'RedisGears for server-side processing'
        }

        loaded = []
        for plugin_name, description in common_plugins.items():
            if plugin_name in self.available_plugins:
                print(f"Loading {plugin_name}: {description}")
                if self.load_plugin(plugin_name):
                    loaded.append(plugin_name)
                else:
                    print(f"Failed to load {plugin_name}")

        return loaded

    def create_plugin_config(self, plugin_name: str, config: dict):
        """
        Create configuration for a plugin.

        Args:
            plugin_name: Plugin name
            config: Configuration dictionary
        """
        self.plugin_configs[plugin_name] = config

    def get_plugin_config(self, plugin_name: str) -> dict:
        """
        Get configuration for a plugin.

        Args:
            plugin_name: Plugin name

        Returns:
            Plugin configuration
        """
        return self.plugin_configs.get(plugin_name, {})

    def validate_plugin_compatibility(self, plugin_name: str) -> dict:
        """
        Validate plugin compatibility with current Redis version.

        Args:
            plugin_name: Plugin name

        Returns:
            Compatibility information
        """
        try:
            # Get Redis version
            info = self.redis.info('server')
            redis_version = info.get('redis_version', 'unknown')

            # Basic compatibility check (simplified)
            compatibility = {
                'redis_version': redis_version,
                'plugin_name': plugin_name,
                'compatible': True,
                'warnings': []
            }

            # Add version-specific checks here
            if plugin_name == 'redisearch':
                # RediSearch compatibility
                version_parts = redis_version.split('.')
                if len(version_parts) >= 2:
                    major = int(version_parts[0])
                    minor = int(version_parts[1])
                    if major < 6:
                        compatibility['compatible'] = False
                        compatibility['warnings'].append("RediSearch requires Redis 6.0+")

            elif plugin_name == 'redisjson':
                # RedisJSON compatibility
                if redis_version.startswith('5.'):
                    compatibility['warnings'].append("RedisJSON works better with Redis 6.0+")

            return compatibility

        except Exception as e:
            return {
                'error': f"Compatibility check failed: {e}",
                'compatible': False
            }

    def monitor_plugin_health(self) -> dict:
        """
        Monitor health of loaded plugins.

        Returns:
            Health status of all loaded plugins
        """
        health_status = {}

        for plugin_name in self.loaded_plugins.keys():
            try:
                info = self.get_plugin_info(plugin_name)
                health_status[plugin_name] = {
                    'status': 'healthy',
                    'info': info
                }
            except Exception as e:
                health_status[plugin_name] = {
                    'status': 'unhealthy',
                    'error': str(e)
                }

        return health_status

    def reload_plugin(self, name: str) -> bool:
        """
        Reload a plugin (unload and load again).

        Args:
            name: Plugin name

        Returns:
            True if reloaded successfully
        """
        try:
            # Store original config
            original_config = self.loaded_plugins.get(name, {})

            # Unload
            if not self.unload_plugin(name):
                return False

            # Wait a moment
            time.sleep(0.1)

            # Reload
            return self.load_plugin(name, original_config.get('path'), original_config.get('args'))

        except Exception as e:
            print(f"Error reloading plugin '{name}': {e}")
            return False

    def create_plugin_backup(self, plugin_name: str) -> dict:
        """
        Create a backup of plugin-related data before operations.

        Args:
            plugin_name: Plugin name

        Returns:
            Backup data
        """
        backup = {
            'timestamp': int(time.time()),
            'plugin_name': plugin_name,
            'config': self.get_plugin_config(plugin_name),
            'loaded_info': self.loaded_plugins.get(plugin_name, {})
        }

        return backup

    def export_plugin_config(self, plugin_name: str) -> str:
        """
        Export plugin configuration as JSON.

        Args:
            plugin_name: Plugin name

        Returns:
            JSON configuration string
        """
        config = {
            'plugin_name': plugin_name,
            'config': self.get_plugin_config(plugin_name),
            'loaded_info': self.loaded_plugins.get(plugin_name, {}),
            'compatibility': self.validate_plugin_compatibility(plugin_name)
        }

        return json.dumps(config, indent=2)

    def import_plugin_config(self, config_json: str) -> bool:
        """
        Import plugin configuration from JSON.

        Args:
            config_json: JSON configuration string

        Returns:
            True if imported successfully
        """
        try:
            config = json.loads(config_json)
            plugin_name = config['plugin_name']

            if 'config' in config:
                self.plugin_configs[plugin_name] = config['config']

            if 'loaded_info' in config:
                self.loaded_plugins[plugin_name] = config['loaded_info']

            return True

        except Exception as e:
            print(f"Error importing plugin config: {e}")
            return False
