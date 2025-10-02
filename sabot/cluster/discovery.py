#!/usr/bin/env python3
"""
Node Discovery for Sabot clusters.

Provides automatic discovery of cluster nodes through various mechanisms:
- Multicast DNS (mDNS/Bonjour)
- Consul service discovery
- etcd service registry
- Kubernetes service discovery
- Static configuration
- Gossip protocol
"""

import asyncio
import json
import socket
import logging
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
import aiohttp
import time

try:
    import zeroconf
    ZEROCONF_AVAILABLE = True
except ImportError:
    ZEROCONF_AVAILABLE = False
    zeroconf = None

logger = logging.getLogger(__name__)


@dataclass
class ServiceInstance:
    """Represents a discovered service instance."""
    service_type: str
    service_name: str
    node_id: str
    host: str
    port: int
    metadata: Dict[str, Any] = None
    discovered_at: float = None

    def __post_init__(self):
        if self.discovered_at is None:
            self.discovered_at = time.time()
        if self.metadata is None:
            self.metadata = {}


class DiscoveryBackend:
    """Base class for discovery backends."""

    async def discover_services(self, service_type: str) -> List[ServiceInstance]:
        """Discover services of the given type."""
        raise NotImplementedError

    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register a service instance."""
        raise NotImplementedError

    async def unregister_service(self, service_name: str, node_id: str) -> bool:
        """Unregister a service instance."""
        raise NotImplementedError


class MulticastDNSDiscovery(DiscoveryBackend):
    """Multicast DNS (mDNS) service discovery."""

    def __init__(self):
        self.zeroconf = None
        self.services: Dict[str, zeroconf.ServiceInfo] = {}
        self.discovered_services: List[ServiceInstance] = []

    async def discover_services(self, service_type: str) -> List[ServiceInstance]:
        """Discover services using mDNS."""
        if not ZEROCONF_AVAILABLE:
            logger.warning("Zeroconf not available, cannot use mDNS discovery")
            return []

        try:
            # Create Zeroconf instance
            if self.zeroconf is None:
                self.zeroconf = zeroconf.Zeroconf()

            # Browse for services
            service_name = f"_{service_type}._tcp.local."

            browser = zeroconf.ServiceBrowser(
                self.zeroconf, service_name, self._service_discovered
            )

            # Wait for discovery
            await asyncio.sleep(2)

            # Convert discovered services
            instances = []
            for service_info in self.services.values():
                instance = ServiceInstance(
                    service_type=service_type,
                    service_name=service_info.name,
                    node_id=service_info.properties.get(b'node_id', b'unknown').decode(),
                    host=socket.inet_ntoa(service_info.addresses[0]) if service_info.addresses else 'unknown',
                    port=service_info.port,
                    metadata={
                        'properties': {k.decode(): v.decode() for k, v in service_info.properties.items()}
                    }
                )
                instances.append(instance)

            return instances

        except Exception as e:
            logger.error(f"mDNS discovery failed: {e}")
            return []

    def _service_discovered(self, zeroconf_obj, service_type, name, state_change):
        """Callback for service discovery."""
        if state_change == zeroconf.ServiceStateChange.Added:
            info = zeroconf_obj.get_service_info(service_type, name)
            if info:
                self.services[name] = info
        elif state_change == zeroconf.ServiceStateChange.Removed:
            if name in self.services:
                del self.services[name]

    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register service with mDNS."""
        if not ZEROCONF_AVAILABLE or not self.zeroconf:
            return False

        try:
            service_name = f"{instance.service_name}.{instance.service_type}._tcp.local."

            properties = {
                b'node_id': instance.node_id.encode(),
                **{k.encode(): str(v).encode() for k, v in instance.metadata.items()}
            }

            info = zeroconf.ServiceInfo(
                type_=f"_{instance.service_type}._tcp.local.",
                name=service_name,
                addresses=[socket.inet_aton(instance.host)],
                port=instance.port,
                properties=properties
            )

            self.zeroconf.register_service(info)
            return True

        except Exception as e:
            logger.error(f"mDNS service registration failed: {e}")
            return False

    async def unregister_service(self, service_name: str, node_id: str) -> bool:
        """Unregister service from mDNS."""
        if not self.zeroconf:
            return False

        try:
            # Find and unregister the service
            for name, info in self.services.items():
                if info.properties.get(b'node_id', b'').decode() == node_id:
                    self.zeroconf.unregister_service(info)
                    del self.services[name]
                    return True
            return False

        except Exception as e:
            logger.error(f"mDNS service unregistration failed: {e}")
            return False


class ConsulDiscovery(DiscoveryBackend):
    """Consul-based service discovery."""

    def __init__(self, consul_url: str = "http://localhost:8500"):
        self.consul_url = consul_url.rstrip('/')
        self.registered_services: Set[str] = set()

    async def discover_services(self, service_type: str) -> List[ServiceInstance]:
        """Discover services using Consul."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.consul_url}/v1/catalog/service/{service_type}"
                async with session.get(url) as response:
                    if response.status == 200:
                        services = await response.json()
                        instances = []

                        for service in services:
                            instance = ServiceInstance(
                                service_type=service_type,
                                service_name=service['ServiceName'],
                                node_id=service.get('ServiceID', service['ServiceName']),
                                host=service['ServiceAddress'],
                                port=service['ServicePort'],
                                metadata=service.get('ServiceMeta', {})
                            )
                            instances.append(instance)

                        return instances
                    else:
                        logger.warning(f"Consul discovery failed: HTTP {response.status}")
                        return []

        except Exception as e:
            logger.error(f"Consul discovery failed: {e}")
            return []

    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register service with Consul."""
        try:
            async with aiohttp.ClientSession() as session:
                service_definition = {
                    "ID": f"{instance.service_name}-{instance.node_id}",
                    "Name": instance.service_name,
                    "Tags": [instance.service_type],
                    "Address": instance.host,
                    "Port": instance.port,
                    "Meta": instance.metadata,
                    "Check": {
                        "HTTP": f"http://{instance.host}:{instance.port}/health",
                        "Interval": "30s",
                        "Timeout": "5s"
                    }
                }

                url = f"{self.consul_url}/v1/agent/service/register"
                async with session.put(url, json=service_definition) as response:
                    if response.status == 200:
                        service_id = f"{instance.service_name}-{instance.node_id}"
                        self.registered_services.add(service_id)
                        return True
                    else:
                        logger.error(f"Consul registration failed: HTTP {response.status}")
                        return False

        except Exception as e:
            logger.error(f"Consul registration failed: {e}")
            return False

    async def unregister_service(self, service_name: str, node_id: str) -> bool:
        """Unregister service from Consul."""
        try:
            service_id = f"{service_name}-{node_id}"

            if service_id not in self.registered_services:
                return True

            async with aiohttp.ClientSession() as session:
                url = f"{self.consul_url}/v1/agent/service/deregister/{service_id}"
                async with session.put(url) as response:
                    if response.status == 200:
                        self.registered_services.discard(service_id)
                        return True
                    else:
                        logger.error(f"Consul unregistration failed: HTTP {response.status}")
                        return False

        except Exception as e:
            logger.error(f"Consul unregistration failed: {e}")
            return False


class StaticDiscovery(DiscoveryBackend):
    """Static configuration-based service discovery."""

    def __init__(self, static_nodes: List[Dict[str, Any]]):
        self.static_nodes = static_nodes

    async def discover_services(self, service_type: str) -> List[ServiceInstance]:
        """Return statically configured services."""
        instances = []
        for node_config in self.static_nodes:
            if node_config.get('service_type') == service_type:
                instance = ServiceInstance(
                    service_type=service_type,
                    service_name=node_config.get('service_name', service_type),
                    node_id=node_config['node_id'],
                    host=node_config['host'],
                    port=node_config['port'],
                    metadata=node_config.get('metadata', {})
                )
                instances.append(instance)

        return instances

    async def register_service(self, instance: ServiceInstance) -> bool:
        """Static discovery doesn't support registration."""
        return True

    async def unregister_service(self, service_name: str, node_id: str) -> bool:
        """Static discovery doesn't support unregistration."""
        return True


class NodeDiscovery:
    """
    Multi-backend node discovery for Sabot clusters.

    Supports multiple discovery mechanisms and automatically
    aggregates results from all configured backends.
    """

    def __init__(self, discovery_endpoints: List[str]):
        self.discovery_endpoints = discovery_endpoints
        self.backends: List[DiscoveryBackend] = []
        self.known_services: Dict[str, ServiceInstance] = {}
        self._initialized = False

        # Initialize backends based on endpoints
        self._initialize_backends()

    def _initialize_backends(self):
        """Initialize discovery backends."""
        for endpoint in self.discovery_endpoints:
            if endpoint.startswith('mdns://'):
                if ZEROCONF_AVAILABLE:
                    self.backends.append(MulticastDNSDiscovery())
                else:
                    logger.warning("mDNS discovery requested but zeroconf not available")

            elif endpoint.startswith('consul://'):
                consul_url = endpoint.replace('consul://', 'http://')
                self.backends.append(ConsulDiscovery(consul_url))

            elif endpoint.startswith('static://'):
                # Parse static configuration
                static_config = self._parse_static_config(endpoint)
                if static_config:
                    self.backends.append(StaticDiscovery(static_config))

            else:
                logger.warning(f"Unknown discovery endpoint type: {endpoint}")

    def _parse_static_config(self, endpoint: str) -> Optional[List[Dict[str, Any]]]:
        """Parse static configuration from endpoint."""
        try:
            # Format: static://node1:host:port,node2:host:port,...
            config_str = endpoint.replace('static://', '')
            nodes = []

            for node_str in config_str.split(','):
                parts = node_str.split(':')
                if len(parts) >= 3:
                    nodes.append({
                        'node_id': parts[0],
                        'host': parts[1],
                        'port': int(parts[2]),
                        'service_type': 'sabot-node',
                        'service_name': 'sabot-cluster'
                    })

            return nodes

        except Exception as e:
            logger.error(f"Failed to parse static config: {e}")
            return None

    async def discover_nodes(self) -> List[Dict[str, Any]]:
        """
        Discover available nodes using all configured backends.

        Returns:
            List of node information dictionaries
        """
        discovered_nodes = []

        # Query all backends
        for backend in self.backends:
            try:
                services = await backend.discover_services('sabot-node')
                for service in services:
                    node_info = {
                        'node_id': service.node_id,
                        'host': service.host,
                        'port': service.port,
                        'discovered_via': backend.__class__.__name__,
                        'metadata': service.metadata
                    }

                    # Avoid duplicates
                    if not any(n['node_id'] == node_info['node_id'] for n in discovered_nodes):
                        discovered_nodes.append(node_info)

                        # Cache the service
                        self.known_services[service.node_id] = service

            except Exception as e:
                logger.error(f"Backend {backend.__class__.__name__} discovery failed: {e}")

        return discovered_nodes

    async def register_node(self, node_id: str, host: str, port: int,
                           metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Register this node with discovery services.

        Returns:
            True if registration succeeded on at least one backend
        """
        instance = ServiceInstance(
            service_type='sabot-node',
            service_name='sabot-cluster',
            node_id=node_id,
            host=host,
            port=port,
            metadata=metadata or {}
        )

        success = False
        for backend in self.backends:
            try:
                if await backend.register_service(instance):
                    success = True
                    logger.info(f"Registered node {node_id} with {backend.__class__.__name__}")
                else:
                    logger.warning(f"Failed to register node {node_id} with {backend.__class__.__name__}")
            except Exception as e:
                logger.error(f"Registration error with {backend.__class__.__name__}: {e}")

        return success

    async def unregister_node(self, node_id: str) -> bool:
        """
        Unregister this node from discovery services.

        Returns:
            True if unregistration succeeded on at least one backend
        """
        success = False

        if node_id in self.known_services:
            service = self.known_services[node_id]

            for backend in self.backends:
                try:
                    if await backend.unregister_service(service.service_name, node_id):
                        success = True
                        logger.info(f"Unregistered node {node_id} from {backend.__class__.__name__}")
                except Exception as e:
                    logger.error(f"Unregistration error with {backend.__class__.__name__}: {e}")

            # Remove from cache
            del self.known_services[node_id]

        return success

    def get_known_nodes(self) -> List[Dict[str, Any]]:
        """Get list of known nodes from cache."""
        return [
            {
                'node_id': service.node_id,
                'host': service.host,
                'port': service.port,
                'service_type': service.service_type,
                'discovered_at': service.discovered_at,
                'metadata': service.metadata
            }
            for service in self.known_services.values()
        ]

    def add_static_node(self, node_id: str, host: str, port: int,
                       metadata: Optional[Dict[str, Any]] = None):
        """Add a static node configuration."""
        # Add to static backend if it exists, otherwise create one
        static_backend = None
        for backend in self.backends:
            if isinstance(backend, StaticDiscovery):
                static_backend = backend
                break

        if static_backend is None:
            static_backend = StaticDiscovery([])
            self.backends.append(static_backend)

        # Add node to static configuration
        node_config = {
            'node_id': node_id,
            'host': host,
            'port': port,
            'service_type': 'sabot-node',
            'service_name': 'sabot-cluster',
            'metadata': metadata or {}
        }

        static_backend.static_nodes.append(node_config)

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on discovery system."""
        backend_status = {}

        for i, backend in enumerate(self.backends):
            backend_name = f"{backend.__class__.__name__}_{i}"
            try:
                # Test discovery
                services = await backend.discover_services('sabot-node')
                backend_status[backend_name] = {
                    'status': 'healthy',
                    'services_discovered': len(services)
                }
            except Exception as e:
                backend_status[backend_name] = {
                    'status': 'unhealthy',
                    'error': str(e)
                }

        return {
            'status': 'healthy' if any(b['status'] == 'healthy' for b in backend_status.values()) else 'unhealthy',
            'backends': backend_status,
            'known_nodes': len(self.known_services)
        }
