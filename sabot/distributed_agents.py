# -*- coding: utf-8 -*-
"""Distributed agent system with Flink-style chaining, inspired by Faust."""

import asyncio
import logging
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union, AsyncIterator, Awaitable
from dataclasses import dataclass, field
from weakref import WeakSet
from contextlib import asynccontextmanager

try:
    from mode import Service
    MODE_AVAILABLE = True
except ImportError:
    MODE_AVAILABLE = False
    Service = object  # Fallback
try:
    from faust.types.tuples import TP
    FAUST_AVAILABLE = True
except ImportError:
    FAUST_AVAILABLE = False
    from typing import NamedTuple
    class TP(NamedTuple):
        topic: str
        partition: int

from .dbos_parallel_controller import DBOSParallelController
from .distributed_coordinator import DistributedCoordinator
from .channels import Channel
from .channel_manager import ChannelManager, ChannelBackend, ChannelConfig

logger = logging.getLogger(__name__)

class DistributedAgentError(Exception):
    """Error in distributed agent operations."""
    pass

@dataclass
class AgentSpec:
    """Specification for a distributed agent."""
    name: str
    func: Callable
    concurrency: int = 1
    isolated_partitions: bool = False
    channel_backend: ChannelBackend = ChannelBackend.MEMORY
    node_affinity: Optional[List[str]] = None  # Preferred nodes
    resource_requirements: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ActorInstance:
    """Runtime instance of an actor on a specific node."""
    actor_id: str
    agent_name: str
    node_id: str
    partition: Optional[TP] = None
    status: str = "starting"  # starting, running, stopped, failed
    last_heartbeat: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    metrics: Dict[str, Any] = field(default_factory=dict)

class DistributedAgent(Service):
    """Distributed agent that can run across multiple nodes with DBOS control."""

    def __init__(self,
                 spec: AgentSpec,
                 coordinator: DistributedCoordinator,
                 channel_manager: ChannelManager,
                 dbos_controller: DBOSParallelController):
        self.spec = spec
        self.coordinator = coordinator
        self.channel_manager = channel_manager
        self.dbos_controller = dbos_controller

        # Runtime state
        self.actors: Dict[str, ActorInstance] = {}
        self.active_partitions: Set[TP] = set()
        self.supervisor_strategy: Optional[Type] = None

        # Communication
        self.input_channel: Optional[Channel] = None
        self.output_channels: Dict[str, Channel] = {}

        Service.__init__(self)

    async def on_start(self) -> None:
        """Start the distributed agent."""
        logger.info(f"Starting distributed agent {self.spec.name}")

        # Create input channel
        self.input_channel = await self.channel_manager.create_channel(
            f"{self.spec.name}_input",
            ChannelConfig(backend=self.spec.channel_backend)
        )

        # Register with coordinator
        await self.coordinator.submit_job(
            {"agent_spec": self.spec, "operation": "deploy"},
            self._deploy_agent_func
        )

        # Start actor supervision
        asyncio.create_task(self._supervise_actors())

    async def on_stop(self) -> None:
        """Stop the distributed agent."""
        logger.info(f"Stopping distributed agent {self.spec.name}")

        # Stop all actors
        for actor in self.actors.values():
            await self._stop_actor(actor.actor_id)

        # Cleanup channels
        if self.input_channel:
            await self.input_channel.close()

        for channel in self.output_channels.values():
            await channel.close()

    async def _deploy_agent_func(self, deployment_data: Dict[str, Any]) -> None:
        """Deploy agent across available nodes."""
        spec = deployment_data["agent_spec"]

        # Get available nodes from DBOS
        available_nodes = await self._get_available_nodes()

        # Decide deployment strategy based on agent requirements
        if spec.isolated_partitions:
            await self._deploy_isolated_partitions(spec, available_nodes)
        else:
            await self._deploy_shared_concurrency(spec, available_nodes)

    async def _get_available_nodes(self) -> List[str]:
        """Get available nodes from coordinator."""
        try:
            stats = self.coordinator.get_cluster_stats()
            return [node["node_id"] for node in stats["nodes"] if node["status"] == "alive"]
        except Exception:
            return []  # Fallback to local only

    async def _deploy_isolated_partitions(self, spec: AgentSpec, nodes: List[str]) -> None:
        """Deploy agent with isolated partitions (one actor per partition)."""
        # Get partitions for this agent (would come from topic metadata)
        partitions = await self._get_agent_partitions(spec.name)

        for i, partition in enumerate(partitions):
            # Select node for this partition
            node_id = nodes[i % len(nodes)] if nodes else "local"

            # Create actor instance
            actor_id = f"{spec.name}_actor_{partition.partition}_{partition.topic}"
            actor = ActorInstance(
                actor_id=actor_id,
                agent_name=spec.name,
                node_id=node_id,
                partition=partition
            )

            self.actors[actor_id] = actor

            # Deploy to node
            await self._deploy_actor_to_node(actor, spec)

    async def _deploy_shared_concurrency(self, spec: AgentSpec, nodes: List[str]) -> None:
        """Deploy agent with shared concurrency across nodes."""
        for i in range(spec.concurrency):
            # Select node (round-robin or DBOS decision)
            node_id = nodes[i % len(nodes)] if nodes else "local"

            # Create actor instance
            actor_id = f"{spec.name}_actor_{i}"
            actor = ActorInstance(
                actor_id=actor_id,
                agent_name=spec.name,
                node_id=node_id
            )

            self.actors[actor_id] = actor

            # Deploy to node
            await self._deploy_actor_to_node(actor, spec)

    async def _deploy_actor_to_node(self, actor: ActorInstance, spec: AgentSpec) -> None:
        """Deploy an actor instance to a specific node."""
        try:
            if actor.node_id == "local":
                # Deploy locally
                await self._start_local_actor(actor, spec)
            else:
                # Deploy remotely via coordinator
                await self.coordinator.submit_job(
                    {
                        "actor": actor,
                        "spec": spec,
                        "operation": "start_actor"
                    },
                    self._remote_actor_starter
                )

            actor.status = "running"
            logger.info(f"Deployed actor {actor.actor_id} to node {actor.node_id}")

        except Exception as e:
            actor.status = "failed"
            logger.error(f"Failed to deploy actor {actor.actor_id}: {e}")

    async def _start_local_actor(self, actor: ActorInstance, spec: AgentSpec) -> None:
        """Start an actor locally."""
        # Create actor task
        task = asyncio.create_task(self._run_actor_loop(actor, spec))
        actor.metrics["task"] = task

    async def _remote_actor_starter(self, data: Dict[str, Any]) -> None:
        """Start actor on remote node (placeholder for remote execution)."""
        actor = data["actor"]
        spec = data["spec"]
        # In real implementation, this would communicate with remote node
        logger.info(f"Remote actor start requested for {actor.actor_id}")

    async def _run_actor_loop(self, actor: ActorInstance, spec: AgentSpec) -> None:
        """Main actor execution loop."""
        try:
            # Get input stream for this actor
            input_stream = await self._get_actor_input_stream(actor)

            # Run agent function
            async for event in input_stream:
                actor.last_heartbeat = asyncio.get_event_loop().time()

                try:
                    # Execute agent function
                    result = await spec.func(event)

                    # Send result to output channels
                    await self._send_to_outputs(result, actor)

                    # Update metrics
                    actor.metrics["processed"] = actor.metrics.get("processed", 0) + 1

                except Exception as e:
                    logger.error(f"Actor {actor.actor_id} error processing event: {e}")
                    actor.metrics["errors"] = actor.metrics.get("errors", 0) + 1

        except Exception as e:
            logger.error(f"Actor {actor.actor_id} loop failed: {e}")
            actor.status = "failed"

    async def _get_actor_input_stream(self, actor: ActorInstance) -> AsyncIterator:
        """Get input stream for actor."""
        if self.input_channel:
            # For now, all actors share the same input channel
            # In full implementation, this would route based on partitioning
            return self.input_channel
        else:
            # Empty stream
            async def empty_stream():
                return
                yield  # pragma: no cover
            return empty_stream()

    async def _send_to_outputs(self, result: Any, actor: ActorInstance) -> None:
        """Send result to output channels."""
        for channel_name, channel in self.output_channels.items():
            await channel.put(result)

    async def _supervise_actors(self) -> None:
        """Supervise actor health and restart failed ones."""
        while not self.should_stop:
            try:
                current_time = asyncio.get_event_loop().time()

                for actor in list(self.actors.values()):
                    # Check heartbeat
                    if current_time - actor.last_heartbeat > 30.0:  # 30 second timeout
                        logger.warning(f"Actor {actor.actor_id} heartbeat timeout")
                        await self._restart_actor(actor)

                    # Check status
                    if actor.status == "failed":
                        logger.warning(f"Actor {actor.actor_id} failed, restarting")
                        await self._restart_actor(actor)

                await asyncio.sleep(10.0)  # Check every 10 seconds

            except Exception as e:
                logger.error(f"Supervisor error: {e}")
                await asyncio.sleep(10.0)

    async def _restart_actor(self, actor: ActorInstance) -> None:
        """Restart a failed actor."""
        try:
            # Stop existing actor
            await self._stop_actor(actor.actor_id)

            # Create new actor instance
            new_actor = ActorInstance(
                actor_id=f"{actor.agent_name}_actor_restart_{uuid.uuid4().hex[:8]}",
                agent_name=actor.agent_name,
                node_id=actor.node_id,
                partition=actor.partition
            )

            self.actors[new_actor.actor_id] = new_actor

            # Deploy new actor
            await self._deploy_actor_to_node(new_actor, self.spec)

            # Remove old actor
            del self.actors[actor.actor_id]

            logger.info(f"Restarted actor {actor.actor_id} -> {new_actor.actor_id}")

        except Exception as e:
            logger.error(f"Failed to restart actor {actor.actor_id}: {e}")

    async def _stop_actor(self, actor_id: str) -> None:
        """Stop a specific actor."""
        if actor_id in self.actors:
            actor = self.actors[actor_id]
            actor.status = "stopped"

            # Cancel task if running locally
            if "task" in actor.metrics:
                actor.metrics["task"].cancel()

    async def send(self, value: Any, key: Any = None, partition: int = None) -> None:
        """Send data to this agent."""
        if self.input_channel:
            await self.input_channel.put(value)

    def add_sink(self, sink) -> None:
        """Add output sink to agent."""
        sink_name = f"sink_{len(self.output_channels)}"
        # In real implementation, this would create appropriate output channel
        # For now, just track the sink
        self.output_channels[sink_name] = sink

    async def _get_agent_partitions(self, agent_name: str) -> List[TP]:
        """Get partitions for agent (placeholder)."""
        # In real implementation, this would query topic metadata
        return [TP(topic=f"{agent_name}_topic", partition=i) for i in range(3)]

class DistributedAgentManager(Service):
    """Manager for distributed agents across the cluster."""

    def __init__(self,
                 coordinator: DistributedCoordinator,
                 channel_manager: ChannelManager,
                 dbos_controller: DBOSParallelController):
        self.coordinator = coordinator
        self.channel_manager = channel_manager
        self.dbos_controller = dbos_controller

        self.agents: Dict[str, DistributedAgent] = {}
        self._agent_dependencies: Dict[str, Set[str]] = {}

        Service.__init__(self)

    async def on_start(self) -> None:
        """Start agent manager."""
        logger.info("Starting distributed agent manager")

    async def on_stop(self) -> None:
        """Stop agent manager."""
        logger.info("Stopping distributed agent manager")

        # Stop all agents
        for agent in self.agents.values():
            await agent.stop()

    async def create_agent(self,
                          name: str,
                          func: Callable,
                          concurrency: int = 1,
                          **kwargs) -> DistributedAgent:
        """Create a new distributed agent."""
        spec = AgentSpec(
            name=name,
            func=func,
            concurrency=concurrency,
            **kwargs
        )

        agent = DistributedAgent(spec, self.coordinator, self.channel_manager, self.dbos_controller)
        self.agents[name] = agent

        # Start the agent
        await agent.start()

        return agent

    def get_agent(self, name: str) -> Optional[DistributedAgent]:
        """Get agent by name."""
        return self.agents.get(name)

    async def connect_agents(self, upstream: str, downstream: str) -> None:
        """Connect output of upstream agent to input of downstream agent."""
        upstream_agent = self.get_agent(upstream)
        downstream_agent = self.get_agent(downstream)

        if not upstream_agent or not downstream_agent:
            raise DistributedAgentError(f"Agent not found: {upstream} or {downstream}")

        # Create connection channel
        connection_channel = await self.channel_manager.create_channel(
            f"{upstream}_to_{downstream}",
            ChannelConfig(backend=ChannelBackend.MEMORY)
        )

        # Connect: upstream output -> connection channel -> downstream input
        upstream_agent.add_sink(connection_channel)
        downstream_agent.input_channel = connection_channel

        # Track dependency
        if upstream not in self._agent_dependencies:
            self._agent_dependencies[upstream] = set()
        self._agent_dependencies[upstream].add(downstream)

    def get_agent_dependencies(self) -> Dict[str, Set[str]]:
        """Get agent dependency graph."""
        return dict(self._agent_dependencies)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive agent statistics."""
        agent_stats = {}
        total_actors = 0
        total_processed = 0
        total_errors = 0

        for name, agent in self.agents.items():
            agent_stats[name] = {
                "actor_count": len(agent.actors),
                "active_partitions": len(agent.active_partitions),
                "input_channel": agent.input_channel is not None,
                "output_channels": len(agent.output_channels),
                "actors": [
                    {
                        "actor_id": actor.actor_id,
                        "node_id": actor.node_id,
                        "status": actor.status,
                        "partition": str(actor.partition) if actor.partition else None,
                        "processed": actor.metrics.get("processed", 0),
                        "errors": actor.metrics.get("errors", 0),
                    }
                    for actor in agent.actors.values()
                ]
            }

            total_actors += len(agent.actors)
            for actor in agent.actors.values():
                total_processed += actor.metrics.get("processed", 0)
                total_errors += actor.metrics.get("errors", 0)

        return {
            "total_agents": len(self.agents),
            "total_actors": total_actors,
            "total_processed": total_processed,
            "total_errors": total_errors,
            "agent_dependencies": self.get_agent_dependencies(),
            "agents": agent_stats,
        }
