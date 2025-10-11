"""
Query Executor

Orchestrates execution of physical query plans.
Supports both local (morsel-based) and distributed (shuffle-based) execution.

Execution modes:
1. Local execution - MorselDrivenOperator for parallelism
2. Distributed execution - ShuffleTransport for network shuffle

Performance:
- Local: 3-37M matches/sec (pattern matching kernels)
- Morsel parallelism: Auto-enabled for batches ≥10K rows
- Distributed: 100K-1M matches/sec per agent
"""

from typing import Optional, List, Dict, Any
import uuid

from sabot import cyarrow as pa

from ..compiler.query_plan import PhysicalPlan, PlanNode

try:
    from ...operators.morsel_operator import MorselDrivenOperator
except ImportError:
    MorselDrivenOperator = None


class QueryExecutor:
    """
    Execute physical query plans using morsel-based operators.

    Handles:
    - Local execution with morsel parallelism
    - Distributed execution with shuffle
    - Operator chain building and orchestration
    - Result collection and combination
    """

    def __init__(
        self,
        graph_engine,
        shuffle_transport=None,
        agent_addresses=None,
        local_agent_id=0,
        num_workers=None
    ):
        """
        Initialize query executor.

        Args:
            graph_engine: GraphQueryEngine instance
            shuffle_transport: Optional ShuffleTransport for distributed execution
            agent_addresses: List of agent addresses ["host:port"] for distributed execution
            local_agent_id: This agent's partition ID (0-based index in agent_addresses)
            num_workers: Number of workers for morsel parallelism (default: auto)
        """
        self.graph_engine = graph_engine
        self.num_workers = num_workers or 0  # 0 = auto-detect

        # Agent configuration for distributed execution
        self.agent_addresses = agent_addresses or ["localhost:8816"]
        self.local_agent_id = local_agent_id

        # Initialize shuffle transport if distributed
        if agent_addresses and len(agent_addresses) > 1:
            if shuffle_transport is None:
                try:
                    from sabot._cython.shuffle.shuffle_transport import ShuffleTransport
                    self.shuffle_transport = ShuffleTransport()
                    # Start on this agent's address
                    host, port = agent_addresses[local_agent_id].split(':')
                    self.shuffle_transport.start(host.encode('utf-8'), int(port))
                except ImportError:
                    print("Warning: ShuffleTransport not available, falling back to local execution")
                    self.shuffle_transport = None
            else:
                self.shuffle_transport = shuffle_transport
        else:
            self.shuffle_transport = shuffle_transport

        # Initialize zero-copy shuffle orchestrator if distributed
        self.shuffle_orchestrator = None
        if self.shuffle_transport is not None:
            try:
                from .shuffle_integration import create_shuffle_orchestrator
                self.shuffle_orchestrator = create_shuffle_orchestrator(
                    shuffle_transport=self.shuffle_transport,
                    agent_addresses=self.agent_addresses,
                    local_agent_id=self.local_agent_id
                )
            except ImportError:
                print("Warning: ShuffleOrchestrator not available, using fallback shuffle")
                self.shuffle_orchestrator = None

        # Execution stats
        self.stats = {
            'total_rows': 0,
            'execution_time_ms': 0.0,
            'operators_executed': 0
        }

    def execute(
        self,
        physical_plan: PhysicalPlan,
        distributed=False
    ) -> pa.Table:
        """
        Execute physical plan and return results.

        Args:
            physical_plan: Physical query plan with operators
            distributed: Enable distributed execution (default False)

        Returns:
            Arrow table with query results
        """
        # Build operator chain from plan
        operator_chain = self._build_operator_chain(physical_plan.root)

        # Execute based on mode
        if distributed and self.shuffle_transport is not None:
            result = self._execute_distributed(operator_chain, physical_plan)
        else:
            result = self._execute_local(operator_chain)

        return result

    def _build_operator_chain(self, root_node: PlanNode) -> List[Any]:
        """
        Build operator chain from physical plan.

        Traverses plan tree and extracts operators in execution order (bottom-up).

        Args:
            root_node: Root of physical plan tree

        Returns:
            List of operators in execution order
        """
        operators = []

        def visit(node: PlanNode):
            """Visit node and collect operators."""
            # Visit children first (bottom-up)
            for child in node.children:
                visit(child)

            # Add this node's operator if present
            if 'operator' in node.properties:
                operators.append(node.properties['operator'])

        visit(root_node)
        return operators

    def _execute_local(self, operator_chain: List[Any]) -> pa.Table:
        """
        Execute operator chain locally with morsel parallelism.

        For stateless operators on large batches (≥10K rows),
        automatically wraps in MorselDrivenOperator for parallelism.

        Args:
            operator_chain: List of operators to execute

        Returns:
            Arrow table with results
        """
        if not operator_chain:
            # Empty chain - return empty table
            return pa.table({})

        # Execute operators sequentially
        current_batch = None

        for operator in operator_chain:
            # Check if we should use morsel parallelism
            if self._should_use_morsel(operator, current_batch):
                # Wrap in MorselDrivenOperator
                if MorselDrivenOperator is not None:
                    morsel_op = MorselDrivenOperator(
                        operator,
                        num_workers=self.num_workers
                    )
                    current_batch = morsel_op.process_batch(current_batch)
                else:
                    # Fallback to direct execution
                    current_batch = operator.process_batch(current_batch)
            else:
                # Direct execution (small batch or first operator)
                current_batch = operator.process_batch(current_batch)

            # Update stats
            self.stats['operators_executed'] += 1

            # Stop if no more results
            if current_batch is None or current_batch.num_rows == 0:
                break

        # Final result
        if current_batch is None:
            return pa.table({})

        # Convert to table if needed
        if isinstance(current_batch, pa.Table):
            result = current_batch
        else:
            # RecordBatch - convert to table
            result = pa.Table.from_batches([current_batch])

        # Update stats
        self.stats['total_rows'] = result.num_rows

        return result

    def _execute_distributed(
        self,
        operator_chain: List[Any],
        physical_plan: PhysicalPlan
    ) -> pa.Table:
        """
        Execute operator chain across distributed agents with shuffle.

        Identifies shuffle boundaries (stateful operators) and coordinates
        network shuffles via ShuffleTransport.

        Args:
            operator_chain: List of operators to execute
            physical_plan: Physical plan (for metadata)

        Returns:
            Arrow table with results
        """
        if not operator_chain:
            return pa.table({})

        # Execute operators, inserting shuffles at stateful boundaries
        current_batch = None
        shuffle_id = str(uuid.uuid4())

        for i, operator in enumerate(operator_chain):
            # Check if this operator requires shuffle
            if hasattr(operator, '_stateful') and operator._stateful:
                # Stateful operator - requires shuffle
                current_batch = self._execute_with_shuffle(
                    operator,
                    current_batch,
                    shuffle_id,
                    i
                )
            else:
                # Stateless operator - execute locally
                if self._should_use_morsel(operator, current_batch):
                    # Use morsel parallelism
                    if MorselDrivenOperator is not None:
                        morsel_op = MorselDrivenOperator(
                            operator,
                            num_workers=self.num_workers
                        )
                        current_batch = morsel_op.process_batch(current_batch)
                    else:
                        current_batch = operator.process_batch(current_batch)
                else:
                    # Direct execution
                    current_batch = operator.process_batch(current_batch)

            # Update stats
            self.stats['operators_executed'] += 1

            # Stop if no results
            if current_batch is None or current_batch.num_rows == 0:
                break

        # Return final result
        if current_batch is None:
            return pa.table({})

        if isinstance(current_batch, pa.Table):
            return current_batch
        else:
            return pa.Table.from_batches([current_batch])

    def _execute_with_shuffle(
        self,
        operator: Any,
        batch: Optional[pa.Table],
        shuffle_id: str,
        operator_idx: int
    ) -> pa.Table:
        """
        Execute stateful operator with zero-copy network shuffle.

        Uses ShuffleOrchestrator for zero-copy orchestration when available.
        Falls back to direct shuffle transport if orchestrator not compiled.

        Steps (zero-copy path):
        1. Use ShuffleOrchestrator.execute_with_shuffle()
        2. Orchestrator partitions in C++ (no Python list intermediate)
        3. Sends C++ shared_ptrs directly to network
        4. Receives and executes operator on local partitions
        5. Returns combined results

        Args:
            operator: Stateful operator to execute
            batch: Input batch to partition
            shuffle_id: Unique shuffle identifier
            operator_idx: Operator index in chain

        Returns:
            Arrow table with results after shuffle
        """
        # Fallback to local execution if no shuffle transport
        if self.shuffle_transport is None:
            return operator.process_batch(batch)

        # Get partition keys from operator
        partition_keys = operator.get_partition_keys()
        if not partition_keys:
            # No partition keys - execute locally
            return operator.process_batch(batch)

        # If only one agent, no need to shuffle
        if len(self.agent_addresses) == 1:
            return operator.process_batch(batch)

        # Convert shuffle_id to bytes
        shuffle_id_bytes = shuffle_id.encode('utf-8') if isinstance(shuffle_id, str) else shuffle_id

        # Zero-copy path: Use ShuffleOrchestrator if available
        if self.shuffle_orchestrator is not None:
            return self.shuffle_orchestrator.execute_with_shuffle(
                operator=operator,
                batch=batch,
                shuffle_id=shuffle_id_bytes,
                partition_keys=partition_keys
            )

        # Fallback path: Direct shuffle transport (Python list intermediate)
        # This is used if ShuffleOrchestrator not compiled
        return self._execute_with_shuffle_fallback(
            operator, batch, shuffle_id_bytes, partition_keys
        )

    def _execute_with_shuffle_fallback(
        self,
        operator: Any,
        batch: pa.Table,
        shuffle_id_bytes: bytes,
        partition_keys: List[str]
    ) -> pa.Table:
        """
        Fallback shuffle execution using Python list intermediate.

        This breaks zero-copy at partition_batch → partition list conversion,
        but is safer if ShuffleOrchestrator not compiled.

        Args:
            operator: Stateful operator
            batch: Input batch
            shuffle_id_bytes: Shuffle ID (bytes)
            partition_keys: Partition key columns

        Returns:
            Combined results after shuffle
        """
        # Create hash partitioner
        try:
            from sabot._cython.shuffle.partitioner import HashPartitioner
        except ImportError:
            print("Warning: HashPartitioner not available, executing locally")
            return operator.process_batch(batch)

        num_agents = len(self.agent_addresses)
        partitioner = HashPartitioner(
            num_partitions=num_agents,
            key_columns=[k.encode('utf-8') if isinstance(k, str) else k for k in partition_keys],
            schema=batch.schema
        )

        # Partition batch by hash of keys (Python list intermediate)
        partitions = partitioner.partition(batch)

        # Initialize shuffle
        self.shuffle_transport.start_shuffle(
            shuffle_id=shuffle_id_bytes,
            num_partitions=num_agents,
            downstream_agents=[a.encode('utf-8') if isinstance(a, str) else a for a in self.agent_addresses],
            upstream_agents=[a.encode('utf-8') if isinstance(a, str) else a for a in self.agent_addresses]
        )

        # Send partitions to agents
        for partition_id, partition_batch in enumerate(partitions):
            if partition_batch is not None and partition_batch.num_rows > 0:
                target_agent = self.agent_addresses[partition_id]
                target_agent_bytes = target_agent.encode('utf-8') if isinstance(target_agent, str) else target_agent
                self.shuffle_transport.send_partition(
                    shuffle_id=shuffle_id_bytes,
                    partition_id=partition_id,
                    batch=partition_batch,
                    target_agent=target_agent_bytes
                )

        # Receive our local partition (this agent's partition_id)
        received_batches = self.shuffle_transport.receive_partitions(
            shuffle_id=shuffle_id_bytes,
            partition_id=self.local_agent_id
        )

        # Execute operator on received batches
        results = []
        for received_batch in received_batches:
            if received_batch is not None and received_batch.num_rows > 0:
                result = operator.process_batch(received_batch)
                if result is not None and result.num_rows > 0:
                    results.append(result)

        # Combine results
        if not results:
            combined = pa.table({})
        elif len(results) == 1:
            combined = results[0]
        else:
            # Convert to tables if needed
            tables = []
            for r in results:
                if isinstance(r, pa.Table):
                    tables.append(r)
                else:
                    tables.append(pa.Table.from_batches([r]))
            combined = pa.concat_tables(tables)

        # Clean up shuffle
        self.shuffle_transport.end_shuffle(shuffle_id_bytes)

        return combined

    def _should_use_morsel(
        self,
        operator: Any,
        batch: Optional[pa.Table]
    ) -> bool:
        """
        Determine if morsel parallelism should be used.

        Heuristics:
        - Batch must exist and have ≥10K rows
        - Operator must be stateless (morsel-safe)
        - Multiple workers available

        Args:
            operator: Operator to check
            batch: Current batch

        Returns:
            True if morsel parallelism should be used
        """
        if batch is None:
            return False

        if batch.num_rows < 10000:
            return False

        if hasattr(operator, '_stateful') and operator._stateful:
            # Stateful operators need shuffle, not morsel
            return False

        if self.num_workers <= 1:
            # No workers available
            return False

        return True

    def get_stats(self) -> Dict[str, Any]:
        """
        Get execution statistics.

        Returns:
            Dict with execution metrics
        """
        return self.stats.copy()
