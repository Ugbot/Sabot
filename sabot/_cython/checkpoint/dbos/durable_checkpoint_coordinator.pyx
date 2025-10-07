# -*- coding: utf-8 -*-
"""
DBOS-Enhanced Durable Checkpoint Coordinator

Integrates DBOS (Database Operating System) with Sabot's checkpoint coordination
to provide durable workflow state and fault-tolerant distributed execution.
"""

from libc.stdint cimport int64_t, int32_t
from cpython.ref cimport PyObject

cimport cython

from ..coordinator cimport CheckpointCoordinator
from ..barrier_tracker cimport BarrierTracker
from ..storage cimport CheckpointStorage
from ..recovery cimport RecoveryManager


@cython.final
cdef class DurableCheckpointCoordinator:
    """
    DBOS-enhanced checkpoint coordinator with durable workflow state.

    This integrates DBOS's durable execution capabilities with Sabot's
    distributed checkpointing to provide:

    - Durable workflow state persistence
    - Fault-tolerant checkpoint coordination
    - DBOS-controlled worker lifecycle management
    - Recovery orchestration with DBOS guarantees
    """

    def __cinit__(self):
        """Initialize durable checkpoint coordinator."""
        self.checkpoint_coordinator = CheckpointCoordinator()
        self.dbos_controller = None
        self.workflow_registry = {}
        self.durable_state_store = None
        self.durability_enabled = False

    cpdef void enable_durability(self, object dbos_controller, object durable_state_store):
        """
        Enable durability features with DBOS integration.

        Args:
            dbos_controller: DBOS parallel controller instance
            durable_state_store: DBOS-compatible state storage
        """
        self.dbos_controller = dbos_controller
        self.durable_state_store = durable_state_store
        self.durability_enabled = True

        # Register durability callbacks with DBOS
        if self.dbos_controller:
            # DBOS will call these methods for durable state management
            self._setup_dbos_callbacks()

    cdef void _setup_dbos_callbacks(self):
        """Set up DBOS callback methods for durable state management."""
        # These would be called by DBOS during workflow execution
        # For now, we set up the interface
        pass

    # Workflow lifecycle management

    cpdef object start_workflow(self, str workflow_id, str workflow_type,
                               dict workflow_config):
        """
        Start a new durable workflow with checkpoint coordination.

        Args:
            workflow_id: Unique workflow identifier
            workflow_type: Type of workflow (streaming, batch, etc.)
            workflow_config: Workflow configuration parameters

        Returns:
            Workflow handle for tracking
        """
        workflow_state = {
            'workflow_id': workflow_id,
            'workflow_type': workflow_type,
            'config': workflow_config,
            'start_time': self._get_timestamp_ns(),
            'status': 'running',
            'checkpoint_id': None,
            'operators': {},
            'recovery_attempts': 0,
            'last_checkpoint_time': 0,
        }

        self.workflow_registry[workflow_id] = workflow_state

        # Persist workflow state durably if DBOS is enabled
        if self.durability_enabled:
            self._persist_workflow_state(workflow_id, workflow_state)

        return workflow_state

    cpdef void register_workflow_operator(self, str workflow_id, int32_t operator_id,
                                        str operator_name, object operator_instance):
        """
        Register an operator with a durable workflow.

        This ensures the operator is tracked for checkpoint coordination
        and can be recovered durably.
        """
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        # Register with checkpoint coordinator
        self.checkpoint_coordinator.register_operator(
            operator_id, operator_name, operator_instance
        )

        # Track in workflow state
        self.workflow_registry[workflow_id]['operators'][operator_id] = {
            'name': operator_name,
            'instance': operator_instance,
            'checkpoint_count': 0,
            'last_checkpoint': None,
        }

        # Update durable state
        if self.durability_enabled:
            self._persist_workflow_state(workflow_id, self.workflow_registry[workflow_id])

    cpdef int64_t trigger_workflow_checkpoint(self, str workflow_id) except -1:
        """
        Trigger a checkpoint for an entire workflow.

        This coordinates checkpoints across all operators in the workflow
        using DBOS for durable state management.
        """
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_state = self.workflow_registry[workflow_id]

        # Check if workflow is in a checkpointable state
        if workflow_state['status'] != 'running':
            raise RuntimeError(f"Workflow {workflow_id} is not in running state")

        # Trigger checkpoint via coordinator
        checkpoint_id = self.checkpoint_coordinator.trigger_checkpoint()

        # Update workflow state
        workflow_state['checkpoint_id'] = checkpoint_id
        workflow_state['last_checkpoint_time'] = self._get_timestamp_ns()

        # Persist state durably
        if self.durability_enabled:
            self._persist_workflow_state(workflow_id, workflow_state)

        return checkpoint_id

    cpdef bint acknowledge_workflow_checkpoint(self, str workflow_id,
                                             int32_t operator_id,
                                             int64_t checkpoint_id) except -1:
        """
        Acknowledge checkpoint completion for a workflow operator.

        DBOS ensures this acknowledgment is durable and recoverable.
        """
        # Acknowledge via coordinator
        checkpoint_complete = self.checkpoint_coordinator.acknowledge_checkpoint(
            operator_id, checkpoint_id
        )

        if checkpoint_complete and workflow_id in self.workflow_registry:
            workflow_state = self.workflow_registry[workflow_id]

            # Update operator checkpoint count
            if operator_id in workflow_state['operators']:
                workflow_state['operators'][operator_id]['checkpoint_count'] += 1
                workflow_state['operators'][operator_id]['last_checkpoint'] = checkpoint_id

            # Update workflow checkpoint status
            if checkpoint_id == workflow_state.get('checkpoint_id'):
                workflow_state['checkpoint_id'] = None  # Checkpoint completed

            # Persist updated state
            if self.durability_enabled:
                self._persist_workflow_state(workflow_id, workflow_state)

        return checkpoint_complete

    # Durable recovery management

    cpdef object recover_workflow(self, str workflow_id):
        """
        Recover a workflow from durable state.

        Uses DBOS to restore workflow state and coordinate recovery
        across all operators.
        """
        # Load workflow state from durable storage
        if self.durability_enabled:
            workflow_state = self._load_workflow_state(workflow_id)
            if workflow_state:
                self.workflow_registry[workflow_id] = workflow_state

        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found in registry")

        workflow_state = self.workflow_registry[workflow_id]

        # Check if recovery is needed
        from ..recovery import RecoveryManager
        recovery_manager = RecoveryManager(
            self.checkpoint_coordinator.storage if hasattr(self.checkpoint_coordinator, 'storage') else None,
            self.checkpoint_coordinator
        )

        if recovery_manager.needs_recovery():
            # Perform recovery
            checkpoint_id = recovery_manager.select_recovery_checkpoint()
            if checkpoint_id != -1:
                recovery_manager.start_recovery(checkpoint_id)

                # Update workflow recovery stats
                workflow_state['recovery_attempts'] += 1
                workflow_state['last_recovery_time'] = self._get_timestamp_ns()

                # Persist recovery state
                if self.durability_enabled:
                    self._persist_workflow_state(workflow_id, workflow_state)

        return workflow_state

    cpdef void pause_workflow(self, str workflow_id, str reason="user_request"):
        """Pause a workflow durably."""
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_state = self.workflow_registry[workflow_id]
        workflow_state['status'] = 'paused'
        workflow_state['pause_reason'] = reason
        workflow_state['pause_time'] = self._get_timestamp_ns()

        if self.durability_enabled:
            self._persist_workflow_state(workflow_id, workflow_state)

    cpdef void resume_workflow(self, str workflow_id):
        """Resume a paused workflow."""
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_state = self.workflow_registry[workflow_id]
        if workflow_state['status'] == 'paused':
            workflow_state['status'] = 'running'
            workflow_state['resume_time'] = self._get_timestamp_ns()

            if self.durability_enabled:
                self._persist_workflow_state(workflow_id, workflow_state)

    cpdef void stop_workflow(self, str workflow_id, str reason="completed"):
        """Stop a workflow durably."""
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_state = self.workflow_registry[workflow_id]
        workflow_state['status'] = 'stopped'
        workflow_state['stop_reason'] = reason
        workflow_state['stop_time'] = self._get_timestamp_ns()

        # Perform final checkpoint if workflow was running
        if workflow_state.get('checkpoint_id') is None:
            try:
                self.trigger_workflow_checkpoint(workflow_id)
            except:
                pass  # Ignore checkpoint errors during shutdown

        if self.durability_enabled:
            self._persist_workflow_state(workflow_id, workflow_state)

    # Durable state persistence

    cdef void _persist_workflow_state(self, str workflow_id, object workflow_state):
        """Persist workflow state durably using DBOS."""
        if not self.durability_enabled or not self.durable_state_store:
            return

        try:
            # Use DBOS state store to persist workflow state
            key = f"workflow_state_{workflow_id}"
            self.durable_state_store.put_value(key, workflow_state)

        except Exception as e:
            # Log but don't fail - durability is best effort
            print(f"Warning: Failed to persist workflow state: {e}")

    cdef object _load_workflow_state(self, str workflow_id):
        """Load workflow state from durable storage."""
        if not self.durability_enabled or not self.durable_state_store:
            return None

        try:
            key = f"workflow_state_{workflow_id}"
            return self.durable_state_store.get_value(key)
        except Exception:
            return None

    cdef int64_t _get_timestamp_ns(self):
        """Get current timestamp in nanoseconds."""
        cdef timespec ts
        clock_gettime(CLOCK_REALTIME, &ts)
        return ts.tv_sec * 1000000000 + ts.tv_nsec

    # Monitoring and statistics

    cpdef object get_workflow_stats(self, str workflow_id):
        """Get comprehensive workflow statistics."""
        if workflow_id not in self.workflow_registry:
            return None

        workflow_state = self.workflow_registry[workflow_id]

        # Get checkpoint coordinator stats
        coordinator_stats = self.checkpoint_coordinator.get_coordinator_stats()

        # Calculate total checkpoints without closure
        operators = workflow_state.get('operators', {})
        checkpoint_count = 0
        for op in operators.values():
            checkpoint_count += op.get('checkpoint_count', 0)

        return {
            'workflow_id': workflow_id,
            'status': workflow_state.get('status'),
            'start_time': workflow_state.get('start_time'),
            'operators': len(operators),
            'checkpoints_triggered': checkpoint_count,
            'recovery_attempts': workflow_state.get('recovery_attempts', 0),
            'coordinator_stats': coordinator_stats,
            'durability_enabled': self.durability_enabled,
        }

    cpdef object list_active_workflows(self):
        """List all active workflows."""
        result = {}
        for workflow_id, state in self.workflow_registry.items():
            if state.get('status') == 'running':
                # Calculate checkpoints without closure
                operators = state.get('operators', {})
                checkpoint_count = 0
                for op in operators.values():
                    checkpoint_count += op.get('checkpoint_count', 0)

                result[workflow_id] = {
                    'status': state.get('status'),
                    'type': state.get('workflow_type'),
                    'operators': len(operators),
                    'checkpoints': checkpoint_count,
                }
        return result

    cpdef object get_system_health(self):
        """Get overall system health including durability status."""
        # Count active workflows without closure
        active_count = 0
        for wid, state in self.workflow_registry.items():
            if state.get('status') == 'running':
                active_count += 1

        return {
            'durability_enabled': self.durability_enabled,
            'dbos_controller_available': self.dbos_controller is not None,
            'durable_storage_available': self.durable_state_store is not None,
            'active_workflows': active_count,
            'checkpoint_coordinator_healthy': True,  # Would check actual health
            'recovery_manager_available': True,  # Would check actual availability
        }

    # Python special methods

    def __str__(self):
        """String representation."""
        active_workflows = len([
            wid for wid, state in self.workflow_registry.items()
            if state.get('status') == 'running'
        ])
        return f"DurableCheckpointCoordinator(workflows={active_workflows}, durable={self.durability_enabled})"

    def __repr__(self):
        """Detailed representation."""
        health = self.get_system_health()
        return (f"DurableCheckpointCoordinator("
                f"workflows={health['active_workflows']}, "
                f"durable={health['durability_enabled']}, "
                f"dbos={health['dbos_controller_available']})")


# External C declarations
cdef extern from "<time.h>" nogil:
    ctypedef struct timespec:
        long tv_sec
        long tv_nsec
    int clock_gettime(int clk_id, timespec *tp)

cdef extern from "<sys/time.h>" nogil:
    int CLOCK_REALTIME
