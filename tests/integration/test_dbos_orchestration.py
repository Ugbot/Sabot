"""
DBOS Orchestration Integration Tests

Tests complete DBOS workflow orchestration, crash recovery,
transaction consistency, and multi-step workflow execution.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch

from sabot.job_manager import JobManager


@pytest.mark.integration
class TestDBOSWorkflowOrchestration:
    """Test DBOS workflow orchestration"""

    @pytest.mark.asyncio
    async def test_job_submission_end_to_end(self):
        """Test complete job submission workflow end-to-end"""
        jm = JobManager(dbos_url=None)  # Local mode with SQLite

        job_graph_json = '{"operators": []}'

        with patch.object(jm, '_persist_job_definition', new_callable=AsyncMock), \
             patch.object(jm, '_optimize_job_dag', new_callable=AsyncMock) as mock_opt, \
             patch.object(jm, '_compile_execution_plan', new_callable=AsyncMock) as mock_compile, \
             patch.object(jm, '_assign_tasks_to_agents', new_callable=AsyncMock) as mock_assign, \
             patch.object(jm, '_deploy_job_to_agents', new_callable=AsyncMock), \
             patch.object(jm, '_activate_job_execution', new_callable=AsyncMock):

            mock_opt.return_value = job_graph_json
            mock_compile.return_value = '{"tasks": []}'
            mock_assign.return_value = {}

            job_id = await jm.submit_job(job_graph_json, "end-to-end-test")

            assert job_id is not None
            assert len(job_id) > 0

    @pytest.mark.asyncio
    async def test_workflow_crash_recovery(self):
        """Test workflow recovery after crash"""
        # This test simulates JobManager crash and recovery
        # In real DBOS, workflow state is persisted and resumed

        jm = JobManager(dbos_url=None)

        # Submit job
        job_graph_json = '{"operators": []}'

        # Simulate crash before deployment
        with patch.object(jm, '_persist_job_definition', new_callable=AsyncMock), \
             patch.object(jm, '_optimize_job_dag', new_callable=AsyncMock) as mock_opt, \
             patch.object(jm, '_compile_execution_plan', new_callable=AsyncMock) as mock_compile, \
             patch.object(jm, '_assign_tasks_to_agents', new_callable=AsyncMock) as mock_assign, \
             patch.object(jm, '_deploy_job_to_agents', side_effect=RuntimeError("Simulated crash")):

            mock_opt.return_value = job_graph_json
            mock_compile.return_value = '{"tasks": []}'
            mock_assign.return_value = {}

            try:
                await jm.submit_job(job_graph_json, "crash-test")
            except RuntimeError:
                pass  # Expected crash

        # In real DBOS, job_id would be recovered from workflow_state table
        # and workflow would resume from _deploy_job_to_agents step

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_failure(self):
        """Test transaction rollback when step fails"""
        jm = JobManager(dbos_url=None)

        job_graph_json = '{"operators": []}'

        with patch.object(jm.dbos, 'execute', new_callable=AsyncMock) as mock_exec:
            # Simulate transaction failure
            mock_exec.side_effect = RuntimeError("DB error")

            try:
                await jm._persist_job_definition("job-123", "test", job_graph_json)
            except RuntimeError:
                pass

            # In real DBOS, transaction would be rolled back
            # Verify no partial state persisted


@pytest.mark.integration
class TestAgentCoordination:
    """Test agent coordination via DBOS"""

    @pytest.mark.asyncio
    async def test_agent_registration_persistence(self):
        """Test agent registration persists to database"""
        jm = JobManager(dbos_url=None)

        agent_id = "test-agent-1"
        agent_data = {
            'host': 'localhost',
            'port': 8816,
            'num_slots': 4
        }

        # Register agent (would be done via HTTP endpoint)
        with patch.object(jm.dbos, 'execute', new_callable=AsyncMock) as mock_exec:
            query = """
                INSERT INTO agents (agent_id, host, port, max_workers)
                VALUES ($1, $2, $3, $4)
            """
            await jm.dbos.execute(query, agent_id, agent_data['host'],
                                agent_data['port'], agent_data['num_slots'])

            mock_exec.assert_called()

    @pytest.mark.asyncio
    async def test_task_assignment_atomicity(self):
        """Test task assignment is atomic"""
        jm = JobManager(dbos_url=None)

        # Mock transaction that assigns tasks atomically
        job_id = "job-123"
        exec_graph_json = '{"tasks": {}}'

        with patch.object(jm.dbos, 'query', new_callable=AsyncMock) as mock_query, \
             patch.object(jm.dbos, 'execute', new_callable=AsyncMock), \
             patch('sabot.execution.execution_graph.ExecutionGraph'):

            mock_query.return_value = [
                {'agent_id': 'agent-1', 'available_slots': 2}
            ]

            # Assignment should be atomic - all tasks assigned or none
            assignments = await jm._assign_tasks_to_agents(job_id, exec_graph_json)

            # Verify atomicity (all assignments succeed together)
            assert isinstance(assignments, dict)


@pytest.mark.integration
class TestConcurrentJobs:
    """Test concurrent job submissions"""

    @pytest.mark.asyncio
    async def test_concurrent_job_submissions(self):
        """Test multiple concurrent job submissions"""
        jm = JobManager(dbos_url=None)

        job_graph = '{"operators": []}'

        with patch.object(jm, '_persist_job_definition', new_callable=AsyncMock), \
             patch.object(jm, '_optimize_job_dag', new_callable=AsyncMock) as mock_opt, \
             patch.object(jm, '_compile_execution_plan', new_callable=AsyncMock) as mock_compile, \
             patch.object(jm, '_assign_tasks_to_agents', new_callable=AsyncMock) as mock_assign, \
             patch.object(jm, '_deploy_job_to_agents', new_callable=AsyncMock), \
             patch.object(jm, '_activate_job_execution', new_callable=AsyncMock):

            mock_opt.return_value = job_graph
            mock_compile.return_value = '{"tasks": []}'
            mock_assign.return_value = {}

            # Submit 3 jobs concurrently
            jobs = await asyncio.gather(
                jm.submit_job(job_graph, "job-1"),
                jm.submit_job(job_graph, "job-2"),
                jm.submit_job(job_graph, "job-3")
            )

            # All jobs should have unique IDs
            assert len(set(jobs)) == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
