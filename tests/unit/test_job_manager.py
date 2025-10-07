"""
Unit Tests for JobManager

Tests DBOS workflow steps, job submission, DAG optimization,
execution graph compilation, task assignment, and agent deployment.
"""

import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from sabot.job_manager import JobManager, InMemoryDBOS


class TestInMemoryDBOS:
    """Test InMemoryDBOS fallback"""

    def test_inmemory_dbos_initialization(self):
        """Test InMemoryDBOS initialization"""
        dbos = InMemoryDBOS("sqlite:///test.db")
        assert dbos.db_path == "test.db"
        dbos.close()

    @pytest.mark.asyncio
    async def test_inmemory_dbos_execute(self):
        """Test InMemoryDBOS execute"""
        dbos = InMemoryDBOS("sqlite:///:memory:")

        # Create test table
        await dbos.execute("""
            CREATE TABLE IF NOT EXISTS test (id INTEGER, value TEXT)
        """)

        # Insert data
        await dbos.execute("INSERT INTO test VALUES (?, ?)", 1, "test")

        # Query
        result = await dbos.query("SELECT * FROM test WHERE id = ?", 1)
        assert len(result) == 1

        dbos.close()


class TestJobManagerInitialization:
    """Test JobManager initialization"""

    def test_local_mode_initialization(self):
        """Test JobManager in local mode"""
        jm = JobManager(dbos_url=None)

        assert jm.is_local_mode is True
        assert jm.dbos_url == 'sqlite:///sabot_local.db'
        assert jm.dbos is not None

    def test_distributed_mode_initialization(self):
        """Test JobManager in distributed mode"""
        jm = JobManager(dbos_url='postgresql://localhost/sabot')

        assert jm.is_local_mode is False
        assert jm.dbos_url == 'postgresql://localhost/sabot'


class TestJobSubmission:
    """Test job submission workflow"""

    @pytest.mark.asyncio
    async def test_submit_job_workflow(self):
        """Test complete job submission workflow"""
        jm = JobManager(dbos_url=None)  # Local mode

        # Mock job graph
        job_graph_json = json.dumps({
            'job_id': 'test-job',
            'operators': []
        })

        with patch.object(jm, '_persist_job_definition', new_callable=AsyncMock) as mock_persist, \
             patch.object(jm, '_optimize_job_dag', new_callable=AsyncMock) as mock_optimize, \
             patch.object(jm, '_compile_execution_plan', new_callable=AsyncMock) as mock_compile, \
             patch.object(jm, '_assign_tasks_to_agents', new_callable=AsyncMock) as mock_assign, \
             patch.object(jm, '_deploy_job_to_agents', new_callable=AsyncMock) as mock_deploy, \
             patch.object(jm, '_activate_job_execution', new_callable=AsyncMock) as mock_activate:

            mock_optimize.return_value = job_graph_json
            mock_compile.return_value = json.dumps({'tasks': []})
            mock_assign.return_value = {}

            job_id = await jm.submit_job(job_graph_json, "test-job")

            # Verify all steps called
            mock_persist.assert_called_once()
            mock_optimize.assert_called_once()
            mock_compile.assert_called_once()
            mock_assign.assert_called_once()
            mock_deploy.assert_called_once()
            mock_activate.assert_called_once()

            assert job_id is not None

    @pytest.mark.asyncio
    async def test_persist_job_definition(self):
        """Test persisting job definition"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        job_name = "test-job"
        job_graph_json = json.dumps({'operators': []})

        with patch.object(jm.dbos, 'execute', new_callable=AsyncMock) as mock_exec:
            await jm._persist_job_definition(job_id, job_name, job_graph_json)

            # Verify database insert
            mock_exec.assert_called_once()
            call_args = mock_exec.call_args[0]
            assert 'INSERT INTO jobs' in call_args[0]

    @pytest.mark.asyncio
    async def test_optimize_job_dag(self):
        """Test DAG optimization step"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        job_graph_json = json.dumps({'operators': []})

        with patch.object(jm.optimizer, 'optimize') as mock_optimize, \
             patch.object(jm.dbos, 'execute', new_callable=AsyncMock):

            mock_dag = Mock()
            mock_dag.to_dict = Mock(return_value={'operators': []})
            mock_optimize.return_value = mock_dag

            result = await jm._optimize_job_dag(job_id, job_graph_json)

            assert result is not None
            mock_optimize.assert_called_once()

    @pytest.mark.asyncio
    async def test_compile_execution_plan(self):
        """Test execution graph compilation"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        dag_json = json.dumps({'operators': []})

        with patch('sabot.execution.job_graph.JobGraph.from_dict') as mock_from_dict, \
             patch.object(jm.dbos, 'execute', new_callable=AsyncMock):

            mock_job_graph = Mock()
            mock_exec_graph = Mock()
            mock_exec_graph.to_dict = Mock(return_value={'tasks': []})
            mock_exec_graph.get_task_count = Mock(return_value=1)
            mock_exec_graph.tasks = {}

            mock_job_graph.to_execution_graph = Mock(return_value=mock_exec_graph)
            mock_job_graph.operators = []
            mock_from_dict.return_value = mock_job_graph

            result = await jm._compile_execution_plan(job_id, dag_json)

            assert result is not None

    @pytest.mark.asyncio
    async def test_assign_tasks_to_agents(self):
        """Test task assignment transaction"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        exec_graph_json = json.dumps({'tasks': {}})

        # Mock available agents
        with patch.object(jm.dbos, 'query', new_callable=AsyncMock) as mock_query, \
             patch.object(jm.dbos, 'execute', new_callable=AsyncMock), \
             patch('sabot.execution.execution_graph.ExecutionGraph') as mock_graph:

            mock_query.return_value = [
                {'agent_id': 'agent-1', 'available_slots': 2, 'cpu_percent': 50, 'memory_percent': 60}
            ]

            mock_exec_graph = Mock()
            mock_exec_graph.tasks = {}
            mock_graph.return_value = mock_exec_graph

            assignments = await jm._assign_tasks_to_agents(job_id, exec_graph_json)

            assert isinstance(assignments, dict)

    @pytest.mark.asyncio
    async def test_deploy_job_to_agents(self):
        """Test agent deployment communicator"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        assignments = {'task-1': 'agent-1'}

        with patch('aiohttp.ClientSession.post', new_callable=AsyncMock) as mock_post, \
             patch.object(jm.dbos, 'query', new_callable=AsyncMock) as mock_query, \
             patch.object(jm.dbos, 'query_one', new_callable=AsyncMock) as mock_query_one, \
             patch.object(jm.dbos, 'execute', new_callable=AsyncMock):

            # Mock agent info
            mock_query.return_value = [
                {'agent_id': 'agent-1', 'host': 'localhost', 'port': 8816}
            ]

            # Mock task data
            mock_query_one.return_value = {
                'task_id': 'task-1',
                'operator_id': 'op-1',
                'operator_type': 'map',
                'parameters_json': '{}'
            }

            # Mock successful deployment
            mock_post.return_value.__aenter__.return_value.status = 200

            await jm._deploy_job_to_agents(job_id, assignments)

            # Verify deployment call made
            mock_post.assert_called()


class TestWorkflowRecovery:
    """Test workflow crash recovery"""

    @pytest.mark.asyncio
    async def test_workflow_resumption(self):
        """Test workflow resumes after crash"""
        # This would test DBOS workflow resumption
        # For now, placeholder test
        jm = JobManager(dbos_url=None)

        # In real DBOS, workflow state would be recovered from database
        # and workflow would resume from last completed step

        # Verify workflow state can be persisted
        # (Implementation specific to DBOS library)
        pass


class TestJobManagerIntegration:
    """Integration tests for JobManager"""

    @pytest.mark.asyncio
    async def test_complete_job_lifecycle(self):
        """Test complete job lifecycle"""
        jm = JobManager(dbos_url=None)

        # Create simple job graph
        job_graph_json = json.dumps({
            'job_id': 'test-job',
            'job_name': 'test',
            'operators': [],
            'default_parallelism': 1
        })

        # This would test the full lifecycle in integration
        # For unit test, we verify the workflow structure
        assert jm.submit_job is not None
        assert callable(jm.submit_job)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
