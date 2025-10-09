#!/usr/bin/env python3
"""
Basic test for JobManager local mode functionality.
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent / "sabot"))

async def test_basic_jobmanager():
    """Test basic JobManager functionality in local mode."""

    print("Testing JobManager local mode...")

    # Import after adding to path
    from sabot.job_manager import JobManager

    # Create JobManager (local mode)
    job_manager = JobManager()

    # Create a simple job graph in the expected format
    import uuid
    from datetime import datetime
    job_id = str(uuid.uuid4())
    job_graph = {
        "job_id": job_id,
        "job_name": "test_job",
        "default_parallelism": 4,
        "checkpoint_interval_ms": 60000,
        "state_backend": "tonbo",
        "created_at": datetime.now().isoformat(),
        "operators": {
            f"{job_id}_source": {
                "operator_id": f"{job_id}_source",
                "operator_type": "source",
                "name": "input_source",
                "upstream_ids": [],
                "downstream_ids": [f"{job_id}_map"],
                "parameters": {"topic": "input_topic"},
                "parallelism": 1,
                "max_parallelism": 1,
                "stateful": False,
                "memory_mb": 256,
                "cpu_cores": 0.5
            },
            f"{job_id}_map": {
                "operator_id": f"{job_id}_map",
                "operator_type": "map",
                "name": "map_transform",
                "upstream_ids": [f"{job_id}_source"],
                "downstream_ids": [f"{job_id}_sink"],
                "parameters": {},
                "parallelism": 1,
                "max_parallelism": 4,
                "stateful": False,
                "memory_mb": 256,
                "cpu_cores": 0.5
            },
            f"{job_id}_sink": {
                "operator_id": f"{job_id}_sink",
                "operator_type": "sink",
                "name": "output_sink",
                "upstream_ids": [f"{job_id}_map"],
                "downstream_ids": [],
                "parameters": {"topic": "output_topic"},
                "parallelism": 1,
                "max_parallelism": 1,
                "stateful": False,
                "memory_mb": 256,
                "cpu_cores": 0.5
            }
        }
    }

    job_graph_json = json.dumps(job_graph)

    try:
        # Submit job
        print("Submitting job...")
        job_id = await job_manager.submit_job(job_graph_json, job_name="test_job")
        print(f"Job submitted: {job_id}")

        # Check job status
        print("Checking job status...")
        status = await job_manager.get_job_status(job_id)
        print(f"Job status: {status}")

        # List jobs
        print("Listing jobs...")
        jobs = await job_manager.list_jobs()
        print(f"Found {len(jobs)} jobs")

        print("✅ JobManager local mode test passed!")

    except Exception as e:
        print(f"❌ JobManager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

async def test_dataflow_api():
    """Test basic dataflow API functionality."""

    print("Testing dataflow API...")

    try:
        from sabot import App

        app = App(id="test_dataflow")

        # Test dataflow decorator (should not fail)
        @app.dataflow("test_pipeline")
        def test_pipeline():
            # Can't actually create a real pipeline without proper imports
            # Just return a placeholder
            return "placeholder_pipeline"

        dataflow = test_pipeline
        print(f"Dataflow created: {dataflow.name}")

        print("✅ Dataflow API test passed!")

    except Exception as e:
        print(f"❌ Dataflow API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

async def main():
    """Run all tests."""

    print("🚀 Running basic functionality tests...")
    print()

    # Initialize database
    print("Initializing database...")
    from sabot.init_db import init_local_database

    # Use temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name

    try:
        init_local_database(db_path)
        print("Database initialized successfully")

        # Set environment variable to use our test database
        os.environ['DBOS_SQLITE_PATH'] = db_path

        # Run tests
        success1 = await test_dataflow_api()
        success2 = await test_basic_jobmanager()

        if success1 and success2:
            print()
            print("🎉 All tests passed! Phase 5+6 implementation is working.")
            return 0
        else:
            print()
            print("💥 Some tests failed.")
            return 1

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
