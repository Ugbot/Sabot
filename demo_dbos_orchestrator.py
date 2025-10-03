#!/usr/bin/env python3
"""
Demo script showing DBOS-based durable orchestration for Sabot CLI operations.

This demonstrates how the CLI now uses DBOS with SQLite for durable execution
of job management operations.
"""

import asyncio
import sys
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent))

from sabot.dbos_orchestrator import init_orchestrator, JobSpec


async def demo_durable_jobs():
    """Demonstrate durable job execution with DBOS."""
    print("🚀 DBOS Orchestrator Demo")
    print("=" * 50)

    # Initialize the orchestrator
    print("📦 Initializing DBOS orchestrator with SQLite...")
    orchestrator = init_orchestrator("./demo_orchestrator.db")
    print(f"✅ Database: {orchestrator.get_database_path()}")

    # Create a job specification
    job_spec = JobSpec(
        job_id="demo-worker-001",
        job_type="worker",
        app_spec="examples.fraud_app:app",
        command="worker",
        args={
            "workers": 1,
            "broker": "kafka://localhost:9092",
            "log_level": "INFO",
        }
    )

    print(f"\n📋 Submitting job: {job_spec.job_id}")
    print(f"   Type: {job_spec.job_type}")
    print(f"   App: {job_spec.app_spec}")

    # Submit the job (this will run the workflow)
    from sabot.dbos_orchestrator import submit_job
    workflow_handle = await submit_job(job_spec)
    print(f"✅ Job submitted successfully: {workflow_handle}")

    # List jobs
    print("\n📊 Listing jobs...")
    jobs = await orchestrator.list_jobs()
    print(f"Found {len(jobs)} jobs")

    # Show database info
    print("\n💾 Database Information:")
    print(f"   Path: {orchestrator.get_database_path()}")
    print(f"   URL: {orchestrator.get_database_url()}")
    print(f"   Size: {Path(orchestrator.get_database_path()).stat().st_size} bytes")

    print("\n🎉 Demo completed successfully!")
    print("\nTo use from CLI:")
    print("  sabot jobs submit worker -A examples.fraud_app:app")
    print("  sabot jobs list")
    print("  sabot jobs status <job-id>")
    print("  sabot jobs db --info")


if __name__ == "__main__":
    asyncio.run(demo_durable_jobs())
