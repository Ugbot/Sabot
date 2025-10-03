"""Integration tests for CLI with fraud detection demo."""
import subprocess
import time
import signal
import pytest
from pathlib import Path


@pytest.fixture
def project_root():
    """Get project root directory."""
    return Path(__file__).parent.parent.parent


def test_cli_loads_fraud_app(project_root):
    """Test CLI can load fraud detection app."""
    cmd = [
        "sabot",
        "-A", "examples.fraud_app:app",
        "worker",
        "--loglevel", "INFO"
    ]

    # Start process
    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    try:
        # Give it 5 seconds to start
        time.sleep(5)

        # Check it's still running
        assert process.poll() is None, "Worker process died unexpectedly"

        # Check stdout for expected messages
        # Note: This is basic, may need adjustment based on actual output

    finally:
        # Clean shutdown
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def test_cli_invalid_app_spec(project_root):
    """Test CLI handles invalid app spec gracefully."""
    cmd = [
        "sabot",
        "-A", "invalid_module:app",
        "worker"
    ]

    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Should exit with error
    returncode = process.wait(timeout=5)
    assert returncode != 0, "Should exit with error code"

    stderr = process.stderr.read()
    assert "Cannot import module" in stderr or "Error" in stderr


def test_cli_broker_override(project_root):
    """Test CLI can override broker URL."""
    cmd = [
        "sabot",
        "-A", "examples.fraud_app:app",
        "worker",
        "--broker", "kafka://test-broker:9092",
        "--loglevel", "DEBUG"
    ]

    process = subprocess.Popen(
        cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    try:
        time.sleep(3)

        # Check process started
        assert process.poll() is None

        # Would need to check logs for broker override
        # This is a basic smoke test

    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

