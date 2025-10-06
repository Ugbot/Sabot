#!/usr/bin/env python3
"""
Test script to verify RAFT integration works conceptually.
This script tests the RAFT functionality without requiring full package installation.
"""

import sys
import os

# Add the sabot package to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

def test_imports():
    """Test that all imports work"""
    try:
        from sabot.app import App, RAFTStream
        from sabot.metrics import get_metrics
        print("✓ Core imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_app_creation():
    """Test creating apps with different configurations"""
    try:
        from sabot.app import App

        # Test basic app creation (no broker)
        app1 = App("test-app")
        assert app1.id == "test-app"
        assert app1.broker is None
        print("✓ Basic app creation successful")

        # Test app with broker
        app2 = App("test-app-kafka", broker="kafka://localhost:9092")
        assert app2.broker == "kafka://localhost:9092"
        print("✓ App with broker creation successful")

        # Test app with GPU enabled (will fall back gracefully if RAFT not available)
        app3 = App("test-app-gpu", enable_gpu=True)
        assert app3.enable_gpu is True
        print("✓ App with GPU configuration successful")

        return True
    except Exception as e:
        print(f"✗ App creation failed: {e}")
        return False

def test_raft_stream_creation():
    """Test creating RAFT streams"""
    try:
        from sabot.app import App, RAFTStream

        app = App("test-app", enable_gpu=False)  # CPU mode for testing

        raft_stream = RAFTStream("test-raft", app)
        assert raft_stream.name == "test-raft"
        assert raft_stream.app == app
        assert raft_stream._gpu_enabled is False  # Should be CPU mode
        print("✓ RAFT stream creation successful")

        return True
    except Exception as e:
        print(f"✗ RAFT stream creation failed: {e}")
        return False

def test_raft_processors():
    """Test RAFT processor creation"""
    try:
        from sabot.app import App, RAFTStream
        import pandas as pd
        import numpy as np

        app = App("test-app", enable_gpu=False)
        raft_stream = RAFTStream("test-raft", app)

        # Test k-means processor creation
        kmeans_processor = raft_stream.kmeans_cluster(n_clusters=3, max_iter=10)
        assert callable(kmeans_processor)
        print("✓ K-means processor creation successful")

        # Test nearest neighbors processor creation
        nn_processor = raft_stream.nearest_neighbors(k=5)
        assert callable(nn_processor)
        print("✓ Nearest neighbors processor creation successful")

        # Test custom GPU transform processor creation
        def custom_transform(df):
            return df * 2

        gpu_processor = raft_stream.gpu_transform(custom_transform)
        assert callable(gpu_processor)
        print("✓ Custom GPU transform processor creation successful")

        return True
    except Exception as e:
        print(f"✗ RAFT processor creation failed: {e}")
        return False

def test_metrics_integration():
    """Test FastRedis metrics integration"""
    try:
        from sabot.metrics import get_metrics

        # Test metrics creation without Redis client
        metrics = get_metrics()
        assert metrics is not None
        print("✓ Metrics creation successful")

        # Test metrics recording
        metrics.record_message_processed("test_agent", "test_topic", 0.1)
        metrics.record_agent_error("test_agent", "test_error")
        metrics.set_active_agents(5)
        print("✓ Metrics recording successful")

        return True
    except Exception as e:
        print(f"✗ Metrics integration failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Testing Sabot RAFT Integration")
    print("=" * 40)

    tests = [
        test_imports,
        test_app_creation,
        test_raft_stream_creation,
        test_raft_processors,
        test_metrics_integration,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All RAFT integration tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
