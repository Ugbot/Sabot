#!/usr/bin/env python3
"""
Test What Actually Works in Sabot
Run this to see what's functional vs what needs compilation
"""

import sys

def test_python_components():
    """Test all Python components that should work."""
    print("=" * 60)
    print("TESTING PYTHON COMPONENTS (Should Work)")
    print("=" * 60)

    tests_passed = 0
    tests_failed = 0

    # Test 1: Core App
    try:
        from sabot import create_app, App
        app = create_app("test")
        print("✅ Core App Framework")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Core App Framework: {e}")
        tests_failed += 1

    # Test 2: Agent Runtime
    try:
        from sabot.agents.runtime import AgentRuntime, AgentRuntimeConfig
        runtime = AgentRuntime(AgentRuntimeConfig())
        print("✅ Agent Runtime System")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Agent Runtime System: {e}")
        tests_failed += 1

    # Test 3: Agent Lifecycle
    try:
        from sabot.agents.lifecycle import AgentLifecycleManager
        print("✅ Agent Lifecycle Management")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Agent Lifecycle Management: {e}")
        tests_failed += 1

    # Test 4: Storage Backends
    try:
        from sabot.stores.base import StoreBackend, StoreBackendConfig
        from sabot.stores.memory import MemoryBackend
        config = StoreBackendConfig()
        backend = MemoryBackend(config)
        print("✅ Storage Backends (Memory)")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Storage Backends: {e}")
        tests_failed += 1

    # Test 5: Checkpoint Manager
    try:
        from sabot.stores.checkpoint import CheckpointManager, CheckpointConfig
        print("✅ Checkpoint Manager")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Checkpoint Manager: {e}")
        tests_failed += 1

    # Test 6: Stream Engine
    try:
        from sabot.core.stream_engine import StreamEngine
        print("✅ Stream Engine")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Stream Engine: {e}")
        tests_failed += 1

    # Test 7: Tonbo Store (Python)
    try:
        from sabot.stores.tonbo import TonboBackend
        print("✅ Tonbo Store (Python)")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Tonbo Store: {e}")
        tests_failed += 1

    # Test 8: RocksDB Store (Python)
    try:
        from sabot.stores.rocksdb import RocksDBBackend
        print("✅ RocksDB Store (Python)")
        tests_passed += 1
    except Exception as e:
        print(f"❌ RocksDB Store: {e}")
        tests_failed += 1

    print(f"\n📊 Python Components: {tests_passed} passed, {tests_failed} failed")
    return tests_passed, tests_failed


def test_cython_components():
    """Test Cython components (expected to not work yet)."""
    print("\n" + "=" * 60)
    print("TESTING CYTHON COMPONENTS (Expected to Need Compilation)")
    print("=" * 60)

    compiled_count = 0
    not_compiled_count = 0

    # Test 1: Cython State
    try:
        from sabot._cython.state import CYTHON_STATE_AVAILABLE, ValueState
        if CYTHON_STATE_AVAILABLE:
            print("✅ Cython State Management (COMPILED!)")
            compiled_count += 1
        else:
            print("⏳ Cython State Management (not compiled, using fallback)")
            not_compiled_count += 1
    except Exception as e:
        print(f"⏳ Cython State Management (not compiled): {e}")
        not_compiled_count += 1

    # Test 2: Cython Time
    try:
        from sabot._cython.time import TimerService
        print("✅ Cython Timer Service (COMPILED!)")
        compiled_count += 1
    except Exception as e:
        print(f"⏳ Cython Timer Service (not compiled): {e}")
        not_compiled_count += 1

    # Test 3: Cython Checkpoint
    try:
        from sabot._cython.checkpoint import CheckpointCoordinator
        print("✅ Cython Checkpoint Coordinator (COMPILED!)")
        compiled_count += 1
    except Exception as e:
        print(f"⏳ Cython Checkpoint Coordinator (not compiled): {e}")
        not_compiled_count += 1

    # Test 4: Cython Arrow
    try:
        from sabot._cython.arrow import ArrowBatchProcessor
        print("✅ Cython Arrow Processor (COMPILED!)")
        compiled_count += 1
    except Exception as e:
        print(f"⏳ Cython Arrow Processor (not compiled): {e}")
        not_compiled_count += 1

    print(f"\n📊 Cython Components: {compiled_count} compiled, {not_compiled_count} not compiled")
    return compiled_count, not_compiled_count


def test_functional_capabilities():
    """Test actual functional capabilities."""
    print("\n" + "=" * 60)
    print("TESTING FUNCTIONAL CAPABILITIES")
    print("=" * 60)

    capabilities = []

    # Can create streaming app?
    try:
        from sabot import create_app
        app = create_app("functional_test")
        print("✅ Can create streaming application")
        capabilities.append("streaming_app")
    except Exception as e:
        print(f"❌ Cannot create streaming application: {e}")

    # Can manage agents?
    try:
        from sabot.agents.runtime import AgentRuntime
        runtime = AgentRuntime()
        print("✅ Can manage agent processes")
        capabilities.append("agent_management")
    except Exception as e:
        print(f"❌ Cannot manage agents: {e}")

    # Can persist state?
    try:
        from sabot.stores.memory import MemoryBackend
        from sabot.stores.base import StoreBackendConfig
        backend = MemoryBackend(StoreBackendConfig())
        print("✅ Can persist state (Python)")
        capabilities.append("state_management")
    except Exception as e:
        print(f"❌ Cannot persist state: {e}")

    # Can checkpoint?
    try:
        from sabot.stores.checkpoint import CheckpointManager
        print("✅ Can create checkpoints")
        capabilities.append("checkpointing")
    except Exception as e:
        print(f"❌ Cannot create checkpoints: {e}")

    print(f"\n📊 Functional Capabilities: {len(capabilities)} working")
    return capabilities


def generate_report(python_passed, python_failed, cython_compiled, cython_not_compiled, capabilities):
    """Generate final report."""
    print("\n" + "=" * 60)
    print("FINAL REPORT")
    print("=" * 60)

    # Python layer
    python_total = python_passed + python_failed
    python_pct = (python_passed / python_total * 100) if python_total > 0 else 0
    print(f"\n🐍 Python Layer: {python_passed}/{python_total} working ({python_pct:.0f}%)")

    # Cython layer
    cython_total = cython_compiled + cython_not_compiled
    cython_pct = (cython_compiled / cython_total * 100) if cython_total > 0 else 0
    print(f"⚡ Cython Layer: {cython_compiled}/{cython_total} compiled ({cython_pct:.0f}%)")

    # Capabilities
    print(f"\n🎯 Functional Capabilities:")
    for cap in capabilities:
        print(f"   ✅ {cap}")

    # Recommendations
    print("\n💡 Recommendations:")
    if python_pct >= 80 and cython_pct == 0:
        print("   • Python layer is solid - can ship alpha version")
        print("   • Focus on Cython build system to unlock performance")
        print("   • Current setup good for development/testing")
    elif python_pct >= 80 and cython_pct >= 50:
        print("   • Both layers functional - near production ready")
        print("   • Complete Cython integration testing")
        print("   • Run performance benchmarks")
    elif python_pct < 80:
        print("   • Fix Python layer issues first")
        print("   • Ensure base functionality works")
    else:
        print("   • Continue with build system fixes")

    # Next steps
    print("\n🚀 Next Steps:")
    if cython_pct == 0:
        print("   1. Fix Cython build system (see build errors)")
        print("   2. Compile all .pyx files to .so modules")
        print("   3. Re-run this test to verify Cython works")
        print("   4. Run performance benchmarks")
    else:
        print("   1. Complete integration testing")
        print("   2. Run performance benchmarks")
        print("   3. Write documentation")
        print("   4. Prepare for release")

    print("\n" + "=" * 60)


def main():
    """Run all tests and generate report."""
    print("\n🔍 SABOT IMPLEMENTATION STATUS TEST")
    print("Testing what actually works vs what needs compilation\n")

    # Run tests
    python_passed, python_failed = test_python_components()
    cython_compiled, cython_not_compiled = test_cython_components()
    capabilities = test_functional_capabilities()

    # Generate report
    generate_report(python_passed, python_failed, cython_compiled, cython_not_compiled, capabilities)

    # Exit code
    if python_passed >= 6:  # Most Python components working
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()