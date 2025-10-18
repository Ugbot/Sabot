#!/usr/bin/env python3
"""
Phase 1 Performance Regression Tests

Validates that architecture refactoring introduces no performance regression.
Tests must pass before proceeding to Phase 2.
"""

import time
import statistics


def test_operator_registry_overhead():
    """Test operator registry lookup performance."""
    print("\n" + "="*60)
    print("Test 1: Operator Registry Overhead")
    print("="*60)
    
    from sabot.operators import get_global_registry
    
    registry = get_global_registry()
    
    # Warm up
    for _ in range(1000):
        registry.has_operator('filter')
    
    # Measure lookup time
    iterations = 100000
    times = []
    
    for _ in range(10):
        start = time.perf_counter()
        for _ in range(iterations):
            registry.has_operator('filter')
        elapsed = time.perf_counter() - start
        times.append((elapsed / iterations) * 1e9)
    
    avg_ns = statistics.mean(times)
    std_ns = statistics.stdev(times)
    
    print(f"  Iterations: {iterations}")
    print(f"  Average: {avg_ns:.0f}ns per lookup")
    print(f"  Std dev: {std_ns:.0f}ns")
    print(f"  Min: {min(times):.0f}ns")
    print(f"  Max: {max(times):.0f}ns")
    
    # Threshold: < 1000ns (1μs)
    threshold_ns = 1000
    if avg_ns < threshold_ns:
        print(f"  ✅ PASS: {avg_ns:.0f}ns < {threshold_ns}ns")
        return True
    else:
        print(f"  ❌ FAIL: {avg_ns:.0f}ns >= {threshold_ns}ns")
        return False


def test_engine_initialization_time():
    """Test Sabot engine initialization performance."""
    print("\n" + "="*60)
    print("Test 2: Engine Initialization Time")
    print("="*60)
    
    from sabot import Sabot
    
    # Measure init time (cold start)
    iterations = 10
    times = []
    
    for _ in range(iterations):
        start = time.perf_counter()
        engine = Sabot(mode='local')
        engine.shutdown()
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1000)  # Convert to ms
    
    avg_ms = statistics.mean(times)
    std_ms = statistics.stdev(times)
    
    print(f"  Iterations: {iterations}")
    print(f"  Average: {avg_ms:.2f}ms")
    print(f"  Std dev: {std_ms:.2f}ms")
    print(f"  Min: {min(times):.2f}ms")
    print(f"  Max: {max(times):.2f}ms")
    
    # Threshold: < 10ms
    threshold_ms = 10
    if avg_ms < threshold_ms:
        print(f"  ✅ PASS: {avg_ms:.2f}ms < {threshold_ms}ms")
        return True
    else:
        print(f"  ❌ FAIL: {avg_ms:.2f}ms >= {threshold_ms}ms")
        return False


def test_api_facade_overhead():
    """Test API facade access performance."""
    print("\n" + "="*60)
    print("Test 3: API Facade Access Overhead")
    print("="*60)
    
    from sabot import Sabot
    
    engine = Sabot(mode='local')
    
    # Warm up
    for _ in range(10000):
        _ = engine.stream
    
    # Measure facade access time
    iterations = 1000000
    times = []
    
    for _ in range(10):
        start = time.perf_counter()
        for _ in range(iterations):
            _ = engine.stream
        elapsed = time.perf_counter() - start
        times.append((elapsed / iterations) * 1e9)
    
    avg_ns = statistics.mean(times)
    std_ns = statistics.stdev(times)
    
    print(f"  Iterations: {iterations}")
    print(f"  Average: {avg_ns:.0f}ns per access")
    print(f"  Std dev: {std_ns:.0f}ns")
    print(f"  Min: {min(times):.0f}ns")
    print(f"  Max: {max(times):.0f}ns")
    
    engine.shutdown()
    
    # Threshold: < 100ns
    threshold_ns = 100
    if avg_ns < threshold_ns:
        print(f"  ✅ PASS: {avg_ns:.0f}ns < {threshold_ns}ns")
        return True
    else:
        print(f"  ❌ FAIL: {avg_ns:.0f}ns >= {threshold_ns}ns")
        return False


def test_state_manager_overhead():
    """Test state manager overhead."""
    print("\n" + "="*60)
    print("Test 4: State Manager Overhead")
    print("="*60)
    
    from sabot.state import StateManager
    
    manager = StateManager({'backend': 'memory'})
    
    # Measure backend access time
    iterations = 100000
    times = []
    
    for _ in range(10):
        start = time.perf_counter()
        for _ in range(iterations):
            _ = manager.backend
        elapsed = time.perf_counter() - start
        times.append((elapsed / iterations) * 1e9)
    
    avg_ns = statistics.mean(times)
    std_ns = statistics.stdev(times)
    
    print(f"  Iterations: {iterations}")
    print(f"  Average: {avg_ns:.0f}ns per access")
    print(f"  Std dev: {std_ns:.0f}ns")
    print(f"  Min: {min(times):.0f}ns")
    print(f"  Max: {max(times):.0f}ns")
    
    manager.close()
    
    # Threshold: < 100ns
    threshold_ns = 100
    if avg_ns < threshold_ns:
        print(f"  ✅ PASS: {avg_ns:.0f}ns < {threshold_ns}ns")
        return True
    else:
        print(f"  ❌ FAIL: {avg_ns:.0f}ns >= {threshold_ns}ns")
        return False


def main():
    """Run all performance validation tests."""
    print("\n" + "="*60)
    print("PHASE 1 PERFORMANCE VALIDATION")
    print("="*60)
    print("\nObjective: Ensure no regression from refactoring")
    print("Threshold: < 5% slowdown acceptable")
    print("="*60)
    
    results = {}
    
    try:
        results['registry'] = test_operator_registry_overhead()
    except Exception as e:
        print(f"  ❌ Registry test error: {e}")
        results['registry'] = False
    
    try:
        results['engine_init'] = test_engine_initialization_time()
    except Exception as e:
        print(f"  ❌ Engine init test error: {e}")
        results['engine_init'] = False
    
    try:
        results['api_facade'] = test_api_facade_overhead()
    except Exception as e:
        print(f"  ❌ API facade test error: {e}")
        results['api_facade'] = False
    
    try:
        results['state_manager'] = test_state_manager_overhead()
    except Exception as e:
        print(f"  ❌ State manager test error: {e}")
        results['state_manager'] = False
    
    # Summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    all_passed = all(results.values())
    passed_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    print(f"\n  Total: {passed_count}/{total_count} tests passed")
    
    if all_passed:
        print("\n  🎯 ALL PERFORMANCE GATES PASSED")
        print("  ✅ No regression detected")
        print("  ✅ Ready for Phase 2")
    else:
        print("\n  ❌ SOME TESTS FAILED")
        print("  ⚠️  Performance regression detected")
        print("  🛑 Fix issues before Phase 2")
    
    return all_passed


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)

