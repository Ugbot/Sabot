"""
Simple Standalone Tests for Sabot DateTime Kernels

Tests datetime kernels without pytest infrastructure to avoid import conflicts.
"""

import pyarrow as pa
from datetime import datetime, timedelta


def test_import():
    """Test that datetime kernels can be imported."""
    try:
        from sabot._cython.arrow import datetime_kernels
        print("✅ Successfully imported datetime_kernels")
        return True
    except ImportError as e:
        print(f"❌ Cannot import datetime_kernels: {e}")
        print("   This is expected if Cython module is not built yet.")
        return False


def test_parse_datetime():
    """Test parsing datetime strings."""
    try:
        from sabot._cython.arrow import datetime_kernels

        dates = pa.array(["2025-11-14", "2024-01-01", "2024-12-25"])
        result = datetime_kernels.parse_datetime(dates, "yyyy-MM-dd")

        assert result.type == pa.timestamp('ns'), f"Expected timestamp type, got {result.type}"
        assert len(result) == 3, f"Expected 3 results, got {len(result)}"

        print("✅ parse_datetime test passed")
        return True
    except Exception as e:
        print(f"❌ parse_datetime test failed: {e}")
        return False


def test_add_days_simd():
    """Test SIMD date arithmetic."""
    try:
        from sabot._cython.arrow import datetime_kernels

        base_dt = datetime(2025, 11, 14, 10, 30, 0)
        timestamps = pa.array([base_dt.timestamp() * 1e9], type=pa.timestamp('ns'))

        # Add 7 days
        result = datetime_kernels.add_days_simd(timestamps, 7)

        assert result.type == pa.timestamp('ns'), f"Expected timestamp type, got {result.type}"
        assert len(result) == 1, f"Expected 1 result, got {len(result)}"

        # Verify result is approximately 7 days later
        expected_ns = (base_dt + timedelta(days=7)).timestamp() * 1e9
        result_ns = result[0].as_py().timestamp() * 1e9
        diff_seconds = abs(result_ns - expected_ns) / 1e9

        assert diff_seconds < 1, f"Result differs by {diff_seconds} seconds"

        print("✅ add_days_simd test passed")
        return True
    except Exception as e:
        print(f"❌ add_days_simd test failed: {e}")
        return False


def test_to_datetime_convenience():
    """Test convenience wrapper function."""
    try:
        from sabot._cython.arrow import datetime_kernels

        # Test auto-detect ISO format
        result = datetime_kernels.to_datetime(["2025-11-14", "2024-01-01"])

        assert result.type == pa.timestamp('ns'), f"Expected timestamp type, got {result.type}"
        assert len(result) == 2, f"Expected 2 results, got {len(result)}"

        print("✅ to_datetime test passed")
        return True
    except Exception as e:
        print(f"❌ to_datetime test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 80)
    print("Sabot DateTime Kernels - Standalone Test Suite")
    print("=" * 80)
    print()

    tests = [
        ("Import Test", test_import),
        ("Parse DateTime", test_parse_datetime),
        ("Add Days SIMD", test_add_days_simd),
        ("To DateTime Convenience", test_to_datetime_convenience),
    ]

    results = []
    for name, test_func in tests:
        print(f"Running: {name}")
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"❌ {name} crashed: {e}")
            results.append((name, False))
        print()

    # Summary
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")

    print()
    print(f"Total: {passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total_count - passed_count} test(s) failed")
        return 1


if __name__ == '__main__':
    exit(main())
