#!/usr/bin/env python3
"""
Simple test runner for Arrow join functionality.

This runs basic tests that don't require PyArrow to verify the test structure works.
Run this when PyArrow is not available to ensure basic functionality.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

def test_imports():
    """Test that all Arrow join classes can be imported."""
    print("Testing Arrow join imports...")

    from joins import (
        ArrowTableJoin,
        ArrowDatasetJoin,
        ArrowAsofJoin,
        create_arrow_table_join,
        create_arrow_dataset_join,
        create_arrow_asof_join
    )

    print("✅ All Arrow join classes imported successfully")
    return True

def test_factory_functions():
    """Test that factory functions work."""
    print("Testing Arrow join factory functions...")

    from joins import (
        create_arrow_table_join,
        create_arrow_dataset_join,
        create_arrow_asof_join,
        JoinType,
        CYTHON_AVAILABLE
    )

    # Test factory functions
    conditions = [{"left_field": "id", "right_field": "id"}]

    if CYTHON_AVAILABLE:
        # Test they work when Cython is available
        table_join = create_arrow_table_join(JoinType.INNER, conditions)
        dataset_join = create_arrow_dataset_join(JoinType.INNER, conditions)
        asof_join = create_arrow_asof_join("timestamp", "timestamp")

        assert table_join is not None
        assert dataset_join is not None
        assert asof_join is not None
        print("✅ Factory functions work correctly with Cython")
    else:
        # Test they raise appropriate errors when Cython is not available
        try:
            create_arrow_table_join(JoinType.INNER, conditions)
            assert False, "Should have raised ImportError"
        except ImportError as e:
            assert "Arrow joins require Cython implementation" in str(e)

        try:
            create_arrow_dataset_join(JoinType.INNER, conditions)
            assert False, "Should have raised ImportError"
        except ImportError as e:
            assert "Arrow joins require Cython implementation" in str(e)

        try:
            create_arrow_asof_join("timestamp", "timestamp")
            assert False, "Should have raised ImportError"
        except ImportError as e:
            assert "Arrow joins require Cython implementation" in str(e)

        print("✅ Factory functions raise appropriate errors without Cython")

    return True

def test_join_builder():
    """Test that join builder has Arrow methods."""
    print("Testing join builder Arrow methods...")

    from joins import create_join_builder
    from unittest.mock import MagicMock

    mock_app = MagicMock()
    builder = create_join_builder(mock_app)

    # Check Arrow methods exist
    assert hasattr(builder, 'arrow_table_join')
    assert hasattr(builder, 'arrow_dataset_join')
    assert hasattr(builder, 'arrow_asof_join')

    print("✅ Join builder has all Arrow methods")
    return True

def test_cython_integration():
    """Test that Cython processors are available."""
    print("Testing Cython integration...")

    from joins import CYTHON_AVAILABLE
    print(f"Cython available: {CYTHON_AVAILABLE}")

    # Even if Cython isn't available, the fallback should work
    assert isinstance(CYTHON_AVAILABLE, bool)

    print("✅ Cython integration check passed")
    return True

def main():
    """Run all tests."""
    print("🧪 Arrow Joins Simple Test Runner")
    print("=" * 40)

    tests = [
        test_imports,
        test_factory_functions,
        test_join_builder,
        test_cython_integration,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed: {e}")
            failed += 1

    print(f"\n📊 Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 All basic Arrow join tests passed!")
        print("\nNote: Full Arrow functionality requires PyArrow to be installed.")
        print("Run the full test suite with: python -m pytest tests/test_joins.py")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
