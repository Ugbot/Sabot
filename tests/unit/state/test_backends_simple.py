#!/usr/bin/env python3
"""Simple test script for pluggable backends (minimal dependencies)."""

import asyncio
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

async def test_memory_backend():
    """Test the memory backend."""
    print("Testing Memory Backend...")

    # Import directly to avoid complex dependencies
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from stores.memory import MemoryBackend
    from stores.base import StoreBackendConfig

    config = StoreBackendConfig(backend_type='memory')
    backend = MemoryBackend(config)

    await backend.start()

    # Test basic operations
    await backend.set('key1', 'value1')
    value = await backend.get('key1')
    assert value == 'value1', f'Expected value1, got {value}'
    print("  ✅ Set/Get works")

    assert await backend.exists('key1'), 'Key should exist'
    assert not await backend.exists('nonexistent'), 'Key should not exist'
    print("  ✅ Exists works")

    assert await backend.delete('key1'), 'Delete should succeed'
    assert not await backend.exists('key1'), 'Key should be gone'
    print("  ✅ Delete works")

    # Test batch operations
    items = {'a': 1, 'b': 2, 'c': 3}
    await backend.batch_set(items)

    for key, expected in items.items():
        value = await backend.get(key)
        assert value == expected, f'Batch set failed for {key}'
    print("  ✅ Batch operations work")

    # Test stats
    stats = await backend.get_stats()
    assert stats['backend_type'] == 'memory'
    assert stats['size'] == 3
    print("  ✅ Stats work")

    await backend.stop()
    print("✅ Memory backend tests passed!")


async def test_arrow_files_backend():
    """Test the Arrow files backend."""
    print("\nTesting Arrow Files Backend...")

    from stores.arrow_files import ArrowFileBackend
    from stores.base import StoreBackendConfig
    from pathlib import Path
    import tempfile

    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(
            backend_type='arrow_files',
            path=Path(temp_dir) / 'test_data'
        )
        backend = ArrowFileBackend(config)

        await backend.start()

        # Test basic operations
        test_data = {'name': 'Alice', 'age': 30}
        await backend.set('user1', test_data)

        value = await backend.get('user1')
        assert value == test_data, f'Expected {test_data}, got {value}'
        print("  ✅ Set/Get works")

        # Force save and reload
        await backend._save_data()
        backend._data.clear()
        backend._loaded = False
        await backend._load_data()

        # Data should persist
        value = await backend.get('user1')
        assert value == test_data, f'Persistence failed: {value}'
        print("  ✅ Persistence works")

        await backend.stop()
        print("✅ Arrow files backend tests passed!")


async def test_backend_factory():
    """Test the backend factory functions."""
    print("\nTesting Backend Factory...")

    from stores.base import create_backend_from_string
    from stores import create_backend_auto

    # Test string creation
    backend1 = create_backend_from_string('memory://')
    assert backend1 is not None
    print("  ✅ String factory works")

    # Test auto creation with fallback
    backend2 = create_backend_auto('memory://', use_cython=True)
    assert backend2 is not None
    print("  ✅ Auto factory works")

    # Quick functionality test
    await backend2.start()
    await backend2.set('test', 'value')
    value = await backend2.get('test')
    assert value == 'value'
    await backend2.stop()
    print("  ✅ Auto-created backend functional")


async def main():
    """Run all tests."""
    print("🧪 Testing Sabot Pluggable Backends")
    print("=" * 40)

    try:
        await test_memory_backend()
        await test_arrow_files_backend()
        await test_backend_factory()

        print("\n🎉 All backend tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
