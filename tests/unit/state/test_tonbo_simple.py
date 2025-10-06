#!/usr/bin/env python3
"""
Simple Tonbo State Backend Test

Basic test to verify Tonbo state backend functionality works.
"""

import asyncio
import tempfile
import sys
from pathlib import Path

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent))

async def test_tonbo_state_backend():
    """Test basic Tonbo state backend functionality."""
    print("🧪 Testing Tonbo State Backend...")

    try:
        # Import components
        from sabot._cython.state.tonbo_state import TonboStateBackend
        from sabot.stores.tonbo import TonboBackend
        from sabot.stores.base import StoreBackendConfig

        print("✅ Imports successful")

        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_db"

            # Create Tonbo store backend
            config = StoreBackendConfig(path=str(db_path))
            tonbo_store = TonboBackend(config)

            print("✅ Created Tonbo backend")

            # Initialize store
            await tonbo_store.start()
            print("✅ Initialized Tonbo store")

            # Create state backend
            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            print("✅ Initialized Tonbo state backend")

            # Test basic operations
            state_backend.set_current_namespace("test_ns")
            print("✅ Set namespace")

            # Test ValueState
            value_state = state_backend.get_value_state("test_value")
            value_state.set_current_key("test_ns", "key1")

            # Test basic value operations
            assert value_state.get() is None
            value_state.update("hello world")
            assert value_state.get() == "hello world"
            value_state.clear()
            assert value_state.get() is None

            print("✅ ValueState operations work")

            # Test ListState
            list_state = state_backend.get_list_state("test_list")
            list_state.set_current_key("test_ns", "key2")

            assert list_state.get() == []
            list_state.add("item1")
            list_state.add("item2")
            assert len(list_state.get()) == 2
            list_state.clear()
            assert list_state.get() == []

            print("✅ ListState operations work")

            # Test MapState
            map_state = state_backend.get_map_state("test_map")
            map_state.set_current_key("test_ns", "key3")

            assert map_state.is_empty() == True
            map_state.put("k1", "v1")
            map_state.put("k2", 42)
            assert map_state.contains("k1") == True
            assert map_state.get("k1") == "v1"
            assert map_state.get("k2") == 42
            assert len(map_state.keys()) == 2
            map_state.clear()
            assert map_state.is_empty() == True

            print("✅ MapState operations work")

            # Test ReducingState
            def sum_reducer(a, b):
                return a + b

            reducing_state = state_backend.get_reducing_state("test_reducing", sum_reducer)
            reducing_state.set_current_key("test_ns", "key4")

            assert reducing_state.get() is None
            reducing_state.add(10)
            reducing_state.add(20)
            assert reducing_state.get() == 30
            reducing_state.clear()
            assert reducing_state.get() is None

            print("✅ ReducingState operations work")

            # Get backend stats
            stats = state_backend.get_backend_stats()
            assert stats['initialized'] == True
            assert stats['backend_type'] == 'tonbo_arrow'

            print("✅ Backend stats work")

            # Clean up
            await tonbo_store.stop()
            print("✅ Cleanup successful")

        print("🎉 ALL TONBO STATE BACKEND TESTS PASSED!")
        return True

    except ImportError as e:
        print(f"⚠️  Tonbo components not available: {e}")
        print("   This is expected if Cython extensions aren't compiled")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run the test."""
    success = await test_tonbo_state_backend()
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
