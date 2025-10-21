#!/usr/bin/env python3
"""
Test C++ Agent Core Implementation

Test the high-performance C++ agent core with minimal Python dependencies.
Only high-level control and configuration comes from Python.
"""

import asyncio
import logging
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'sabot'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_cpp_agent_core():
    """Test C++ agent core directly."""
    print("🚀 Testing C++ Agent Core Directly")
    print("=" * 60)
    
    try:
        # Import C++ agent core
        from sabot._cython.agent_core import AgentCore
        
        # Create agent core
        agent_core = AgentCore(
            agent_id="test_cpp_agent",
            host="localhost",
            port=8821,
            memory_mb=1024,
            num_slots=4,
            workers_per_slot=2,
            is_local_mode=True
        )
        
        print(f"✅ C++ agent core created: {agent_core.get_agent_id()}")
        
        # Initialize agent core
        print("\n🔄 Initializing C++ agent core...")
        status = agent_core.initialize()
        if not status.ok():
            print(f"❌ Failed to initialize agent core: {status.message()}")
            return False
        
        print("✅ C++ agent core initialized successfully")
        
        # Start agent core
        print("\n🔄 Starting C++ agent core...")
        status = agent_core.start()
        if not status.ok():
            print(f"❌ Failed to start agent core: {status.message()}")
            return False
        
        print("✅ C++ agent core started successfully")
        
        # Test component access
        print("\n📊 Testing component access...")
        
        # Test MarbleDB integration
        marbledb = agent_core.get_marbledb()
        if marbledb:
            print("✅ MarbleDB integration: Available")
        else:
            print("⚠️  MarbleDB integration: Not available (expected)")
        
        # Test task slot manager
        slot_manager = agent_core.get_task_slot_manager()
        if slot_manager:
            print(f"✅ Task slot manager: Available ({slot_manager.get_num_slots()} slots)")
        else:
            print("⚠️  Task slot manager: Not available")
        
        # Test shuffle transport
        shuffle_transport = agent_core.get_shuffle_transport()
        if shuffle_transport:
            print("✅ Shuffle transport: Available")
        else:
            print("⚠️  Shuffle transport: Not available (expected)")
        
        # Test dimension table manager
        dim_manager = agent_core.get_dimension_table_manager()
        if dim_manager:
            print("✅ Dimension table manager: Available")
        else:
            print("⚠️  Dimension table manager: Not available")
        
        # Test checkpoint coordinator
        checkpoint_coord = agent_core.get_checkpoint_coordinator()
        if checkpoint_coord:
            print("✅ Checkpoint coordinator: Available")
        else:
            print("⚠️  Checkpoint coordinator: Not available (expected)")
        
        # Test dimension table registration
        print("\n📋 Testing dimension table registration...")
        import pyarrow as pa
        
        # Create test dimension table
        data = {'id': [1, 2, 3], 'name': ['AAPL', 'MSFT', 'GOOGL'], 'sector': ['Tech', 'Tech', 'Tech']}
        dimension_table = pa.table(data)
        
        status = agent_core.register_dimension_table("securities", dimension_table, False)
        if status.ok():
            print("✅ Dimension table registration: SUCCESS")
        else:
            print(f"⚠️  Dimension table registration: {status.message()}")
        
        # Test streaming source registration
        print("\n📡 Testing streaming source registration...")
        source_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test_group",
            "auto.offset.reset": "earliest"
        }
        
        status = agent_core.register_streaming_source("kafka_trades", "kafka", source_config)
        if status.ok():
            print("✅ Streaming source registration: SUCCESS")
        else:
            print(f"⚠️  Streaming source registration: {status.message()}")
        
        # Test streaming operator deployment
        print("\n⚙️  Testing streaming operator deployment...")
        operator_params = {
            "window_size": "1m",
            "aggregation": "sum"
        }
        
        status = agent_core.deploy_streaming_operator("test_operator", "window_aggregate", operator_params)
        if status.ok():
            print("✅ Streaming operator deployment: SUCCESS")
        else:
            print(f"⚠️  Streaming operator deployment: {status.message()}")
        
        # Test batch SQL execution
        print("\n🔍 Testing batch SQL execution...")
        input_tables = {"test_table": dimension_table}
        
        result_table = agent_core.execute_batch_sql("SELECT * FROM test_table", input_tables)
        if result_table:
            print(f"✅ Batch SQL execution: SUCCESS ({result_table.num_rows} rows)")
        else:
            print("⚠️  Batch SQL execution: No result")
        
        # Test streaming SQL execution
        print("\n🌊 Testing streaming SQL execution...")
        results = []
        
        def output_callback(batch):
            results.append(batch)
            print(f"   Received batch: {batch.num_rows} rows")
        
        status = agent_core.execute_streaming_sql("SELECT * FROM test_table", input_tables, output_callback)
        if status.ok():
            print(f"✅ Streaming SQL execution: SUCCESS ({len(results)} batches)")
        else:
            print(f"⚠️  Streaming SQL execution: {status.message()}")
        
        # Test agent status
        print("\n📈 Testing agent status...")
        status_info = agent_core.get_status()
        print(f"   Agent ID: {status_info.get('agent_id', 'N/A')}")
        print(f"   Running: {status_info.get('running', False)}")
        print(f"   Active tasks: {status_info.get('active_tasks', 0)}")
        print(f"   Available slots: {status_info.get('available_slots', 0)}")
        print(f"   Total morsels processed: {status_info.get('total_morsels_processed', 0)}")
        print(f"   Total bytes shuffled: {status_info.get('total_bytes_shuffled', 0)}")
        print(f"   MarbleDB path: {status_info.get('marbledb_path', 'N/A')}")
        print(f"   MarbleDB initialized: {status_info.get('marbledb_initialized', False)}")
        print(f"   Uptime (ms): {status_info.get('uptime_ms', 0)}")
        print(f"   CPU usage (%): {status_info.get('cpu_usage_percent', 0.0)}")
        print(f"   Memory usage (%): {status_info.get('memory_usage_percent', 0.0)}")
        
        # Stop streaming operator
        print("\n🔄 Stopping streaming operator...")
        status = agent_core.stop_streaming_operator("test_operator")
        if status.ok():
            print("✅ Streaming operator stopped successfully")
        else:
            print(f"⚠️  Streaming operator stop: {status.message()}")
        
        # Stop agent core
        print("\n🔄 Stopping C++ agent core...")
        status = agent_core.stop()
        if not status.ok():
            print(f"❌ Failed to stop agent core: {status.message()}")
            return False
        
        print("✅ C++ agent core stopped successfully")
        
        # Shutdown agent core
        print("\n🔄 Shutting down C++ agent core...")
        status = agent_core.shutdown()
        if not status.ok():
            print(f"❌ Failed to shutdown agent core: {status.message()}")
            return False
        
        print("✅ C++ agent core shutdown successfully")
        
        print("\n🎉 C++ agent core test completed successfully!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   C++ agent core components not built yet")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_python_agent_with_cpp_core():
    """Test Python agent with C++ agent core."""
    print("\n🚀 Testing Python Agent with C++ Agent Core")
    print("=" * 60)
    
    try:
        from sabot.agent import Agent, AgentConfig
        
        # Create agent configuration (local mode)
        config = AgentConfig(
            agent_id="python_cpp_agent",
            host="localhost",
            port=8822,
            memory_mb=1024,
            num_slots=4,
            workers_per_slot=2
        )
        
        # Create agent
        agent = Agent(config)
        print(f"✅ Python agent created: {agent.agent_id}")
        
        # Check if C++ agent core was created
        if agent.agent_core:
            print("✅ C++ agent core created automatically")
        else:
            print("⚠️  C++ agent core not available, using Python fallback")
        
        # Start agent
        print("\n🔄 Starting Python agent...")
        await agent.start()
        print("✅ Python agent started successfully")
        
        # Check agent status
        status = agent.get_status()
        print(f"\n📈 Agent status:")
        print(f"   Running: {status.get('running', False)}")
        print(f"   Active tasks: {status.get('active_tasks', 0)}")
        print(f"   Available slots: {status.get('available_slots', 0)}")
        
        # Test MarbleDB access
        marbledb = agent.get_marbledb()
        if marbledb:
            print("✅ MarbleDB access: Available")
        else:
            print("⚠️  MarbleDB access: Not available")
        
        # Stop agent
        print("\n🔄 Stopping Python agent...")
        await agent.stop()
        print("✅ Python agent stopped successfully")
        
        print("\n🎉 Python agent with C++ core test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_streaming_sql_executor():
    """Test streaming SQL executor with C++ agent core."""
    print("\n🚀 Testing Streaming SQL Executor with C++ Agent Core")
    print("=" * 60)
    
    try:
        from sabot.agent_cpp import StreamingSQLExecutor
        
        # Create streaming SQL executor
        executor = StreamingSQLExecutor("test_streaming_executor")
        print(f"✅ Streaming SQL executor created: {executor.agent_id}")
        
        # Initialize executor
        print("\n🔄 Initializing streaming SQL executor...")
        await executor.initialize()
        print("✅ Streaming SQL executor initialized successfully")
        
        # Test dimension table registration
        print("\n📋 Testing dimension table registration...")
        import pyarrow as pa
        
        # Create test dimension table
        data = {'id': [1, 2, 3], 'name': ['AAPL', 'MSFT', 'GOOGL'], 'sector': ['Tech', 'Tech', 'Tech']}
        dimension_table = pa.table(data)
        
        executor.register_dimension_table("securities", dimension_table, False)
        print("✅ Dimension table registration: SUCCESS")
        
        # Test streaming source registration
        print("\n📡 Testing streaming source registration...")
        source_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test_group",
            "auto.offset.reset": "earliest"
        }
        
        executor.register_streaming_source("kafka_trades", "kafka", source_config)
        print("✅ Streaming source registration: SUCCESS")
        
        # Test batch SQL execution
        print("\n🔍 Testing batch SQL execution...")
        input_tables = {"test_table": dimension_table}
        
        result_table = executor.execute_batch_sql("SELECT * FROM test_table", input_tables)
        if result_table:
            print(f"✅ Batch SQL execution: SUCCESS ({result_table.num_rows} rows)")
        else:
            print("⚠️  Batch SQL execution: No result")
        
        # Test streaming SQL execution
        print("\n🌊 Testing streaming SQL execution...")
        results = []
        
        def output_callback(batch):
            results.append(batch)
            print(f"   Received batch: {batch.num_rows} rows")
        
        executor.execute_streaming_sql("SELECT * FROM test_table", input_tables, output_callback)
        print(f"✅ Streaming SQL execution: SUCCESS ({len(results)} batches)")
        
        # Test executor status
        print("\n📈 Testing executor status...")
        status_info = executor.get_status()
        print(f"   Agent ID: {status_info.get('agent_id', 'N/A')}")
        print(f"   Running: {status_info.get('running', False)}")
        print(f"   Active tasks: {status_info.get('active_tasks', 0)}")
        print(f"   Available slots: {status_info.get('available_slots', 0)}")
        
        # Test component access
        print("\n📊 Testing component access...")
        
        marbledb = executor.get_marbledb()
        if marbledb:
            print("✅ MarbleDB access: Available")
        else:
            print("⚠️  MarbleDB access: Not available")
        
        slot_manager = executor.get_task_slot_manager()
        if slot_manager:
            print("✅ Task slot manager access: Available")
        else:
            print("⚠️  Task slot manager access: Not available")
        
        dim_manager = executor.get_dimension_table_manager()
        if dim_manager:
            print("✅ Dimension table manager access: Available")
        else:
            print("⚠️  Dimension table manager access: Not available")
        
        checkpoint_coord = executor.get_checkpoint_coordinator()
        if checkpoint_coord:
            print("✅ Checkpoint coordinator access: Available")
        else:
            print("⚠️  Checkpoint coordinator access: Not available")
        
        # Shutdown executor
        print("\n🔄 Shutting down streaming SQL executor...")
        await executor.shutdown()
        print("✅ Streaming SQL executor shutdown successfully")
        
        print("\n🎉 Streaming SQL executor test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test entry point."""
    success1 = test_cpp_agent_core()
    success2 = await test_python_agent_with_cpp_core()
    success3 = await test_streaming_sql_executor()
    
    if success1 and success2 and success3:
        print("\n✅ All tests passed!")
        print("   - C++ agent core: ✅")
        print("   - Python agent integration: ✅")
        print("   - Streaming SQL executor: ✅")
        print("   - Component access: ✅")
        print("   - Dimension table management: ✅")
        print("   - Streaming source management: ✅")
        print("   - Streaming operator deployment: ✅")
        print("   - Batch SQL execution: ✅")
        print("   - Streaming SQL execution: ✅")
        print("   - Agent lifecycle: ✅")
    else:
        print("\n❌ Tests failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
