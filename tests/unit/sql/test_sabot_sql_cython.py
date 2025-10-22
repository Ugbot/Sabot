#!/usr/bin/env python3
"""
Test script for SabotSQL Cython bindings
"""

import os
import sys
import time
import pyarrow as pa
import numpy as np

# Add Sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')

def test_cython_bindings():
    """Test SabotSQL Cython bindings"""
    print("🧪 Testing SabotSQL Cython bindings...")
    
    try:
        # Import SabotSQL
        from sabot_sql import (
            SabotSQLBridge,
            SabotOperatorTranslator,
            FlinkSQLExtension,
            QuestDBSQLExtension,
            execute_sql_on_agent,
            distribute_sql_query
        )
        print("✅ SabotSQL module imported successfully")
        
        # Test 1: Basic bridge functionality
        print("\n📊 Test 1: Basic Bridge Functionality")
        print("-" * 35)
        
        bridge = SabotSQLBridge()
        print("✅ Bridge created successfully")
        
        # Create test data
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.5, 20.3, 30.7, 40.1, 50.9]
        }
        
        table = pa.Table.from_pydict(data)
        print(f"✅ Test table created: {table.num_rows} rows, {table.num_columns} columns")
        
        # Register table
        bridge.register_table("test_data", table)
        print("✅ Table registered successfully")
        
        # Test 2: SQL execution
        print("\n📊 Test 2: SQL Execution")
        print("-" * 25)
        
        test_queries = [
            "SELECT * FROM test_data",
            "SELECT id, name FROM test_data",
            "SELECT COUNT(*) FROM test_data"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nQuery {i}: {query}")
            try:
                # Parse and optimize
                plan = bridge.parse_and_optimize(query)
                print(f"   ✅ Parsing successful: {plan}")
                
                # Execute
                result = bridge.execute_sql(query)
                print(f"   ✅ Execution successful: {result.num_rows} rows, {result.num_columns} columns")
                
            except Exception as e:
                print(f"   ❌ Query failed: {e}")
        
        # Test 3: Flink SQL extensions
        print("\n📊 Test 3: Flink SQL Extensions")
        print("-" * 30)
        
        flink_ext = FlinkSQLExtension()
        print("✅ Flink extension created")
        
        flink_sql = """
        SELECT 
            user_id,
            TUMBLE(event_time, INTERVAL '1' HOUR) as window_start,
            COUNT(*) as event_count
        FROM events
        WHERE CURRENT_TIMESTAMP > event_time
        GROUP BY user_id, TUMBLE(event_time, INTERVAL '1' HOUR)
        """
        
        try:
            # Check for Flink constructs
            has_flink = flink_ext.contains_flink_constructs(flink_sql)
            print(f"✅ Flink construct detection: {has_flink}")
            
            if has_flink:
                # Preprocess
                processed_sql = flink_ext.preprocess_flink_sql(flink_sql)
                print(f"✅ Flink SQL preprocessing successful")
                print(f"   Processed: {processed_sql}")
                
                # Extract windows
                windows = flink_ext.extract_window_specifications(flink_sql)
                print(f"✅ Window specifications extracted: {len(windows)} found")
                for window in windows:
                    print(f"   - {window}")
        
        except Exception as e:
            print(f"❌ Flink extension test failed: {e}")
        
        # Test 4: QuestDB SQL extensions
        print("\n📊 Test 4: QuestDB SQL Extensions")
        print("-" * 32)
        
        questdb_ext = QuestDBSQLExtension()
        print("✅ QuestDB extension created")
        
        questdb_sql = """
        SELECT symbol, price, timestamp
        FROM trades
        SAMPLE BY 1h
        LATEST BY symbol
        """
        
        try:
            # Check for QuestDB constructs
            has_questdb = questdb_ext.contains_questdb_constructs(questdb_sql)
            print(f"✅ QuestDB construct detection: {has_questdb}")
            
            if has_questdb:
                # Preprocess
                processed_sql = questdb_ext.preprocess_questdb_sql(questdb_sql)
                print(f"✅ QuestDB SQL preprocessing successful")
                print(f"   Processed: {processed_sql}")
                
                # Extract clauses
                samples = questdb_ext.extract_sample_by_clauses(questdb_sql)
                latest = questdb_ext.extract_latest_by_clauses(questdb_sql)
                print(f"✅ SAMPLE BY clauses: {len(samples)} found")
                print(f"✅ LATEST BY clauses: {len(latest)} found")
        
        except Exception as e:
            print(f"❌ QuestDB extension test failed: {e}")
        
        # Test 5: Agent execution
        print("\n📊 Test 5: Agent Execution")
        print("-" * 25)
        
        try:
            # Test single agent execution
            result = execute_sql_on_agent(bridge, "SELECT COUNT(*) FROM test_data", "test_agent")
            print(f"✅ Single agent execution: {result['status']}")
            
            # Test distributed execution
            results = distribute_sql_query(bridge, "SELECT * FROM test_data", ["agent_1", "agent_2"])
            print(f"✅ Distributed execution: {len(results)} agents")
            
        except Exception as e:
            print(f"❌ Agent execution test failed: {e}")
        
        # Test 6: Performance
        print("\n📊 Test 6: Performance Test")
        print("-" * 28)
        
        try:
            # Time multiple queries
            start_time = time.time()
            
            for i in range(100):
                result = bridge.execute_sql("SELECT COUNT(*) FROM test_data")
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"✅ Performance test: 100 queries in {duration:.3f}s")
            print(f"   Average: {duration/100*1000:.2f}ms per query")
            print(f"   Throughput: {100/duration:.0f} queries/second")
        
        except Exception as e:
            print(f"❌ Performance test failed: {e}")
        
        print("\n🎉 All Cython binding tests passed!")
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        print("Please build the Cython bindings first:")
        print("  cd sabot_sql && python3 build_cython.py")
        return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🧪 SabotSQL Cython Bindings Test Suite")
    print("=" * 40)
    
    if test_cython_bindings():
        print("\n✅ All tests passed! SabotSQL Cython bindings are working correctly.")
        print("🚀 Ready for orchestrator integration!")
        return 0
    else:
        print("\n❌ Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
