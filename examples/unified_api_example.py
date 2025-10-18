#!/usr/bin/env python3
"""
Unified Sabot API Example

Demonstrates the new unified architecture where Stream, SQL, and Graph APIs
all work together seamlessly through a single Sabot engine.
"""

from sabot import Sabot, create_engine


def example_local_mode():
    """Example: Local mode (single machine)."""
    print("=" * 60)
    print("Example 1: Local Mode")
    print("=" * 60)
    
    # Create engine in local mode
    engine = Sabot(mode='local')
    
    # Use Stream API
    print("\n1. Stream API:")
    try:
        stream = engine.stream.from_parquet('data.parquet')
        filtered = stream.filter(lambda b: b.column('amount') > 1000)
        print(f"   Created stream pipeline")
    except Exception as e:
        print(f"   Stream example: {e}")
    
    # Use SQL API
    print("\n2. SQL API:")
    try:
        # Register table
        # engine.sql.register_table('events', events_table)
        
        # Execute SQL
        # result = engine.sql("SELECT customer_id, SUM(amount) FROM events GROUP BY customer_id")
        print(f"   SQL API ready (need to register tables)")
    except Exception as e:
        print(f"   SQL example: {e}")
    
    # Use Graph API
    print("\n3. Graph API:")
    try:
        # matches = engine.graph.cypher("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
        print(f"   Graph API ready (need to load graph)")
    except Exception as e:
        print(f"   Graph example: {e}")
    
    # Get engine stats
    print("\n4. Engine Stats:")
    stats = engine.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Cleanup
    engine.shutdown()
    print("\n✅ Local mode example complete")


def example_distributed_mode():
    """Example: Distributed mode (cluster)."""
    print("\n" + "=" * 60)
    print("Example 2: Distributed Mode")
    print("=" * 60)
    
    # Create engine in distributed mode
    engine = create_engine(
        mode='distributed',
        coordinator='localhost:8080',
        state_path='./sabot_state_dist'
    )
    
    print(f"\n✅ Created distributed engine")
    print(f"   Mode: {engine.mode}")
    print(f"   Coordinator: {engine.config.get('coordinator')}")
    
    # Engine has shuffle service in distributed mode
    if engine._shuffle_service:
        print(f"   Shuffle service: Initialized")
    else:
        print(f"   Shuffle service: Not initialized (expected without cluster)")
    
    # Engine has orchestrator in distributed mode
    if engine._orchestrator:
        print(f"   Orchestrator: Initialized")
    else:
        print(f"   Orchestrator: Not initialized (expected without cluster)")
    
    engine.shutdown()
    print("\n✅ Distributed mode example complete")


def example_operator_registry():
    """Example: Operator registry usage."""
    print("\n" + "=" * 60)
    print("Example 3: Operator Registry")
    print("=" * 60)
    
    from sabot.operators import get_global_registry, create_operator
    
    # Get registry
    registry = get_global_registry()
    
    print(f"\n✅ Operator Registry:")
    print(f"   Total operators: {len(registry.list_operators())}")
    
    # List operators by type
    operators = registry.list_operators()
    if operators:
        print(f"\n   Registered operators:")
        for op_name in sorted(operators)[:10]:  # Show first 10
            metadata = registry.get_metadata(op_name)
            if metadata:
                print(f"     - {op_name}: {metadata.get('description', 'N/A')}")
            else:
                print(f"     - {op_name}")
        
        if len(operators) > 10:
            print(f"     ... and {len(operators) - 10} more")
    
    print("\n✅ Operator registry example complete")


def example_state_management():
    """Example: Unified state management."""
    print("\n" + "=" * 60)
    print("Example 4: State Management")
    print("=" * 60)
    
    from sabot.state import create_state_manager, BackendType
    
    # Create state manager (auto-selects backend)
    manager = create_state_manager({'backend': 'auto', 'state_path': './test_state'})
    
    print(f"\n✅ State Manager:")
    print(f"   Backend type: {manager.backend.get_backend_type().value}")
    print(f"   Backend class: {manager.backend.__class__.__name__}")
    
    # Get backend info
    info = manager.get_backend_info()
    print(f"\n   Backend info:")
    for key, value in info.items():
        print(f"     {key}: {value}")
    
    # Cleanup
    manager.close()
    print("\n✅ State management example complete")


def example_composable_apis():
    """Example: APIs composing together."""
    print("\n" + "=" * 60)
    print("Example 5: Composable APIs (Future)")
    print("=" * 60)
    
    print("\n📝 Future capability (after full unification):")
    print("""
    # Stream + SQL together
    engine = Sabot()
    stream = engine.stream.from_kafka('transactions')
    sql_result = engine.sql('''
        SELECT customer_id, SUM(amount) as total
        FROM stream
        GROUP BY customer_id
    ''')
    
    # SQL + Graph together
    enriched = sql_result.graph_enrich('customer_id', engine.graph)
    
    # All APIs return compatible types
    final = enriched.to_kafka('output')
    """)
    
    print("\n✅ This will work once all APIs share unified logical plans")


def main():
    """Run all examples."""
    print("\n" + "="*60)
    print("Sabot Unified Architecture Examples")
    print("="*60)
    
    try:
        example_local_mode()
    except Exception as e:
        print(f"\n❌ Local mode example failed: {e}")
    
    try:
        example_distributed_mode()
    except Exception as e:
        print(f"\n❌ Distributed mode example failed: {e}")
    
    try:
        example_operator_registry()
    except Exception as e:
        print(f"\n❌ Operator registry example failed: {e}")
    
    try:
        example_state_management()
    except Exception as e:
        print(f"\n❌ State management example failed: {e}")
    
    try:
        example_composable_apis()
    except Exception as e:
        print(f"\n❌ Composable APIs example failed: {e}")
    
    print("\n" + "="*60)
    print("Examples Complete")
    print("="*60)
    print("\n📚 Key Takeaways:")
    print("   1. Single Sabot() entry point for all functionality")
    print("   2. Unified operator registry (no duplication)")
    print("   3. Pluggable state backends (MarbleDB primary)")
    print("   4. APIs designed to compose (Stream + SQL + Graph)")
    print("   5. Performance maintained (C++ → Cython → Python)")
    print("\n🚀 Architecture refactoring in progress...")


if __name__ == '__main__':
    main()

