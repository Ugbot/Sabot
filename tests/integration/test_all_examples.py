#!/usr/bin/env python3
"""Test all converted examples to verify they're structured correctly."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def test_example(module_path, example_name):
    """Test a single example."""
    try:
        # Import the example module
        parts = module_path.split('.')
        module = __import__(module_path, fromlist=[parts[-1]])

        # Get the app
        app = getattr(module, 'app', None)
        if app is None:
            print(f"   ❌ {example_name}: No app found")
            return False

        # Check agents
        agents = list(app._agents.values()) if hasattr(app, '_agents') else []

        print(f"   ✅ {example_name}: {len(agents)} agent(s), broker={app.broker}")
        return True

    except Exception as e:
        print(f"   ❌ {example_name}: {type(e).__name__}: {e}")
        return False


async def main():
    """Test all converted examples."""
    print("🧪 Testing All Converted Examples")
    print("=" * 60)

    examples = [
        ("examples.core.basic_pipeline", "Core: basic_pipeline.py"),
        ("examples.streaming.windowed_analytics", "Streaming: windowed_analytics.py"),
        ("examples.streaming.multi_agent_coordination", "Streaming: multi_agent_coordination.py"),
        ("examples.streaming.agent_processing", "Streaming: agent_processing.py"),
        ("examples.storage.pluggable_backends", "Storage: pluggable_backends.py"),
        ("examples.storage.materialized_views", "Storage: materialized_views.py"),
        ("examples.data.arrow_operations", "Data: arrow_operations.py"),
        ("examples.data.joins_demo", "Data: joins_demo.py"),
        ("examples.telemetry_demo", "Root: telemetry_demo.py"),
    ]

    passed = 0
    failed = 0

    for module_path, example_name in examples:
        success = await test_example(module_path, example_name)
        if success:
            passed += 1
        else:
            failed += 1

    print("\n" + "=" * 60)
    print(f"📊 Results: {passed} passed, {failed} failed out of {passed + failed}")

    if failed == 0:
        print("\n✅ All examples have correct structure!")
        print("\n📝 Next steps:")
        print("   1. Start Kafka: Already running on localhost:19092")
        print("   2. Install CLI deps: pip install typer")
        print("   3. Run examples: python -m sabot -A examples.module:app worker")
    else:
        print(f"\n⚠️  {failed} example(s) need fixing")


if __name__ == "__main__":
    asyncio.run(main())
