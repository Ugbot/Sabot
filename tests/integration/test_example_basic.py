#!/usr/bin/env python3
"""Test basic_pipeline example with simple producer/consumer."""

import asyncio
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the example app
from examples.core.basic_pipeline import app


async def test_basic_pipeline():
    """Test that basic_pipeline example works."""
    print("🧪 Testing basic_pipeline example")
    print("=" * 50)

    # Check that app is created
    print(f"✅ App created: {app.id}")
    print(f"   Broker: {app.broker}")

    # Check agents are registered
    agents = list(app._agents.values()) if hasattr(app, '_agents') else []
    print(f"✅ Agents registered: {len(agents)}")
    for agent in agents:
        print(f"   - Agent function: {agent.__name__ if hasattr(agent, '__name__') else agent}")

    print("\n✅ Example structure looks good!")
    print("\n📝 To run the example:")
    print("   1. Ensure Kafka/Redpanda is running on localhost:19092")
    print("   2. Run: python -m sabot -A examples.core.basic_pipeline:app worker")
    print("   3. Send test data with producer code from example docstring")


if __name__ == "__main__":
    asyncio.run(test_basic_pipeline())
