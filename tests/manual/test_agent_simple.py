"""
Simple test to verify agent can start without crashing.
"""

import asyncio
import inspect
from sabot.agent import Agent, AgentConfig


async def main():
    print("Creating agent...")
    config = AgentConfig(
        agent_id="test_agent",
        host="0.0.0.0",
        port=19000,
        num_slots=2
    )

    agent = Agent(config, job_manager=None)

    print(f"Agent shuffle_transport type: {type(agent.shuffle_transport)}")
    print(f"Agent shuffle_transport.start: {agent.shuffle_transport.start}")
    print(f"Agent shuffle_transport.start signature: {inspect.signature(agent.shuffle_transport.start)}")

    print("Starting agent...")
    try:
        await agent.start()
        print(f"Agent started successfully!")
        print(f"Status: {agent.get_status()}")

        # Wait a bit
        await asyncio.sleep(1)

        print("Stopping agent...")
        await agent.stop()
        print(f"Agent stopped. Final status: {agent.get_status()}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
