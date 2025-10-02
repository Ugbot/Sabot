#!/usr/bin/env python3
"""
Sabot CLI - Faust-style command line interface

This CLI provides Faust-style commands for starting Sabot workers and agents.
It supports:
- Starting multiple workers that distribute across topics/partitions
- Automatic load balancing
- Worker lifecycle management
- Status monitoring
"""

import asyncio
import argparse
import signal
import sys
import os
import multiprocessing as mp
from typing import List, Optional
import time


class SabotWorker:
    """A Sabot worker process that handles agent execution."""

    def __init__(self, worker_id: int, topics: List[str], concurrency: int = 1):
        self.worker_id = worker_id
        self.topics = topics
        self.concurrency = concurrency
        self.process: Optional[mp.Process] = None
        self.status_queue = mp.Queue()
        self.control_queue = mp.Queue()

    def start(self):
        """Start the worker process."""
        self.process = mp.Process(
            target=self._worker_process,
            args=(self.worker_id, self.topics, self.concurrency,
                  self.status_queue, self.control_queue)
        )
        self.process.start()
        print(f"🚀 Started worker {self.worker_id} (PID: {self.process.pid})")

    def stop(self):
        """Stop the worker process."""
        if self.process and self.process.is_alive():
            self.control_queue.put("STOP")
            self.process.join(timeout=5.0)
            if self.process.is_alive():
                self.process.terminate()
                print(f"⚠️  Force terminated worker {self.worker_id}")
            else:
                print(f"✅ Stopped worker {self.worker_id}")

    def get_status(self):
        """Get worker status."""
        if not self.status_queue.empty():
            try:
                return self.status_queue.get_nowait()
            except:
                pass
        return None

    def _worker_process(self, worker_id: int, topics: List[str], concurrency: int,
                       status_queue: mp.Queue, control_queue: mp.Queue):
        """Worker process main function."""
        print(f"🤖 Worker {worker_id} started - handling topics: {topics}")

        # Simulate agent execution
        agents = []
        for i in range(concurrency):
            agent_id = f"agent_{worker_id}_{i}"
            agents.append({
                'id': agent_id,
                'topics': topics,
                'status': 'running',
                'processed': 0,
                'errors': 0
            })

        # Worker main loop
        while True:
            try:
                # Check for control messages
                if not control_queue.empty():
                    msg = control_queue.get_nowait()
                    if msg == "STOP":
                        print(f"🛑 Worker {worker_id} received stop signal")
                        break

                # Simulate processing work
                for agent in agents:
                    if random.random() < 0.1:  # 10% chance per iteration
                        agent['processed'] += random.randint(1, 10)
                        if random.random() < 0.05:  # 5% error rate
                            agent['errors'] += 1

                # Send status updates
                status = {
                    'worker_id': worker_id,
                    'agents': agents.copy(),
                    'uptime': time.time() - time.time(),  # Would track actual start time
                    'topics': topics
                }
                status_queue.put(status)

                time.sleep(1.0)  # Status update interval

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Worker {worker_id} error: {e}")
                break

        print(f"👋 Worker {worker_id} shutting down")


class SabotCLI:
    """Faust-style CLI for Sabot."""

    def __init__(self):
        self.workers: List[SabotWorker] = []
        self.running = False

    def worker_command(self, args):
        """Start worker command (faust worker style)."""
        print("🎼 Sabot Worker")
        print("=" * 15)

        # Parse topics
        topics = args.topics.split(',') if args.topics else ['default-topic']

        # Determine number of workers
        num_workers = args.workers or mp.cpu_count()

        print(f"🚀 Starting {num_workers} workers")
        print(f"📋 Topics: {topics}")
        print(f"⚡ Concurrency per worker: {args.concurrency}")
        print()

        # Create workers
        for i in range(num_workers):
            # Distribute topics across workers (simple round-robin for demo)
            worker_topics = [topics[j % len(topics)] for j in range(i, i + len(topics), num_workers)]
            if not worker_topics:  # Ensure each worker gets at least one topic
                worker_topics = [topics[i % len(topics)]]

            worker = SabotWorker(
                worker_id=i,
                topics=worker_topics,
                concurrency=args.concurrency
            )
            self.workers.append(worker)

        # Start all workers
        for worker in self.workers:
            worker.start()

        self.running = True

        # Set up signal handlers
        def signal_handler(signum, frame):
            print("\n🛑 Received shutdown signal...")
            self.shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Monitor workers
        try:
            self.monitor_workers()
        except KeyboardInterrupt:
            self.shutdown()

    def monitor_workers(self):
        """Monitor running workers."""
        print("📊 Monitoring workers... (Ctrl+C to stop)")
        print()

        start_time = time.time()

        while self.running:
            time.sleep(2.0)  # Update interval

            # Collect status from all workers
            worker_statuses = []
            for worker in self.workers:
                status = worker.get_status()
                if status:
                    worker_statuses.append(status)

            if worker_statuses:
                self.display_status(worker_statuses, time.time() - start_time)

    def display_status(self, worker_statuses: List[dict], uptime: float):
        """Display cluster status."""
        print(f"\n📈 Cluster Status (uptime: {uptime:.0f}s)")
        print("-" * 40)

        total_agents = 0
        total_processed = 0
        total_errors = 0

        for status in worker_statuses:
            worker_id = status['worker_id']
            agents = status['agents']
            topics = status['topics']

            print(f"Worker {worker_id}: {len(agents)} agents, topics: {topics}")

            for agent in agents:
                agent_processed = agent['processed']
                agent_errors = agent['errors']
                total_agents += 1
                total_processed += agent_processed
                total_errors += agent_errors

                status_emoji = "✅" if agent_errors == 0 else "⚠️"
                print(f"  {status_emoji} {agent['id']}: {agent_processed} processed, {agent_errors} errors")

        print(f"\n📊 Totals: {total_agents} agents, {total_processed} processed, {total_errors} errors")

    def shutdown(self):
        """Shutdown all workers."""
        if not self.running:
            return

        print("\n🔄 Shutting down workers...")
        self.running = False

        # Stop all workers
        for worker in self.workers:
            worker.stop()

        print("✅ All workers stopped. Goodbye!")
        sys.exit(0)

    def agents_command(self, args):
        """Show agents command."""
        print("🤖 Sabot Agents")
        print("=" * 13)

        if not self.workers:
            print("❌ No workers running. Start workers first with 'sabot worker'")
            return

        print("Active Agents:")
        for worker in self.workers:
            status = worker.get_status()
            if status:
                for agent in status['agents']:
                    print(f"  • {agent['id']} (worker {status['worker_id']}) - {agent['status']}")

    def topics_command(self, args):
        """Show topics command."""
        print("📋 Sabot Topics")
        print("=" * 12)

        if not self.workers:
            print("❌ No workers running. Start workers first with 'sabot worker'")
            return

        all_topics = set()
        for worker in self.workers:
            status = worker.get_status()
            if status:
                all_topics.update(status['topics'])

        print("Active Topics:")
        for topic in sorted(all_topics):
            print(f"  • {topic}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sabot - Faust-style streaming engine CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start 4 workers handling 'user-events' topic
  sabot worker --topics user-events --workers 4

  # Start workers with high concurrency
  sabot worker --topics orders,payments --concurrency 10

  # Show running agents
  sabot agents

  # Show active topics
  sabot topics
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Worker command
    worker_parser = subparsers.add_parser(
        'worker',
        help='Start Sabot workers (Faust-style)'
    )
    worker_parser.add_argument(
        '--topics',
        help='Comma-separated list of topics to consume',
        default='default-topic'
    )
    worker_parser.add_argument(
        '--workers',
        type=int,
        help='Number of worker processes (default: CPU count)'
    )
    worker_parser.add_argument(
        '--concurrency',
        type=int,
        default=1,
        help='Number of concurrent agents per worker (default: 1)'
    )

    # Agents command
    agents_parser = subparsers.add_parser(
        'agents',
        help='Show running agents'
    )

    # Topics command
    topics_parser = subparsers.add_parser(
        'topics',
        help='Show active topics'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Initialize CLI
    cli = SabotCLI()

    # Execute command
    if args.command == 'worker':
        cli.worker_command(args)
    elif args.command == 'agents':
        cli.agents_command(args)
    elif args.command == 'topics':
        cli.topics_command(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
