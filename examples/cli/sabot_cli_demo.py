#!/usr/bin/env python3
"""
Sabot CLI Demo - Faust-style Agent Management

This demo shows Sabot's CLI capabilities for managing agents in a cluster,
similar to how Faust manages workers and agents. It demonstrates:

- Agent lifecycle management (start, stop, restart)
- Supervision with auto-restart
- Real-time monitoring and health checks
- Scaling and concurrency control
- Cluster-wide status and coordination

This is a standalone demo that simulates the CLI functionality.
"""

import asyncio
import time
import random
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys

# Mock Rich imports for demo (in case not available)
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich.live import Live
    from rich.spinner import Spinner
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.prompt import Prompt, Confirm
    from rich import print as rprint
    console = Console()
    RICH_AVAILABLE = True
except ImportError:
    # Fallback if rich not available
    class MockConsole:
        def print(self, *args, **kwargs):
            if args:
                print(args[0])
            else:
                print()  # Empty line
        def status(self, text): return MockStatus()
        def rule(self, text): print(f"{'='*50}")

    class MockStatus:
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class MockTable:
        def __init__(self, title=None):
            self.title = title
            self.rows = []

        def add_column(self, header, **kwargs):
            pass  # Mock implementation

        def add_row(self, *args):
            print(" | ".join(str(arg) for arg in args))

        def __str__(self):
            return f"Table: {self.title}"

    console = MockConsole()
    Table = MockTable
    Live = None
    RICH_AVAILABLE = False


class AgentStatus(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    RESTARTING = "restarting"


class WorkerStatus(Enum):
    OFFLINE = "offline"
    STARTING = "starting"
    ONLINE = "online"
    DEGRADED = "degraded"
    SHUTTING_DOWN = "shutting_down"


@dataclass
class MockAgent:
    """Mock agent for demonstration."""
    name: str
    topic: str
    status: AgentStatus = AgentStatus.STOPPED
    concurrency: int = 1
    workers: int = 1
    supervised: bool = False
    start_time: Optional[float] = None
    processed_messages: int = 0
    errors: int = 0
    restarts: int = 0

    @property
    def uptime(self) -> str:
        if self.start_time:
            seconds = int(time.time() - self.start_time)
            return f"{seconds}s"
        return "0s"

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "topic": self.topic,
            "status": self.status.value,
            "concurrency": self.concurrency,
            "workers": self.workers,
            "supervised": self.supervised,
            "uptime": self.uptime,
            "processed": self.processed_messages,
            "errors": self.errors
        }


@dataclass
class MockWorker:
    """Mock worker process."""
    worker_id: str
    hostname: str
    status: WorkerStatus = WorkerStatus.OFFLINE
    agents: List[str] = field(default_factory=list)
    start_time: Optional[float] = None
    cpu_percent: float = 0.0
    memory_mb: float = 0.0

    @property
    def uptime(self) -> str:
        if self.start_time:
            seconds = int(time.time() - self.start_time)
            return f"{seconds}s"
        return "0s"


class SabotCLIDemo:
    """Demonstration of Sabot CLI agent management capabilities."""

    def __init__(self):
        self.agents: Dict[str, MockAgent] = {}
        self.workers: Dict[str, MockWorker] = {}
        self.supervision_active = False
        self.monitoring_active = False

        # Initialize with some mock data
        self._init_mock_data()

    def _init_mock_data(self):
        """Initialize mock agents and workers."""
        # Mock agents
        self.agents = {
            "user_processor": MockAgent("user_processor", "user-events", AgentStatus.RUNNING, 2, 1, True),
            "order_validator": MockAgent("order_validator", "orders", AgentStatus.RUNNING, 1, 1, False),
            "fraud_detector": MockAgent("fraud_detector", "transactions", AgentStatus.STOPPED, 3, 2, True),
            "metrics_aggregator": MockAgent("metrics_aggregator", "metrics", AgentStatus.FAILED, 1, 1, False),
            "data_enricher": MockAgent("data_enricher", "raw-data", AgentStatus.RUNNING, 2, 1, True),
        }

        # Set some start times and stats
        current_time = time.time()
        for agent in self.agents.values():
            if agent.status == AgentStatus.RUNNING:
                agent.start_time = current_time - random.randint(300, 3600)  # 5min to 1hr ago
                agent.processed_messages = random.randint(1000, 10000)
                agent.errors = random.randint(0, 50)

        # Mock workers
        self.workers = {
            "worker-01": MockWorker("worker-01", "node-01", WorkerStatus.ONLINE),
            "worker-02": MockWorker("worker-02", "node-02", WorkerStatus.ONLINE),
            "worker-03": MockWorker("worker-03", "node-03", WorkerStatus.DEGRADED),
        }

        # Assign agents to workers
        self.workers["worker-01"].agents = ["user_processor", "order_validator"]
        self.workers["worker-02"].agents = ["fraud_detector", "data_enricher"]
        self.workers["worker-03"].agents = ["metrics_aggregator"]

        # Set worker stats
        for worker in self.workers.values():
            if worker.status == WorkerStatus.ONLINE:
                worker.start_time = current_time - random.randint(1800, 7200)  # 30min to 2hr ago
                worker.cpu_percent = random.uniform(10, 80)
                worker.memory_mb = random.uniform(200, 800)

    # ============================================================================
    # AGENT MANAGEMENT COMMANDS
    # ============================================================================

    def agents_list(self, detailed: bool = False, running_only: bool = False,
                   pattern: str = None, node: str = None):
        """List all agents with filtering options."""
        console.print("[bold blue]Sabot Agents[/bold blue]")

        # Filter agents
        filtered_agents = []
        for agent in self.agents.values():
            if running_only and agent.status != AgentStatus.RUNNING:
                continue
            if pattern and pattern.lower() not in agent.name.lower():
                continue
            if node and node not in [w.worker_id for w in self.workers.values()
                                   if agent.name in w.agents]:
                continue
            filtered_agents.append(agent)

        if not filtered_agents:
            console.print("[dim]No agents found matching criteria[/dim]")
            return

        table = Table()
        table.add_column("Agent Name", style="cyan")
        table.add_column("Topic", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Concurrency", style="yellow")
        table.add_column("Workers", style="blue")

        if detailed:
            table.add_column("Processed", style="purple")
            table.add_column("Errors", style="red")
            table.add_column("Uptime", style="dim cyan")
            table.add_column("Restarts", style="orange")

        for agent in filtered_agents:
            # Status coloring
            status_color = {
                AgentStatus.RUNNING: "[bold green]",
                AgentStatus.STOPPED: "[bold red]",
                AgentStatus.FAILED: "[bold red]",
                AgentStatus.STARTING: "[bold yellow]",
                AgentStatus.RESTARTING: "[bold yellow]"
            }.get(agent.status, "[white]")

            status_display = f"{status_color}{agent.status.value}[/{status_color[1:]}]"

            row = [
                agent.name,
                agent.topic,
                status_display,
                str(agent.concurrency),
                str(agent.workers)
            ]

            if detailed:
                row.extend([
                    str(agent.processed_messages),
                    str(agent.errors),
                    agent.uptime,
                    str(agent.restarts)
                ])

            table.add_row(*row)

        console.print(table)

        running_count = sum(1 for a in filtered_agents if a.status == AgentStatus.RUNNING)
        console.print(f"\n[dim]Showing {len(filtered_agents)} agents ({running_count} running)[/dim]")

    def agents_start(self, names: List[str] = None, concurrency: int = 1,
                    workers: int = 1, supervise: bool = False):
        """Start one or more agents."""
        if names:
            console.print(f"[bold blue]Starting agents: {', '.join(names)}[/bold blue]")
        else:
            # Start all stopped agents
            names = [name for name, agent in self.agents.items()
                    if agent.status == AgentStatus.STOPPED]
            console.print(f"[bold blue]Starting all stopped agents: {', '.join(names)}[/bold blue]")

        if not names:
            console.print("[yellow]No agents to start[/yellow]")
            return

        started_count = 0

        for agent_name in names:
            if agent_name not in self.agents:
                console.print(f"[red]Agent '{agent_name}' not found[/red]")
                continue

            agent = self.agents[agent_name]

            with console.status(f"[bold green]Starting agent '{agent_name}'...") as status:
                time.sleep(0.5)  # Simulate startup time

                agent.status = AgentStatus.STARTING
                time.sleep(0.3)

                agent.status = AgentStatus.RUNNING
                agent.start_time = time.time()
                agent.concurrency = concurrency
                agent.workers = workers
                agent.supervised = supervise

            console.print(f"[bold green]✓[/bold green] Agent '{agent_name}' started")
            console.print(f"   [dim]• Concurrency: {concurrency}[/dim]")
            console.print(f"   [dim]• Workers: {workers}[/dim]")
            console.print(f"   [dim]• Supervision: {'enabled' if supervise else 'disabled'}[/dim]")

            started_count += 1

        console.print(f"\n[bold green]✓[/bold green] Started {started_count} agent(s)")

        if supervise:
            console.print("[dim]Agents are running under supervision - they will auto-restart on failure[/dim]")

    def agents_supervise(self, agents: List[str] = None, restart_delay: float = 5.0,
                         max_restarts: int = 3, health_check_interval: float = 10.0):
        """Start supervision mode for agents."""
        console.print("[bold blue]Starting Faust-style Agent Supervision[/bold blue]")

        if agents:
            console.print(f"[dim]Supervising agents: {', '.join(agents)}[/dim]")
        else:
            agents = [name for name, agent in self.agents.items()
                     if agent.status == AgentStatus.RUNNING]
            console.print(f"[dim]Supervising all running agents: {', '.join(agents)}[/dim]")

        console.print(f"[dim]• Restart delay: {restart_delay}s[/dim]")
        console.print(f"[dim]• Max restarts: {max_restarts}[/dim]")
        console.print(f"[dim]• Health check interval: {health_check_interval}s[/dim]")
        console.print()

        # Create supervision table
        supervise_table = Table(title="Agent Supervision Status")
        supervise_table.add_column("Agent", style="cyan")
        supervise_table.add_column("Status", style="green")
        supervise_table.add_column("Restarts", style="yellow")
        supervise_table.add_column("Uptime", style="blue")
        supervise_table.add_column("Health", style="magenta")

        # Initial status
        agent_supervision = {}
        for agent_name in agents:
            if agent_name in self.agents:
                agent_supervision[agent_name] = {
                    "status": self.agents[agent_name].status.value,
                    "restarts": 0,
                    "uptime": "0s",
                    "health": "healthy"
                }

        def update_supervision_display():
            supervise_table.rows = []  # Clear existing rows

            for agent_name, status in agent_supervision.items():
                status_color = {
                    "running": "[bold green]",
                    "failed": "[bold red]",
                    "restarting": "[bold yellow]"
                }.get(status["status"], "[white]")

                status_display = f"{status_color}{status['status']}[/{status_color[1:]}]"
                health_color = "[bold green]" if status["health"] == "healthy" else "[bold red]"
                health_display = f"{health_color}{status['health']}[/{health_color[1:]}]"

                supervise_table.add_row(
                    agent_name,
                    status_display,
                    str(status["restarts"]),
                    status["uptime"],
                    health_display
                )

        try:
            if Live and RICH_AVAILABLE:
                with Live(console=console, refresh_per_second=1) as live:
                    start_time = time.time()

                    while True:
                        current_time = time.time()

                        # Simulate health checks and failures
                        for agent_name in agents:
                            if agent_name in self.agents:
                                agent = self.agents[agent_name]
                                supervision = agent_supervision[agent_name]

                                # Random failures (very low probability)
                                if (supervision["status"] == "running" and
                                    random.random() < 0.005):  # 0.5% chance

                                    supervision["status"] = "failed"
                                    supervision["health"] = "unhealthy"
                                    console.print(f"[bold red]💥 Agent '{agent_name}' failed![/bold red]")

                                    # Auto-restart if under limit
                                    if supervision["restarts"] < max_restarts:
                                        supervision["status"] = "restarting"
                                        supervision["restarts"] += 1

                                        async def restart_agent():
                                            await asyncio.sleep(restart_delay)
                                            supervision["status"] = "running"
                                            supervision["health"] = "healthy"
                                            agent.start_time = time.time()
                                            console.print(f"[bold green]✓ Agent '{agent_name}' restarted[/bold green]")

                                        asyncio.create_task(restart_agent())

                                # Update uptime for running agents
                                if supervision["status"] == "running":
                                    uptime_seconds = int(current_time - start_time)
                                    supervision["uptime"] = f"{uptime_seconds}s"

                        update_supervision_display()
                        live.update(supervise_table)
                        time.sleep(health_check_interval)
            else:
                # Fallback without Live display
                console.print("[yellow]Rich Live display not available, running basic supervision[/yellow]")
                time.sleep(10)
                console.print("[dim]Supervision completed[/dim]")

        except KeyboardInterrupt:
            console.print("\n[dim]Supervision stopped[/dim]")

            # Final status report
            final_report = Table(title="Supervision Summary")
            final_report.add_column("Agent", style="cyan")
            final_report.add_column("Final Status", style="green")
            final_report.add_column("Total Restarts", style="yellow")
            final_report.add_column("Survival Rate", style="blue")

            for agent_name, status in agent_supervision.items():
                survival_rate = "100%" if status["restarts"] == 0 else ".1f"
                final_report.add_row(agent_name, status["status"], str(status["restarts"]), survival_rate)

            console.print(final_report)

    def agents_monitor(self, agents: List[str] = None, interval: float = 2.0,
                      alerts: bool = False):
        """Monitor agent health and performance."""
        console.print("[bold blue]Agent Monitoring Dashboard[/bold blue]")
        console.print(f"[dim]Update interval: {interval}s[/dim]")

        if agents:
            console.print(f"[dim]Monitoring agents: {', '.join(agents)}[/dim]")
        else:
            agents = [name for name, agent in self.agents.items()
                     if agent.status == AgentStatus.RUNNING]
            console.print(f"[dim]Monitoring running agents: {', '.join(agents)}[/dim]")

        if alerts:
            console.print("[dim]Alert notifications: enabled[/dim]")
        console.print()

        # Create monitoring table
        monitor_table = Table(title="Agent Performance Monitor")
        monitor_table.add_column("Agent", style="cyan")
        monitor_table.add_column("Status", style="green")
        monitor_table.add_column("CPU %", style="red")
        monitor_table.add_column("Memory MB", style="blue")
        monitor_table.add_column("Messages/s", style="yellow")
        monitor_table.add_column("Errors", style="magenta")
        monitor_table.add_column("Uptime", style="dim cyan")

        try:
            if Live:
                with Live(console=console, refresh_per_second=1/interval) as live:
                    start_time = time.time()

                    while True:
                        monitor_table.rows = []  # Clear existing rows

                        for agent_name in agents:
                            if agent_name in self.agents:
                                agent = self.agents[agent_name]

                                # Mock monitoring data
                                status = agent.status.value
                                cpu = ".1f"
                                memory = str(random.randint(50, 500))
                                throughput = str(random.randint(10, 200))
                                errors = str(random.randint(0, 5))
                                uptime = agent.uptime

                                # Color coding
                                status_color = {
                                    "running": "[bold green]",
                                    "failed": "[bold red]",
                                    "warning": "[bold yellow]"
                                }.get(status, "[white]")

                                status_display = f"{status_color}{status}[/{status_color[1:]}]"

                                # Alerts for high CPU or errors
                                if alerts and (float(cpu) > 80 or int(errors) > 3):
                                    console.print(f"[bold red]🚨 ALERT: Agent '{agent_name}' - High CPU ({cpu}%) or errors ({errors})[/bold red]")

                                monitor_table.add_row(agent_name, status_display, cpu, memory, throughput, errors, uptime)

                        live.update(monitor_table)
                        time.sleep(interval)
            else:
                # Fallback without Live display
                console.print("[yellow]Rich Live display not available, running basic monitoring[/yellow]")
                time.sleep(5)
                console.print("[dim]Monitoring completed[/dim]")

        except KeyboardInterrupt:
            console.print("\n[dim]Monitoring stopped[/dim]")

    # ============================================================================
    # WORKER MANAGEMENT COMMANDS
    # ============================================================================

    def workers_list(self, detailed: bool = False):
        """List all worker processes."""
        console.print("[bold blue]Sabot Workers[/bold blue]")

        table = Table()
        table.add_column("Worker ID", style="cyan")
        table.add_column("Hostname", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Agents", style="yellow")

        if detailed:
            table.add_column("CPU %", style="red")
            table.add_column("Memory MB", style="blue")
            table.add_column("Uptime", style="dim cyan")

        for worker in self.workers.values():
            # Status coloring
            status_color = {
                WorkerStatus.ONLINE: "[bold green]",
                WorkerStatus.DEGRADED: "[bold yellow]",
                WorkerStatus.OFFLINE: "[bold red]",
                WorkerStatus.STARTING: "[bold yellow]",
                WorkerStatus.SHUTTING_DOWN: "[bold yellow]"
            }.get(worker.status, "[white]")

            status_display = f"{status_color}{worker.status.value}[/{status_color[1:]}]"

            row = [
                worker.worker_id,
                worker.hostname,
                status_display,
                str(len(worker.agents))
            ]

            if detailed:
                cpu_display = ".1f" if worker.status == WorkerStatus.ONLINE else "0.0"
                mem_display = ".0f" if worker.status == WorkerStatus.ONLINE else "0"
                uptime_display = worker.uptime if worker.status == WorkerStatus.ONLINE else "0s"

                row.extend([cpu_display, mem_display, uptime_display])

            table.add_row(*row)

        console.print(table)

        online_count = sum(1 for w in self.workers.values() if w.status == WorkerStatus.ONLINE)
        total_agents = sum(len(w.agents) for w in self.workers.values())
        console.print(f"\n[dim]Workers: {online_count}/{len(self.workers)} online, {total_agents} total agents[/dim]")

    # ============================================================================
    # CLUSTER STATUS
    # ============================================================================

    def status(self, detailed: bool = False):
        """Show overall cluster status."""
        console.print("[bold blue]Sabot Cluster Status[/bold blue]")

        # Cluster overview
        status_table = Table(title="Cluster Overview")
        status_table.add_column("Component", style="cyan")
        status_table.add_column("Status", style="green")
        status_table.add_column("Count", style="yellow")
        status_table.add_column("Health", style="magenta")

        # Count components
        online_workers = sum(1 for w in self.workers.values() if w.status == WorkerStatus.ONLINE)
        running_agents = sum(1 for a in self.agents.values() if a.status == AgentStatus.RUNNING)
        failed_agents = sum(1 for a in self.agents.values() if a.status == AgentStatus.FAILED)

        status_table.add_row("Workers", "running" if online_workers > 0 else "offline",
                           f"{online_workers}/{len(self.workers)}", "[bold green]healthy[/bold green]")
        status_table.add_row("Agents", "running" if running_agents > 0 else "mixed",
                           f"{running_agents + failed_agents}", "[bold green]healthy[/bold green]")
        status_table.add_row("Topics", "active", "8", "[bold green]healthy[/bold green]")
        status_table.add_row("Streams", "active", "5", "[bold green]healthy[/bold green]")

        console.print(status_table)

        if detailed:
            console.print("\n[bold]Performance Metrics:[/bold]")
            console.print("   Throughput: [bold green]2,450 msg/s[/bold green]")
            console.print("   Latency: [bold green]38.7ms avg[/bold green]")
            console.print("   Error Rate: [bold green]0.02%[/bold green]")

            console.print("\n[bold]Resource Utilization:[/bold]")
            total_cpu = sum(w.cpu_percent for w in self.workers.values() if w.status == WorkerStatus.ONLINE)
            total_memory = sum(w.memory_mb for w in self.workers.values() if w.status == WorkerStatus.ONLINE)
            avg_cpu = total_cpu / max(1, online_workers)
            avg_memory = total_memory / max(1, online_workers)

            console.print(f"   Workers: [bold green]{avg_cpu:.1f}%[/bold green] avg CPU")
            console.print(f"   Workers: [bold green]{avg_memory:.0f}MB[/bold green] avg memory")
            console.print(f"   Agents: [bold green]{running_agents}/{len(self.agents)}[/bold green] running")
        console.print(f"\n[dim]Last updated: {time.strftime('%H:%M:%S')}[/dim]")


def main():
    """Main CLI demo function."""
    import argparse

    parser = argparse.ArgumentParser(description="Sabot CLI Demo - Faust-style Agent Management")
    parser.add_argument("command", choices=[
        "agents", "workers", "status", "supervise", "monitor"
    ], help="Command to run")
    parser.add_argument("subcommand", nargs="?", help="Subcommand")
    parser.add_argument("--detailed", "-d", action="store_true", help="Show detailed information")
    parser.add_argument("--running-only", "-r", action="store_true", help="Show only running items")
    parser.add_argument("--pattern", "-p", help="Filter by name pattern")
    parser.add_argument("--alerts", "-a", action="store_true", help="Enable alerts")
    parser.add_argument("--interval", "-i", type=float, default=2.0, help="Update interval")

    args = parser.parse_args()

    cli = SabotCLIDemo()

    if args.command == "status":
        cli.status(detailed=args.detailed)

    elif args.command == "agents":
        if args.subcommand == "list":
            cli.agents_list(detailed=args.detailed, running_only=args.running_only, pattern=args.pattern)
        elif args.subcommand == "supervise":
            cli.agents_supervise()
        elif args.subcommand == "monitor":
            cli.agents_monitor(alerts=args.alerts, interval=args.interval)
        else:
            print("Available agent commands: list, supervise, monitor")

    elif args.command == "workers":
        if args.subcommand == "list":
            cli.workers_list(detailed=args.detailed)
        else:
            print("Available worker commands: list")

    elif args.command == "supervise":
        cli.agents_supervise()

    elif args.command == "monitor":
        cli.agents_monitor(alerts=args.alerts, interval=args.interval)

    else:
        print("Available commands: status, agents, workers, supervise, monitor")
        print("Try: python sabot_cli_demo.py status --detailed")


if __name__ == "__main__":
    main()
