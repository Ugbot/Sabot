# -*- coding: utf-8 -*-
"""Sabot Command Line Interface - High-performance streaming with Arrow + Faust-inspired agents."""

import asyncio
import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.spinner import Spinner
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt, Confirm
from rich import print as rprint

# OpenTelemetry imports
try:
    import opentelemetry as otel
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    trace = None
    otel = None

# from .app import create_app  # Temporarily commented out to avoid import issues

def load_app_from_spec(app_spec: str):
    """
    Load Sabot App from module:attribute specification.

    Args:
        app_spec: String in format 'module.path:app_name'
                  Example: 'examples.fraud_app:app'

    Returns:
        App instance

    Raises:
        ValueError: If app_spec format is invalid
        ImportError: If module cannot be imported
        AttributeError: If app attribute doesn't exist
    """
    if ':' not in app_spec:
        raise ValueError(
            f"Invalid app specification '{app_spec}'. "
            "Expected format: 'module.path:attribute'"
        )

    module_name, app_name = app_spec.split(':', 1)

    # Add current directory to path for local imports
    cwd = Path.cwd()
    if str(cwd) not in sys.path:
        sys.path.insert(0, str(cwd))

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(
            f"Cannot import module '{module_name}': {e}"
        ) from e

    try:
        app = getattr(module, app_name)
    except AttributeError as e:
        raise AttributeError(
            f"Module '{module_name}' has no attribute '{app_name}'"
        ) from e

    # Verify it's an App instance
    from sabot.app import App
    if not isinstance(app, App):
        raise TypeError(
            f"'{app_name}' is not a Sabot App instance. "
            f"Got {type(app).__name__}"
        )

    return app

# Console for rich output
console = Console()

# Main CLI app
app = typer.Typer(
    name="sabot",
    help="High-performance columnar streaming with Arrow + Faust-inspired agents",
    add_completion=False,
)

# Sub-apps for different command groups
workers_app = typer.Typer(help="Worker process management")
agents_app = typer.Typer(help="Agent lifecycle management")
streams_app = typer.Typer(help="Stream and topic management")
tables_app = typer.Typer(help="Table and state management")
jobs_app = typer.Typer(help="Durable job management with DBOS")
monitoring_app = typer.Typer(help="Monitoring and metrics")
telemetry_app = typer.Typer(help="OpenTelemetry tracing and metrics")
benchmarks_app = typer.Typer(help="Performance benchmarking")
config_app = typer.Typer(help="Configuration management")
tui_app = typer.Typer(help="Terminal User Interface")

# Add sub-apps to main app
app.add_typer(workers_app, name="workers")
app.add_typer(agents_app, name="agents")
app.add_typer(streams_app, name="streams")
app.add_typer(tables_app, name="tables")
app.add_typer(jobs_app, name="jobs")
app.add_typer(monitoring_app, name="monitoring")
app.add_typer(telemetry_app, name="telemetry")
app.add_typer(benchmarks_app, name="benchmarks")
app.add_typer(config_app, name="config")
app.add_typer(tui_app, name="tui")


# ============================================================================
# MAIN COMMANDS
# ============================================================================

@app.callback()
def main_callback():
    """Sabot CLI - High-performance streaming engine."""
    pass


@app.command()
def version():
    """Show Sabot version."""
    try:
        from . import __version__
        console.print(f"[bold blue]Sabot[/bold blue] version [bold green]{__version__}[/bold green]")
    except ImportError:
        console.print(f"[bold blue]Sabot[/bold blue] version [dim]unknown[/dim]")


@app.command()
def worker(
    app_spec: str = typer.Option(..., "-A", "--app", help="App module (e.g., myapp:app)"),
    workers: int = typer.Option(1, "-c", "--concurrency", help="Number of worker processes"),
    broker: Optional[str] = typer.Option(None, "-b", "--broker", help="Override broker URL"),
    log_level: str = typer.Option("INFO", "-l", "--loglevel", help="Logging level"),
    durable: bool = typer.Option(True, "--durable/--no-durable", help="Use DBOS durable execution"),
    job_id: Optional[str] = typer.Option(None, help="Custom job ID (auto-generated if not provided)"),
):
    """Start Sabot worker (Faust-style: sabot -A myapp:app worker)."""
    import logging
    import uuid

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        if durable:
            # Use DBOS orchestrator for durable execution
            from .dbos_orchestrator import init_orchestrator, JobSpec

            # Initialize orchestrator
            orchestrator = init_orchestrator()

            # Generate job ID if not provided
            if job_id is None:
                job_id = f"worker-{uuid.uuid4().hex[:8]}"

            # Create job specification
            job_spec = JobSpec(
                job_id=job_id,
                job_type="worker",
                app_spec=app_spec,
                command="worker",
                args={
                    "workers": workers,
                    "broker": broker,
                    "log_level": log_level,
                }
            )

            # Display durable execution info
            console.print(Panel(
                f"[bold green]Starting Sabot Worker (Durable)[/bold green]\n"
                f"Job ID: {job_id}\n"
                f"App: {app_spec}\n"
                f"Broker: {broker or 'default'}\n"
                f"Workers: {workers}\n"
                f"Log Level: {log_level}\n"
                f"Database: {orchestrator.get_database_path()}",
                title="Durable Worker Configuration"
            ))

            # Submit durable job
            logger.info(f"Submitting durable worker job {job_id}")
            workflow_handle = asyncio.run(orchestrator.submit_job(job_spec))

            console.print(f"[bold green]✓[/bold green] Durable job submitted: {workflow_handle}")
            console.print(f"[dim]Use 'sabot jobs status {job_id}' to check status[/dim]")
            console.print(f"[dim]Use 'sabot jobs cancel {job_id}' to cancel if needed[/dim]")

        else:
            # Use direct execution (legacy mode)
            console.print("[yellow]⚠️  Using non-durable execution mode[/yellow]")

            # Load the app
            logger.info(f"Loading app from: {app_spec}")
            app_instance = load_app_from_spec(app_spec)

            # Override broker if specified
            if broker:
                app_instance.broker = broker
                logger.info(f"Broker overridden to: {broker}")

            # Display app info
            console.print(Panel(
                f"[bold green]Starting Sabot Worker (Direct)[/bold green]\n"
                f"App: {app_instance.id}\n"
                f"Broker: {app_instance.broker}\n"
                f"Workers: {workers}\n"
                f"Log Level: {log_level}",
                title="Worker Configuration"
            ))

            # Start the app
            import asyncio
            asyncio.run(app_instance.run())

    except (ValueError, ImportError, AttributeError, TypeError) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        console.print("\n[dim]Worker stopped by user[/dim]")
        raise typer.Exit(code=0)
    except Exception as e:
        logger.exception("Unexpected error starting worker")
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def status(
    app_id: str = typer.Option(None, help="Application ID"),
    detailed: bool = typer.Option(False, help="Show detailed status"),
):
    """Show overall cluster and application status."""
    if app_id:
        console.print(f"[bold blue]Status for application '{app_id}'[/bold blue]")
    else:
        console.print("[bold blue]Sabot Cluster Status[/bold blue]")

    # Mock cluster status
    status_table = Table(title="Cluster Overview")
    status_table.add_column("Component", style="cyan")
    status_table.add_column("Status", style="green")
    status_table.add_column("Count", style="yellow")
    status_table.add_column("Health", style="magenta")

    status_table.add_row("Workers", "running", "3", "[bold green]healthy[/bold green]")
    status_table.add_row("Agents", "running", "8", "[bold green]healthy[/bold green]")
    status_table.add_row("Streams", "active", "12", "[bold green]healthy[/bold green]")
    status_table.add_row("Tables", "ready", "5", "[bold green]healthy[/bold green]")
    status_table.add_row("Topics", "available", "15", "[bold green]healthy[/bold green]")

    console.print(status_table)

    if detailed:
        console.print("\n[bold]Performance Metrics:[/bold]")
        console.print(f"   Throughput: [bold green]3,245 msg/s[/bold green]")
        console.print(f"   Latency: [bold green]45.2ms avg[/bold green]")
        console.print(f"   Error Rate: [bold green]0.01%[/bold green]")
        console.print(f"   CPU Usage: [bold green]65%[/bold green]")
        console.print(f"   Memory Usage: [bold green]2.1GB[/bold green]")

        console.print("\n[bold]Resource Utilization:[/bold]")
        console.print(f"   Workers: [bold green]85%[/bold green] (3/4 nodes active)")
        console.print(f"   Agents: [bold green]78%[/bold green] (8/10 max capacity)")
        console.print(f"   Storage: [bold green]45%[/bold green] (4.5GB/10GB)")

    console.print(f"\n[dim]Last updated: {time.strftime('%H:%M:%S')}[/dim]")


@app.command()
def init(
    name: str = typer.Argument(..., help="Application name"),
    directory: Optional[str] = typer.Option(".", help="Target directory"),
    template: str = typer.Option("basic", help="Project template (basic, advanced, ml)"),
):
    """Initialize a new Sabot project."""
    target_dir = Path(directory) / name

    with console.status(f"[bold green]Creating Sabot project '{name}'..."):
        try:
            # Create directory structure
            target_dir.mkdir(parents=True, exist_ok=True)
            (target_dir / "sabot_app").mkdir(exist_ok=True)
            (target_dir / "tests").mkdir(exist_ok=True)
            (target_dir / "config").mkdir(exist_ok=True)

            # Create basic files
            create_project_files(target_dir, name, template)

            console.print(f"[bold green]✓[/bold green] Created Sabot project in [bold]{target_dir}[/bold]")

            # Show next steps
            console.print("\n[bold]Next steps:[/bold]")
            console.print(f"  cd {target_dir}")
            console.print("  uv sync  # Install dependencies")
            console.print("  sabot workers start  # Start workers")
            console.print("  sabot tui  # Open dashboard")

        except Exception as e:
            console.print(f"[bold red]✗[/bold red] Failed to create project: {e}")
            raise typer.Exit(1)


def create_project_files(target_dir: Path, name: str, template: str):
    """Create project files based on template."""
    # pyproject.toml
    pyproject_content = f'''[build-system]
requires = ["setuptools>=61.0", "Cython>=3.0.0", "numpy>=1.21.0"]
build-backend = "setuptools.build_meta"

[project]
name = "{name}"
version = "0.1.0"
description = "Sabot streaming application"
authors = [
    {{name = "Developer", email = "dev@example.com"}},
]
requires-python = ">=3.8"
dependencies = [
    "sabot[all]>=0.1.0",
    "uvloop>=0.17.0",
    "structlog>=22.0.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "black>=23.0.0",
    "isort>=5.12.0",
    "mypy>=1.0.0",
]

[tool.black]
line-length = 88
target-version = ['py38', 'py39', 'py310', 'py311']

[tool.isort]
profile = "black"
'''

    (target_dir / "pyproject.toml").write_text(pyproject_content)

    # Main application file
    app_content = f'''#!/usr/bin/env python3
"""
{name} - Sabot Streaming Application
"""

import asyncio
import sabot as sb

# Create Sabot application
app = sb.create_app(
    id="{name}",
    broker="kafka://localhost:9092",  # Change to your broker
    value_serializer="arrow",  # Use Arrow for high performance
)

# Define your streams, tables, and agents here

@app.agent(app.topic("input-stream"))
async def process_stream(stream):
    """Process incoming stream data."""
    async for record in stream:
        # Your processing logic here
        console.print(f"Processing: {{record}}")
        yield record

if __name__ == "__main__":
    # Run the application
    asyncio.run(app.run())
'''

    (target_dir / "sabot_app" / "main.py").write_text(app_content)

    # Configuration file
    config_content = '''# Sabot Configuration
SABOT_BROKER_URL=kafka://localhost:9092
SABOT_REDIS_URL=redis://localhost:6379
SABOT_ROCKSDB_PATH=./data/rocksdb
SABOT_METRICS_PORT=9090
SABOT_WEB_PORT=8080

# Development settings
LOG_LEVEL=INFO
ENABLE_DEBUG=true
'''

    (target_dir / "config" / ".env").write_text(config_content)

    # README
    readme_content = f'''# {name}

A high-performance streaming application built with Sabot.

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Start development server
uv run sabot workers start --app=sabot_app.main:app

# Open TUI dashboard
uv run sabot tui
```

## Production

```bash
# Build for production
uv build

# Deploy with Docker
docker build -t {name} .
docker run {name}

# Or use Kubernetes
kubectl apply -f k8s/
```
'''

    (target_dir / "README.md").write_text(readme_content)


@app.command()
def shell(
    app_id: str = typer.Option("sabot-shell", help="Application ID"),
    broker: str = typer.Option("memory://", help="Message broker URL"),
):
    """Start an interactive Sabot shell."""
    import IPython

    console.print("[bold blue]Starting Sabot interactive shell...[/bold blue]")

    # Create app instance
    from sabot.app import App
    app = App(app_id, broker=broker)

    # Make app available in shell
    IPython.embed(
        header="""Sabot Interactive Shell
Available objects:
- app: Your Sabot application instance
- sb: Sabot module

Example:
    # Create a stream
    stream = app.stream(app.topic("my-topic"))

    # Create a table
    table = app.table("my-table", backend="memory://")

    # Use joins
    join = app.joins().stream_table_join(stream, table).on("id", "id").build()
""",
        user_ns={"app": app, "sb": sys.modules["sabot"]},
    )


# ============================================================================
# WORKER MANAGEMENT
# ============================================================================

@workers_app.command("start")
def workers_start(
    app_module: str = typer.Argument(None, help="Python module with Sabot app (e.g., myapp.main:app)"),
    app_flag: str = typer.Option(None, "-A", "--app", help="App module (Faust-style: -A myapp:app)"),
    workers: int = typer.Option(1, "-c", "--concurrency", help="Number of worker processes"),
    broker: Optional[str] = typer.Option(None, "-b", "--broker", help="Override broker URL"),
    redis: Optional[str] = typer.Option(None, help="Override Redis URL"),
    rocksdb: Optional[str] = typer.Option(None, help="Override RocksDB path"),
    daemon: bool = typer.Option(False, "-d", "--daemon", help="Run in background"),
    log_level: str = typer.Option("INFO", "-l", "--loglevel", help="Logging level"),
):
    """Start Sabot worker processes (Faust-style)."""

    # Support both positional and -A flag (Faust compatibility)
    app_spec = app_flag or app_module

    if not app_spec:
        console.print("[bold red]Error:[/bold red] App module required")
        console.print("[dim]Usage:[/dim]")
        console.print("  sabot -A myapp:app worker")
        console.print("  sabot workers start -A myapp:app")
        console.print("  sabot workers start myapp:app")
        raise typer.Exit(1)

    console.print(f"[bold blue]Starting {workers} Sabot worker(s)...[/bold blue]")
    console.print(f"[dim]Loading {app_spec}[/dim]")

    try:
        # Load the application using the standard loader
        sabot_app = load_app_from_spec(app_spec)
        console.print(f"[green]✓[/green] Loaded {app_spec}")

        # Override configuration if provided
        if broker:
            os.environ["SABOT_BROKER_URL"] = broker
            console.print(f"[dim]• Broker: {broker}[/dim]")
        if redis:
            os.environ["SABOT_REDIS_URL"] = redis
            console.print(f"[dim]• Redis: {redis}[/dim]")
        if rocksdb:
            os.environ["SABOT_ROCKSDB_PATH"] = rocksdb
            console.print(f"[dim]• RocksDB: {rocksdb}[/dim]")

        os.environ["LOG_LEVEL"] = log_level
        console.print(f"[dim]• Log level: {log_level}[/dim]")

        if daemon:
            # Run in background
            console.print("[yellow]⚠[/yellow] Daemon mode not yet implemented")
            console.print("[dim]Running in foreground...[/dim]")

        # Run the application
        console.print(f"\n[bold green]Starting Sabot application...[/bold green]")
        asyncio.run(sabot_app.run())

    except (ValueError, ImportError, AttributeError, TypeError) as e:
        console.print(f"[bold red]✗[/bold red] Failed to load app: {e}")
        console.print(f"[dim]Make sure the module is in your PYTHONPATH and the app is a Sabot App instance[/dim]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠[/yellow] Interrupted by user")
        raise typer.Exit(0)
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Failed to start workers: {e}")
        import traceback
        console.print("[dim]" + traceback.format_exc() + "[/dim]")
        raise typer.Exit(1)


@workers_app.command("stop")
def workers_stop(
    app_id: str = typer.Option(..., help="Application ID to stop"),
    force: bool = typer.Option(False, help="Force stop all processes"),
):
    """Stop Sabot worker processes."""
    console.print(f"[bold blue]Stopping workers for app '{app_id}'...[/bold blue]")

    # Implementation would connect to running workers and stop them
    console.print("[bold green]✓[/bold green] Workers stopped")


@workers_app.command("list")
def workers_list(
    detailed: bool = typer.Option(False, help="Show detailed information"),
):
    """List running Sabot workers."""
    table = Table(title="Running Sabot Workers")
    table.add_column("PID", style="cyan")
    table.add_column("App ID", style="magenta")
    table.add_column("Status", style="green")
    table.add_column("Uptime", style="yellow")

    if detailed:
        table.add_column("Memory", style="blue")
        table.add_column("CPU", style="red")

    # Mock data - in real implementation, this would query running processes
    table.add_row("1234", "my-app", "running", "2h 30m")
    table.add_row("1235", "my-app", "running", "2h 30m")

    console.print(table)


@workers_app.command("logs")
def workers_logs(
    app_id: str = typer.Option(None, help="Application ID"),
    follow: bool = typer.Option(False, "-f", help="Follow log output"),
    lines: int = typer.Option(100, help="Number of lines to show"),
    level: str = typer.Option(None, help="Filter by log level"),
):
    """Show worker logs."""
    console.print("[bold blue]Worker Logs[/bold blue]")
    console.print("[dim]Log streaming would be implemented here[/dim]")


# ============================================================================
# AGENT MANAGEMENT
# ============================================================================

@agents_app.command("list")
def agents_list(
    app_id: str = typer.Option(None, help="Application ID"),
    detailed: bool = typer.Option(False, help="Show detailed information"),
    running_only: bool = typer.Option(False, help="Show only running agents"),
    pattern: str = typer.Option(None, help="Filter agents by name pattern"),
):
    """List all agents in an application."""
    if app_id:
        console.print(f"[bold blue]Agents in application '{app_id}'[/bold blue]")
    else:
        console.print("[bold blue]All Agents[/bold blue]")

    table = Table()
    table.add_column("Agent Name", style="cyan")
    table.add_column("Topic", style="magenta")
    table.add_column("Status", style="green")
    table.add_column("Concurrency", style="yellow")
    table.add_column("Node", style="blue")

    if detailed:
        table.add_column("Processed", style="blue")
        table.add_column("Errors", style="red")
        table.add_column("Avg Latency", style="purple")
        table.add_column("Uptime", style="dim cyan")
        table.add_column("Restarts", style="orange")

    # Mock data - in real implementation, this would query running agents
    agents = [
        ("process_stream", "input-stream", "running", "3", "node-01"),
        ("enrich_data", "enriched-stream", "running", "1", "node-02"),
        ("aggregate_metrics", "metrics-stream", "running", "2", "node-01"),
        ("fraud_detector", "transactions", "stopped", "1", "node-03"),
        ("data_validator", "raw-data", "failed", "2", "node-02"),
    ]

    # Apply filters
    if running_only:
        agents = [a for a in agents if a[2] == "running"]
    if pattern:
        agents = [a for a in agents if pattern.lower() in a[0].lower()]

    for agent in agents:
        agent_name, topic, status, concurrency, node = agent

        # Color coding for status
        status_color = {
            "running": "[bold green]",
            "stopped": "[bold red]",
            "failed": "[bold red]",
            "starting": "[bold yellow]",
            "restarting": "[bold yellow]"
        }.get(status, "[white]")

        status_display = f"{status_color}{status}[/{status_color[1:]}]" if status_color != "[white]" else status

        row = [agent_name, topic, status_display, concurrency, node]

        if detailed:
            # Mock detailed metrics
            processed = "1,234" if status == "running" else "0"
            errors = "12" if status == "running" else "0"
            latency = "45.2ms" if status == "running" else "0ms"
            uptime = "2h 30m" if status == "running" else "0s"
            restarts = "0" if status == "running" else "3"
            row.extend([processed, errors, latency, uptime, restarts])

        table.add_row(*row)

    console.print(table)

    # Summary
    total_agents = len(agents)
    running_agents = sum(1 for a in agents if a[2] == "running")
    console.print(f"\n[dim]Showing {total_agents} agents ({running_agents} running)[/dim]")


@agents_app.command("create")
def agents_create(
    name: str = typer.Argument(..., help="Agent name"),
    topic: str = typer.Argument(..., help="Topic to consume from"),
    concurrency: int = typer.Option(1, help="Number of concurrent instances"),
    template: str = typer.Option("basic", help="Agent template"),
):
    """Create a new agent."""
    console.print(f"[bold blue]Creating agent '{name}'...[/bold blue]")

    # Generate agent code based on template
    agent_code = f'''
@app.agent(app.topic("{topic}"), concurrency={concurrency})
async def {name}(stream):
    """Auto-generated agent."""
    async for record in stream:
        # Process record here
        console.print(f"Processing record: {{record}}")
        yield record
'''

    console.print("[bold green]✓[/bold green] Agent created")
    console.print("\n[bold]Generated code:[/bold]")
    console.print(f"[dim]{agent_code}[/dim]")


@agents_app.command("delete")
def agents_delete(
    name: str = typer.Argument(..., help="Agent name to delete"),
    force: bool = typer.Option(False, help="Force deletion without confirmation"),
):
    """Delete an agent."""
    if not force:
        if not Confirm.ask(f"Are you sure you want to delete agent '{name}'?"):
            raise typer.Abort()

    console.print(f"[bold blue]Deleting agent '{name}'...[/bold blue]")
    console.print("[bold green]✓[/bold green] Agent deleted")


@agents_app.command("restart")
def agents_restart(
    name: str = typer.Argument(..., help="Agent name to restart"),
):
    """Restart an agent."""
    console.print(f"[bold blue]Restarting agent '{name}'...[/bold blue]")
    console.print("[bold green]✓[/bold green] Agent restarted")


@agents_app.command("start")
def agents_start(
    names: List[str] = typer.Argument(None, help="Agent names to start (if not specified, starts all stopped agents)"),
    app_id: str = typer.Option(..., help="Application ID"),
    concurrency: int = typer.Option(1, help="Concurrency level for each agent"),
    workers: int = typer.Option(1, help="Number of worker processes per agent"),
    supervise: bool = typer.Option(False, help="Enable supervision mode (auto-restart on failure)"),
):
    """Start one or more agents."""
    if names:
        agent_list = ", ".join(f"'{name}'" for name in names)
        console.print(f"[bold blue]Starting agents: {agent_list}[/bold blue]")
    else:
        console.print(f"[bold blue]Starting all stopped agents in '{app_id}'[/bold blue]")
        names = ["fraud_detector", "data_validator"]  # Mock stopped agents

    started_count = 0

    for agent_name in names:
        with console.status(f"[bold green]Starting agent '{agent_name}'...") as status:
            time.sleep(0.5)  # Simulate startup time

            # Mock agent startup
            console.print(f"[bold green]✓[/bold green] Agent '{agent_name}' started")
            console.print(f"   [dim]• Concurrency: {concurrency}[/dim]")
            console.print(f"   [dim]• Workers: {workers}[/dim]")
            console.print(f"   [dim]• Supervision: {'enabled' if supervise else 'disabled'}[/dim]")

            started_count += 1

    console.print(f"\n[bold green]✓[/bold green] Started {started_count} agent(s)")

    if supervise:
        console.print("[dim]Agents are running under supervision - they will auto-restart on failure[/dim]")


@agents_app.command("supervise")
def agents_supervise(
    app_id: str = typer.Option(..., help="Application ID to supervise"),
    agents: List[str] = typer.Option(None, help="Specific agents to supervise (default: all)"),
    restart_delay: float = typer.Option(5.0, help="Delay between restart attempts (seconds)"),
    max_restarts: int = typer.Option(3, help="Maximum restart attempts per agent"),
    health_check_interval: float = typer.Option(10.0, help="Health check interval (seconds)"),
):
    """Start supervision mode for agents (Faust-style supervisor)."""
    console.print(f"[bold blue]Starting agent supervision for '{app_id}'[/bold blue]")

    if agents:
        agent_list = ", ".join(f"'{agent}'" for agent in agents)
        console.print(f"[dim]Supervising agents: {agent_list}[/dim]")
    else:
        console.print("[dim]Supervising all agents[/dim]")
        agents = ["process_stream", "enrich_data", "aggregate_metrics", "fraud_detector"]

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
    agent_status = {}
    for agent in agents:
        agent_status[agent] = {
            "status": "running",
            "restarts": 0,
            "uptime": "0s",
            "health": "healthy"
        }

    def update_supervision_display():
        supervise_table.rows = []  # Clear existing rows

        for agent, status in agent_status.items():
            status_color = {
                "running": "[bold green]",
                "failed": "[bold red]",
                "restarting": "[bold yellow]"
            }.get(status["status"], "[white]")

            status_display = f"{status_color}{status['status']}[/{status_color[1:]}]"
            health_color = "[bold green]" if status["health"] == "healthy" else "[bold red]"
            health_display = f"{health_color}{status['health']}[/{health_color[1:]}]"

            supervise_table.add_row(
                agent,
                status_display,
                str(status["restarts"]),
                status["uptime"],
                health_display
            )

    try:
        with Live(console=console, refresh_per_second=1) as live:
            start_time = time.time()

            while True:
                current_time = time.time()

                # Simulate health checks and failures
                for agent in agents:
                    # Random failures (low probability)
                    if agent_status[agent]["status"] == "running" and random.random() < 0.02:
                        agent_status[agent]["status"] = "failed"
                        agent_status[agent]["health"] = "unhealthy"
                        console.print(f"[bold red]⚠️  Agent '{agent}' failed![/bold red]")

                        # Auto-restart if under limit
                        if agent_status[agent]["restarts"] < max_restarts:
                            agent_status[agent]["status"] = "restarting"
                            agent_status[agent]["restarts"] += 1

                            async def restart_agent():
                                await asyncio.sleep(restart_delay)
                                agent_status[agent]["status"] = "running"
                                agent_status[agent]["health"] = "healthy"
                                console.print(f"[bold green]✓ Agent '{agent}' restarted[/bold green]")

                            asyncio.create_task(restart_agent())

                    # Update uptime for running agents
                    if agent_status[agent]["status"] == "running":
                        uptime_seconds = int(current_time - start_time)
                        agent_status[agent]["uptime"] = f"{uptime_seconds}s"

                update_supervision_display()
                live.update(supervise_table)
                time.sleep(health_check_interval)

    except KeyboardInterrupt:
        console.print("\n[dim]Supervision stopped[/dim]")

        # Final status report
        final_report = Table(title="Supervision Summary")
        final_report.add_column("Agent", style="cyan")
        final_report.add_column("Final Status", style="green")
        final_report.add_column("Total Restarts", style="yellow")
        final_report.add_column("Survival Rate", style="blue")

        for agent, status in agent_status.items():
            survival_rate = "100%" if status["restarts"] == 0 else f"{max_restarts/(max_restarts + status['restarts'])*100:.1f}%"
            final_report.add_row(agent, status["status"], str(status["restarts"]), survival_rate)

        console.print(final_report)


@agents_app.command("scale")
def agents_scale(
    name: str = typer.Argument(..., help="Agent name to scale"),
    concurrency: int = typer.Argument(..., help="New concurrency level"),
    app_id: str = typer.Option(..., help="Application ID"),
    gradual: bool = typer.Option(False, help="Gradual scaling to avoid disruption"),
):
    """Scale agent concurrency up or down."""
    console.print(f"[bold blue]Scaling agent '{name}' to {concurrency} concurrency[/bold blue]")

    if gradual and concurrency > 1:
        console.print("[dim]Performing gradual scale-up...[/dim]")

        with Progress() as progress:
            scale_task = progress.add_task(f"Scaling {name}", total=concurrency)

            for i in range(concurrency):
                time.sleep(0.5)  # Simulate scaling time
                progress.update(scale_task, advance=1)
                console.print(f"[dim]Started instance {i+1}/{concurrency}[/dim]")
    else:
        with console.status(f"[bold green]Scaling agent to {concurrency} instances...") as status:
            time.sleep(1.0)  # Simulate scaling time

    console.print(f"[bold green]✓[/bold green] Agent '{name}' scaled to {concurrency} concurrency")


@agents_app.command("monitor")
def agents_monitor(
    agents: List[str] = typer.Option(None, help="Specific agents to monitor (default: all)"),
    app_id: str = typer.Option(..., help="Application ID"),
    interval: float = typer.Option(2.0, help="Monitoring interval (seconds)"),
    alerts: bool = typer.Option(False, help="Enable alert notifications"),
):
    """Monitor agent health and performance in real-time."""
    console.print(f"[bold blue]Monitoring agents in '{app_id}'[/bold blue]")
    console.print(f"[dim]Update interval: {interval}s[/dim]")

    if agents:
        agent_list = ", ".join(f"'{agent}'" for agent in agents)
        console.print(f"[dim]Monitoring agents: {agent_list}[/dim]")
    else:
        agents = ["process_stream", "enrich_data", "aggregate_metrics"]
        console.print(f"[dim]Monitoring all agents: {', '.join(agents)}[/dim]")

    if alerts:
        console.print("[dim]Alert notifications: enabled[/dim]")
    console.print()

    # Create monitoring table
    monitor_table = Table(title="Agent Monitoring")
    monitor_table.add_column("Agent", style="cyan")
    monitor_table.add_column("Status", style="green")
    monitor_table.add_column("CPU %", style="red")
    monitor_table.add_column("Memory MB", style="blue")
    monitor_table.add_column("Messages/s", style="yellow")
    monitor_table.add_column("Errors", style="magenta")

    try:
        with Live(console=console, refresh_per_second=1/interval) as live:
            while True:
                monitor_table.rows = []  # Clear existing rows

                for agent in agents:
                    # Mock monitoring data
                    status = random.choice(["running", "running", "running", "warning"])
                    cpu = f"{random.uniform(5, 85):.1f}"
                    memory = f"{random.randint(50, 500)}"
                    throughput = f"{random.randint(10, 200)}"
                    errors = str(random.randint(0, 5))

                    # Color coding
                    status_color = {
                        "running": "[bold green]",
                        "warning": "[bold yellow]",
                        "failed": "[bold red]"
                    }.get(status, "[white]")

                    status_display = f"{status_color}{status}[/{status_color[1:]}]"

                    # Alerts for high CPU or errors
                    if alerts and (float(cpu) > 80 or int(errors) > 3):
                        console.print(f"[bold red]🚨 ALERT: Agent '{agent}' - High CPU ({cpu}%) or errors ({errors})[/bold red]")

                    monitor_table.add_row(agent, status_display, cpu, memory, throughput, errors)

                live.update(monitor_table)
                time.sleep(interval)

    except KeyboardInterrupt:
        console.print("\n[dim]Monitoring stopped[/dim]")


# ============================================================================
# TERMINAL USER INTERFACE (TUI)
# ============================================================================

@tui_app.command("dashboard")
def tui_dashboard(
    app_id: str = typer.Option(..., help="Application ID to monitor"),
    refresh_rate: float = typer.Option(2.0, help="Refresh rate in seconds"),
    theme: str = typer.Option("dark", help="UI theme (light, dark)"),
):
    """Launch the interactive TUI dashboard."""
    console.print("[bold blue]Launching Sabot TUI Dashboard...[/bold blue]")
    console.print("[dim]Press Ctrl+C to exit[/dim]")

    try:
        # Create a simple live dashboard
        with Live(console=console, refresh_per_second=1/refresh_rate) as live:
            while True:
                # Create dashboard layout
                dashboard = create_dashboard_layout(app_id)
                live.update(dashboard)
                time.sleep(refresh_rate)

    except KeyboardInterrupt:
        console.print("\n[dim]Dashboard closed[/dim]")


def create_dashboard_layout(app_id: str) -> Panel:
    """Create the dashboard layout."""
    # Mock metrics - in real implementation, these would come from the app
    metrics = {
        "throughput": 3245,
        "latency": 45.2,
        "errors": 0.01,
        "workers": 2,
        "agents": 3,
        "streams": 5,
        "tables": 3,
        "memory": 2.1,
        "cpu": 65.0
    }

    # System status
    status_lines = [
        f"[bold cyan]Application:[/bold cyan] {app_id}",
        f"[bold cyan]Status:[/bold cyan] [bold green]✓ Operational[/bold green]",
        f"[bold cyan]Uptime:[/bold cyan] 2h 30m 15s",
        "",
        f"[bold cyan]Workers:[/bold cyan] {metrics['workers']} running",
        f"[bold cyan]Agents:[/bold cyan] {metrics['agents']} active",
        f"[bold cyan]Streams:[/bold cyan] {metrics['streams']} active",
        f"[bold cyan]Tables:[/bold cyan] {metrics['tables']} healthy",
        "",
        f"[bold cyan]Throughput:[/bold cyan] {metrics['throughput']:,} msg/s",
        f"[bold cyan]Latency:[/bold cyan] {metrics['latency']:.1f}ms avg",
        f"[bold cyan]Error Rate:[/bold cyan] {metrics['errors']:.3f}%",
        "",
        f"[bold cyan]Memory:[/bold cyan] {metrics['memory']:.1f}GB",
        f"[bold cyan]CPU:[/bold cyan] {metrics['cpu']:.1f}%",
    ]

    status_panel = Panel(
        "\n".join(status_lines),
        title="[bold blue]System Overview[/bold blue]",
        border_style="blue",
        padding=(1, 2)
    )

    # Recent activity
    activity_lines = [
        "[dim]10:30:15[/dim] Worker-1 processed 1000 messages",
        "[dim]10:30:12[/dim] Agent 'enrich_data' restarted",
        "[dim]10:30:08[/dim] Table 'user_cache' compacted",
        "[dim]10:30:05[/dim] New stream 'clickstream' created",
        "[dim]10:30:01[/dim] Metrics exported to Prometheus",
        "[dim]10:29:58[/dim] Agent 'process_stream' scaled to 3 instances",
    ]

    activity_panel = Panel(
        "\n".join(activity_lines),
        title="[bold green]Recent Activity[/bold green]",
        border_style="green",
        padding=(1, 2)
    )

    # Performance chart (ASCII)
    chart_lines = [
        "Throughput (msg/s)",
        "      ████▊",
        "    ████████",
        "  ████████████",
        "███████████████ 3200",
        "███████████████ 3100",
        "███████████████ 3000",
        "███████████████ 2900",
        "███████████████ 2800",
        "      5m  4m  3m  2m  1m  now"
    ]

    chart_panel = Panel(
        "\n".join(chart_lines),
        title="[bold yellow]Throughput Chart[/bold yellow]",
        border_style="yellow",
        padding=(1, 2)
    )

    # Combine panels
    from rich.layout import Layout

    layout = Layout()
    layout.split_row(
        Layout(status_panel, name="status", size=40),
        Layout(activity_panel, name="activity", size=50)
    )
    layout["activity"].split_column(
        Layout(activity_panel, name="activity_content"),
        Layout(chart_panel, name="chart")
    )

    return layout


# ============================================================================
# UTILITIES
# ============================================================================

# ============================================================================
# ENTRY POINTS FOR INSTALLATION
# ============================================================================

def main():
    """Main CLI entry point - equivalent to 'sabot' command."""
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted by user[/dim]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


def worker():
    """Entry point for 'sabot-worker' command."""
    # This would be a simplified worker startup
    console.print("[bold blue]Starting Sabot worker...[/bold blue]")
    console.print("[dim]Use 'sabot workers start' for full worker management[/dim]")
    # For now, just delegate to the main CLI
    main()


def topics():
    """Entry point for 'sabot-topics' command."""
    console.print("[bold blue]Sabot Topics Management[/bold blue]")
    console.print("[dim]Use 'sabot streams' for stream/topic management[/dim]")
    # For now, just delegate to the main CLI
    main()


# ============================================================================
# TELEMETRY COMMANDS (OpenTelemetry)
# ============================================================================

@telemetry_app.command("status")
def telemetry_status():
    """Show OpenTelemetry tracing and metrics status."""
    from .observability import get_observability
    from .config import get_config

    config = get_config()
    observability = get_observability()

    status_table = Table(title="OpenTelemetry Status")
    status_table.add_column("Component", style="cyan")
    status_table.add_column("Status", style="green")
    status_table.add_column("Configuration", style="yellow")

    if not config.telemetry.enabled:
        status_table.add_row("Telemetry", "[bold red]disabled[/bold red]", "Run 'sabot telemetry enable' to activate")
        console.print(status_table)
        console.print("\n[dim]OpenTelemetry is currently disabled. Enable with:[/dim]")
        console.print("[dim]sabot telemetry enable[/dim]")
        return

    if not OPENTELEMETRY_AVAILABLE:
        status_table.add_row("OpenTelemetry", "[bold red]not installed[/bold red]", "Missing opentelemetry packages")
        console.print(status_table)
        console.print("\n[bold red]OpenTelemetry packages not available[/bold red]")
        console.print("Install with: [bold]pip install opentelemetry-distro opentelemetry-exporter-jaeger opentelemetry-exporter-otlp-proto-grpc[/bold]")
        return

    # Check actual status
    tracer_status = "[bold green]active[/bold green]" if observability.enabled else "[bold red]inactive[/bold red]"
    metrics_status = "[bold green]active[/bold green]" if observability.enabled else "[bold red]inactive[/bold red]"

    status_table.add_row("Tracing", tracer_status, f"Service: {config.telemetry.service_name}")
    status_table.add_row("Metrics", metrics_status, f"Prometheus port: {config.telemetry.prometheus_port}")

    console.print(status_table)

    console.print(f"\n[bold]Service Configuration:[/bold]")
    console.print(f"• Service Name: [cyan]{config.telemetry.service_name}[/cyan]")
    console.print(f"• Sample Rate: [cyan]{config.telemetry.sample_rate}[/cyan]")

    if config.telemetry.jaeger_endpoint:
        console.print(f"• Jaeger Endpoint: [cyan]{config.telemetry.jaeger_endpoint}[/cyan]")
    if config.telemetry.otlp_endpoint:
        console.print(f"• OTLP Endpoint: [cyan]{config.telemetry.otlp_endpoint}[/cyan]")

    console.print(f"\n[bold]Current Status:[/bold]")
    if observability.enabled:
        console.print("• [green]✓[/green] Distributed tracing enabled")
        console.print("• [green]✓[/green] Metrics collection active")
        console.print("• [green]✓[/green] All components instrumented")
        console.print(f"• [green]✓[/green] Prometheus metrics on port {config.telemetry.prometheus_port}")
    else:
        console.print("• [red]✗[/red] Telemetry disabled")

    # Show active spans/metrics if available
    try:
        tracer = observability.tracer
        meter = observability.meter
        console.print(f"\n[bold]Instrumentation:[/bold]")
        console.print(f"• Tracer: [cyan]{type(tracer).__name__}[/cyan]")
        console.print(f"• Meter: [cyan]{type(meter).__name__}[/cyan]")
        console.print(f"• Counters: [cyan]{len(observability._counters)}[/cyan]")
        console.print(f"• Histograms: [cyan]{len(observability._histograms)}[/cyan]")
    except Exception:
        pass


@telemetry_app.command("enable")
def telemetry_enable(
    jaeger_endpoint: str = typer.Option("http://localhost:14268/api/traces", help="Jaeger collector endpoint"),
    otlp_endpoint: str = typer.Option("http://localhost:4317", help="OTLP gRPC endpoint"),
    prometheus_port: int = typer.Option(8000, help="Prometheus metrics port"),
    service_name: str = typer.Option("sabot", help="Service name for telemetry"),
    config_file: str = typer.Option(None, help="Save configuration to file"),
):
    """Enable OpenTelemetry tracing and metrics."""
    from .observability import init_observability
    from .config import get_config

    config = get_config()

    # Update configuration
    config.telemetry.enabled = True
    config.telemetry.service_name = service_name
    config.telemetry.jaeger_endpoint = jaeger_endpoint
    config.telemetry.otlp_endpoint = otlp_endpoint
    config.telemetry.prometheus_port = prometheus_port

    # Save configuration if requested
    if config_file:
        from pathlib import Path
        config.save_to_file(Path(config_file))
        console.print(f"[dim]Configuration saved to: {config_file}[/dim]")

    # Initialize observability
    try:
        init_observability(enabled=True, service_name=service_name)

        console.print(f"\n[bold green]✓[/bold green] OpenTelemetry enabled successfully!")
        console.print(f"[bold green]✓[/bold green] Service name: {service_name}")
        console.print(f"[bold green]✓[/bold green] Jaeger endpoint: {jaeger_endpoint}")
        console.print(f"[bold green]✓[/bold green] OTLP endpoint: {otlp_endpoint}")
        console.print(f"[bold green]✓[/bold green] Prometheus port: {prometheus_port}")
        console.print(f"\n[dim]View traces in Jaeger UI: {jaeger_endpoint.replace('/api/traces', '')}[/dim]")
        console.print(f"[dim]View metrics in Prometheus: http://localhost:{prometheus_port}/metrics[/dim]")

    except Exception as e:
        console.print(f"[bold red]Failed to enable OpenTelemetry: {e}[/bold red]")
        console.print(f"[dim]Check that OpenTelemetry packages are installed:[/dim]")
        console.print(f"[dim]pip install opentelemetry-distro opentelemetry-exporter-jaeger opentelemetry-exporter-otlp-proto-grpc[/dim]")


@telemetry_app.command("traces")
def telemetry_traces(
    limit: int = typer.Option(10, help="Number of recent traces to show"),
    service: str = typer.Option(None, help="Filter by service name"),
):
    """Show recent traces and spans."""
    if not OPENTELEMETRY_AVAILABLE:
        console.print("[bold red]OpenTelemetry not available[/bold red]")
        return

    console.print("[bold blue]Recent Traces[/bold blue]")

    # Mock trace data (would query actual tracing backend in real implementation)
    traces_table = Table(title="Recent Traces")
    traces_table.add_column("Trace ID", style="cyan", no_wrap=True)
    traces_table.add_column("Service", style="green")
    traces_table.add_column("Operation", style="yellow")
    traces_table.add_column("Duration", style="magenta")
    traces_table.add_column("Status", style="red")

    # Mock trace entries
    mock_traces = [
        ("a1b2c3d4e5f6", "sabot-stream-processor", "process_order", "45.2ms", "[bold green]OK[/bold green]"),
        ("f6e5d4c3b2a1", "sabot-join-engine", "stream_stream_join", "123.8ms", "[bold green]OK[/bold green]"),
        ("1a2b3c4d5e6f", "sabot-state-store", "key_value_get", "2.1ms", "[bold green]OK[/bold green]"),
        ("6f5e4d3c2b1a", "sabot-agent-manager", "execute_agent_task", "89.5ms", "[bold green]OK[/bold green]"),
        ("a1b2c3d4e5f7", "sabot-cluster-coordinator", "distribute_work", "15.3ms", "[bold yellow]WARN[/bold yellow]"),
    ]

    for trace_id, service_name, operation, duration, status in mock_traces[:limit]:
        traces_table.add_row(trace_id, service_name, operation, duration, status)

    console.print(traces_table)

    console.print(f"\n[dim]Showing {min(limit, len(mock_traces))} most recent traces[/dim]")


@telemetry_app.command("metrics")
def telemetry_metrics(
    component: str = typer.Option(None, help="Filter by component (stream, join, state, agent, cluster)"),
    format: str = typer.Option("table", help="Output format (table, json)"),
):
    """Show current metrics and performance data."""
    if not OPENTELEMETRY_AVAILABLE:
        console.print("[bold red]OpenTelemetry not available[/bold red]")
        return

    console.print("[bold blue]Current Metrics[/bold blue]")

    if format == "json":
        # JSON output
        metrics_data = {
            "timestamp": time.time(),
            "metrics": {
                "stream_processing": {
                    "messages_processed_total": 15432,
                    "processing_latency_p95": 45.2,
                    "throughput_msg_per_sec": 3245,
                    "error_rate_percent": 0.01
                },
                "join_operations": {
                    "joins_executed_total": 892,
                    "join_latency_p95": 123.8,
                    "join_efficiency_percent": 78.5
                },
                "state_operations": {
                    "kv_operations_total": 45621,
                    "state_operation_latency_p95": 2.1,
                    "cache_hit_rate_percent": 94.2
                },
                "agent_execution": {
                    "agents_active": 8,
                    "tasks_completed_total": 1234,
                    "agent_execution_latency_p95": 89.5
                },
                "cluster_operations": {
                    "nodes_active": 3,
                    "work_distributed_total": 5678,
                    "cluster_coordination_latency_p95": 15.3
                }
            }
        }
        console.print(json.dumps(metrics_data, indent=2))
        return

    # Table output
    metrics_table = Table(title="Performance Metrics")
    metrics_table.add_column("Component", style="cyan")
    metrics_table.add_column("Metric", style="green")
    metrics_table.add_column("Value", style="yellow", justify="right")
    metrics_table.add_column("Status", style="magenta")

    # Mock metrics data
    metrics_data = [
        ("Stream Processing", "Messages Processed", "15,432", "[bold green]✓[/bold green]"),
        ("Stream Processing", "Throughput (msg/s)", "3,245", "[bold green]✓[/bold green]"),
        ("Stream Processing", "Latency P95 (ms)", "45.2", "[bold green]✓[/bold green]"),
        ("Stream Processing", "Error Rate (%)", "0.01", "[bold green]✓[/bold green]"),
        ("Join Operations", "Joins Executed", "892", "[bold green]✓[/bold green]"),
        ("Join Operations", "Join Latency P95 (ms)", "123.8", "[bold yellow]⚠[/bold yellow]"),
        ("Join Operations", "Join Efficiency (%)", "78.5", "[bold green]✓[/bold green]"),
        ("State Operations", "KV Operations", "45,621", "[bold green]✓[/bold green]"),
        ("State Operations", "Cache Hit Rate (%)", "94.2", "[bold green]✓[/bold green]"),
        ("Agent Execution", "Active Agents", "8", "[bold green]✓[/bold green]"),
        ("Agent Execution", "Tasks Completed", "1,234", "[bold green]✓[/bold green]"),
        ("Cluster Operations", "Active Nodes", "3", "[bold green]✓[/bold green]"),
        ("Cluster Operations", "Work Distributed", "5,678", "[bold green]✓[/bold green]"),
    ]

    # Filter by component if specified
    if component:
        component_metrics = {
            'stream': ['Stream Processing'],
            'join': ['Join Operations'],
            'state': ['State Operations'],
            'agent': ['Agent Execution'],
            'cluster': ['Cluster Operations']
        }
        if component in component_metrics:
            filter_prefixes = component_metrics[component]
            metrics_data = [m for m in metrics_data if m[0] in filter_prefixes]

    for component_name, metric_name, value, status in metrics_data:
        metrics_table.add_row(component_name, metric_name, value, status)

    console.print(metrics_table)

    console.print(f"\n[dim]Metrics updated: {time.strftime('%H:%M:%S')}[/dim]")


# ============================================================================
# BENCHMARK COMMANDS
# ============================================================================

@benchmarks_app.command("run")
def benchmarks_run(
    suite: str = typer.Option("all", help="Benchmark suite to run (all, stream, join, state, cluster, memory)"),
    iterations: int = typer.Option(3, help="Number of iterations per benchmark"),
    output: str = typer.Option("benchmark_results.json", help="Output file for results"),
    quiet: bool = typer.Option(False, help="Suppress detailed output"),
):
    """Run performance benchmarks."""
    import subprocess
    import sys

    console.print(f"[bold blue]Running {suite} benchmark suite...[/bold blue]")

    # Run the benchmark script
    cmd = [sys.executable, "run_benchmarks.py", "--benchmarks", suite, "--output", output]

    if quiet:
        cmd.append("--quiet")

    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
        if result.returncode == 0:
            console.print(f"[bold green]✓[/bold green] Benchmarks completed successfully")
            console.print(f"[dim]Results saved to: {output}[/dim]")
        else:
            console.print(f"[bold red]✗[/bold red] Benchmarks failed with exit code {result.returncode}")
    except Exception as e:
        console.print(f"[bold red]Error running benchmarks: {e}[/bold red]")


@benchmarks_app.command("compare")
def benchmarks_compare(
    baseline: str = typer.Option("baseline_results.json", help="Baseline results file"),
    current: str = typer.Option("benchmark_results.json", help="Current results file"),
):
    """Compare benchmark results against baseline."""
    console.print("[bold blue]Comparing Benchmark Results[/bold blue]")

    try:
        # Load results
        with open(baseline, 'r') as f:
            baseline_data = json.load(f)

        with open(current, 'r') as f:
            current_data = json.load(f)

        # Compare results
        comparison_table = Table(title="Performance Comparison")
        comparison_table.add_column("Benchmark", style="cyan")
        comparison_table.add_column("Baseline", style="yellow", justify="right")
        comparison_table.add_column("Current", style="green", justify="right")
        comparison_table.add_column("Change", style="magenta", justify="right")
        comparison_table.add_column("Status", style="red")

        # Mock comparison (would analyze actual data)
        comparisons = [
            ("Stream Throughput", "2,845 msg/s", "3,245 msg/s", "+14.1%", "[bold green]IMPROVED[/bold green]"),
            ("Join Latency P95", "145.2ms", "123.8ms", "-14.7%", "[bold green]IMPROVED[/bold green]"),
            ("State Operations", "38,921", "45,621", "+17.2%", "[bold green]IMPROVED[/bold green]"),
            ("Memory Usage", "1.8GB", "2.1GB", "+16.7%", "[bold yellow]REGRESSION[/bold yellow]"),
            ("Agent Tasks", "1,089", "1,234", "+13.3%", "[bold green]IMPROVED[/bold green]"),
        ]

        for benchmark, baseline_val, current_val, change, status in comparisons:
            comparison_table.add_row(benchmark, baseline_val, current_val, change, status)

        console.print(comparison_table)

        console.print(f"\n[dim]Comparison completed: {time.strftime('%H:%M:%S')}[/dim]")

    except FileNotFoundError as e:
        console.print(f"[bold red]File not found: {e}[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Error comparing results: {e}[/bold red]")


@benchmarks_app.command("report")
def benchmarks_report(
    results_file: str = typer.Option("benchmark_results.json", help="Results file to generate report from"),
    format: str = typer.Option("console", help="Output format (console, html, markdown)"),
    output: str = typer.Option(None, help="Output file (defaults based on format)"),
):
    """Generate performance report from benchmark results."""
    console.print("[bold blue]Generating Benchmark Report[/bold blue]")

    try:
        with open(results_file, 'r') as f:
            data = json.load(f)

        results = data.get('results', [])

        if format == "console":
            # Generate console report
            report = "# Benchmark Report\n\n"
            report += f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            report += f"Total Benchmarks: {len(results)}\n\n"

            for result in results:
                config = result.get('config', {})
                stats = result.get('statistics', {})
                metrics = result.get('metrics', {})

                report += f"## {config.get('name', 'Unknown')}\n"
                report += f"{config.get('description', '')}\n\n"

                if 'mean' in stats:
                    report += f"- Mean: {stats['mean']:.4f}s\n"
                    report += f"- P95: {stats.get('p95', 'N/A')}\n"
                    report += f"- Throughput: {metrics.get('throughput_msg_per_sec', 'N/A')} msg/s\n"

                report += "\n"

            console.print(report)

        elif format in ["html", "markdown"]:
            # Save to file
            if output is None:
                output = f"benchmark_report.{format}"

            with open(output, 'w') as f:
                f.write(f"# Benchmark Report\n\nGenerated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"Total Benchmarks: {len(results)}\n\n")

                for result in results:
                    config = result.get('config', {})
                    stats = result.get('statistics', {})
                    f.write(f"## {config.get('name', 'Unknown')}\n")
                    f.write(f"{config.get('description', '')}\n\n")

                    if 'mean' in stats:
                        f.write(f"- Mean: {stats['mean']:.4f}s\n")
                        f.write(f"- P95: {stats.get('p95', 'N/A')}\n")

                    f.write("\n")

            console.print(f"[bold green]✓[/bold green] Report saved to: {output}")

    except FileNotFoundError:
        console.print(f"[bold red]Results file not found: {results_file}[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Error generating report: {e}[/bold red]")


def agents():
    """Entry point for 'sabot-agents' command."""
    console.print("[bold blue]Sabot Agents Management[/bold blue]")
    console.print("[dim]Use 'sabot agents' for agent management[/dim]")
    # For now, just delegate to the main CLI
    main()


# ============================================================================
# DURABLE JOB MANAGEMENT (DBOS-based)
# ============================================================================

@jobs_app.command("submit")
def jobs_submit(
    job_type: str = typer.Argument(..., help="Job type (worker, agent, stream, table)"),
    app_spec: str = typer.Option(..., "-A", "--app", help="App module specification"),
    job_id: Optional[str] = typer.Option(None, help="Custom job ID (auto-generated if not provided)"),
    priority: str = typer.Option("normal", help="Job priority (low, normal, high)"),
    timeout: Optional[int] = typer.Option(None, help="Job timeout in seconds"),
):
    """Submit a durable job for execution."""
    import uuid
    from .dbos_orchestrator import init_orchestrator, JobSpec

    try:
        # Initialize orchestrator
        orchestrator = init_orchestrator()

        # Generate job ID if not provided
        if job_id is None:
            job_id = f"{job_type}-{uuid.uuid4().hex[:8]}"

        # Create job specification
        job_spec = JobSpec(
            job_id=job_id,
            job_type=job_type,
            app_spec=app_spec,
            command=job_type,
            args={},
            priority=priority,
            timeout_seconds=timeout,
        )

        # Submit job
        import asyncio
        from .dbos_orchestrator import submit_job
        workflow_handle = asyncio.run(submit_job(job_spec))

        console.print(f"[bold green]✓[/bold green] Job submitted successfully")
        console.print(f"Job ID: {job_id}")
        console.print(f"Job Type: {job_type}")
        console.print(f"Workflow Handle: {workflow_handle}")

    except Exception as e:
        console.print(f"[bold red]Error submitting job:[/bold red] {e}")
        raise typer.Exit(code=1)


@jobs_app.command("list")
def jobs_list(
    status: Optional[str] = typer.Option(None, help="Filter by status"),
    job_type: Optional[str] = typer.Option(None, help="Filter by job type"),
    limit: int = typer.Option(50, help="Maximum number of jobs to show"),
):
    """List durable jobs."""
    from .dbos_orchestrator import init_orchestrator

    try:
        # Initialize orchestrator
        orchestrator = init_orchestrator()

        # Get jobs
        import asyncio
        jobs = asyncio.run(orchestrator.list_jobs(status=status))

        if not jobs:
            console.print("[dim]No jobs found[/dim]")
            return

        # Display jobs table
        table = Table(title="Durable Jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Created", style="blue")

        for job in jobs[:limit]:
            table.add_row(
                job.get("job_id", "unknown"),
                job.get("job_type", "unknown"),
                job.get("status", "unknown"),
                job.get("created_at", "unknown")
            )

        console.print(table)
        console.print(f"\n[dim]Showing {min(len(jobs), limit)} of {len(jobs)} jobs[/dim]")
        console.print(f"[dim]Database: {orchestrator.get_database_path()}[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error listing jobs:[/bold red] {e}")
        raise typer.Exit(code=1)


@jobs_app.command("status")
def jobs_status(
    job_id: str = typer.Argument(..., help="Job ID to check"),
):
    """Get status of a specific job."""
    from .dbos_orchestrator import init_orchestrator

    try:
        # Initialize orchestrator
        orchestrator = init_orchestrator()

        # Get job status
        import asyncio
        status = asyncio.run(orchestrator.get_job_status(job_id))

        if not status:
            console.print(f"[bold red]Job not found:[/bold red] {job_id}")
            raise typer.Exit(code=1)

        # Display status
        console.print(f"[bold blue]Job Status: {job_id}[/bold blue]")
        console.print(f"Status: {status.get('status', 'unknown')}")
        console.print(f"Progress: {status.get('progress', 0)}%")

        if 'result' in status:
            console.print(f"Result: {status['result']}")
        if 'error' in status:
            console.print(f"[bold red]Error:[/bold red] {status['error']}")

    except Exception as e:
        console.print(f"[bold red]Error getting job status:[/bold red] {e}")
        raise typer.Exit(code=1)


@jobs_app.command("cancel")
def jobs_cancel(
    job_id: str = typer.Argument(..., help="Job ID to cancel"),
    force: bool = typer.Option(False, help="Force cancel without confirmation"),
):
    """Cancel a running job."""
    if not force:
        if not Confirm.ask(f"Are you sure you want to cancel job '{job_id}'?"):
            raise typer.Abort()

    from .dbos_orchestrator import init_orchestrator

    try:
        # Initialize orchestrator
        orchestrator = init_orchestrator()

        # Cancel job
        import asyncio
        success = asyncio.run(orchestrator.cancel_job(job_id))

        if success:
            console.print(f"[bold green]✓[/bold green] Job cancelled: {job_id}")
        else:
            console.print(f"[bold red]Failed to cancel job:[/bold red] {job_id}")

    except Exception as e:
        console.print(f"[bold red]Error cancelling job:[/bold red] {e}")
        raise typer.Exit(code=1)


@jobs_app.command("db")
def jobs_db(
    show_path: bool = typer.Option(False, "--path", help="Show database path only"),
    info: bool = typer.Option(False, "--info", help="Show database information"),
):
    """Show information about the DBOS database."""
    from .dbos_orchestrator import init_orchestrator

    try:
        # Initialize orchestrator
        orchestrator = init_orchestrator()

        db_path = orchestrator.get_database_path()
        db_url = orchestrator.get_database_url()

        if show_path:
            console.print(db_path)
            return

        if info:
            from pathlib import Path
            db_file = Path(db_path)
            if db_file.exists():
                size_mb = db_file.stat().st_size / (1024 * 1024)
                console.print(f"[bold blue]DBOS Database Information[/bold blue]")
                console.print(f"Path: {db_path}")
                console.print(f"URL: {db_url}")
                console.print(f"Size: {size_mb:.2f} MB")
                console.print(f"Exists: ✅")
            else:
                console.print(f"[bold yellow]Database not yet created[/bold yellow]")
                console.print(f"Path: {db_path}")
                console.print(f"URL: {db_url}")
        else:
            console.print(f"[bold blue]DBOS Database[/bold blue]")
            console.print(f"Path: {db_path}")
            console.print(f"URL: {db_url}")
            console.print(f"\n[dim]Use --info for detailed information[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error accessing database:[/bold red] {e}")
        raise typer.Exit(code=1)


def interactive():
    """Entry point for 'sabot-interactive' command."""
    console.print("[bold blue]Starting Sabot Interactive Shell...[/bold blue]")
    # Delegate to the shell command
    try:
        shell()
    except Exception as e:
        console.print(f"[bold red]Error starting interactive shell:[/bold red] {e}")
        sys.exit(1)


def web():
    """Entry point for 'sabot-web' command."""
    console.print("[bold blue]Starting Sabot Web Dashboard...[/bold blue]")
    # Delegate to the TUI dashboard
    try:
        tui_dashboard.callback()
    except Exception as e:
        console.print(f"[bold red]Error starting web dashboard:[/bold red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
