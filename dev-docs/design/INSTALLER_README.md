# Sabot Installer - Faust-Style Installation

This document describes how Sabot can be installed and used just like Faust, providing the same developer experience with pip-based installation and CLI tools.

## 🎯 Installation Options

### 1. Install from Source (Development Mode)

```bash
# Clone and enter the repository
git clone https://github.com/sabot/sabot.git
cd sabot

# Install in development mode (recommended for development)
pip install -e .

# Or install with specific features
pip install -e ".[kafka]"     # With Kafka support
pip install -e ".[gpu]"       # With GPU acceleration
pip install -e ".[all]"       # All optional dependencies
```

### 2. Install from PyPI (Future)

```bash
# When available on PyPI
pip install sabot
pip install sabot[kafka]      # With Kafka support
pip install sabot[redis]      # With Redis support
pip install sabot[flight]     # With Arrow Flight
pip install sabot[gpu]        # With GPU acceleration
pip install sabot[all]        # Everything
```

### 3. Docker Installation

```bash
# Build the Docker image
docker build -t sabot .

# Run Sabot CLI
docker run -it sabot sabot --help

# Run with mounted volume for development
docker run -it -v $(pwd):/app sabot sabot status
```

## 🛠️ CLI Usage (Just Like Faust)

After installation, Sabot provides CLI commands similar to Faust:

```bash
# Get help
sabot --help

# Show version
sabot version

# Show cluster status
sabot status --detailed

# List agents
sabot agents list --running-only --detailed

# Start agents with supervision
sabot agents start fraud_detector order_validator --concurrency 3 --supervise

# Supervise running agents (auto-restart on failure)
sabot agents supervise --restart-delay 5.0 --max-restarts 3

# Scale agent concurrency
sabot agents scale my-agent 5 --gradual

# Monitor agents in real-time
sabot agents monitor --alerts --interval 2.0

# List workers
sabot workers list --detailed

# Start workers
sabot workers start myapp.main:app --workers 3 --broker kafka://localhost:9092

# Interactive shell
sabot shell --broker memory://

# Web dashboard
sabot web --port 8080
```

## 📦 Package Structure

The installer creates the following package structure:

```
sabot/
├── sabot/                    # Main package
│   ├── __init__.py          # Package initialization
│   ├── __main__.py          # Module execution entry point
│   ├── cli.py               # CLI implementation with Faust-style commands
│   ├── sabot_types.py       # Type definitions
│   ├── app.py               # Main application class
│   ├── _cython/             # Cython optimizations
│   └── py.typed             # Type hints marker
├── examples/                # Example applications
├── tests/                   # Test suite
├── pyproject.toml           # Modern Python packaging
├── setup.py                 # Traditional setuptools
├── requirements.txt         # Core dependencies
├── MANIFEST.in              # Package manifest
├── Dockerfile               # Container build
├── test_install.py          # Installation verification
└── README.md                # Documentation
```

## 🔧 Entry Points

The installer defines these CLI entry points (similar to Faust):

- `sabot` - Main CLI command
- `sabot-worker` - Worker process management
- `sabot-agents` - Agent lifecycle management
- `sabot-interactive` - Interactive shell
- `sabot-web` - Web dashboard

## 🧪 Testing Installation

Run the installation test to verify everything works:

```bash
# Test package structure and basic functionality
python test_install.py
```

## 🔄 Development Workflow

For developers working on Sabot:

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Check CLI functionality
sabot --help
sabot agents list

# Test examples
cd examples
python sabot_cli_demo.py status
python simplified_demo.py
```

## 📋 Dependencies

### Core Dependencies (Always Installed)
- `pyarrow>=10.0.0` - Columnar data processing
- `rich>=12.0.0` - Beautiful CLI output
- `typer>=0.7.0` - CLI framework
- `pydantic>=2.0.0` - Data validation
- `orjson>=3.9.0` - Fast JSON serialization
- `uvloop>=0.17.0` - High-performance event loop
- `structlog>=22.0.0` - Structured logging

### Optional Dependencies
- **Kafka**: `aiokafka`, `confluent-kafka`
- **Redis**: `redis` (or custom fastredis)
- **GPU**: `cudf`, `cupy`, RAFT libraries
- **Flight**: `pyarrow[flight]`
- **SQL**: `duckdb`
- **Dev**: Testing and development tools

## 🚀 Usage Examples

### Basic Agent Creation (Like Faust)

```python
import sabot as sb

# Create app (like Faust)
app = sb.create_app(
    id="my-app",
    broker="kafka://localhost:9092"
)

# Define agent (like Faust)
@app.agent(app.topic("orders"))
async def process_orders(stream):
    async for order in stream:
        # Process order
        result = {"processed": True, "order_id": order["id"]}
        yield result

if __name__ == "__main__":
    # Run like Faust
    app.run()
```

### CLI Operations (Like Faust)

```bash
# Start workers (like Faust)
sabot workers start myapp:app --workers 3

# List agents
sabot agents list

# Monitor in real-time
sabot agents monitor --alerts
```

## ✅ Installation Verification

After installation, verify everything works:

1. **Package Import**: `import sabot` works
2. **CLI Help**: `sabot --help` shows commands
3. **CLI Demo**: `python examples/sabot_cli_demo.py status` works
4. **Entry Points**: All `sabot-*` commands are available
5. **Module Execution**: `python -m sabot --help` works

## 🔄 Compatibility

Sabot's installer maintains compatibility with:
- **Python 3.8+**: Modern Python versions
- **pip**: Standard package installer
- **setuptools**: Traditional Python packaging
- **Docker**: Containerized deployments
- **Kubernetes**: Orchestrated deployments

## 🎯 Key Features Delivered

✅ **Pip Installable**: `pip install sabot`
✅ **CLI Commands**: Full Faust-style CLI
✅ **Entry Points**: Multiple executable commands
✅ **Optional Dependencies**: Feature-gated installations
✅ **Development Mode**: `pip install -e .`
✅ **Docker Support**: Containerized installation
✅ **Type Hints**: Full type annotation support
✅ **Comprehensive CLI**: Agent management, supervision, monitoring
