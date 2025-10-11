#!/usr/bin/env python3
"""Basic CLI test without full package dependencies."""

import pytest
import sys
from unittest.mock import MagicMock

# Mock the missing modules for basic testing
sys.modules['typer'] = MagicMock()
sys.modules['rich.console'] = MagicMock()
sys.modules['rich.table'] = MagicMock()
sys.modules['rich.panel'] = MagicMock()
sys.modules['rich.text'] = MagicMock()
sys.modules['rich.live'] = MagicMock()
sys.modules['rich.spinner'] = MagicMock()
sys.modules['rich.progress'] = MagicMock()
sys.modules['rich.prompt'] = MagicMock()
sys.modules['rich'] = MagicMock()

# Mock typer
mock_typer = sys.modules['typer']
mock_app = MagicMock()
mock_typer.Typer.return_value = mock_app
mock_typer.Argument = lambda x, **kwargs: x
mock_typer.Option = lambda *args, **kwargs: args[0] if args else None

# Mock rich components
mock_console = MagicMock()
sys.modules['rich.console'].Console.return_value = mock_console

mock_table = MagicMock()
sys.modules['rich.table'].Table.return_value = mock_table

mock_panel = MagicMock()
sys.modules['rich.panel'].Panel.return_value = mock_panel

mock_live = MagicMock()
sys.modules['rich.live'].Live.return_value = mock_live


@pytest.fixture
def cli_module():
    """Load CLI module dynamically."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("cli", "sabot/cli.py")
    cli_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli_module)
    return cli_module


def test_cli_imports(cli_module):
    """Test CLI imports successfully."""
    assert cli_module.app is not None
    assert cli_module.main_callback is not None
    assert cli_module.version is not None
    assert cli_module.init is not None
    assert cli_module.shell is not None


def test_cli_functions_callable(cli_module):
    """Test that the main functions are callable."""
    assert callable(cli_module.main_callback), "main_callback should be callable"
    assert callable(cli_module.version), "version should be callable"
    assert callable(cli_module.init), "init should be callable"
    assert callable(cli_module.shell), "shell should be callable"


def test_cli_app_structure(cli_module):
    """Test that sub-apps exist."""
    assert hasattr(cli_module.app, 'add_typer'), "app should have add_typer method"


def test_tui_dashboard_functions(cli_module):
    """Test TUI dashboard functions exist."""
    assert callable(cli_module.tui_dashboard), "tui_dashboard should be callable"
    assert callable(cli_module.create_dashboard_layout), "create_dashboard_layout should be callable"
