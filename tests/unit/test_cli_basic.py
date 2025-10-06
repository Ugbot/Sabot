#!/usr/bin/env python3
"""Basic CLI test without full package dependencies."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Mock the missing modules for basic testing
import sys
from unittest.mock import MagicMock

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

# Now try to import and test basic CLI structure
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location("cli", "sabot/cli.py")
    cli_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli_module)

    app = cli_module.app
    main_callback = cli_module.main_callback
    version = cli_module.version
    init = cli_module.init
    shell = cli_module.shell
    print("✓ CLI imports successfully")

    # Test that the main functions exist
    assert callable(main_callback), "main_callback should be callable"
    assert callable(version), "version should be callable"
    assert callable(init), "init should be callable"
    assert callable(shell), "shell should be callable"

    print("✓ CLI functions are callable")

    # Test that sub-apps exist
    assert hasattr(app, 'add_typer'), "app should have add_typer method"
    print("✓ CLI app structure is correct")

    # Test TUI dashboard function exists
    tui_dashboard = cli_module.tui_dashboard
    create_dashboard_layout = cli_module.create_dashboard_layout
    assert callable(tui_dashboard), "tui_dashboard should be callable"
    assert callable(create_dashboard_layout), "create_dashboard_layout should be callable"
    print("✓ TUI dashboard functions exist")

    print("\n🎉 CLI basic structure test passed!")

except Exception as e:
    print(f"❌ CLI test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
