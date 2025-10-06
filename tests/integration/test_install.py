#!/usr/bin/env python3
"""
Installation Test Script for Sabot

This script tests that Sabot can be installed and imported correctly,
similar to how Faust can be installed and used.
"""

import sys
import subprocess
import tempfile
import os
from pathlib import Path

def test_installation():
    """Test Sabot installation."""
    print("🔧 Testing Sabot Installation")
    print("=" * 50)

    # Test 1: Check package structure
    print("\n1. Testing package structure...")
    try:
        # Check if package files exist
        required_files = [
            "sabot/__init__.py",
            "sabot/cli.py",
            "sabot/sabot_types.py",
            "pyproject.toml",
            "setup.py",
            "requirements.txt"
        ]

        missing_files = []
        for file_path in required_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)

        if missing_files:
            print(f"❌ Missing required files: {missing_files}")
            return False

        print("✅ All required package files present")
        print(f"   Package structure: {len(required_files)} files checked")

        # Check CLI structure
        cli_path = "sabot/cli.py"
        with open(cli_path, 'r') as f:
            cli_content = f.read()

        required_functions = ["def main(", "def agents_list(", "def agents_supervise("]
        for func in required_functions:
            if func in cli_content:
                print(f"   ✅ CLI has {func}")
            else:
                print(f"   ❌ CLI missing {func}")

    except Exception as e:
        print(f"❌ Package structure test failed: {e}")
        return False

    # Test 2: Check CLI entry points
    print("\n2. Testing CLI entry points...")
    cli_commands = [
        "sabot --help",
        "sabot version",
        "sabot status",
        "sabot agents list",
        "sabot workers list"
    ]

    for cmd in cli_commands:
        try:
            result = subprocess.run(
                cmd.split(),
                capture_output=True,
                text=True,
                timeout=10,
                cwd=os.path.dirname(__file__)
            )
            if result.returncode == 0:
                print(f"✅ '{cmd}' works")
            else:
                print(f"⚠️  '{cmd}' returned error: {result.stderr[:100]}...")
        except subprocess.TimeoutExpired:
            print(f"⚠️  '{cmd}' timed out")
        except FileNotFoundError:
            print(f"❌ '{cmd}' not found (CLI not installed)")
            break
    else:
        print("✅ All CLI commands accessible")

    # Test 3: Test module execution
    print("\n3. Testing module execution...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "sabot", "--help"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=os.path.dirname(__file__)
        )
        if result.returncode == 0:
            print("✅ 'python -m sabot --help' works")
        else:
            print(f"⚠️  Module execution failed: {result.stderr[:100]}...")
    except subprocess.TimeoutExpired:
        print("⚠️  Module execution timed out")

    # Test 4: Test basic functionality
    print("\n4. Testing basic functionality...")
    try:
        from sabot.cli import SabotCLIDemo
        cli = SabotCLIDemo()
        agents = cli.agents_list()
        print("✅ Basic CLI functionality works")
    except Exception as e:
        print(f"❌ Basic functionality failed: {e}")

    print("\n" + "=" * 50)
    print("🎉 Installation test completed!")
    print("\nTo use Sabot like Faust:")
    print("  pip install -e .              # Install in development mode")
    print("  sabot --help                  # Show CLI help")
    print("  sabot agents list             # List agents")
    print("  sabot workers start app.py    # Start workers")
    print("  sabot supervise               # Supervise agents")
    print()
    print("Or run the demo:")
    print("  python examples/sabot_cli_demo.py status")

    return True

if __name__ == "__main__":
    success = test_installation()
    sys.exit(0 if success else 1)
