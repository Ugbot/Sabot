#!/usr/bin/env python3
"""Install missing dependencies for Sabot."""

import subprocess
import sys
import importlib

def install_package(package_name, pip_name=None):
    """Install a package if not already available."""
    if pip_name is None:
        pip_name = package_name

    try:
        importlib.import_module(package_name)
        print(f"✅ {package_name} already available")
        return True
    except ImportError:
        print(f"📦 Installing {pip_name}...")
        try:
            # Try uv first
            result = subprocess.run([
                'uv', 'add', pip_name
            ], cwd='.', capture_output=True, text=True)

            if result.returncode == 0:
                print(f"✅ Successfully installed {pip_name} with uv")
                return True
            else:
                print(f"⚠️  uv failed for {pip_name}, trying pip...")
        except FileNotFoundError:
            pass

        # Try pip
        try:
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', pip_name
            ], capture_output=True, text=True)

            if result.returncode == 0:
                print(f"✅ Successfully installed {pip_name} with pip")
                return True
            else:
                print(f"❌ Failed to install {pip_name}: {result.stderr}")
                return False
        except Exception as e:
            print(f"❌ Failed to install {pip_name}: {e}")
            return False

def main():
    """Install all required dependencies."""
    print("🔧 Installing Sabot Dependencies")
    print("=" * 40)

    dependencies = [
        ('prometheus_client', 'prometheus-client'),
        ('mode', 'mode-streaming'),
        ('pyarrow', None),
        ('pandas', None),
    ]

    installed = 0
    total = len(dependencies)

    for module_name, package_name in dependencies:
        if install_package(module_name, package_name):
            installed += 1

    print(f"\n📊 Installation Summary: {installed}/{total} packages installed")

    if installed == total:
        print("🎉 All dependencies installed successfully!")
        print("🚀 You can now run: python test_minimal_components.py")
    else:
        print("⚠️  Some dependencies failed to install.")
        print("💡 Try running: pip install prometheus-client mode-streaming pyarrow pandas")

if __name__ == "__main__":
    main()
