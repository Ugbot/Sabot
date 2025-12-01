#!/usr/bin/env python3
"""
MarbleDB Development Build Script

Quick rebuild of MarbleDB C++ library and Cython bindings.
Designed for fast iteration during MarbleDB development.

Usage:
    python build_marbledb.py              # Incremental build
    python build_marbledb.py --clean      # Clean build
    python build_marbledb.py --debug      # Build with debug symbols
    python build_marbledb.py --verbose    # Show all build output
    python build_marbledb.py --lib-only   # Only build C++ library
    python build_marbledb.py --cython-only # Only rebuild Cython module
"""

import os
import sys
import subprocess
import shutil
import argparse
import time
from pathlib import Path

# ANSI colors
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
BLUE = '\033[94m'
RESET = '\033[0m'

# Project paths
PROJECT_ROOT = Path(__file__).parent
MARBLEDB_DIR = PROJECT_ROOT / "MarbleDB"
MARBLEDB_BUILD_DIR = MARBLEDB_DIR / "build"
ARROW_INSTALL = PROJECT_ROOT / "vendor" / "arrow" / "cpp" / "build" / "install"
CYTHON_MODULE_DIR = PROJECT_ROOT / "sabot" / "_cython" / "state"
CYTHON_SOURCE = CYTHON_MODULE_DIR / "marbledb_backend.cpp"
CYTHON_OUTPUT = None  # Will be set based on Python version


def get_python_config():
    """Get Python include paths and version info."""
    import sysconfig

    include_path = sysconfig.get_path('include')

    # Try to get numpy include path
    try:
        import numpy
        numpy_include = numpy.get_include()
    except ImportError:
        numpy_include = None

    # Get Python version for .so naming
    version_info = sys.version_info
    suffix = f"cpython-{version_info.major}{version_info.minor}-darwin.so"

    return {
        'include': include_path,
        'numpy_include': numpy_include,
        'so_suffix': suffix,
    }


def print_step(message):
    """Print a build step."""
    print(f"{BLUE}→ {message}{RESET}")


def print_success(message):
    """Print success message."""
    print(f"{GREEN}✅ {message}{RESET}")


def print_error(message):
    """Print error message."""
    print(f"{RED}❌ {message}{RESET}")


def print_warning(message):
    """Print warning message."""
    print(f"{YELLOW}⚠️  {message}{RESET}")


def run_command(cmd, cwd=None, verbose=False, env=None):
    """Run a command and return success status."""
    if verbose:
        print(f"  $ {' '.join(cmd)}")
        stdout = None
        stderr = None
    else:
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            env=env,
            check=True
        )
        return True, ""
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        return False, error_msg


def clean_marbledb(full_clean=False):
    """Clean MarbleDB build artifacts."""
    print_step("Cleaning MarbleDB build...")

    if full_clean:
        if MARBLEDB_BUILD_DIR.exists():
            shutil.rmtree(MARBLEDB_BUILD_DIR)
            print_success("Removed entire build directory")
    else:
        # Just remove the library to force relink
        libmarble = MARBLEDB_BUILD_DIR / "libmarble.a"
        if libmarble.exists():
            libmarble.unlink()
            print_success("Removed libmarble.a")

        # Clean object files for changed sources
        obj_dir = MARBLEDB_BUILD_DIR / "CMakeFiles" / "marble_static.dir" / "src" / "core"
        if obj_dir.exists():
            for obj_file in obj_dir.glob("*.o"):
                obj_file.unlink()
            print_success("Removed core object files")


def clean_cython():
    """Clean Cython module."""
    print_step("Cleaning Cython module...")

    for so_file in CYTHON_MODULE_DIR.glob("marbledb_backend*.so"):
        so_file.unlink()
        print_success(f"Removed {so_file.name}")


def build_marbledb_cpp(debug=False, verbose=False):
    """Build MarbleDB C++ library."""
    print_step("Building MarbleDB C++ library...")

    start_time = time.time()

    # Create build directory
    MARBLEDB_BUILD_DIR.mkdir(parents=True, exist_ok=True)

    # Configure with CMake if needed
    cmake_cache = MARBLEDB_BUILD_DIR / "CMakeCache.txt"
    if not cmake_cache.exists():
        print_step("Configuring with CMake...")

        cmake_args = ['cmake', '..']
        if debug:
            cmake_args.extend(['-DCMAKE_BUILD_TYPE=Debug', '-DCMAKE_CXX_FLAGS=-O0 -g'])
        else:
            cmake_args.extend(['-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_FLAGS=-O2'])

        success, error = run_command(cmake_args, cwd=MARBLEDB_BUILD_DIR, verbose=verbose)
        if not success:
            print_error(f"CMake configuration failed:\n{error[:500]}")
            return False

    # Build with make
    print_step("Compiling C++ sources...")

    import multiprocessing
    num_jobs = multiprocessing.cpu_count()

    success, error = run_command(
        ['make', 'marble_static', f'-j{num_jobs}'],
        cwd=MARBLEDB_BUILD_DIR,
        verbose=verbose
    )

    if not success:
        print_error(f"Build failed:\n{error[:500]}")
        return False

    elapsed = time.time() - start_time
    libmarble = MARBLEDB_BUILD_DIR / "libmarble.a"
    if libmarble.exists():
        size_mb = libmarble.stat().st_size / (1024 * 1024)
        print_success(f"Built libmarble.a ({size_mb:.1f} MB) in {elapsed:.1f}s")
        return True
    else:
        print_error("Build completed but libmarble.a not found")
        return False


def build_cython_module(debug=False, verbose=False):
    """Build Cython marbledb_backend module."""
    print_step("Building Cython marbledb_backend module...")

    start_time = time.time()

    # Get Python configuration
    py_config = get_python_config()

    if py_config['numpy_include'] is None:
        print_error("NumPy not found - required for Cython build")
        return False

    # Check that source files exist
    if not CYTHON_SOURCE.exists():
        print_error(f"Cython source not found: {CYTHON_SOURCE}")
        return False

    libmarble = MARBLEDB_BUILD_DIR / "libmarble.a"
    if not libmarble.exists():
        print_error(f"libmarble.a not found - run with --lib-only first")
        return False

    # Find clang++
    clang_paths = [
        '/opt/homebrew/opt/llvm/bin/clang++',
        '/usr/local/opt/llvm/bin/clang++',
        'clang++',
    ]
    clang = None
    for path in clang_paths:
        if shutil.which(path):
            clang = path
            break

    if not clang:
        print_error("clang++ not found")
        return False

    # Output file
    output_file = CYTHON_MODULE_DIR / f"marbledb_backend.{py_config['so_suffix']}"

    # Build command
    compile_args = [
        clang,
        '-std=c++17',
        '-O0' if debug else '-O2',
        '-g' if debug else '',
        '-shared',
        '-fPIC',
        '-undefined', 'dynamic_lookup',
        f'-I{py_config["include"]}',
        f'-I{py_config["numpy_include"]}',
        f'-I{ARROW_INSTALL / "include"}',
        f'-I{MARBLEDB_DIR / "include"}',
        str(CYTHON_SOURCE),
        str(libmarble),
        f'-L{ARROW_INSTALL / "lib"}',
        '-larrow',
        '-lparquet',
        '-o', str(output_file),
    ]

    # Remove empty args
    compile_args = [arg for arg in compile_args if arg]

    if verbose:
        print(f"  $ {' '.join(compile_args)}")

    success, error = run_command(compile_args, cwd=CYTHON_MODULE_DIR, verbose=verbose)

    if not success:
        print_error(f"Cython build failed:\n{error[:500]}")
        return False

    elapsed = time.time() - start_time
    if output_file.exists():
        size_kb = output_file.stat().st_size / 1024
        print_success(f"Built {output_file.name} ({size_kb:.0f} KB) in {elapsed:.1f}s")
        return True
    else:
        print_error("Build completed but .so file not found")
        return False


def verify_build():
    """Verify the build by importing the module."""
    print_step("Verifying build...")

    # Set up library path
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = f"{ARROW_INSTALL / 'lib'}:{env.get('DYLD_LIBRARY_PATH', '')}"

    # Test import
    test_code = '''
import sys
sys.path.insert(0, "{project_root}")
try:
    from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
    print("Import successful")
except Exception as e:
    print(f"Import failed: {{e}}")
    sys.exit(1)
'''.format(project_root=PROJECT_ROOT)

    result = subprocess.run(
        [sys.executable, '-c', test_code],
        env=env,
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print_success("Module imports successfully")
        return True
    else:
        print_error(f"Module import failed: {result.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description='MarbleDB Development Build')
    parser.add_argument('--clean', action='store_true', help='Clean rebuild')
    parser.add_argument('--clean-all', action='store_true', help='Full clean (removes CMake cache)')
    parser.add_argument('--debug', action='store_true', help='Build with debug symbols')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show build output')
    parser.add_argument('--lib-only', action='store_true', help='Only build C++ library')
    parser.add_argument('--cython-only', action='store_true', help='Only rebuild Cython module')
    parser.add_argument('--no-verify', action='store_true', help='Skip import verification')

    args = parser.parse_args()

    print(f"\n{BLUE}═══════════════════════════════════════════════════════════════{RESET}")
    print(f"{BLUE}  MarbleDB Development Build{RESET}")
    print(f"{BLUE}═══════════════════════════════════════════════════════════════{RESET}\n")

    start_time = time.time()

    # Clean if requested
    if args.clean or args.clean_all:
        clean_marbledb(full_clean=args.clean_all)
        if not args.lib_only:
            clean_cython()

    # Build C++ library
    if not args.cython_only:
        if not build_marbledb_cpp(debug=args.debug, verbose=args.verbose):
            print_error("C++ build failed")
            return 1

    # Build Cython module
    if not args.lib_only:
        if not build_cython_module(debug=args.debug, verbose=args.verbose):
            print_error("Cython build failed")
            return 1

    # Verify build
    if not args.no_verify and not args.lib_only:
        if not verify_build():
            print_warning("Build completed but verification failed")

    total_time = time.time() - start_time
    print(f"\n{GREEN}═══════════════════════════════════════════════════════════════{RESET}")
    print(f"{GREEN}  Build completed in {total_time:.1f}s{RESET}")
    print(f"{GREEN}═══════════════════════════════════════════════════════════════{RESET}\n")

    return 0


if __name__ == '__main__':
    sys.exit(main())
