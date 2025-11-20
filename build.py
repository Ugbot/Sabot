#!/usr/bin/env python3
"""
Unified Build System for Sabot

Builds everything in the correct order:
1. Vendored C++ libraries (Arrow, DuckDB)
2. Vendored Python extensions (CyRedis, RocksDB, Tonbo)
3. Sabot Cython extensions (55 modules)

Usage:
    python build.py                  # Build everything
    python build.py --clean          # Remove build artifacts
    python build.py --clean-all      # Remove everything including vendor builds
    python build.py --skip-arrow     # Don't rebuild Arrow
    python build.py --skip-vendor    # Skip all vendor builds
    python build.py --dev            # Debug build
    python build.py --parallel 8     # Override parallel jobs
"""

import os
import sys
import subprocess
import shutil
import glob
import argparse
import time
from pathlib import Path
import multiprocessing

# ANSI colors
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
BLUE = '\033[94m'
RESET = '\033[0m'

# Project paths
PROJECT_ROOT = Path(__file__).parent
ARROW_SOURCE = PROJECT_ROOT / "vendor" / "arrow" / "cpp"
ARROW_INSTALL = ARROW_SOURCE / "build" / "install"
CYREDIS_DIR = PROJECT_ROOT / "vendor" / "cyredis"
ROCKSDB_VENDOR_DIR = PROJECT_ROOT / "vendor" / "rocksdb"
ROCKSDB_INSTALL = ROCKSDB_VENDOR_DIR / "install"
DUCKDB_VENDOR_DIR = PROJECT_ROOT / "vendor" / "duckdb"
DUCKDB_BUILD_DIR = DUCKDB_VENDOR_DIR / "build" / "release"
DUCKDB_INSTALL = DUCKDB_VENDOR_DIR / "install"
TONBO_DIR = PROJECT_ROOT / "vendor" / "tonbo" / "bindings" / "python"
TONBO_FFI_DIR = PROJECT_ROOT / "vendor" / "tonbo" / "tonbo-ffi"
MARBLEDB_DIR = PROJECT_ROOT / "MarbleDB"
MARBLEDB_BUILD_DIR = MARBLEDB_DIR / "build"
POSTGRESQL_WAL2JSON_DIR = PROJECT_ROOT / "vendor" / "postgresql" / "wal2json"
SABOT_CYTHON_DIR = PROJECT_ROOT / "sabot" / "_cython"
SABOT_C_DIR = PROJECT_ROOT / "sabot" / "_c"  # Additional Cython modules

# Modules to exclude from build (known issues)
EXCLUDED_MODULES = [
    'arrow/flight_client.pyx',  # Duplicate cdef declarations + closure in cpdef
    'arrow_core.pyx',  # Compiler crash in AnalyseDeclarationsTransform (in _cython/)
    'flight/flight_server.pyx',  # Missing Arrow Flight pxd files + nested class in cpdef
    'materialized_views.pyx',  # Incorrect RocksDB API usage (needs vendored bindings)
    'checkpoint/dbos/durable_state_store.pyx',  # Missing stores/base.pxd dependency
]

# Modules in _c/ directory are built separately and not excluded


def print_phase(num, total, message):
    """Print a build phase header."""
    print(f"\n{BLUE}[{num}/{total}] {message}{RESET}")
    print("=" * 60)


def print_success(message):
    """Print success message."""
    print(f"{GREEN}✅ {message}{RESET}")


def print_skip(message):
    """Print skip message."""
    print(f"{YELLOW}⏭️  {message}{RESET}")


def print_error(message):
    """Print error message."""
    print(f"{RED}❌ {message}{RESET}")


def find_executable(name, alternatives=None):
    """Find executable in PATH."""
    alternatives = alternatives or []
    for exe_name in [name] + alternatives:
        if shutil.which(exe_name):
            return exe_name
    return None


# ==============================================================================
# Phase 1: Dependency Detection
# ==============================================================================

def check_dependencies():
    """Check for required and optional dependencies."""
    print_phase(1, 7, "Checking dependencies...")

    deps = {
        'cmake': find_executable('cmake', ['cmake3']),
        'cxx': find_executable('c++', ['g++', 'clang++']),
        'python': sys.executable,
        'maturin': find_executable('maturin'),
        'cargo': find_executable('cargo'),
        'rust': find_executable('rustc'),
    }

    # Check Arrow
    arrow_built = (ARROW_INSTALL / "lib").exists() and any(
        (ARROW_INSTALL / "lib").glob("libarrow.*")
    )

    # Check RocksDB (vendored build)
    rocksdb_built = (ROCKSDB_INSTALL / "lib").exists() and any(
        (ROCKSDB_INSTALL / "lib").glob("librocksdb.*")
    )

    # Check DuckDB (vendored build)
    duckdb_built = (DUCKDB_BUILD_DIR / "src").exists() and any(
        (DUCKDB_BUILD_DIR / "src").glob("libduckdb.*")
    )

    # Check MarbleDB (local build)
    marbledb_built = MARBLEDB_BUILD_DIR.exists() and (MARBLEDB_BUILD_DIR / "libmarble.a").exists()

    # Check hiredis for CyRedis
    hiredis_available = False
    try:
        result = subprocess.run(['pkg-config', '--exists', 'hiredis'],
                              capture_output=True)
        hiredis_available = result.returncode == 0
    except FileNotFoundError:
        pass

    # Check Python version
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    # Print results
    if arrow_built:
        print_success(f"Arrow C++ found at {ARROW_INSTALL}")
    else:
        print_skip("Arrow C++ not built yet")

    if rocksdb_built:
        print_success(f"RocksDB found at {ROCKSDB_INSTALL}")
    else:
        print_skip("RocksDB not built yet")

    if duckdb_built:
        print_success(f"DuckDB found at {DUCKDB_BUILD_DIR}")
    else:
        print_skip("DuckDB not built yet")

    if marbledb_built:
        print_success(f"MarbleDB found at {MARBLEDB_BUILD_DIR}")
    else:
        print_skip("MarbleDB not built yet")

    if hiredis_available:
        print_success("hiredis found (CyRedis will be built)")
    else:
        print_skip("hiredis not found (CyRedis will be skipped)")

    if deps['cargo'] and deps['rust']:
        print_success(f"Rust toolchain found ({deps['cargo']})")
    else:
        print_skip("Rust toolchain not found (Tonbo will be skipped)")

    print_success(f"Python {python_version} at {deps['python']}")

    return {
        'arrow_built': arrow_built,
        'rocksdb_built': rocksdb_built,
        'duckdb_built': duckdb_built,
        'marbledb_built': marbledb_built,
        'hiredis_available': hiredis_available,
        'rust_available': deps['cargo'] and deps['rust'],
        'maturin_available': deps['maturin'] is not None,
        'cmake': deps['cmake'],
        'python': deps['python'],
    }


# ==============================================================================
# Phase 2: Build Arrow C++
# ==============================================================================

def build_arrow_cpp(skip=False):
    """Build Apache Arrow C++ library."""
    print_phase(2, 7, "Building Arrow C++...")

    if skip:
        print_skip("Skipped by user (--skip-arrow)")
        return ARROW_INSTALL if (ARROW_INSTALL / "lib").exists() else None

    # Check if already built
    lib_extensions = [".so", ".dylib", ".dll", ".a"]
    arrow_lib_files = [ARROW_INSTALL / "lib" / f"libarrow{ext}" for ext in lib_extensions]

    if any(f.exists() for f in arrow_lib_files):
        print_success("Already built")
        return ARROW_INSTALL

    # Check if Arrow source exists
    if not ARROW_SOURCE.exists():
        print_error("Arrow source not found")
        print("Run: git submodule update --init --recursive vendor/arrow")
        return None

    # Check for CMake
    cmake = find_executable("cmake", ["cmake3"])
    if not cmake:
        print_error("CMake not found - install cmake first")
        return None

    # Create build directory
    arrow_build_dir = ARROW_SOURCE / "build"
    arrow_build_dir.mkdir(parents=True, exist_ok=True)

    # Determine parallel jobs
    num_jobs = multiprocessing.cpu_count()

    print(f"Configuring Arrow (using {num_jobs} parallel jobs)...")

    # CMake configuration
    cmake_args = [
        cmake,
        str(ARROW_SOURCE),
        f"-DCMAKE_INSTALL_PREFIX={ARROW_INSTALL}",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
        "-DARROW_BUILD_STATIC=OFF",
        "-DARROW_BUILD_SHARED=ON",
        "-DARROW_COMPUTE=ON",
        "-DARROW_CSV=ON",
        "-DARROW_DATASET=ON",
        "-DARROW_FILESYSTEM=ON",
        "-DARROW_FLIGHT=ON",
        "-DARROW_IPC=ON",
        "-DARROW_JSON=ON",
        "-DARROW_PARQUET=ON",
        "-DARROW_PYTHON=ON",
        "-DARROW_WITH_ZLIB=ON",
        "-DARROW_WITH_ZSTD=ON",
        "-DARROW_WITH_LZ4=ON",
        "-DARROW_WITH_SNAPPY=ON",
        "-DARROW_WITH_BROTLI=ON",
        "-DARROW_S3=OFF",
        "-DARROW_GCS=OFF",
        "-DARROW_HDFS=OFF",
        "-DARROW_ORC=OFF",
        "-DARROW_CUDA=OFF",
        "-DARROW_GANDIVA=OFF",
        "-DARROW_BUILD_TESTS=OFF",
        "-DARROW_BUILD_BENCHMARKS=OFF",
        "-DARROW_BUILD_EXAMPLES=OFF",
        "-DARROW_BUILD_UTILITIES=ON",
        "-DARROW_DEPENDENCY_SOURCE=AUTO",
    ]

    try:
        subprocess.run(cmake_args, cwd=arrow_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"CMake configuration failed: {e.stderr.decode()}")
        return None

    print("Building Arrow C++ (20-60 minutes on first build)...")

    build_args = [
        cmake,
        "--build", ".",
        "--config", "Release",
        "--parallel", str(num_jobs),
    ]

    try:
        subprocess.run(build_args, cwd=arrow_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"Arrow build failed: {e.stderr.decode()}")
        return None

    # Install Arrow
    install_args = [cmake, "--install", "."]
    try:
        subprocess.run(install_args, cwd=arrow_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"Arrow installation failed: {e.stderr.decode()}")
        return None

    print_success(f"Arrow C++ built successfully")
    return ARROW_INSTALL


def build_vendored_pyarrow(arrow_home, skip=False):
    """Build PyArrow from vendored Arrow source."""
    print_phase("2.5", 8, "Building PyArrow from vendored source...")

    if skip:
        print_skip("Skipped by user (--skip-arrow)")
        return False

    pyarrow_src = PROJECT_ROOT / "vendor/arrow/python"

    # Check if already installed by trying to import
    try:
        import pyarrow
        pyarrow_file = Path(pyarrow.__file__)
        # Check if it's our vendored version
        if "vendor/arrow/python" in str(pyarrow_file):
            print_success("Already installed (vendored)")
            return True
    except ImportError:
        pass

    # Check if Arrow Python source exists
    if not pyarrow_src.exists() or not (pyarrow_src / "setup.py").exists():
        print_error("PyArrow source not found in vendor/arrow/python")
        return False

    # Set environment for Arrow location
    env = os.environ.copy()
    env["ARROW_HOME"] = str(arrow_home)
    env["PYARROW_WITH_PARQUET"] = "1"
    env["PYARROW_WITH_FLIGHT"] = "1"
    env["PYARROW_WITH_DATASET"] = "1"
    env["PYARROW_WITH_COMPUTE"] = "1"
    env["PYARROW_WITH_CSV"] = "1"
    env["PYARROW_WITH_JSON"] = "1"
    env["PYARROW_INSTALL_TESTS"] = "0"

    print("Building PyArrow Python bindings...")

    # Build extension modules
    try:
        subprocess.run([
            sys.executable, "setup.py", "build_ext", "--inplace"
        ], cwd=pyarrow_src, env=env, check=True,
           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"PyArrow build_ext failed: {e.stderr.decode()}")
        return False

    print("Installing PyArrow in development mode...")

    # Install in development mode (no build isolation, no deps)
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", "-e", ".",
            "--no-build-isolation", "--no-deps"
        ], cwd=pyarrow_src, check=True,
           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"PyArrow install failed: {e.stderr.decode()}")
        return False

    # Verify installation
    try:
        import pyarrow
        pyarrow_file = Path(pyarrow.__file__)
        if "vendor/arrow/python" in str(pyarrow_file):
            print_success(f"PyArrow installed from vendored source (v{pyarrow.__version__})")
            return True
        else:
            print_error(f"PyArrow installed but not from vendored source: {pyarrow_file}")
            return False
    except ImportError as e:
        print_error(f"PyArrow import failed after install: {e}")
        return False


def build_rocksdb_cpp(skip=False):
    """Build RocksDB C++ library."""
    print_phase(3, 8, "Building RocksDB C++...")

    if skip:
        print_skip("Skipped by user (--skip-rocksdb)")
        return ROCKSDB_INSTALL if (ROCKSDB_INSTALL / "lib").exists() else None

    # Check if already built
    lib_extensions = [".so", ".dylib", ".dll", ".a"]
    rocksdb_lib_files = [ROCKSDB_INSTALL / "lib" / f"librocksdb{ext}" for ext in lib_extensions]

    if any(f.exists() for f in rocksdb_lib_files):
        print_success("Already built")
        return ROCKSDB_INSTALL

    # Check if RocksDB source exists
    if not ROCKSDB_VENDOR_DIR.exists():
        print_error("RocksDB source not found")
        print("Run: git submodule update --init --recursive vendor/rocksdb")
        return None

    # Check for CMake
    cmake = find_executable("cmake", ["cmake3"])
    if not cmake:
        print_error("CMake not found - install cmake first")
        return None

    # Create build directory
    rocksdb_build_dir = ROCKSDB_VENDOR_DIR / "build"
    rocksdb_build_dir.mkdir(parents=True, exist_ok=True)

    # Determine parallel jobs
    num_jobs = multiprocessing.cpu_count()

    print(f"Configuring RocksDB (using {num_jobs} parallel jobs)...")

    # CMake configuration for RocksDB
    cmake_args = [
        cmake,
        str(ROCKSDB_VENDOR_DIR),
        f"-DCMAKE_INSTALL_PREFIX={ROCKSDB_INSTALL}",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DWITH_TESTS=OFF",
        "-DWITH_BENCHMARK_TOOLS=OFF",
        "-DWITH_TOOLS=OFF",
        "-DWITH_CORE_TOOLS=OFF",
        "-DWITH_GFLAGS=OFF",
        "-DPORTABLE=ON",
        "-DUSE_RTTI=ON",
    ]

    try:
        subprocess.run(cmake_args, cwd=rocksdb_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"CMake configuration failed: {e.stderr.decode()}")
        return None

    print("Building RocksDB C++ (10-30 minutes on first build)...")

    build_args = [
        cmake,
        "--build", ".",
        "--config", "Release",
        "--parallel", str(num_jobs),
    ]

    try:
        subprocess.run(build_args, cwd=rocksdb_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"RocksDB build failed: {e.stderr.decode()}")
        return None

    # Install RocksDB
    install_args = [cmake, "--install", "."]
    try:
        subprocess.run(install_args, cwd=rocksdb_build_dir, check=True,
                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print_error(f"RocksDB installation failed: {e.stderr.decode()}")
        return None

    print_success(f"RocksDB C++ built successfully")
    return ROCKSDB_INSTALL


def build_duckdb_cpp(skip=False):
    """Build vendored DuckDB C++ library (Phase 3.5)."""
    print_phase("3.5", 8, "Building vendored DuckDB C++...")

    if skip:
        print_skip("Skipped by user (--skip-vendor)")
        return None

    if not DUCKDB_VENDOR_DIR.exists():
        print_error(f"DuckDB source not found at {DUCKDB_VENDOR_DIR}")
        return None

    # Check if already built
    if (DUCKDB_BUILD_DIR / "src").exists() and any((DUCKDB_BUILD_DIR / "src").glob("libduckdb.*")):
        print_success(f"DuckDB already built at {DUCKDB_BUILD_DIR}")
        return DUCKDB_BUILD_DIR

    cmake = find_executable('cmake', ['cmake3'])
    if not cmake:
        print_error("CMake not found")
        return None

    # Detect number of CPU cores
    try:
        num_jobs = multiprocessing.cpu_count()
    except:
        num_jobs = 4

    print(f"Building DuckDB C++ (using {num_jobs} parallel jobs)...")
    print(f"Source: {DUCKDB_VENDOR_DIR}")
    print(f"Build dir: {DUCKDB_BUILD_DIR}")

    # Create build directory
    DUCKDB_BUILD_DIR.mkdir(parents=True, exist_ok=True)

    # Configure DuckDB with CMake
    # DuckDB uses its own Makefile system, but we can use make directly
    print("Configuring and building DuckDB...")

    try:
        # DuckDB has a Makefile in the root, use it directly
        subprocess.run(
            ['make', 'release', f'-j{num_jobs}'],
            cwd=DUCKDB_VENDOR_DIR,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        print_success(f"DuckDB C++ built successfully")
        return DUCKDB_BUILD_DIR
    except subprocess.CalledProcessError as e:
        print_error(f"DuckDB build failed: {e.stderr.decode()[:200]}")
        return None


# ==============================================================================
# Phase 4: Build Vendored Python Extensions
# ==============================================================================

def build_cyredis(deps):
    """Build CyRedis extension."""
    if not deps['hiredis_available']:
        print_skip("CyRedis (hiredis not available)")
        return False

    if not CYREDIS_DIR.exists():
        print_skip("CyRedis (source not found)")
        return False

    print("Building CyRedis...")
    start = time.time()

    try:
        subprocess.run(
            [deps['python'], 'setup.py', 'build_ext', '--inplace'],
            cwd=CYREDIS_DIR,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        elapsed = time.time() - start
        print_success(f"CyRedis built ({elapsed:.1f}s)")
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"CyRedis build failed: {e.stderr.decode()[:200]}")
        return False


def build_rocksdb(deps):
    """Check for RocksDB library availability (now vendored)."""
    if not deps['rocksdb_built']:
        print_skip("RocksDB (library not built)")
        return False

    # Using vendored RocksDB library
    print_success(f"RocksDB library available at {ROCKSDB_INSTALL}")
    return True


def build_duckdb(deps):
    """Check for DuckDB library availability (now vendored)."""
    if not deps['duckdb_built']:
        print_skip("DuckDB (library not built)")
        return False

    # Using vendored DuckDB library
    print_success(f"DuckDB library available at {DUCKDB_BUILD_DIR}")
    return True


def build_tonbo(deps):
    """Check for Tonbo FFI library availability."""
    tonbo_ffi_lib = TONBO_FFI_DIR / "target" / "release" / "libtonbo_ffi.dylib"

    if not tonbo_ffi_lib.exists():
        print_skip(f"Tonbo FFI library not found (run: cd vendor/tonbo/tonbo-ffi && cargo build --release)")
        return False

    print_success(f"Tonbo FFI library found")
    return True


def build_vendor_extensions(deps, skip=False):
    """Build all vendored Python extensions."""
    print_phase(3, 7, "Building vendored Python extensions...")

    if skip:
        print_skip("Skipped by user (--skip-vendor)")
        return {
            'cyredis': False,
            'rocksdb': False,
            'duckdb': False,
            'tonbo': False,
            'marbledb': deps['marbledb_built'],  # MarbleDB is built separately
        }

    results = {
        'cyredis': build_cyredis(deps),
        'rocksdb': build_rocksdb(deps),
        'duckdb': build_duckdb(deps),
        'tonbo': build_tonbo(deps),
        'marbledb': deps['marbledb_built'],  # MarbleDB is built separately
    }

    return results


# ==============================================================================
# Phase 4: Discover Sabot Cython Modules
# ==============================================================================

def discover_cython_modules():
    """Discover all .pyx files in sabot/_cython/ and sabot/_c/."""
    print_phase(5, 8, "Discovering Sabot Cython modules...")

    # Scan both _cython and _c directories
    pyx_files = list(SABOT_CYTHON_DIR.rglob("*.pyx"))
    pyx_files_c = list(SABOT_C_DIR.rglob("*.pyx"))
    pyx_files.extend(pyx_files_c)

    # Categorize by dependencies
    modules = {
        'core': [],      # Arrow only
        'simple': [],    # No special deps
        'rocksdb': [],   # Needs RocksDB
        'duckdb': [],    # Needs DuckDB
        'tonbo': [],     # Needs Tonbo
        'marbledb': [],  # Needs MarbleDB
        'mixed': [],     # Needs both RocksDB and Tonbo
        'postgresql': [], # Needs PostgreSQL libpq
    }

    # Keywords to detect dependencies
    rocksdb_keywords = ['rocksdb', 'RocksDB']
    duckdb_keywords = ['duckdb', 'DuckDB']
    tonbo_keywords = ['tonbo', 'Tonbo']
    marbledb_keywords = ['marble', 'MarbleDB', 'LSMTree', 'lsm_storage']
    postgresql_keywords = ['libpq', 'postgresql', 'postgres']

    for pyx_file in pyx_files:
        # Get relative path (works for both _cython and _c dirs)
        if pyx_file.is_relative_to(SABOT_CYTHON_DIR):
            rel_path = pyx_file.relative_to(SABOT_CYTHON_DIR)
        elif pyx_file.is_relative_to(SABOT_C_DIR):
            # For _c/ files, include the _c prefix to make module names unique
            rel_path = Path("_c") / pyx_file.relative_to(SABOT_C_DIR)
        else:
            print_skip(f"Skipping file outside known directories: {pyx_file}")
            continue

        # Skip excluded modules
        if str(rel_path) in EXCLUDED_MODULES:
            print_skip(f"Excluded (known issues): {rel_path}")
            continue

        # Read file to check dependencies
        try:
            content = pyx_file.read_text()
            has_rocksdb = any(kw in content for kw in rocksdb_keywords)
            has_duckdb = any(kw in content for kw in duckdb_keywords)
            has_tonbo = any(kw in content for kw in tonbo_keywords)
            has_marbledb = any(kw in content for kw in marbledb_keywords)
            has_postgresql = any(kw in content for kw in postgresql_keywords)

            if has_rocksdb and has_tonbo:
                modules['mixed'].append(rel_path)
            elif has_rocksdb:
                modules['rocksdb'].append(rel_path)
            elif has_duckdb:
                modules['duckdb'].append(rel_path)
            elif has_tonbo:
                modules['tonbo'].append(rel_path)
            elif has_marbledb:
                modules['marbledb'].append(rel_path)
            elif has_postgresql:
                modules['postgresql'].append(rel_path)
            elif 'arrow' in content.lower() or 'flight' in content.lower():
                modules['core'].append(rel_path)
            else:
                modules['simple'].append(rel_path)
        except Exception as e:
            print_skip(f"Could not categorize {rel_path}: {e}")
            modules['simple'].append(rel_path)

    # Print summary
    print(f"Found {len(pyx_files)} Cython modules:")
    print(f"  - Core (Arrow): {len(modules['core'])}")
    print(f"  - Simple: {len(modules['simple'])}")
    print(f"  - RocksDB: {len(modules['rocksdb'])}")
    print(f"  - DuckDB: {len(modules['duckdb'])}")
    print(f"  - Tonbo: {len(modules['tonbo'])}")
    print(f"  - MarbleDB: {len(modules['marbledb'])}")
    print(f"  - PostgreSQL: {len(modules['postgresql'])}")
    print(f"  - Mixed: {len(modules['mixed'])}")

    return modules


# ==============================================================================
# Phase 5: Build Sabot Cython Extensions
# ==============================================================================

def build_sabot_extensions(deps, vendor_results, modules):
    """Build Sabot Cython extensions."""
    print_phase(6, 8, "Building Sabot Cython extensions...")

    # Import after checking dependencies
    try:
        import numpy as np
        from setuptools import setup, Extension
        from Cython.Build import cythonize
    except ImportError as e:
        print_error(f"Missing build dependencies: {e}")
        return {}

    # Common paths
    arrow_include = str(ARROW_INSTALL / "include")
    arrow_lib = str(ARROW_INSTALL / "lib")

    # Import cross-platform build utilities
    sys.path.insert(0, str(PROJECT_ROOT))
    from build_utils import get_compile_args

    common_include_dirs = [
        np.get_include(),
        arrow_include,
        str(PROJECT_ROOT / "vendor" / "arrow" / "python"),
        str(PROJECT_ROOT / "vendor" / "arrow" / "python" / "pyarrow"),
        str(PROJECT_ROOT / "vendor" / "arrow" / "python" / "pyarrow" / "src"),
        str(PROJECT_ROOT / "vendor" / "duckdb" / "third_party" / "concurrentqueue"),  # Lock-free queue
        str(SABOT_C_DIR),  # For morsel_executor.hpp
        str(SABOT_CYTHON_DIR / "graph" / "storage"),  # For graph_storage.h
        str(SABOT_CYTHON_DIR / "graph" / "traversal"),  # For bfs.h, dfs.h, etc.
        str(SABOT_CYTHON_DIR / "graph" / "query"),  # For pattern_match.h
    ]

    common_library_dirs = [arrow_lib]
    common_libraries = ["arrow", "arrow_flight"]

    # Get platform-appropriate SIMD compile args (ARM NEON or x86 AVX2)
    base_args = ["-std=c++17", "-Wno-unused-function", "-Wno-deprecated-declarations"]
    common_compile_args = get_compile_args(base_args)

    # Add rpath for runtime library loading (use @loader_path relative rpath on macOS)
    import sys
    if sys.platform == "darwin":
        # On macOS, use @loader_path relative paths for portability
        # and add headerpad for install_name_tool compatibility
        rel_path_to_arrow = os.path.relpath(arrow_lib, SABOT_CYTHON_DIR)
        common_link_args = [
            "-Wl,-headerpad_max_install_names",  # Allow rpath modification later
            f"-Wl,-rpath,@loader_path/{rel_path_to_arrow}",
            f"-Wl,-rpath,{arrow_lib}",  # Also add absolute path as fallback
        ]
    else:
        common_link_args = [
            f"-Wl,-rpath,{arrow_lib}",
        ]

    # Helper function to get module name and path
    def get_module_info(module_path):
        """Get module name and file path for a module_path."""
        if str(module_path).startswith('_c'):
            # Module from sabot/_c/
            rel_module_path = module_path.relative_to(Path("_c"))
            module_name = f"sabot._c.{str(rel_module_path.with_suffix('')).replace(os.sep, '.')}"
            pyx_path = str(SABOT_C_DIR / rel_module_path)
        else:
            # Module from sabot/_cython/
            module_name = f"sabot._cython.{str(module_path.with_suffix('')).replace(os.sep, '.')}"
            pyx_path = str(SABOT_CYTHON_DIR / module_path)
        return module_name, pyx_path

    # Build modules by category
    results = {'built': [], 'skipped': [], 'failed': []}

    # Always build: core + simple
    extensions_to_build = []

    for module_path in modules['core'] + modules['simple']:
        module_name, pyx_path = get_module_info(module_path)

        # Special cases: modules that need their .cpp files included
        sources = [pyx_path]
        if 'morsel_executor' in str(module_path):
            cpp_path = str(SABOT_C_DIR / "morsel_executor_impl.cpp")
            sources.append(cpp_path)
        elif 'task_slot_manager' in str(module_path):
            cpp_path = str(SABOT_C_DIR / "task_slot_manager_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/storage/graph_storage' in str(module_path):
            # Graph storage module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "storage" / "graph_storage_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/bfs' in str(module_path):
            # BFS traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "bfs_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/dfs' in str(module_path):
            # DFS traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "dfs_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/shortest_paths' in str(module_path):
            # Shortest paths traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "shortest_paths_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/pagerank' in str(module_path):
            # PageRank traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "pagerank_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/centrality' in str(module_path):
            # Centrality traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "centrality_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/triangle_counting' in str(module_path):
            # Triangle counting traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "triangle_counting_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/connected_components' in str(module_path):
            # Connected components traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "connected_components_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/traversal/strongly_connected_components' in str(module_path):
            # Strongly connected components traversal module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "traversal" / "strongly_connected_components_impl.cpp")
            sources.append(cpp_path)
        elif 'graph/query/pattern_match' in str(module_path):
            # Pattern match query module needs its C++ implementation
            cpp_path = str(SABOT_CYTHON_DIR / "graph" / "query" / "pattern_match_impl.cpp")
            sources.append(cpp_path)
        elif 'ipc_reader' in str(module_path):
            # ipc_reader is pure Cython wrapping Arrow C++ IPC
            # No additional .cpp files needed - uses Arrow C++ library directly
            pass

        extensions_to_build.append(Extension(
            module_name,
            sources,
            include_dirs=common_include_dirs,
            library_dirs=common_library_dirs,
            libraries=common_libraries,
            extra_compile_args=common_compile_args,
            extra_link_args=common_link_args,
            language="c++",
        ))

    # Conditionally build RocksDB modules
    if vendor_results['rocksdb']:
        for module_path in modules['rocksdb']:
            module_name, pyx_path = get_module_info(module_path)

            # Add vendored RocksDB paths
            rocksdb_include_dirs = common_include_dirs + [str(ROCKSDB_INSTALL / "include")]
            rocksdb_library_dirs = common_library_dirs + [str(ROCKSDB_INSTALL / "lib")]
            rocksdb_link_args = common_link_args + [f"-Wl,-rpath,{str(ROCKSDB_INSTALL / 'lib')}"]

            extensions_to_build.append(Extension(
                module_name,
                [pyx_path],
                include_dirs=rocksdb_include_dirs,
                library_dirs=rocksdb_library_dirs,
                libraries=common_libraries + ["rocksdb"],
                extra_compile_args=common_compile_args,
                extra_link_args=rocksdb_link_args,
                language="c++",
            ))
    else:
        results['skipped'].extend(modules['rocksdb'])

    # Conditionally build MarbleDB modules
    if vendor_results['marbledb']:
        for module_path in modules['marbledb']:
            module_name, pyx_path = get_module_info(module_path)

            # Add MarbleDB paths
            marbledb_include_dirs = common_include_dirs + [str(MARBLEDB_DIR / "include")]
            marbledb_library_dirs = common_library_dirs + [str(MARBLEDB_BUILD_DIR)]
            marbledb_link_args = common_link_args + [f"-Wl,-rpath,{str(MARBLEDB_BUILD_DIR)}"]

            extensions_to_build.append(Extension(
                module_name,
                [pyx_path],
                include_dirs=marbledb_include_dirs,
                library_dirs=marbledb_library_dirs,
                libraries=common_libraries + ["marble"],
                extra_compile_args=common_compile_args,
                extra_link_args=marbledb_link_args,
                language="c++",
            ))
    else:
        results['skipped'].extend(modules['marbledb'])

    # Conditionally build DuckDB modules
    if vendor_results['duckdb']:
        for module_path in modules['duckdb']:
            module_name, pyx_path = get_module_info(module_path)

            # Add vendored DuckDB paths
            duckdb_include_dirs = common_include_dirs + [str(DUCKDB_VENDOR_DIR / "src" / "include")]
            duckdb_library_dirs = common_library_dirs + [str(DUCKDB_BUILD_DIR / "src")]
            duckdb_link_args = common_link_args + [f"-Wl,-rpath,{str(DUCKDB_BUILD_DIR / 'src')}"]

            extensions_to_build.append(Extension(
                module_name,
                [pyx_path],
                include_dirs=duckdb_include_dirs,
                library_dirs=duckdb_library_dirs,
                libraries=common_libraries + ["duckdb"],
                extra_compile_args=common_compile_args,
                extra_link_args=duckdb_link_args,
                language="c++",
            ))
    else:
        results['skipped'].extend(modules['duckdb'])

    # Conditionally build Tonbo modules
    if vendor_results['tonbo']:
        for module_path in modules['tonbo']:
            module_name, pyx_path = get_module_info(module_path)

            # Add Tonbo FFI paths
            tonbo_include_dirs = common_include_dirs + [str(TONBO_FFI_DIR)]
            tonbo_library_dirs = common_library_dirs + [str(TONBO_FFI_DIR / "target" / "release")]
            tonbo_link_args = common_link_args + [f"-Wl,-rpath,{str(TONBO_FFI_DIR / 'target' / 'release')}"]

            extensions_to_build.append(Extension(
                module_name,
                [pyx_path],
                include_dirs=tonbo_include_dirs,
                library_dirs=tonbo_library_dirs,
                libraries=common_libraries + ["tonbo_ffi"],
                extra_compile_args=common_compile_args,
                extra_link_args=tonbo_link_args,
                language="c++",
            ))
    else:
        results['skipped'].extend(modules['tonbo'])

    # Conditionally build mixed modules
    if vendor_results['rocksdb'] and vendor_results['tonbo']:
        for module_path in modules['mixed']:
            module_name, pyx_path = get_module_info(module_path)

            # Add both vendored RocksDB and Tonbo FFI paths
            mixed_include_dirs = common_include_dirs + [
                str(ROCKSDB_INSTALL / "include"),
                str(TONBO_FFI_DIR)
            ]
            mixed_library_dirs = common_library_dirs + [
                str(ROCKSDB_INSTALL / "lib"),
                str(TONBO_FFI_DIR / "target" / "release")
            ]
            mixed_link_args = common_link_args + [
                f"-Wl,-rpath,{str(ROCKSDB_INSTALL / 'lib')}",
                f"-Wl,-rpath,{str(TONBO_FFI_DIR / 'target' / 'release')}"
            ]

            extensions_to_build.append(Extension(
                module_name,
                [pyx_path],
                include_dirs=mixed_include_dirs,
                library_dirs=mixed_library_dirs,
                libraries=common_libraries + ["rocksdb", "tonbo_ffi"],
                extra_compile_args=common_compile_args,
                extra_link_args=mixed_link_args,
                language="c++",
            ))
    else:
        results['skipped'].extend(modules['mixed'])

    # Conditionally build PostgreSQL modules
    # PostgreSQL libpq is usually available system-wide
    for module_path in modules['postgresql']:
        module_name, pyx_path = get_module_info(module_path)

        # PostgreSQL libpq paths (system installation)
        postgresql_include_dirs = common_include_dirs + ["/opt/homebrew/include/postgresql@14"]
        postgresql_library_dirs = common_library_dirs + [
            "/opt/homebrew/lib",
            "/opt/homebrew/Cellar/postgresql@14/14.19/lib/postgresql@14"
        ]
        postgresql_link_args = common_link_args + [
            "-Wl,-rpath,/opt/homebrew/lib",
            "-Wl,-rpath,/opt/homebrew/Cellar/postgresql@14/14.19/lib/postgresql@14"
        ]

        extensions_to_build.append(Extension(
            module_name,
            [pyx_path],
            include_dirs=postgresql_include_dirs,
            library_dirs=postgresql_library_dirs,
            libraries=common_libraries + ["pq"],  # libpq
            extra_compile_args=common_compile_args,
            extra_link_args=postgresql_link_args,
            language="c++",
        ))

    # Build all extensions - use individual compilation to identify all failures
    if extensions_to_build:
        print(f"Building {len(extensions_to_build)} Cython extensions individually...")
        print("(This is slower but provides accurate diagnostics)")
        start = time.time()

        import glob
        compiler_directives = {
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "nonecheck": False,
        }

        for i, ext in enumerate(extensions_to_build, 1):
            module_short_name = ext.name.split('.')[-1]
            print(f"[{i}/{len(extensions_to_build)}] Building {module_short_name}...", end=' ', flush=True)

            try:
                # Attempt to cythonize and build this single extension
                setup(
                    name="sabot",
                    ext_modules=cythonize(
                        [ext],
                        compiler_directives=compiler_directives,
                        quiet=True,
                    ),
                    script_args=['build_ext', '--inplace', '--quiet'],
                )

                # Check if .so file was created
                module_parts = ext.name.split('.')
                if module_parts[0] == 'sabot' and module_parts[1] == '_cython':
                    rel_path = '/'.join(module_parts[2:])
                    so_pattern = str(SABOT_CYTHON_DIR / f"{rel_path}*.so")

                    if glob.glob(so_pattern):
                        results['built'].append(ext.name)
                        print("✅")
                    else:
                        results['failed'].append(ext.name)
                        print("❌ (no .so)")
                else:
                    # Non-standard module path
                    results['failed'].append(ext.name)
                    print("❌ (path)")

            except Exception as e:
                results['failed'].append(ext.name)
                error_msg = str(e).split('\n')[0][:60]
                print(f"❌ ({error_msg}...)")

        elapsed = time.time() - start
        built_count = len(results['built'])
        failed_count = len(results['failed'])

        if built_count > 0:
            print_success(f"Built {built_count}/{len(extensions_to_build)} modules ({elapsed:.1f}s)")
        if failed_count > 0:
            print_error(f"Failed {failed_count}/{len(extensions_to_build)} modules")

    return results


# ==============================================================================
# Phase 6: Validation
# ==============================================================================

def validate_builds():
    """Validate that built modules can be imported."""
    print_phase(7, 8, "Validating builds...")

    # Find all .so files
    so_files = list(SABOT_CYTHON_DIR.rglob("*.so"))

    if not so_files:
        print_skip("No compiled modules found")
        return False

    print(f"Found {len(so_files)} compiled modules")

    # Check Python version matches
    py_version_tag = f"cpython-{sys.version_info.major}{sys.version_info.minor}"

    mismatched = []
    matched = []
    for so_file in so_files:
        if py_version_tag in so_file.name:
            matched.append(so_file.name)
        else:
            mismatched.append(so_file.name)

    if matched:
        print_success(f"{len(matched)} modules match Python {sys.version_info.major}.{sys.version_info.minor}")

    if mismatched:
        print_error(f"{len(mismatched)} modules have different Python version:")
        for name in mismatched[:5]:
            print(f"    {name}")

    # Return true if we have any valid modules
    return len(matched) > 0


# ==============================================================================
# Phase 7: Summary
# ==============================================================================

def print_summary(arrow_ok, vendor_results, cython_results, elapsed_total):
    """Print build summary."""
    print_phase(8, 8, f"Build Summary ({elapsed_total:.1f}s total)")

    print("\nVendored Libraries:")
    if arrow_ok:
        print_success("Arrow C++")
    else:
        print_error("Arrow C++ (failed)")

    for name, status in vendor_results.items():
        if status:
            print_success(name.capitalize())
        else:
            print_skip(name.capitalize())

    print("\nSabot Cython Modules:")
    built_count = len(cython_results.get('built', []))
    skipped_count = len(cython_results.get('skipped', []))
    failed_count = len(cython_results.get('failed', []))
    total = built_count + skipped_count + failed_count

    if built_count:
        print_success(f"Built: {built_count}/{total}")
    if skipped_count:
        print_skip(f"Skipped: {skipped_count}/{total} (missing dependencies)")
    if failed_count:
        print_error(f"Failed: {failed_count}/{total}")

    print(f"\n{GREEN}🎉 Build complete!{RESET}")
    print(f"\nNext steps:")
    print(f"  1. Test: DYLD_LIBRARY_PATH={ARROW_INSTALL}/lib:$DYLD_LIBRARY_PATH python test_agent_simple.py")
    print(f"  2. Run: sabot --help")


# ==============================================================================
# Clean Commands
# ==============================================================================

def clean_build_artifacts(clean_all=False):
    """Remove build artifacts."""
    print("Cleaning build artifacts...")

    patterns = [
        "**/*.so",
        "**/*.c",
        "**/*.cpp",
        "**/__pycache__",
        "**/*.pyc",
        "build/",
        "*.egg-info/",
    ]

    removed = 0
    for pattern in patterns:
        for path in PROJECT_ROOT.rglob(pattern):
            try:
                if path.is_file():
                    path.unlink()
                    removed += 1
                elif path.is_dir():
                    shutil.rmtree(path)
                    removed += 1
            except Exception as e:
                print_error(f"Could not remove {path}: {e}")

    print_success(f"Removed {removed} build artifacts")

    if clean_all:
        print("\nRemoving vendored builds...")
        dirs_to_remove = [
            ARROW_SOURCE / "build",
            CYREDIS_DIR / "build",
            ROCKSDB_DIR / "build",
        ]

        for dir_path in dirs_to_remove:
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    print_success(f"Removed {dir_path}")
                except Exception as e:
                    print_error(f"Could not remove {dir_path}: {e}")


# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Unified build system for Sabot")
    parser.add_argument('--clean', action='store_true', help="Remove build artifacts")
    parser.add_argument('--clean-all', action='store_true', help="Remove all builds including vendors")
    parser.add_argument('--skip-arrow', action='store_true', help="Skip Arrow C++ build")
    parser.add_argument('--skip-vendor', action='store_true', help="Skip all vendor builds")
    parser.add_argument('--dev', action='store_true', help="Debug build (not implemented yet)")
    parser.add_argument('--parallel', type=int, help="Override parallel jobs")

    args = parser.parse_args()

    # Handle clean
    if args.clean or args.clean_all:
        clean_build_artifacts(clean_all=args.clean_all)
        return 0

    # Start build
    start_time = time.time()

    print(f"{BLUE}╔════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BLUE}║           Sabot Unified Build System                      ║{RESET}")
    print(f"{BLUE}╚════════════════════════════════════════════════════════════╝{RESET}")

    # Phase 1: Check dependencies
    deps = check_dependencies()

    # Phase 2: Build Arrow C++
    arrow_install = build_arrow_cpp(skip=args.skip_arrow)
    if not arrow_install:
        print_error("Arrow C++ build failed - cannot continue")
        return 1

    # Phase 2.5: Build PyArrow from vendored source
    # NOTE: We don't actually need PyArrow installed - our Cython modules use .pxd files directly
    # pyarrow_ok = build_vendored_pyarrow(arrow_install, skip=args.skip_arrow)
    # if not pyarrow_ok:
    #     print_error("PyArrow build failed - cannot continue")
    #     return 1
    print_skip("PyArrow build skipped - using .pxd bindings directly")

    # Phase 3: Build RocksDB
    rocksdb_install = build_rocksdb_cpp(skip=args.skip_vendor)
    if not rocksdb_install:
        print_error("RocksDB C++ build failed - some modules will be skipped")
        # Don't fail - RocksDB modules will just be skipped

    # Phase 3.5: Build DuckDB
    duckdb_build = build_duckdb_cpp(skip=args.skip_vendor)
    if not duckdb_build:
        print_error("DuckDB C++ build failed - DuckDB connectors will be skipped")
        # Don't fail - DuckDB modules will just be skipped

    # Phase 4: Build vendor extensions
    vendor_results = build_vendor_extensions(deps, skip=args.skip_vendor)

    # Phase 5: Discover Cython modules
    modules = discover_cython_modules()

    # Phase 6: Build Sabot extensions
    cython_results = build_sabot_extensions(deps, vendor_results, modules)

    # Phase 7: Validate
    validation_ok = validate_builds()

    # Phase 8: Summary
    elapsed_total = time.time() - start_time
    print_summary(arrow_install is not None, vendor_results, cython_results, elapsed_total)

    return 0 if validation_ok else 1


if __name__ == "__main__":
    sys.exit(main())
