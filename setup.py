#!/usr/bin/env python3
"""
Setup script for Sabot - High-performance columnar streaming engine.

This setup script provides a traditional setuptools-based installation
while pyproject.toml provides the modern configuration.
"""

import os
import sys
from pathlib import Path
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
import subprocess
import glob

# Add current directory to path so we can import sabot
sys.path.insert(0, os.path.dirname(__file__))

try:
    # Try to import Cython
    from Cython.Build import cythonize
    HAS_CYTHON = True
except ImportError:
    HAS_CYTHON = False

# Detect system libraries and include paths
def detect_system_libs():
    """Detect system libraries and return include/library paths."""

    # NumPy
    numpy_include = None
    try:
        import numpy
        numpy_include = numpy.get_include()
    except ImportError:
        pass

    # PyArrow (use vendored Arrow from submodule)
    arrow_include = None
    arrow_cpp_include = None
    arrow_cpp_api_include = None
    arrow_cpp_build_include = None
    arrow_libs = []

    # Use vendored Arrow from vendor/arrow
    # For direct C++ bindings, we use the C++ headers directly
    arrow_cpp_api_dir = os.path.join(os.path.dirname(__file__), "vendor", "arrow", "cpp", "src")
    arrow_cpp_build_dir = os.path.join(os.path.dirname(__file__), "vendor", "arrow", "cpp", "build", "install", "include")
    arrow_lib_dir = os.path.join(os.path.dirname(__file__), "vendor", "arrow", "cpp", "build", "install", "lib")

    # Vendored Arrow Python bindings (.pxd files for Flight, etc)
    arrow_python_dir = os.path.join(os.path.dirname(__file__), "vendor", "arrow", "python")
    arrow_cpp_src_dir = arrow_cpp_api_dir

    # Check for our vendored Arrow C++ build first (preferred)
    if os.path.exists(arrow_lib_dir) and os.path.exists(arrow_cpp_build_dir):
        arrow_cpp_api_include = arrow_cpp_api_dir if os.path.exists(arrow_cpp_api_dir) else None
        arrow_cpp_build_include = arrow_cpp_build_dir
        arrow_libs = [arrow_lib_dir]
        print("✅ Using vendored Arrow C++ build for direct bindings")
        print(f"   Headers: {arrow_cpp_build_include}")
        print(f"   Libraries: {arrow_lib_dir}")
    else:
        # Fallback to PyArrow for legacy compatibility
        try:
            import pyarrow as pa
            arrow_include = pa.get_include()
            arrow_cpp_include = arrow_include
            arrow_cpp_api_include = arrow_include
            arrow_cpp_build_include = arrow_include
            arrow_libs = pa.get_library_dirs()
            print(f"⚠️  Using installed PyArrow fallback: {pa.__version__}")
            print("   (Consider building vendored Arrow for better performance)")
        except ImportError:
            print("❌ No Arrow installation found - direct C++ bindings unavailable")

    # RocksDB (system installation)
    rocksdb_include = None
    rocksdb_lib = None

    # Check common system paths
    system_paths = [
        "/opt/homebrew",  # macOS Homebrew
        "/usr/local",     # macOS/Linux local
        "/usr",           # Linux system
    ]

    for base_path in system_paths:
        rocksdb_inc = os.path.join(base_path, "include", "rocksdb")
        rocksdb_lib_path = os.path.join(base_path, "lib")

        if os.path.exists(rocksdb_inc) and os.path.exists(rocksdb_lib_path):
            # Check for librocksdb
            rocksdb_libs = glob.glob(os.path.join(rocksdb_lib_path, "librocksdb*"))
            if rocksdb_libs:
                rocksdb_include = os.path.dirname(rocksdb_inc)
                rocksdb_lib = rocksdb_lib_path
                break

    # Tonbo FFI (vendored Rust library)
    tonbo_include = None
    tonbo_lib = None
    tonbo_ffi_dir = os.path.join(os.path.dirname(__file__), "tonbo", "tonbo-ffi")
    tonbo_header = os.path.join(tonbo_ffi_dir, "tonbo_ffi.h")
    tonbo_lib_dir = os.path.join(tonbo_ffi_dir, "target", "release")

    if os.path.exists(tonbo_header) and os.path.exists(tonbo_lib_dir):
        # Check for libtonbo_ffi
        tonbo_libs = (
            glob.glob(os.path.join(tonbo_lib_dir, "libtonbo_ffi.so")) +
            glob.glob(os.path.join(tonbo_lib_dir, "libtonbo_ffi.dylib")) +
            glob.glob(os.path.join(tonbo_lib_dir, "libtonbo_ffi.a"))
        )
        if tonbo_libs:
            tonbo_include = tonbo_ffi_dir
            tonbo_lib = tonbo_lib_dir
            print(f"✅ Found Tonbo FFI library: {tonbo_libs[0]}")
        else:
            print(f"⚠️  Tonbo FFI not built yet - run: cd tonbo/tonbo-ffi && cargo build --release")
    else:
        print("⚠️  Tonbo FFI directory not found")

    return {
        'numpy_include': numpy_include,
        'arrow_include': arrow_include,
        'arrow_cpp_include': arrow_cpp_include,
        'arrow_cpp_api_include': arrow_cpp_api_include,
        'arrow_cpp_build_include': arrow_cpp_build_include,
        'arrow_python_dir': arrow_python_dir,
        'arrow_libs': arrow_libs,
        'rocksdb_include': rocksdb_include,
        'rocksdb_lib': rocksdb_lib,
        'tonbo_include': tonbo_include,
        'tonbo_lib': tonbo_lib,
    }

# Check for existing .pyx files
def find_pyx_files():
    """Find all .pyx files in the sabot package only."""
    pyx_files = []
    sabot_dir = Path(__file__).parent / "sabot"

    # Build internal Cython extensions
    # All extensions use internal implementations, no external C dependencies
    # Building only modules that compile cleanly without .pxd declarations
    working_extensions = [
        # Core state management (WORKING - builds successfully)
        "sabot/_cython/state/state_backend.pyx",
        "sabot/_cython/state/value_state.pyx",
        "sabot/_cython/state/list_state.pyx",
        "sabot/_cython/state/map_state.pyx",
        "sabot/_cython/state/reducing_state.pyx",
        "sabot/_cython/state/aggregating_state.pyx",
        "sabot/_cython/state/rocksdb_state.pyx",
        "sabot/_cython/state/tonbo_state.pyx",  # Re-enabled to fix compilation issues

        # Time management (WORKING - builds successfully)
        "sabot/_cython/time/timers.pyx",
        "sabot/_cython/time/watermark_tracker.pyx",
        "sabot/_cython/time/time_service.pyx",
        "sabot/_cython/time/event_time.pyx",

        # Checkpoint coordination (WORKING - builds successfully)
        "sabot/_cython/checkpoint/barrier.pyx",
        "sabot/_cython/checkpoint/barrier_tracker.pyx",
        "sabot/_cython/checkpoint/coordinator.pyx",
        "sabot/_cython/checkpoint/storage.pyx",
        "sabot/_cython/checkpoint/recovery.pyx",

        # Storage backends (WORKING - builds successfully)
        "sabot/_cython/stores_base.pyx",
        "sabot/_cython/stores_memory.pyx",

        # Core processing modules
        "sabot/_cython/agents.pyx",
        # "sabot/_cython/windows.pyx",  # TODO: Multiple cdef in control flow statements (needs extensive fixes)
        # "sabot/_cython/joins.pyx",  # TODO: Complex nested templates with PyObject* causing typedef issues
        "sabot/_cython/channels.pyx",
        # "sabot/_cython/morsel_parallelism.pyx",  # TODO: AtomicCounter and FastMorsel pointer issues (advanced feature)
        # "sabot/_cython/materialized_views.pyx",  # TODO: Same PyObject* template issue as joins

        # Tonbo LSM storage wrapper
        "sabot/_cython/tonbo_wrapper.pyx",  # Tonbo Cython wrapper
        "sabot/_cython/tonbo_arrow.pyx",  # Tonbo Arrow integration

        # Internal Arrow implementation (simple, no C API)
        # "sabot/_cython/arrow_core_simple.pyx",  # Pure Python fallback, no C++ libs needed - still requires arrow headers

        # Zero-copy Arrow compute functions
        "sabot/_c/arrow_core.pyx",  # Core zero-copy compute functions (window_ids, sort_and_take, hash_join)
        "sabot/_c/data_loader.pyx",  # High-performance data loader (CSV, Arrow IPC)
        "sabot/_c/materialization_engine.pyx",  # Unified materialization (dimension tables + analytical views)

        # Zero-copy Arrow operators (using cimport pyarrow.lib)
        "sabot/core/_ops.pyx",  # Core zero-copy stream operators
        "sabot/_cython/arrow/batch_processor.pyx",  # Zero-copy RecordBatch processor
        "sabot/_cython/arrow/window_processor.pyx",  # Window processing
        "sabot/_cython/arrow/join_processor.pyx",  # Join operations
        # "sabot/_cython/arrow/flight_client.pyx",  # Arrow Flight client - needs fixes

        # Streaming operators (Phase 1: Transform operators)
        "sabot/_cython/operators/transform.pyx",  # map, filter, select, flatMap, union

        # Streaming operators (Phase 2: Aggregation operators)
        "sabot/_cython/operators/aggregations.pyx",  # groupBy, reduce, aggregate, distinct

        # Streaming operators (Phase 3: Join operators)
        "sabot/_cython/operators/joins.pyx",  # hash join, interval join, as-of join

        # Shuffle operators (Phase 4: Operator parallelism)
        "sabot/_cython/shuffle/partitioner.pyx",  # Hash/Range/RoundRobin partitioning
        "sabot/_cython/shuffle/shuffle_buffer.pyx",  # Buffer management with spill-to-disk

        # Lock-free shuffle transport (LMAX Disruptor-style)
        "sabot/_cython/shuffle/lock_free_queue.pyx",  # SPSC/MPSC lock-free ring buffers
        "sabot/_cython/shuffle/atomic_partition_store.pyx",  # Atomic hash table
        "sabot/_cython/shuffle/flight_transport_lockfree.pyx",  # Lock-free Flight transport (uses vendored Arrow)

        "sabot/_cython/shuffle/flight_transport.pyx",  # Arrow Flight C++ wrapper (legacy)
        "sabot/_cython/shuffle/shuffle_transport.pyx",  # Arrow Flight-based network transport
        "sabot/_cython/shuffle/shuffle_manager.pyx",  # Shuffle coordination
    ]

    for pyx_path in working_extensions:
        full_path = Path(__file__).parent / pyx_path
        if full_path.exists():
            pyx_files.append(pyx_path)
        else:
            print(f"Warning: {pyx_path} not found, skipping")

    # Skip complex extensions for now
    # TODO: Add back once basic build works
    # for pyx_file in sabot_dir.rglob("*.pyx"):
    #     if not str(pyx_file).startswith(str(sabot_dir)):
    #         continue
    #
    #     skip_files = [
    #         "arrow_core.pyx",
    #         "arrow_core_simple.pyx",
    #     ]
    #     if pyx_file.name in skip_files:
    #         continue
    #
    #     pyx_files.append(str(pyx_file.relative_to(sabot_dir.parent)))

    return sorted(pyx_files)  # Sort for consistent builds

def create_extensions():
    """Create Cython extensions with proper library detection."""
    extensions = []
    system_libs = None  # Make available outside if block

    if HAS_CYTHON:
        pyx_files = find_pyx_files()
        system_libs = detect_system_libs()

        print(f"Building {len(pyx_files)} Cython extensions...")
        print(f"Detected libraries: {system_libs}")

        for pyx_file in pyx_files:
            module_name = pyx_file.replace("/", ".").replace("\\", ".").replace(".pyx", "")

            # Base include dirs (always include NumPy if available)
            # Also include sabot root for internal cimports (e.g., sabot._c.arrow_core)
            include_dirs = ["."]  # Current directory for sabot.* imports
            if system_libs['numpy_include']:
                include_dirs.append(system_libs['numpy_include'])

            # Base library dirs
            library_dirs = []
            libraries = []

            # Special handling for different extension types
            if "rocksdb" in pyx_file.lower():
                # RocksDB extension needs special linking
                if system_libs['rocksdb_include']:
                    include_dirs.append(system_libs['rocksdb_include'])
                if system_libs['rocksdb_lib']:
                    library_dirs.append(system_libs['rocksdb_lib'])

                libraries = ["rocksdb"]
                language = "c++"

                print(f"Configuring RocksDB extension: {module_name}")
                print(f"  Include dirs: {include_dirs}")
                print(f"  Library dirs: {library_dirs}")

            elif "arrow" in pyx_file.lower() or "core/_ops" in pyx_file or "materialization_engine" in pyx_file or "shuffle" in pyx_file.lower():
                # Arrow extensions - different handling based on implementation
                if system_libs['arrow_include']:
                    include_dirs.append(system_libs['arrow_include'])
                if system_libs['arrow_cpp_include']:
                    include_dirs.append(system_libs['arrow_cpp_include'])
                if system_libs['arrow_cpp_api_include']:
                    include_dirs.append(system_libs['arrow_cpp_api_include'])
                if system_libs['arrow_cpp_build_include']:
                    include_dirs.append(system_libs['arrow_cpp_build_include'])
                # Add vendored Arrow Python bindings for Flight .pxd files
                if system_libs['arrow_python_dir']:
                    include_dirs.append(system_libs['arrow_python_dir'])

                # Check if this module uses cimport pyarrow.lib directly or imports from modules that do
                try:
                    with open(pyx_file, 'r') as f:
                        content = f.read()
                        if ("cimport pyarrow.lib" in content or
                            "from .batch_processor cimport" in content or
                            "_c/arrow_core" in pyx_file):
                            # Uses cimport pyarrow.lib directly or imports from modules that do - no external Arrow libs needed
                            libraries = []
                            print(f"Configuring Arrow extension (cimport pyarrow.lib, no external linking): {module_name}")
                        elif "flight_transport" in pyx_file:
                            # Flight transport uses C++ Flight API - needs arrow_flight library
                            if system_libs['arrow_libs']:
                                library_dirs.extend(system_libs['arrow_libs'])
                            libraries = ["arrow", "arrow_flight"]
                            print(f"Configuring Arrow Flight extension (linking arrow_flight): {module_name}")
                        elif ("import pyarrow" in content and "cimport pyarrow" not in content):
                            # Uses Python-level pyarrow imports only - no external linking needed
                            libraries = []
                            print(f"Configuring Arrow extension (Python pyarrow imports, no external linking): {module_name}")
                        else:
                            # Legacy Arrow usage - link libarrow
                            if system_libs['arrow_libs']:
                                library_dirs.extend(system_libs['arrow_libs'])
                            libraries = ["arrow"]
                            print(f"Configuring Arrow extension (linking against libarrow): {module_name}")
                except:
                    # Fallback
                    libraries = []
                    print(f"Configuring Arrow extension (fallback): {module_name}")

                language = "c++"

            elif "redis" in pyx_file.lower():
                # Redis extensions (requires hiredis)
                libraries = ["hiredis"]
                language = "c++"

                print(f"Configuring Redis extension: {module_name}")

            elif "tonbo" in pyx_file.lower():
                # Tonbo extensions (Rust FFI via C bindings)
                if system_libs['tonbo_include']:
                    include_dirs.append(system_libs['tonbo_include'])
                if system_libs['tonbo_lib']:
                    library_dirs.append(system_libs['tonbo_lib'])
                    libraries = ["tonbo_ffi"]
                    print(f"Configuring Tonbo extension with FFI: {module_name}")
                    print(f"  Include dirs: {include_dirs}")
                    print(f"  Library dirs: {library_dirs}")
                else:
                    print(f"⚠️  Configuring Tonbo extension WITHOUT FFI (not built): {module_name}")

                language = "c++"

            else:
                # Standard Cython extension - use C++ since we use libcpp everywhere
                language = "c++"

                # Add rocksdb includes to all modules since many modules import rocksdb_state
                if system_libs['rocksdb_include'] and system_libs['rocksdb_include'] not in include_dirs:
                    include_dirs.append(system_libs['rocksdb_include'])

            # Create the extension
            ext = Extension(
                module_name,
                [pyx_file],
                include_dirs=include_dirs,
                libraries=libraries,
                library_dirs=library_dirs,
                extra_compile_args=[
                    "-O3",
                    "-march=native",
                    "-mtune=native",
                    "-Wno-unused-function",
                    "-Wno-maybe-uninitialized",
                ],
                extra_link_args=[],
                language=language,
            )

            # Add C++17 for C++ extensions
            if language == "c++":
                ext.extra_compile_args.append("-std=c++17")

            # For modules that cimport pyarrow.lib, don't link external Arrow libs
            # PyArrow provides all necessary Arrow functionality internally
            if ("arrow" in pyx_file.lower() or "core/_ops" in pyx_file):
                if "cimport pyarrow.lib" in open(pyx_file).read():
                    # These modules use cimport pyarrow.lib, so don't add external Arrow libs
                    pass
                elif system_libs['arrow_libs']:
                    arrow_lib_dir = system_libs['arrow_libs'][0]
                    ext.extra_link_args.extend([
                        f"-Wl,-rpath,{arrow_lib_dir}",
                    ])

            extensions.append(ext)

    return extensions, system_libs

class BuildExt(build_ext):
    """Custom build_ext command to handle Cython compilation."""

    def build_extensions(self):
        # Set compiler flags for optimization
        if self.compiler.compiler_type == "unix":
            for ext in self.extensions:
                ext.extra_compile_args.extend([
                    "-Wno-unused-function",
                    "-Wno-maybe-uninitialized",
                ])

        super().build_extensions()

# Read README for long description
this_directory = Path(__file__).parent
readme_path = this_directory / "README.md"
long_description = ""
if readme_path.exists():
    long_description = readme_path.read_text()

setup(
    name="sabot",
    version="0.1.0",
    description="High-performance columnar streaming with Arrow + Faust-inspired agents",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Sabot Team",
    author_email="team@sabot.io",
    url="https://github.com/sabot/sabot",
    license="Apache-2.0",

    packages=find_packages(exclude=["tests*", "examples*", "docs*"]),
    include_package_data=True,

    # Cython extensions
    ext_modules=(lambda: (
        lambda exts, libs: cythonize(
            exts,
            compiler_directives={
                'language_level': "3",
                'boundscheck': False,
                'wraparound': False,
                'initializedcheck': False,
                'nonecheck': False,
                'cdivision': True,
                'embedsignature': True,
            },
            build_dir="build",
            include_path=[
                ".",  # Current directory for sabot.* imports
                libs['arrow_include'] if libs and libs['arrow_include'] else ".",
                libs['arrow_cpp_build_include'] if libs and libs.get('arrow_cpp_build_include') else ".",
            ],
        )
    )(*create_extensions()) if HAS_CYTHON else create_extensions()[0]
    )(),

    cmdclass={
        'build_ext': BuildExt,
    },

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],

    python_requires=">=3.8",

    # Core dependencies (minimal set - using internal Cython implementations)
    install_requires=[
        # NO pyarrow - using internal Cython implementation
        # NO rocksdb - using internal Cython implementation with SQLite fallback
        "uvloop>=0.17.0",
        "structlog>=22.0.0",
        "rich>=12.0.0",
        "typer>=0.7.0",
        "pydantic>=2.0.0",
        "orjson>=3.9.0",
        "msgpack>=1.0.0",  # Binary serialization for distributed systems
        "avro>=1.11.0",  # Avro serialization for Kafka compatibility
        "protobuf>=4.0.0",  # Protobuf serialization (C++ accelerated)
        "jsonschema>=4.0.0",  # JSON Schema validation for Kafka
        "httpx>=0.24.0",  # HTTP client for Schema Registry
        "numpy>=1.21.0",  # Still needed for numerical operations
        "psutil>=5.9.0",
        "prometheus-client>=0.19.0",
        "mode>=2.0.0",
        "sqlalchemy>=2.0.0",
        "alembic>=1.12.0",
        "Cython>=3.0.0",  # Required for building extensions
    ],

    # Optional dependencies (for compatibility or advanced features)
    extras_require={
        # External libraries (optional, internal implementations preferred)
        "external-arrow": ["pyarrow>=10.0.0"],  # Optional: use if you need full Arrow C++ API
        "external-rocksdb": ["rocksdb>=1.0.0"],  # Optional: use if you need Python RocksDB binding
        "flight": ["pyarrow[flight]>=10.0.0"],  # Flight requires full pyarrow

        # GPU acceleration
        "gpu": ["cudf>=22.0.0", "cupy>=12.0.0", "raft-dask>=23.0.0", "pylibraft>=23.0.0"],

        # Additional integrations
        "sql": ["duckdb>=0.7.0"],
        "kafka": [
            "aiokafka>=0.8.0",
            "confluent-kafka[avro,protobuf,schema-registry]>=2.0.0",
        ],
        "redis": ["redis>=5.0.0", "hiredis>=2.0.0"],

        # Channel support (using internal implementations)
        "channels": [
            "sabot[kafka]",
            "sabot[redis]",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.12.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.3.0",
            "myst-parser>=2.0.0",
            "jupyter>=1.0.0",
            "ipykernel>=6.0.0",
            "notebook>=7.0.0",
            "memory-profiler>=0.61.0",
            "line-profiler>=4.1.0",
        ],
        "all": [
            "sabot[flight,gpu,sql,dev]",
        ],
    },

    # Entry points for command-line scripts
    entry_points={
        "console_scripts": [
            "sabot = sabot.cli:main",
            "sabot-worker = sabot.cli:worker",
            "sabot-topics = sabot.cli:topics",
            "sabot-agents = sabot.cli:agents",
            "sabot-interactive = sabot.cli:interactive",
            "sabot-web = sabot.cli:web",
        ],
    },

    # Package data
    package_data={
        "sabot": ["py.typed"],
    },

    zip_safe=False,
)