# MarbleDB Project Structure

This document outlines the clean, organized project structure for MarbleDB.

## 📁 Root Directory Structure

```
MarbleDB/
├── CMakeLists.txt           # Main build configuration
├── PROJECT_STRUCTURE.md     # This file
├── README.md               # Project overview and getting started
├── build.log              # Build output (generated)
│
├── cmake/                  # CMake configuration files
│   └── MarbleDBConfig.cmake.in
│
├── docs/                   # All documentation
│   ├── README.md          # Documentation overview
│   ├── api/               # API reference docs
│   │   └── API_SURFACE.md # Complete API documentation
│   └── guides/            # User guides and tutorials
│       ├── README.md      # Main README
│       ├── README_API.md  # API usage guide
│       └── [various guides...]
│
├── examples/               # Code examples organized by category
│   ├── basic/             # Simple usage examples
│   │   ├── basic_example.cpp
│   │   ├── basic_table_example.cpp
│   │   ├── simple_embedded_example.cpp
│   │   ├── simple_record_demo.cpp
│   │   ├── skipping_index_concept_demo.cpp
│   │   └── CMakeLists.txt
│   ├── advanced/          # Complex feature examples
│   │   ├── advanced_queries_example.cpp
│   │   ├── analytics_performance_example.cpp
│   │   ├── arctic_bitemporal_demo.cpp
│   │   ├── [many more advanced examples...]
│   │   └── CMakeLists.txt
│   ├── language_bindings/ # Language-specific examples
│   │   ├── c_api_example.c
│   │   ├── python_api_example.py
│   │   └── CMakeLists.txt
│   ├── demos/             # Interactive demonstrations
│   │   ├── pushdown_demo.cpp
│   │   └── CMakeLists.txt
│   └── CMakeLists.txt     # Examples build configuration
│
├── include/               # Public header files
│   └── marble/           # Main include directory
│       ├── api.h         # C++ API
│       ├── c_api.h       # C API for language bindings
│       ├── [other headers...]
│       └── raft/         # Raft-specific headers
│
├── src/                   # Source code implementation
│   ├── CMakeLists.txt    # Source build configuration
│   ├── main.cpp          # Optional main executable
│   ├── core/             # Core database functionality
│   │   ├── api.cpp       # C++ API implementation
│   │   ├── c_api.cpp     # C API implementation
│   │   ├── db.cpp        # Database core
│   │   └── [other core files...]
│   ├── raft/             # Raft consensus implementation
│   │   ├── raft_server.cpp
│   │   ├── marble_log_store.cpp
│   │   └── [other raft files...]
│   ├── compaction/       # Compaction logic
│   ├── storage/          # Storage utilities
│   └── util/             # Utility functions
│       └── status.cpp
│
├── scripts/               # Build and utility scripts
│   ├── advanced_queries_demo.sh
│   ├── duckdb_optimizations_demo.sh
│   ├── raft_demo.sh
│   └── [other demo scripts...]
│
├── tests/                 # Comprehensive test suite
│   ├── CMakeLists.txt    # Test build configuration
│   ├── README.md         # Test documentation
│   ├── run_tests.sh      # Test runner script
│   ├── test_utils.h      # Test utilities header
│   ├── test_utils.cpp    # Test utilities implementation
│   ├── unit/             # Unit tests (isolated components)
│   │   ├── test_record_system.cpp
│   │   ├── test_pushdown.cpp
│   │   └── test_status.cpp
│   ├── integration/      # Integration tests (component interaction)
│   │   ├── test_query_execution.cpp
│   │   ├── test_marble_core.cpp
│   │   └── test_arctic_bitemporal.cpp
│   └── performance/      # Performance tests and benchmarks
│       └── test_pushdown_performance.cpp
│
├── benchmarks/            # Performance benchmarking tools
│   ├── CMakeLists.txt
│   ├── common.hpp
│   └── write_bench.cpp
│
└── vendor/                # External dependencies
    └── nuraft/           # Raft consensus library
```

## 🏗️ Directory Purposes

### Core Directories
- **`include/marble/`**: Public API headers - what users include
- **`src/`**: Implementation - internal code organization
- **`examples/`**: Usage examples - organized by complexity and feature
- **`tests/`**: Test suite - comprehensive coverage with proper organization

### Organization Directories
- **`docs/`**: All documentation in structured subdirectories
- **`scripts/`**: Build and demo scripts (not part of main build)
- **`cmake/`**: CMake configuration files
- **`vendor/`**: External dependencies (managed separately)

### Build Artifacts (Generated)
- **`build/`**: CMake build directory (generated, can be cleaned)
- **`build.log`**: Build output (generated, can be ignored)

## 📋 File Organization Principles

### 1. **Clear Separation of Concerns**
- Headers in `include/`, implementations in `src/`
- Tests separate from main code
- Examples separate from core functionality
- Documentation centralized in `docs/`

### 2. **Logical Grouping**
- Examples grouped by complexity: `basic/`, `advanced/`, `language_bindings/`
- Tests grouped by type: `unit/`, `integration/`, `performance/`
- Documentation grouped by purpose: `api/`, `guides/`

### 3. **Build System Integration**
- Each major directory has its own `CMakeLists.txt`
- Clear dependencies and build ordering
- Test organization matches CMake structure

### 4. **Developer Experience**
- Scripts in dedicated directory
- Benchmarks separate from tests
- Clear naming conventions
- Comprehensive documentation

## 🚀 Usage Patterns

### For Users
```bash
# Build and install
cmake -B build
make -C build install

# Use from CMake project
find_package(MarbleDB REQUIRED)
target_link_libraries(my_app marble_c_api)
```

### For Developers
```bash
# Run all tests
cd tests && ./run_tests.sh all

# Build examples
make -C build c_api_example
./build/c_api_example

# Run performance tests
cd tests && ./run_tests.sh performance
```

### For Contributors
```bash
# Add new example
# Put in examples/basic/ for simple examples
# Put in examples/advanced/ for complex features

# Add new test
# Unit tests go in tests/unit/
# Integration tests go in tests/integration/

# Add documentation
# API docs go in docs/api/
# Guides go in docs/guides/
```

## 🔧 Maintenance

### Clean Build
```bash
# Clean everything
rm -rf build/
cd tests && ./run_tests.sh clean

# Fresh build
cmake -B build
make -C build -j$(nproc)
```

### Update Structure
When adding new components:
1. Choose appropriate directory based on purpose
2. Update relevant `CMakeLists.txt` files
3. Add to this documentation
4. Ensure tests are included

## 📊 Project Health

This structure ensures:
- **Clear organization**: Easy to find files and understand purpose
- **Build efficiency**: Proper dependency management
- **Developer productivity**: Intuitive navigation and usage
- **Maintainability**: Clear separation of concerns
- **Scalability**: Easy to add new components without clutter

---

**Last updated**: Files organized for clean, maintainable project structure with clear separation of concerns and logical grouping.
