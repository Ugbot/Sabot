# MarbleDB CMake Build System - Library and Server Support

This document demonstrates how MarbleDB's CMake build system supports building both as an embeddable library and as a standalone server binary.

## Build Configuration Options

MarbleDB's CMakeLists.txt provides the following build options:

```cmake
option(MARBLE_BUILD_STATIC "Build static library" ON)
option(MARBLE_BUILD_SHARED "Build shared library" OFF)
option(MARBLE_BUILD_SERVER "Build MarbleDB server binary" ON)
option(MARBLE_BUILD_EXAMPLES "Build MarbleDB examples" ON)
option(MARBLE_BUILD_TESTS "Build MarbleDB tests" ON)
```

## Build Modes

### 1. Library-Only Build (Default)

```bash
# Build only static library
cmake .. -DMARBLE_BUILD_STATIC=ON -DMARBLE_BUILD_SHARED=OFF -DMARBLE_BUILD_SERVER=OFF
make

# Result: libmarble.a (static library)
```

### 2. Shared Library Build

```bash
# Build shared library
cmake .. -DMARBLE_BUILD_STATIC=OFF -DMARBLE_BUILD_SHARED=ON -DMARBLE_BUILD_SERVER=OFF
make

# Result: libmarble.so/libmarble.dylib (shared library)
```

### 3. Server Binary Build

```bash
# Build server binary
cmake .. -DMARBLE_BUILD_STATIC=ON -DMARBLE_BUILD_SHARED=OFF -DMARBLE_BUILD_SERVER=ON
make

# Result: libmarble.a (static library) + marbledb (server binary)
```

### 4. Full Build (Library + Server + Examples)

```bash
# Build everything
cmake .. -DMARBLE_BUILD_STATIC=ON -DMARBLE_BUILD_SHARED=ON -DMARBLE_BUILD_SERVER=ON -DMARBLE_BUILD_EXAMPLES=ON
make

# Result: libmarble.a, libmarble.so, marbledb server, examples
```

## Using MarbleDB as an Embedded Library

### CMake Integration

To use MarbleDB as an embedded library in your project:

```cmake
# Find MarbleDB (after installation)
find_package(MarbleDB REQUIRED)

# Link against the library
add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE MarbleDB::marble)

# Or link against shared library
target_link_libraries(my_app PRIVATE MarbleDB::marble_shared)
```

### C++ Usage Example

```cpp
#include <marble/db.h>
#include <iostream>

using namespace marble;

int main() {
    // Open/create a database
    std::unique_ptr<Database> db;
    auto status = Database::Open("./my_data", &db);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.message() << std::endl;
        return 1;
    }

    // Use the database...
    std::cout << "Database opened successfully!" << std::endl;

    return 0;
}
```

## Using MarbleDB as a Server

### Starting the Server

```bash
# Start server with default settings
./marbledb

# Start server with custom data directory
./marbledb --data-dir /path/to/data

# Start server with Raft consensus enabled
./marbledb --enable-raft --raft-port 9090

# Get help
./marbledb --help
```

### Server Configuration Options

```
MarbleDB Server

Usage: marbledb [options]

Options:
  --host HOST          Server host (default: 0.0.0.0)
  --port PORT          Server port (default: 8080)
  --data-dir DIR       Data directory (default: ./marble_data)
  --enable-raft        Enable Raft consensus
  --raft-port PORT     Raft port (default: 9090)
  --raft-cluster NODES Raft cluster nodes
  --disable-flight     Disable Arrow Flight server
  --flight-port PORT   Flight port (default: 8081)
  --help, -h           Show this help message
```

## CMake Target Organization

The CMake build system creates the following targets:

### Library Targets
- `marble_static` - Static library target
- `marble_shared` - Shared library target
- `marble` - Alias to the primary library (static by default)

### Executable Targets
- `marbledb_server` - Server binary (when MARBLE_BUILD_SERVER=ON)

### Example Targets
- Various example executables (when MARBLE_BUILD_EXAMPLES=ON)

### Test Targets
- `marble_tests` - Test executable (when MARBLE_BUILD_TESTS=ON)

## Installation

After building, install MarbleDB:

```bash
make install
```

This installs:
- Libraries: `lib/libmarble.a`, `lib/libmarble.so`
- Headers: `include/marble/*.h`
- Server binary: `bin/marbledb`
- CMake config files: `lib/cmake/MarbleDB/`

## Cross-Platform Compatibility

The build system supports:
- Linux (static/shared libraries, server binary)
- macOS (static/shared libraries, server binary)
- Windows (static/shared libraries, server binary - with MSVC)

## Dependencies

MarbleDB links against:
- Apache Arrow (bundled or system)
- Boost filesystem/system
- Threads
- OpenSSL (for Raft consensus)
- NuRaft (for distributed consensus, optional)

## Example Project Structure

```
my_project/
├── CMakeLists.txt
├── main.cpp
└── cmake/
    └── FindMarbleDB.cmake  # Or use installed config
```

### CMakeLists.txt Example

```cmake
cmake_minimum_required(VERSION 3.20)
project(MyApp)

find_package(MarbleDB REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE MarbleDB::marble)
```

## Architecture Benefits

1. **Embedded Usage**: Applications can embed MarbleDB directly
2. **Server Mode**: Standalone server for client-server architectures
3. **Library Evolution**: Same codebase serves both use cases
4. **Flexible Deployment**: Choose static/shared linking as needed
5. **Cross-Platform**: Consistent build system across platforms

This dual-mode build system makes MarbleDB suitable for a wide range of deployment scenarios, from embedded analytics in applications to standalone analytical databases.
