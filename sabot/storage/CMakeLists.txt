cmake_minimum_required(VERSION 3.15)
project(sabot_storage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find dependencies
# Arrow - try to find it, otherwise use common paths
set(ARROW_ROOT "${CMAKE_SOURCE_DIR}/../../../vendor/arrow/cpp/build/install")
if(EXISTS "${ARROW_ROOT}/include")
    set(Arrow_INCLUDE_DIRS "${ARROW_ROOT}/include")
    set(Arrow_LIBRARIES "${ARROW_ROOT}/lib/libarrow.a")
    message(STATUS "Found Arrow in vendor directory: ${Arrow_INCLUDE_DIRS}")
else()
    # Try system Arrow
    find_package(Arrow QUIET)
    if(Arrow_FOUND AND Arrow_INCLUDE_DIRS)
        message(STATUS "Found Arrow via find_package: ${Arrow_INCLUDE_DIRS}")
    else()
        # Fallback to common Homebrew path
        if(EXISTS "/opt/homebrew/include/arrow")
            set(Arrow_INCLUDE_DIRS "/opt/homebrew/include")
            set(Arrow_LIBRARIES "/opt/homebrew/lib/libarrow.a")
            message(STATUS "Found Arrow in Homebrew: ${Arrow_INCLUDE_DIRS}")
        else()
            message(FATAL_ERROR "Could not find Arrow headers. Please install Arrow or set ARROW_ROOT.")
        endif()
    endif()
endif()

# Find MarbleDB (may not have FindMarbleDB.cmake, so set manually)
set(MARBLE_ROOT "${CMAKE_SOURCE_DIR}/../../MarbleDB")
set(MarbleDB_INCLUDE_DIRS "${MARBLE_ROOT}/include")
set(MarbleDB_LIBRARIES "${MARBLE_ROOT}/build/libmarble.a")

# Include directories - must include Arrow before MarbleDB since MarbleDB includes Arrow
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${Arrow_INCLUDE_DIRS}
    ${MarbleDB_INCLUDE_DIRS}
)

# Also add Arrow to compile flags for MarbleDB includes
if(Arrow_INCLUDE_DIRS)
    add_compile_options(-I${Arrow_INCLUDE_DIRS})
endif()

# Source files
set(SOURCES
    marbledb_backend.cpp
    factory.cpp
)

# Header files
set(HEADERS
    interface.h
    marbledb_backend.h
)

# Create static library
add_library(sabot_storage STATIC ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(sabot_storage
    PUBLIC
        ${Arrow_LIBRARIES}
        ${MarbleDB_LIBRARIES}
)

# Install
install(TARGETS sabot_storage
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES ${HEADERS}
    DESTINATION include/sabot/storage
)

