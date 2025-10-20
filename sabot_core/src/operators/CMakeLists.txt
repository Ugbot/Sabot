# Operator registry library
add_library(sabot_operators STATIC
    registry.cpp
)

target_include_directories(sabot_operators PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Set C++17 standard
set_target_properties(sabot_operators PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Enable optimization
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(sabot_operators PRIVATE
        -O3
        -march=native
        -DNDEBUG
    )
endif()

# Enable profiling in debug builds
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(sabot_operators PRIVATE
        DEBUG=1
        SABOT_PROFILE_REGISTRY=1
    )
endif()

# Install
install(TARGETS sabot_operators
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

