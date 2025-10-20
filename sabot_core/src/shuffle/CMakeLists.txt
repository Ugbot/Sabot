# Shuffle coordinator library
add_library(sabot_shuffle STATIC
    coordinator.cpp
)

target_include_directories(sabot_shuffle PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Set C++17 standard
set_target_properties(sabot_shuffle PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Enable optimization
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(sabot_shuffle PRIVATE
        -O3
        -march=native
        -DNDEBUG
    )
endif()

# Install
install(TARGETS sabot_shuffle
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

