# Benchmarks for MarbleDB
cmake_minimum_required(VERSION 3.16)

# Write benchmark
add_executable(write_bench write_bench.cpp common.hpp)
target_link_libraries(write_bench marble ${ARROW_LIBRARIES} ${Boost_LIBRARIES})
target_include_directories(write_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Set C++ standard
set_target_properties(write_bench PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Compiler flags
if(MSVC)
    target_compile_options(write_bench PRIVATE /W4)
else()
    target_compile_options(write_bench PRIVATE -Wall -Wextra)
endif()

# Install benchmarks
install(TARGETS write_bench
    RUNTIME DESTINATION bin
)
