# Query optimizer library
add_library(sabot_query STATIC
    logical_plan.cpp
    optimizer.cpp
    optimizer_enhanced.cpp
    optimizer_type.cpp
    rules.cpp
    pushdown/filter_pushdown.cpp
    pushdown/projection_pushdown.cpp
    join_order/cardinality_estimator.cpp
    join_order/cost_model.cpp
    join_order/join_order_optimizer.cpp
    rule/constant_folding.cpp
    rule/arithmetic_simplification.cpp
    rule/comparison_simplification.cpp
    rule/conjunction_simplification.cpp
    expression_rewriter.cpp
)

target_include_directories(sabot_query PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Link to Arrow if available
if(TARGET Arrow::arrow_static)
    target_link_libraries(sabot_query PUBLIC Arrow::arrow_static)
elseif(TARGET arrow_shared)
    target_link_libraries(sabot_query PUBLIC arrow_shared)
endif()

# Set C++17 standard
set_target_properties(sabot_query PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Enable optimization
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(sabot_query PRIVATE
        -O3
        -march=native
        -DNDEBUG
    )
endif()

# Enable profiling in debug builds
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(sabot_query PRIVATE
        DEBUG=1
        SABOT_OPTIMIZER_PROFILING=1
    )
endif()

# Install
install(TARGETS sabot_query
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

