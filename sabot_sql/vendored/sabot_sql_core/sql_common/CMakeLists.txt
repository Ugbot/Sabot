# Core SabotSQL common sources
set(SABOT_SQL_COMMON_SOURCES
    allocator.cpp
    array.cpp
    assert.cpp
    case_insensitive_map.cpp
    checksum.cpp
    circular_buffer.cpp
    column_data.cpp
    column_vector.cpp
    constants.cpp
    data_chunk.cpp
    data_ptr.cpp
    sabot_sql.cpp
    enums.cpp
    exception.cpp
    file_system.cpp
    helper.cpp
    idx_t.cpp
    large_object_storage.cpp
    limits.cpp
    luhn.cpp
    operator_cast.cpp
    radix.cpp
    random_engine.cpp
    serializer.cpp
    string.cpp
    string_util.cpp
    symbols.cpp
    types.cpp
    utf8proc.cpp
    vector.cpp
    vector_cache.cpp
    vector_operations.cpp
    winapi.cpp
)

# Add serializer sources
file(GLOB SERIALIZER_SOURCES "serializer/*.cpp")
list(APPEND SABOT_SQL_COMMON_SOURCES ${SERIALIZER_SOURCES})

# Add types sources
file(GLOB TYPES_SOURCES "types/*.cpp")
list(APPEND SABOT_SQL_COMMON_SOURCES ${TYPES_SOURCES})

# Add vector operations sources
file(GLOB VECTOR_OP_SOURCES "types/vector_operations/*.cpp")
list(APPEND SABOT_SQL_COMMON_SOURCES ${VECTOR_OP_SOURCES})

# Create object library
add_library(sabot_sql_common STATIC ${SABOT_SQL_COMMON_SOURCES})

# Set parent scope variable
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common>
    PARENT_SCOPE)