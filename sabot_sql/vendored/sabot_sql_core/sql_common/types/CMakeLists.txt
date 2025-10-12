add_subdirectory(column)
add_subdirectory(row)

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library(
  sabot_sql_common_types
  STATIC
  batched_data_collection.cpp
  bit.cpp
  blob.cpp
  cast_helpers.cpp
  conflict_manager.cpp
  conflict_info.cpp
  data_chunk.cpp
  date.cpp
  decimal.cpp
  hash.cpp
  hugeint.cpp
  uhugeint.cpp
  uuid.cpp
  hyperloglog.cpp
  interval.cpp
  list_segment.cpp
  selection_vector.cpp
  string_heap.cpp
  string_type.cpp
  timestamp.cpp
  time.cpp
  validity_mask.cpp
  value.cpp
  bignum.cpp
  vector_buffer.cpp
  vector.cpp
  vector_cache.cpp
  vector_constants.cpp
  geometry.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_types>
    PARENT_SCOPE)
