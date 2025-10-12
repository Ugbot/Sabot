if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library(
  sabot_sql_common_types_column
  STATIC
  column_data_allocator.cpp
  column_data_collection.cpp
  column_data_collection_segment.cpp
  column_data_consumer.cpp
  partitioned_column_data.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_types_column>
    PARENT_SCOPE)
