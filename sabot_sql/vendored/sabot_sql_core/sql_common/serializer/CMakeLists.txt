add_library(
  sabot_sql_common_serializer
  STATIC
  binary_serializer.cpp
  binary_deserializer.cpp
  buffered_file_reader.cpp
  buffered_file_writer.cpp
  memory_stream.cpp
  serializer.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_serializer>
    PARENT_SCOPE)
