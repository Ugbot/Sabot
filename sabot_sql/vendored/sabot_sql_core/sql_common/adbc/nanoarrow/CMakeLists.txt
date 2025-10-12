add_library(sabot_sql_adbc_nanoarrow STATIC metadata.cpp schema.cpp
                  allocator.cpp single_batch_array_stream.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_adbc_nanoarrow>
    PARENT_SCOPE)
