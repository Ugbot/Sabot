add_library(sabot_sql_main_chunk_scan_state STATIC query_result.cpp
                  batched_data_collection.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main_chunk_scan_state>
    PARENT_SCOPE)
