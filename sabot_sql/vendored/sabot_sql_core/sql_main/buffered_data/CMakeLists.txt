add_library(sabot_sql_main_buffered_data STATIC buffered_data.cpp
                  simple_buffered_data.cpp batched_buffered_data.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main_buffered_data>
    PARENT_SCOPE)
