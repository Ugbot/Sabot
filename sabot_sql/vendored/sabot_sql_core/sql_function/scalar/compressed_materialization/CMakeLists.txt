add_library(sabot_sql_func_compressed_materialization STATIC
                  compress_integral.cpp compress_string.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_compressed_materialization>
    PARENT_SCOPE)
