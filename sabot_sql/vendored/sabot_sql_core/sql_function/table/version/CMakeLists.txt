add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")
add_definitions(-DDUCKDB_VERSION="\""${DUCKDB_VERSION}"\"")

add_library(sabot_sql_func_table_version STATIC pragma_version.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_table_version>
    PARENT_SCOPE)
