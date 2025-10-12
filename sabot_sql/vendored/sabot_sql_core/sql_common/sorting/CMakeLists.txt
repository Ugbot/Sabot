add_library(sabot_sql_sorting STATIC hashed_sort.cpp sort.cpp sorted_run.cpp
                  sorted_run_merger.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_sorting>
    PARENT_SCOPE)
