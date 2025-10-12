add_library(sabot_sql_aggr_distr STATIC count.cpp first_last_any.cpp
                  minmax.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_aggr_distr>
    PARENT_SCOPE)
