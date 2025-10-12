add_library(sabot_sql_optimizer_matcher STATIC expression_matcher.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_optimizer_matcher>
    PARENT_SCOPE)
