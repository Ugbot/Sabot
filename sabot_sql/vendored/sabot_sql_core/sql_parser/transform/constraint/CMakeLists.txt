add_library(sabot_sql_transformer_constraint STATIC transform_constraint.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_transformer_constraint>
    PARENT_SCOPE)
