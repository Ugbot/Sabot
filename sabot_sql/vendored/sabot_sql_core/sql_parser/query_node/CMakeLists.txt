add_library(sabot_sql_query_node STATIC recursive_cte_node.cpp cte_node.cpp
                  select_node.cpp set_operation_node.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_query_node>
    PARENT_SCOPE)
