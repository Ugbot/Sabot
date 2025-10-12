add_subdirectory(expression)
add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES}
    PARENT_SCOPE)
