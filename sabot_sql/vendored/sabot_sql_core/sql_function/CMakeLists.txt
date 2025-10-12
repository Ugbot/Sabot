add_subdirectory(aggregate_function)
add_subdirectory(built_in_functions)
add_subdirectory(cast)
add_subdirectory(compression)
add_subdirectory(copy_function)
add_subdirectory(extension_function)
add_subdirectory(function)
add_subdirectory(macro_function)
add_subdirectory(pragma_function)
add_subdirectory(scalar_function)
add_subdirectory(table_function)
add_subdirectory(table_in_out_function)

add_library(
  sabot_sql_function
  STATIC
  function.cpp
  function_binder.cpp
  function_set.cpp
)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_function>
    PARENT_SCOPE)