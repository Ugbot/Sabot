add_library(
  sabot_sql_common_tree_renderer
  STATIC
  json_tree_renderer.cpp
  html_tree_renderer.cpp
  graphviz_tree_renderer.cpp
  text_tree_renderer.cpp
  yaml_tree_renderer.cpp
  tree_renderer.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_tree_renderer>
    PARENT_SCOPE)
