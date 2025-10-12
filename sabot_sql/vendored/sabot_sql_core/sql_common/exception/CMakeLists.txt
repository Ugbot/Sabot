add_library(
  sabot_sql_common_exception STATIC binder_exception.cpp catalog_exception.cpp
  conversion_exception.cpp parser_exception.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_exception>
    PARENT_SCOPE)
