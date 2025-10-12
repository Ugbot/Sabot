add_library(sabot_sql_catalog_entries_dependency STATIC dependency_entry.cpp
                  dependency_subject_entry.cpp dependency_dependent_entry.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_catalog_entries_dependency>
    PARENT_SCOPE)
