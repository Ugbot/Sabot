add_library(sabot_sql_main_secret STATIC secret.cpp secret_manager.cpp
                  secret_storage.cpp default_secrets.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main_secret>
    PARENT_SCOPE)
