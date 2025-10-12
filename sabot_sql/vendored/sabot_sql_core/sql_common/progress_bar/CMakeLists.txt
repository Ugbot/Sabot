add_library(sabot_sql_progress_bar STATIC progress_bar.cpp
                  terminal_progress_bar_display.cpp unscented_kalman_filter.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_progress_bar>
    PARENT_SCOPE)
