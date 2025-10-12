add_library(
  sabot_sql_sort
  STATIC
  comparators.cpp
  merge_sorter.cpp
  partition_state.cpp
  radix_sort.cpp
  sort_state.cpp
  sorted_block.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_sort>
    PARENT_SCOPE)
