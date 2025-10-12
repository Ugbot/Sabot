add_subdirectory(system)
add_subdirectory(version)
add_subdirectory(arrow)
add_library(
  sabot_sql_func_table
  STATIC
  arrow.cpp
  arrow_conversion.cpp
  checkpoint.cpp
  direct_file_reader.cpp
  glob.cpp
  query_function.cpp
  range.cpp
  repeat.cpp
  repeat_row.cpp
  copy_csv.cpp
  read_csv.cpp
  read_sabot_sql.cpp
  sniff_csv.cpp
  read_file.cpp
  system_functions.cpp
  summary.cpp
  table_scan.cpp
  unnest.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_table>
    PARENT_SCOPE)
