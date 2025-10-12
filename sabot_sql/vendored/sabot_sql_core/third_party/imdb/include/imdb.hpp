#pragma once

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql.hpp"

namespace imdb {
//! Adds the IMDB tables to the database
void dbgen(sabot_sql::SabotSQL &database);

//! Gets the specified IMDB JOB Query number as a string
std::string get_query(int query);

} // namespace imdb
