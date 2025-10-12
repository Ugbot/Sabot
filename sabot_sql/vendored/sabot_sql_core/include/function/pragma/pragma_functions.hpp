//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/pragma/pragma_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/pragma_function.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct PragmaQueries {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaFunctions {
	static void RegisterFunction(BuiltinFunctions &set);
};

string PragmaShowTables(const string &catalog = "", const string &schema = "");
string PragmaShowTablesExpanded();
string PragmaShowDatabases();
string PragmaShowVariables();
string PragmaShow(const string &table_name);

} // namespace sabot_sql
