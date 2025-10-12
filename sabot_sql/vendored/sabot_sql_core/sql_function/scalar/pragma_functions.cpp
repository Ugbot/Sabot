#include "sabot_sql/function/pragma/pragma_functions.hpp"

namespace sabot_sql {

void BuiltinFunctions::RegisterPragmaFunctions() {
	Register<PragmaQueries>();
	Register<PragmaFunctions>();
}

} // namespace sabot_sql
