#include "sabot_sql/planner/operator/logical_pragma.hpp"

namespace sabot_sql {

idx_t LogicalPragma::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace sabot_sql
