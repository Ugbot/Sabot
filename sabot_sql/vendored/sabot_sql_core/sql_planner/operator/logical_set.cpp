#include "sabot_sql/planner/operator/logical_set.hpp"

namespace sabot_sql {

idx_t LogicalSet::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace sabot_sql
