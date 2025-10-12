#include "sabot_sql/planner/operator/logical_reset.hpp"

namespace sabot_sql {

idx_t LogicalReset::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace sabot_sql
