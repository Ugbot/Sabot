#include "sabot_sql/planner/operator/logical_prepare.hpp"

namespace sabot_sql {

idx_t LogicalPrepare::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace sabot_sql
