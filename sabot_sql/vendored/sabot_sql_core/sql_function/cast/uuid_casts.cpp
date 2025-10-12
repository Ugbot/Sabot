#include "sabot_sql/function/cast/default_casts.hpp"
#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/function/cast/vector_cast_helpers.hpp"

namespace sabot_sql {

BoundCastInfo DefaultCasts::UUIDCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// uuid to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, sabot_sql::CastFromUUID>);
	case LogicalTypeId::BLOB:
		// uuid to blob
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, sabot_sql::CastFromUUIDToBlob>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace sabot_sql
