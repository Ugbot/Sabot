#include "sabot_sql/function/cast/default_casts.hpp"
#include "sabot_sql/function/cast/vector_cast_helpers.hpp"

namespace sabot_sql {

BoundCastInfo DefaultCasts::PointerCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// pointer to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<uintptr_t, sabot_sql::CastFromPointer>);
	default:
		return nullptr;
	}
}

} // namespace sabot_sql
