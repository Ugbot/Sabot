#include "sabot_sql/function/cast/default_casts.hpp"
#include "sabot_sql/function/cast/vector_cast_helpers.hpp"

namespace sabot_sql {

BoundCastInfo DefaultCasts::BlobCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// blob to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, sabot_sql::CastFromBlob>);
	case LogicalTypeId::UUID:
		// blob to uuid
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, hugeint_t, sabot_sql::TryCastBlobToUUID>);
	case LogicalTypeId::AGGREGATE_STATE:
		return DefaultCasts::ReinterpretCast;
	case LogicalTypeId::BIT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, sabot_sql::CastFromBlobToBit>);

	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace sabot_sql
