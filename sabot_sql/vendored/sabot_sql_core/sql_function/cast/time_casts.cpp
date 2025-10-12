#include "sabot_sql/function/cast/default_casts.hpp"
#include "sabot_sql/function/cast/vector_cast_helpers.hpp"
#include "sabot_sql/common/operator/string_cast.hpp"
namespace sabot_sql {

BoundCastInfo DefaultCasts::DateCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// date to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<date_t, sabot_sql::StringCast>);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, sabot_sql::TryCast>);
	case LogicalTypeId::TIMESTAMP_NS:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_ns_t, sabot_sql::TryCastToTimestampNS>);
	case LogicalTypeId::TIMESTAMP_SEC:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, sabot_sql::TryCastToTimestampSec>);
	case LogicalTypeId::TIMESTAMP_MS:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, sabot_sql::TryCastToTimestampMS>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_t, sabot_sql::StringCast>);
	case LogicalTypeId::TIME_TZ:
		// time to time with time zone
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_t, dtime_tz_t, sabot_sql::Cast>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeNsCastSwitch(BindCastInput &input, const LogicalType &src, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time (ns) to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_ns_t, sabot_sql::StringCast>);
	case LogicalTypeId::TIME:
		// time (ns) to time (µs)
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_ns_t, dtime_t, sabot_sql::Cast>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time with time zone to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_tz_t, sabot_sql::StringCastTZ>);
	case LogicalTypeId::TIME:
		// time with time zone to time
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_tz_t, dtime_t, sabot_sql::Cast>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampCastSwitch(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, sabot_sql::StringCast>);
	case LogicalTypeId::DATE:
		// timestamp to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, sabot_sql::Cast>);
	case LogicalTypeId::TIME:
		// timestamp to time
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, sabot_sql::Cast>);
	case LogicalTypeId::TIME_TZ:
		// timestamp to time_tz
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, sabot_sql::Cast>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone
		return ReinterpretCast;
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampUsToNs>);
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampUsToMs>);
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampUsToSec>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp with time zone to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, sabot_sql::StringCastTZ>);
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, sabot_sql::Cast>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us)
		return ReinterpretCast;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampNsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (ns) to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_ns_t, sabot_sql::StringCast>);
	case LogicalTypeId::DATE:
		// timestamp (ns) to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, sabot_sql::CastTimestampNsToDate>);
	case LogicalTypeId::TIME:
		// timestamp (ns) to time
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, sabot_sql::CastTimestampNsToTime>);
	case LogicalTypeId::TIME_NS:
		// timestamp (ns) to time (ns)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_ns_t, dtime_ns_t, sabot_sql::CastTimestampNsToTimeNs>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampNsToUs>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ns) to timestamp with time zone (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampNsToUs>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampMsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (ms) to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, sabot_sql::CastFromTimestampMS>);
	case LogicalTypeId::DATE:
		// timestamp (ms) to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, sabot_sql::CastTimestampMsToDate>);
	case LogicalTypeId::TIME:
		// timestamp (ms) to time
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, sabot_sql::CastTimestampMsToTime>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampMsToUs>);
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (ms) to timestamp (ns)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampMsToNs>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ms) to timestamp with timezone (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampMsToUs>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampSecCastSwitch(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (s) to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, sabot_sql::CastFromTimestampSec>);
	case LogicalTypeId::DATE:
		// timestamp (s) to date
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, sabot_sql::CastTimestampSecToDate>);
	case LogicalTypeId::TIME:
		// timestamp (s) to time
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, sabot_sql::CastTimestampSecToTime>);
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (s) to timestamp (ms)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampSecToMs>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampSecToUs>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (s) to timestamp with timezone (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampSecToUs>);
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (s) to timestamp (ns)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, sabot_sql::CastTimestampSecToNs>);
	default:
		return TryVectorNullCast;
	}
}
BoundCastInfo DefaultCasts::IntervalCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<interval_t, sabot_sql::StringCast>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace sabot_sql
