#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/numeric_utils.hpp"
#include "sabot_sql/common/types/date.hpp"
#include "sabot_sql/common/types/time.hpp"
#include "sabot_sql/common/types/timestamp.hpp"

using sabot_sql::Date;
using sabot_sql::Time;
using sabot_sql::Timestamp;

using sabot_sql::date_t;
using sabot_sql::dtime_t;
using sabot_sql::timestamp_ms_t;
using sabot_sql::timestamp_ns_t;
using sabot_sql::timestamp_sec_t;
using sabot_sql::timestamp_t;

sabot_sql_date_struct sabot_sql_from_date(sabot_sql_date date) {
	int32_t year, month, day;
	Date::Convert(date_t(date.days), year, month, day);

	sabot_sql_date_struct result;
	result.year = year;
	result.month = sabot_sql::UnsafeNumericCast<int8_t>(month);
	result.day = sabot_sql::UnsafeNumericCast<int8_t>(day);
	return result;
}

sabot_sql_date sabot_sql_to_date(sabot_sql_date_struct date) {
	sabot_sql_date result;
	result.days = Date::FromDate(date.year, date.month, date.day).days;
	return result;
}

bool sabot_sql_is_finite_date(sabot_sql_date date) {
	return Date::IsFinite(date_t(date.days));
}

sabot_sql_time_struct sabot_sql_from_time(sabot_sql_time time) {
	int32_t hour, minute, second, micros;
	Time::Convert(dtime_t(time.micros), hour, minute, second, micros);

	sabot_sql_time_struct result;
	result.hour = sabot_sql::UnsafeNumericCast<int8_t>(hour);
	result.min = sabot_sql::UnsafeNumericCast<int8_t>(minute);
	result.sec = sabot_sql::UnsafeNumericCast<int8_t>(second);
	result.micros = micros;
	return result;
}

sabot_sql_time_tz_struct sabot_sql_from_time_tz(sabot_sql_time_tz input) {
	sabot_sql_time_tz_struct result;
	sabot_sql_time time;

	sabot_sql::dtime_tz_t time_tz(input.bits);

	time.micros = time_tz.time().micros;

	result.time = sabot_sql_from_time(time);
	result.offset = time_tz.offset();
	return result;
}

sabot_sql_time_tz sabot_sql_create_time_tz(int64_t micros, int32_t offset) {
	sabot_sql_time_tz time;
	time.bits = sabot_sql::dtime_tz_t(sabot_sql::dtime_t(micros), offset).bits;
	return time;
}

sabot_sql_time sabot_sql_to_time(sabot_sql_time_struct time) {
	sabot_sql_time result;
	result.micros = Time::FromTime(time.hour, time.min, time.sec, time.micros).micros;
	return result;
}

sabot_sql_timestamp_struct sabot_sql_from_timestamp(sabot_sql_timestamp ts) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp_t(ts.micros), date, time);

	sabot_sql_date ddate;
	ddate.days = date.days;

	sabot_sql_time dtime;
	dtime.micros = time.micros;

	sabot_sql_timestamp_struct result;
	result.date = sabot_sql_from_date(ddate);
	result.time = sabot_sql_from_time(dtime);
	return result;
}

sabot_sql_timestamp sabot_sql_to_timestamp(sabot_sql_timestamp_struct ts) {
	date_t date = date_t(sabot_sql_to_date(ts.date).days);
	dtime_t time = dtime_t(sabot_sql_to_time(ts.time).micros);

	sabot_sql_timestamp result;
	result.micros = Timestamp::FromDatetime(date, time).value;
	return result;
}

bool sabot_sql_is_finite_timestamp(sabot_sql_timestamp ts) {
	return Timestamp::IsFinite(timestamp_t(ts.micros));
}

bool sabot_sql_is_finite_timestamp_s(sabot_sql_timestamp_s ts) {
	return Timestamp::IsFinite(timestamp_sec_t(ts.seconds));
}

bool sabot_sql_is_finite_timestamp_ms(sabot_sql_timestamp_ms ts) {
	return Timestamp::IsFinite(timestamp_ms_t(ts.millis));
}

bool sabot_sql_is_finite_timestamp_ns(sabot_sql_timestamp_ns ts) {
	return Timestamp::IsFinite(timestamp_ns_t(ts.nanos));
}
