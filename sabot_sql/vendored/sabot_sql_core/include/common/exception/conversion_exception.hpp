//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception/conversion_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/optional_idx.hpp"

namespace sabot_sql {

class ConversionException : public Exception {
public:
	SABOT_SQL_API explicit ConversionException(const string &msg);
	SABOT_SQL_API explicit ConversionException(optional_idx error_location, const string &msg);
	SABOT_SQL_API ConversionException(const PhysicalType orig_type, const PhysicalType new_type);
	SABOT_SQL_API ConversionException(const LogicalType &orig_type, const LogicalType &new_type);

	template <typename... ARGS>
	explicit ConversionException(const string &msg, ARGS... params)
	    : ConversionException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit ConversionException(optional_idx error_location, const string &msg, ARGS... params)
	    : ConversionException(error_location, ConstructMessage(msg, params...)) {
	}
};

} // namespace sabot_sql
