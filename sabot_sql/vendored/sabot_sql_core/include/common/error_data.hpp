//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/error_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/string.hpp"

namespace sabot_sql {
class ParsedExpression;
class TableRef;

class ErrorData {
public:
	//! Not initialized, default constructor
	SABOT_SQL_API ErrorData();
	//! From std::exception
	SABOT_SQL_API ErrorData(const std::exception &ex); // NOLINT: allow implicit construction from exception
	//! From a raw string and exception type
	SABOT_SQL_API ErrorData(ExceptionType type, const string &raw_message);
	//! From a raw string
	SABOT_SQL_API explicit ErrorData(const string &raw_message);

public:
	//! Throw the error
	[[noreturn]] SABOT_SQL_API void Throw(const string &prepended_message = "") const;
	//! Get the internal exception type of the error.
	SABOT_SQL_API const ExceptionType &Type() const;
	//! Used in clients like C-API, creates the final message and returns a reference to it
	SABOT_SQL_API const string &Message() const {
		return final_message;
	}
	SABOT_SQL_API const string &RawMessage() const {
		return raw_message;
	}
	SABOT_SQL_API void Merge(const ErrorData &other);
	SABOT_SQL_API bool operator==(const ErrorData &other) const;

	//! Returns true, if this error data contains an exception, else false.
	inline bool HasError() const {
		return initialized;
	}
	const unordered_map<string, string> &ExtraInfo() const {
		return extra_info;
	}

	SABOT_SQL_API void FinalizeError();
	SABOT_SQL_API void AddErrorLocation(const string &query);
	SABOT_SQL_API void ConvertErrorToJSON();

	SABOT_SQL_API void AddQueryLocation(optional_idx query_location);
	SABOT_SQL_API void AddQueryLocation(QueryErrorContext error_context);
	SABOT_SQL_API void AddQueryLocation(const ParsedExpression &ref);
	SABOT_SQL_API void AddQueryLocation(const TableRef &ref);

private:
	//! Whether this ErrorData contains an exception or not
	bool initialized;
	//! The ExceptionType of the preserved exception
	ExceptionType type;
	//! The message the exception was constructed with (does not contain the Exception Type)
	string raw_message;
	//! The final message (stored in the preserved error for compatibility reasons with C-API)
	string final_message;
	//! Extra exception info
	unordered_map<string, string> extra_info;

private:
	SABOT_SQL_API static string SanitizeErrorMessage(string error);
	SABOT_SQL_API string ConstructFinalMessage() const;
};

} // namespace sabot_sql
