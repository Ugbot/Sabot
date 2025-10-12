//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/error_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/map.hpp"

namespace sabot_sql {
class String;
class ClientContext;
class DatabaseInstance;
class TransactionException;

enum class ErrorType : uint16_t {
	// error message types
	UNSIGNED_EXTENSION = 0,
	INVALIDATED_TRANSACTION = 1,
	INVALIDATED_DATABASE = 2,

	// this should always be the last value
	ERROR_COUNT,
	INVALID = 65535,
};

//! The error manager class is responsible for formatting error messages
//! It allows for error messages to be overridden by extensions and clients
class ErrorManager {
public:
	template <typename... ARGS>
	string FormatException(ErrorType error_type, ARGS... params) {
		vector<ExceptionFormatValue> values;
		return FormatExceptionRecursive(error_type, values, params...);
	}

	SABOT_SQL_API string FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values);

	template <class T, typename... ARGS>
	string FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values, T param,
	                                ARGS... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatExceptionRecursive(error_type, values, params...);
	}

	template <typename... ARGS>
	static string FormatException(ClientContext &context, ErrorType error_type, ARGS... params) {
		return Get(context).FormatException(error_type, params...);
	}

	SABOT_SQL_API static InvalidInputException InvalidUnicodeError(const String &input, const string &context);

	SABOT_SQL_API static FatalException InvalidatedDatabase(ClientContext &context, const string &invalidated_msg);
	SABOT_SQL_API static TransactionException InvalidatedTransaction(ClientContext &context);

	//! Adds a custom error for a specific error type
	void AddCustomError(ErrorType type, string new_error);

	SABOT_SQL_API static ErrorManager &Get(ClientContext &context);
	SABOT_SQL_API static ErrorManager &Get(DatabaseInstance &context);

private:
	map<ErrorType, string> custom_errors;
};

} // namespace sabot_sql
