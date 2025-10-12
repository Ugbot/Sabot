//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception/binder_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/query_error_context.hpp"

namespace sabot_sql {

class BinderException : public Exception {
public:
	SABOT_SQL_API explicit BinderException(const string &msg, const unordered_map<string, string> &extra_info);
	SABOT_SQL_API explicit BinderException(const string &msg);

	template <typename... ARGS>
	explicit BinderException(const string &msg, ARGS... params) : BinderException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit BinderException(const TableRef &ref, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(ref)) {
	}
	template <typename... ARGS>
	explicit BinderException(const ParsedExpression &expr, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
	template <typename... ARGS>
	explicit BinderException(const Expression &expr, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
	template <typename... ARGS>
	explicit BinderException(QueryErrorContext error_context, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_context)) {
	}
	template <typename... ARGS>
	explicit BinderException(optional_idx error_location, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_location)) {
	}

	static BinderException ColumnNotFound(const string &name, const vector<string> &similar_bindings,
	                                      QueryErrorContext context = QueryErrorContext());
	static BinderException NoMatchingFunction(const string &catalog_name, const string &schema_name, const string &name,
	                                          const vector<LogicalType> &arguments, const vector<string> &candidates);
	static BinderException Unsupported(ParsedExpression &expr, const string &message);
};

} // namespace sabot_sql
