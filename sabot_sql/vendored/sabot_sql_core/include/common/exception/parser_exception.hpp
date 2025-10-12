//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception/parser_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/optional_idx.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

class ParserException : public Exception {
public:
	SABOT_SQL_API explicit ParserException(const string &msg);
	SABOT_SQL_API explicit ParserException(const string &msg, const unordered_map<string, string> &extra_info);

	template <typename... ARGS>
	explicit ParserException(const string &msg, ARGS... params) : ParserException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit ParserException(optional_idx error_location, const string &msg, ARGS... params)
	    : ParserException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_location)) {
	}
	template <typename... ARGS>
	explicit ParserException(const ParsedExpression &expr, const string &msg, ARGS... params)
	    : ParserException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}

	static ParserException SyntaxError(const string &query, const string &error_message, optional_idx error_location);
};

} // namespace sabot_sql
