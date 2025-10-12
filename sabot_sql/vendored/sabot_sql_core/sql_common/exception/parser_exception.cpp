#include "sabot_sql/common/exception/parser_exception.hpp"
#include "sabot_sql/common/to_string.hpp"
#include "sabot_sql/parser/query_error_context.hpp"

namespace sabot_sql {

ParserException::ParserException(const string &msg) : Exception(ExceptionType::PARSER, msg) {
}

ParserException::ParserException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::PARSER, msg, extra_info) {
}

ParserException ParserException::SyntaxError(const string &query, const string &error_message,
                                             optional_idx error_location) {
	return ParserException(error_message, Exception::InitializeExtraInfo("SYNTAX_ERROR", error_location));
}

} // namespace sabot_sql
