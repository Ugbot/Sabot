//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/simplified_token.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {

//! Simplified tokens are a simplified (dense) representation of the lexer
//! Used for simple syntax highlighting in the tests
enum class SimplifiedTokenType : uint8_t {
	SIMPLIFIED_TOKEN_IDENTIFIER,
	SIMPLIFIED_TOKEN_NUMERIC_CONSTANT,
	SIMPLIFIED_TOKEN_STRING_CONSTANT,
	SIMPLIFIED_TOKEN_OPERATOR,
	SIMPLIFIED_TOKEN_KEYWORD,
	SIMPLIFIED_TOKEN_COMMENT,
	SIMPLIFIED_TOKEN_ERROR
};

struct SimplifiedToken {
	SimplifiedTokenType type;
	idx_t start;
};

enum class KeywordCategory : uint8_t {
	KEYWORD_RESERVED,
	KEYWORD_UNRESERVED,
	KEYWORD_TYPE_FUNC,
	KEYWORD_COL_NAME,
	KEYWORD_NONE
};

struct ParserKeyword {
	string name;
	KeywordCategory category;
};

} // namespace sabot_sql
