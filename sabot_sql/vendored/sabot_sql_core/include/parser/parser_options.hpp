//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parser_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {
class ParserExtension;

struct ParserOptions {
	bool preserve_identifier_case = true;
	bool integer_division = false;
	idx_t max_expression_depth = 1000;
	const vector<ParserExtension> *extensions = nullptr;
};

} // namespace sabot_sql
