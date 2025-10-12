//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// postgres_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include "nodes/pg_list.hpp"
#include "pg_simplified_token.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {
class PostgresParser {
public:
	PostgresParser();
	~PostgresParser();

	bool success;
	sabot_sql_libpgquery::PGList *parse_tree;
	std::string error_message;
	int error_location;
public:
	void Parse(const std::string &query);
	static sabot_sql::vector<sabot_sql_libpgquery::PGSimplifiedToken> Tokenize(const std::string &query);

	static sabot_sql_libpgquery::PGKeywordCategory IsKeyword(const std::string &text);
	static sabot_sql::vector<sabot_sql_libpgquery::PGKeyword> KeywordList();

	static void SetPreserveIdentifierCase(bool downcase);
};

}
