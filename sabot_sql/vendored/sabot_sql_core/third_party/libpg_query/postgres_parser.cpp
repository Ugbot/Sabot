#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"
#include "parser/scansup.hpp"
#include "common/keywords.hpp"

namespace sabot_sql {

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {}

void PostgresParser::Parse(const std::string &query) {
	sabot_sql_libpgquery::pg_parser_init();
	sabot_sql_libpgquery::parse_result res;
	pg_parser_parse(query.c_str(), &res);
	success = res.success;

	if (success) {
		parse_tree = res.parse_tree;
	} else {
		error_message = std::string(res.error_message);
		error_location = res.error_location;
	}
}

vector<sabot_sql_libpgquery::PGSimplifiedToken> PostgresParser::Tokenize(const std::string &query) {
	sabot_sql_libpgquery::pg_parser_init();
	auto tokens = sabot_sql_libpgquery::tokenize(query.c_str());
	sabot_sql_libpgquery::pg_parser_cleanup();
	return std::move(tokens);
}

PostgresParser::~PostgresParser()  {
    sabot_sql_libpgquery::pg_parser_cleanup();
}

sabot_sql_libpgquery::PGKeywordCategory PostgresParser::IsKeyword(const std::string &text) {
	return sabot_sql_libpgquery::is_keyword(text.c_str());
}

vector<sabot_sql_libpgquery::PGKeyword> PostgresParser::KeywordList() {
	// FIXME: because of this, we might need to change the libpg_query library to use sabot_sql::vector
	vector<sabot_sql_libpgquery::PGKeyword> tmp(sabot_sql_libpgquery::keyword_list());
	return tmp;
}

void PostgresParser::SetPreserveIdentifierCase(bool preserve) {
	sabot_sql_libpgquery::set_preserve_identifier_case(preserve);
}

}
