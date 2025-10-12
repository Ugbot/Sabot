//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/extension_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/parser_extension.hpp"

namespace sabot_sql {

class ExtensionStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXTENSION_STATEMENT;

public:
	ExtensionStatement(ParserExtension extension, unique_ptr<ParserExtensionParseData> parse_data);

	//! The ParserExtension this statement was generated from
	ParserExtension extension;
	//! The parse data for this specific statement
	unique_ptr<ParserExtensionParseData> parse_data;

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
