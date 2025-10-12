//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/verification/parsed_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/verification/statement_verifier.hpp"

namespace sabot_sql {

class ParsedStatementVerifier : public StatementVerifier {
public:
	explicit ParsedStatementVerifier(unique_ptr<SQLStatement> statement_p,
	                                 optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);

	bool RequireEquality() const override {
		return false;
	}
};

} // namespace sabot_sql
