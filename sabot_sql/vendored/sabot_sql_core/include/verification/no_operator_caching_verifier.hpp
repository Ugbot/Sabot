//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/verification/unoptimized_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/verification/statement_verifier.hpp"

namespace sabot_sql {

class NoOperatorCachingVerifier : public StatementVerifier {
public:
	explicit NoOperatorCachingVerifier(unique_ptr<SQLStatement> statement_p,
	                                   optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement_p,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);

	bool DisableOperatorCaching() const override {
		return true;
	}
};

} // namespace sabot_sql
