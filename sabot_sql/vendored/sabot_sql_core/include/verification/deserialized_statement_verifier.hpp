//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/verification/deserialized_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/verification/statement_verifier.hpp"

namespace sabot_sql {

class DeserializedStatementVerifier : public StatementVerifier {
public:
	explicit DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p,
	                                       optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
};

} // namespace sabot_sql
