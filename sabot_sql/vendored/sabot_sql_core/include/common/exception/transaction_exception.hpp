//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception/transaction_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

class TransactionException : public Exception {
public:
	SABOT_SQL_API explicit TransactionException(const string &msg);

	template <typename... ARGS>
	explicit TransactionException(const string &msg, ARGS... params)
	    : TransactionException(ConstructMessage(msg, params...)) {
	}
};

} // namespace sabot_sql
