//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/transaction_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {
class DuckTransaction;
class Transaction;

struct TransactionData {
	TransactionData(DuckTransaction &transaction_p); // NOLINT: allow implicit conversion
	TransactionData(transaction_t transaction_id_p, transaction_t start_time_p);

	optional_ptr<DuckTransaction> transaction;
	transaction_t transaction_id;
	transaction_t start_time;
};

} // namespace sabot_sql
