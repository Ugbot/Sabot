//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/assert.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {

class ClientContext;
class MetaTransaction;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	explicit TransactionContext(ClientContext &context);
	~TransactionContext();

	MetaTransaction &ActiveTransaction() {
		if (!current_transaction) {
			throw InternalException("TransactionContext::ActiveTransaction called without active transaction");
		}
		return *current_transaction;
	}

	bool HasActiveTransaction() const {
		return current_transaction.get();
	}

	void BeginTransaction();
	void Commit();
	void Rollback(optional_ptr<ErrorData>);
	void ClearTransaction();

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

private:
	ClientContext &context;
	bool auto_commit;

	unique_ptr<MetaTransaction> current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace sabot_sql
