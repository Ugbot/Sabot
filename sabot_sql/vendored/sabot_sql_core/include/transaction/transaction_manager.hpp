//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/atomic.hpp"

namespace sabot_sql {

class AttachedDatabase;
class ClientContext;
class Catalog;
struct ClientLockWrapper;
class DatabaseInstance;
class Transaction;

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
public:
	explicit TransactionManager(AttachedDatabase &db);
	virtual ~TransactionManager();

	//! Start a new transaction
	virtual Transaction &StartTransaction(ClientContext &context) = 0;
	//! Commit the given transaction. Returns a non-empty error message on failure.
	virtual ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) = 0;
	//! Rollback the given transaction
	virtual void RollbackTransaction(Transaction &transaction) = 0;

	virtual void Checkpoint(ClientContext &context, bool force = false) = 0;

	static TransactionManager &Get(AttachedDatabase &db);

	virtual bool IsDuckTransactionManager() {
		return false;
	}

	AttachedDatabase &GetDB() {
		return db;
	}

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace sabot_sql
