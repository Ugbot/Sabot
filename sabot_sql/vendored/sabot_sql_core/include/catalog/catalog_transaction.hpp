//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {
class Catalog;
class ClientContext;
class DatabaseInstance;
class Transaction;

struct CatalogTransaction {
	CatalogTransaction(Catalog &catalog, ClientContext &context);
	CatalogTransaction(DatabaseInstance &db, transaction_t transaction_id_p, transaction_t start_time_p);

	optional_ptr<DatabaseInstance> db;
	optional_ptr<ClientContext> context;
	optional_ptr<Transaction> transaction;
	transaction_t transaction_id;
	transaction_t start_time;

	bool HasContext() const {
		return context;
	}
	ClientContext &GetContext();

	static CatalogTransaction GetSystemCatalogTransaction(ClientContext &context);
	static CatalogTransaction GetSystemTransaction(DatabaseInstance &db);
};

} // namespace sabot_sql
