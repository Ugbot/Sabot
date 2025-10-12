#include "sabot_sql/catalog/catalog_transaction.hpp"
#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/transaction/duck_transaction.hpp"
#include "sabot_sql/main/database.hpp"

namespace sabot_sql {

CatalogTransaction::CatalogTransaction(Catalog &catalog, ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog);
	this->db = &DatabaseInstance::GetDatabase(context);
	if (!transaction.IsDuckTransaction()) {
		this->transaction_id = transaction_t(-1);
		this->start_time = transaction_t(-1);
	} else {
		auto &dtransaction = transaction.Cast<DuckTransaction>();
		this->transaction_id = dtransaction.transaction_id;
		this->start_time = dtransaction.start_time;
	}
	this->transaction = &transaction;
	this->context = &context;
}

CatalogTransaction::CatalogTransaction(DatabaseInstance &db, transaction_t transaction_id_p, transaction_t start_time_p)
    : db(&db), context(nullptr), transaction(nullptr), transaction_id(transaction_id_p), start_time(start_time_p) {
}

ClientContext &CatalogTransaction::GetContext() {
	if (!context) {
		throw InternalException("Attempting to get a context in a CatalogTransaction without a context");
	}
	return *context;
}

CatalogTransaction CatalogTransaction::GetSystemCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(Catalog::GetSystemCatalog(context), context);
}

CatalogTransaction CatalogTransaction::GetSystemTransaction(DatabaseInstance &db) {
	return CatalogTransaction(db, 1, 1);
}

} // namespace sabot_sql
