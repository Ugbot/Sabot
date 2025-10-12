#include "sabot_sql/parser/statement/transaction_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

TransactionType TransformTransactionType(sabot_sql_libpgquery::PGTransactionStmtKind kind) {
	switch (kind) {
	case sabot_sql_libpgquery::PG_TRANS_STMT_BEGIN:
	case sabot_sql_libpgquery::PG_TRANS_STMT_START:
		return TransactionType::BEGIN_TRANSACTION;
	case sabot_sql_libpgquery::PG_TRANS_STMT_COMMIT:
		return TransactionType::COMMIT;
	case sabot_sql_libpgquery::PG_TRANS_STMT_ROLLBACK:
		return TransactionType::ROLLBACK;
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", kind);
	}
}

TransactionModifierType TransformTransactionModifier(sabot_sql_libpgquery::PGTransactionStmtType type) {
	switch (type) {
	case sabot_sql_libpgquery::PG_TRANS_TYPE_DEFAULT:
		return TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER;
	case sabot_sql_libpgquery::PG_TRANS_TYPE_READ_ONLY:
		return TransactionModifierType::TRANSACTION_READ_ONLY;
	case sabot_sql_libpgquery::PG_TRANS_TYPE_READ_WRITE:
		return TransactionModifierType::TRANSACTION_READ_WRITE;
	default:
		throw NotImplementedException("Transaction modifier %d not implemented yet", type);
	}
}

unique_ptr<TransactionStatement> Transformer::TransformTransaction(sabot_sql_libpgquery::PGTransactionStmt &stmt) {
	//	stmt.transaction_type
	auto type = TransformTransactionType(stmt.kind);
	auto info = make_uniq<TransactionInfo>(type);
	info->modifier = TransformTransactionModifier(stmt.transaction_type);
	return make_uniq<TransactionStatement>(std::move(info));
}

} // namespace sabot_sql
