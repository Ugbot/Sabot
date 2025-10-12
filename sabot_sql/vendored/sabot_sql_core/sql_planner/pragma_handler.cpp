#include "sabot_sql/planner/pragma_handler.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/parser/parser.hpp"

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "sabot_sql/parser/statement/multi_statement.hpp"
#include "sabot_sql/parser/parsed_data/bound_pragma_info.hpp"
#include "sabot_sql/function/function.hpp"

#include "sabot_sql/main/client_context.hpp"

#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/common/file_system.hpp"
#include "sabot_sql/function/function_binder.hpp"

namespace sabot_sql {

PragmaHandler::PragmaHandler(ClientContext &context) : context(context) {
}

void PragmaHandler::HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::MULTI_STATEMENT) {
			auto &multi_statement = statements[i]->Cast<MultiStatement>();
			for (auto &stmt : multi_statement.statements) {
				new_statements.push_back(std::move(stmt));
			}
			continue;
		}
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT) {
			// PRAGMA statement: check if we need to replace it by a new set of statements
			PragmaHandler handler(context);
			string new_query;
			bool expanded = handler.HandlePragma(*statements[i], new_query);
			if (expanded) {
				// this PRAGMA statement gets replaced by a new query string
				// push the new query string through the parser again and add it to the transformer
				Parser parser(context.GetParserOptions());
				parser.ParseQuery(new_query);
				// insert the new statements and remove the old statement
				for (idx_t j = 0; j < parser.statements.size(); j++) {
					new_statements.push_back(std::move(parser.statements[j]));
				}
				continue;
			}
		}
		new_statements.push_back(std::move(statements[i]));
	}
	statements = std::move(new_statements);
}

void PragmaHandler::HandlePragmaStatements(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements) {
	// first check if there are any pragma statements
	bool found_pragma = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT ||
		    statements[i]->type == StatementType::MULTI_STATEMENT) {
			found_pragma = true;
			break;
		}
	}
	if (!found_pragma) {
		// no pragmas: skip this step
		return;
	}
	context.RunFunctionInTransactionInternal(lock, [&]() { HandlePragmaStatementsInternal(statements); });
}

bool PragmaHandler::HandlePragma(SQLStatement &statement, string &resulting_query) {
	auto info = statement.Cast<PragmaStatement>().info->Copy();
	QueryErrorContext error_context(statement.stmt_location);
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindPragma(*info, error_context);
	if (bound_info->function.query) {
		FunctionParameters parameters {bound_info->parameters, bound_info->named_parameters};
		resulting_query = bound_info->function.query(context, parameters);
		return true;
	}
	return false;
}

} // namespace sabot_sql
