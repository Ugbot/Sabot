#include "sabot_sql/parser/transformer.hpp"

#include "sabot_sql/parser/expression/list.hpp"
#include "sabot_sql/parser/statement/list.hpp"
#include "sabot_sql/parser/tableref/emptytableref.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/parser/query_node/cte_node.hpp"
#include "sabot_sql/parser/parser_options.hpp"

namespace sabot_sql {

Transformer::Transformer(ParserOptions &options)
    : parent(nullptr), options(options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::Transformer(Transformer &parent)
    : parent(&parent), options(parent.options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::~Transformer() {
}

void Transformer::Clear() {
	ClearParameters();
	pivot_entries.clear();
}

bool Transformer::TransformParseTree(sabot_sql_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	InitializeStackCheck();
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		Clear();
		auto n = PGPointerCast<sabot_sql_libpgquery::PGNode>(entry->data.ptr_value);
		auto stmt = TransformStatement(*n);
		D_ASSERT(stmt);
		if (HasPivotEntries()) {
			stmt = CreatePivotStatement(std::move(stmt));
		}
		statements.push_back(std::move(stmt));
	}
	return true;
}

void Transformer::InitializeStackCheck() {
	stack_depth = 0;
}

StackChecker<Transformer> Transformer::StackCheck(idx_t extra_stack) {
	auto &root = RootTransformer();
	D_ASSERT(root.stack_depth != DConstants::INVALID_INDEX);
	if (root.stack_depth + extra_stack >= options.max_expression_depth) {
		throw ParserException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      options.max_expression_depth);
	}
	return StackChecker<Transformer>(root, extra_stack);
}

unique_ptr<SQLStatement> Transformer::TransformStatement(sabot_sql_libpgquery::PGNode &stmt) {
	auto result = TransformStatementInternal(stmt);
	if (!named_param_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = named_param_map;
	}
	return result;
}

Transformer &Transformer::RootTransformer() {
	reference<Transformer> node = *this;
	while (node.get().parent) {
		node = *node.get().parent;
	}
	return node.get();
}

const Transformer &Transformer::RootTransformer() const {
	reference<const Transformer> node = *this;
	while (node.get().parent) {
		node = *node.get().parent;
	}
	return node.get();
}

idx_t Transformer::ParamCount() const {
	auto &root = RootTransformer();
	return root.prepared_statement_parameter_index;
}

void Transformer::SetParamCount(idx_t new_count) {
	auto &root = RootTransformer();
	root.prepared_statement_parameter_index = new_count;
}

void Transformer::ClearParameters() {
	auto &root = RootTransformer();
	root.prepared_statement_parameter_index = 0;
	root.named_param_map.clear();
}

static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type) {
	// Mixing positional/auto-increment and named parameters is not supported
	if (last_type == PreparedParamType::INVALID) {
		return;
	}
	if (last_type == PreparedParamType::NAMED) {
		if (new_type != PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
	if (last_type != PreparedParamType::NAMED) {
		if (new_type == PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
}

void Transformer::SetParam(const string &identifier, idx_t index, PreparedParamType type) {
	auto &root = RootTransformer();
	ParamTypeCheck(root.last_param_type, type);
	root.last_param_type = type;
	D_ASSERT(!root.named_param_map.count(identifier));
	root.named_param_map[identifier] = index;
}

bool Transformer::GetParam(const string &identifier, idx_t &index, PreparedParamType type) {
	auto &root = RootTransformer();
	ParamTypeCheck(root.last_param_type, type);
	auto entry = root.named_param_map.find(identifier);
	if (entry == root.named_param_map.end()) {
		return false;
	}
	index = entry->second;
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatementInternal(sabot_sql_libpgquery::PGNode &stmt) {
	switch (stmt.type) {
	case sabot_sql_libpgquery::T_PGRawStmt: {
		auto &raw_stmt = PGCast<sabot_sql_libpgquery::PGRawStmt>(stmt);
		auto result = TransformStatement(*raw_stmt.stmt);
		if (result) {
			result->stmt_location = NumericCast<idx_t>(raw_stmt.stmt_location);
			result->stmt_length = NumericCast<idx_t>(raw_stmt.stmt_len);
		}
		return result;
	}
	case sabot_sql_libpgquery::T_PGSelectStmt:
		return TransformSelectStmt(PGCast<sabot_sql_libpgquery::PGSelectStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateStmt:
		return TransformCreateTable(PGCast<sabot_sql_libpgquery::PGCreateStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateSchemaStmt:
		return TransformCreateSchema(PGCast<sabot_sql_libpgquery::PGCreateSchemaStmt>(stmt));
	case sabot_sql_libpgquery::T_PGViewStmt:
		return TransformCreateView(PGCast<sabot_sql_libpgquery::PGViewStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateSeqStmt:
		return TransformCreateSequence(PGCast<sabot_sql_libpgquery::PGCreateSeqStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateFunctionStmt:
		return TransformCreateFunction(PGCast<sabot_sql_libpgquery::PGCreateFunctionStmt>(stmt));
	case sabot_sql_libpgquery::T_PGDropStmt:
		return TransformDrop(PGCast<sabot_sql_libpgquery::PGDropStmt>(stmt));
	case sabot_sql_libpgquery::T_PGInsertStmt:
		return TransformInsert(PGCast<sabot_sql_libpgquery::PGInsertStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCopyStmt:
		return TransformCopy(PGCast<sabot_sql_libpgquery::PGCopyStmt>(stmt));
	case sabot_sql_libpgquery::T_PGTransactionStmt:
		return TransformTransaction(PGCast<sabot_sql_libpgquery::PGTransactionStmt>(stmt));
	case sabot_sql_libpgquery::T_PGDeleteStmt:
		return TransformDelete(PGCast<sabot_sql_libpgquery::PGDeleteStmt>(stmt));
	case sabot_sql_libpgquery::T_PGUpdateStmt:
		return TransformUpdate(PGCast<sabot_sql_libpgquery::PGUpdateStmt>(stmt));
	case sabot_sql_libpgquery::T_PGUpdateExtensionsStmt:
		return TransformUpdateExtensions(PGCast<sabot_sql_libpgquery::PGUpdateExtensionsStmt>(stmt));
	case sabot_sql_libpgquery::T_PGIndexStmt:
		return TransformCreateIndex(PGCast<sabot_sql_libpgquery::PGIndexStmt>(stmt));
	case sabot_sql_libpgquery::T_PGAlterTableStmt:
		return TransformAlter(PGCast<sabot_sql_libpgquery::PGAlterTableStmt>(stmt));
	case sabot_sql_libpgquery::T_PGAlterDatabaseStmt:
		return TransformAlterDatabase(PGCast<sabot_sql_libpgquery::PGAlterDatabaseStmt>(stmt));
	case sabot_sql_libpgquery::T_PGRenameStmt:
		return TransformRename(PGCast<sabot_sql_libpgquery::PGRenameStmt>(stmt));
	case sabot_sql_libpgquery::T_PGPrepareStmt:
		return TransformPrepare(PGCast<sabot_sql_libpgquery::PGPrepareStmt>(stmt));
	case sabot_sql_libpgquery::T_PGExecuteStmt:
		return TransformExecute(PGCast<sabot_sql_libpgquery::PGExecuteStmt>(stmt));
	case sabot_sql_libpgquery::T_PGDeallocateStmt:
		return TransformDeallocate(PGCast<sabot_sql_libpgquery::PGDeallocateStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateTableAsStmt:
		return TransformCreateTableAs(PGCast<sabot_sql_libpgquery::PGCreateTableAsStmt>(stmt));
	case sabot_sql_libpgquery::T_PGPragmaStmt:
		return TransformPragma(PGCast<sabot_sql_libpgquery::PGPragmaStmt>(stmt));
	case sabot_sql_libpgquery::T_PGExportStmt:
		return TransformExport(PGCast<sabot_sql_libpgquery::PGExportStmt>(stmt));
	case sabot_sql_libpgquery::T_PGImportStmt:
		return TransformImport(PGCast<sabot_sql_libpgquery::PGImportStmt>(stmt));
	case sabot_sql_libpgquery::T_PGExplainStmt:
		return TransformExplain(PGCast<sabot_sql_libpgquery::PGExplainStmt>(stmt));
	case sabot_sql_libpgquery::T_PGVacuumStmt:
		return TransformVacuum(PGCast<sabot_sql_libpgquery::PGVacuumStmt>(stmt));
	case sabot_sql_libpgquery::T_PGVariableShowStmt:
		return TransformShowStmt(PGCast<sabot_sql_libpgquery::PGVariableShowStmt>(stmt));
	case sabot_sql_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelectStmt(PGCast<sabot_sql_libpgquery::PGVariableShowSelectStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCallStmt:
		return TransformCall(PGCast<sabot_sql_libpgquery::PGCallStmt>(stmt));
	case sabot_sql_libpgquery::T_PGVariableSetStmt:
		return TransformSet(PGCast<sabot_sql_libpgquery::PGVariableSetStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCheckPointStmt:
		return TransformCheckpoint(PGCast<sabot_sql_libpgquery::PGCheckPointStmt>(stmt));
	case sabot_sql_libpgquery::T_PGLoadStmt:
		return TransformLoad(PGCast<sabot_sql_libpgquery::PGLoadStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateTypeStmt:
		return TransformCreateType(PGCast<sabot_sql_libpgquery::PGCreateTypeStmt>(stmt));
	case sabot_sql_libpgquery::T_PGAlterSeqStmt:
		return TransformAlterSequence(PGCast<sabot_sql_libpgquery::PGAlterSeqStmt>(stmt));
	case sabot_sql_libpgquery::T_PGAttachStmt:
		return TransformAttach(PGCast<sabot_sql_libpgquery::PGAttachStmt>(stmt));
	case sabot_sql_libpgquery::T_PGDetachStmt:
		return TransformDetach(PGCast<sabot_sql_libpgquery::PGDetachStmt>(stmt));
	case sabot_sql_libpgquery::T_PGUseStmt:
		return TransformUse(PGCast<sabot_sql_libpgquery::PGUseStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCopyDatabaseStmt:
		return TransformCopyDatabase(PGCast<sabot_sql_libpgquery::PGCopyDatabaseStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCreateSecretStmt:
		return TransformSecret(PGCast<sabot_sql_libpgquery::PGCreateSecretStmt>(stmt));
	case sabot_sql_libpgquery::T_PGDropSecretStmt:
		return TransformDropSecret(PGCast<sabot_sql_libpgquery::PGDropSecretStmt>(stmt));
	case sabot_sql_libpgquery::T_PGCommentOnStmt:
		return TransformCommentOn(PGCast<sabot_sql_libpgquery::PGCommentOnStmt>(stmt));
	case sabot_sql_libpgquery::T_PGMergeIntoStmt:
		return TransformMergeInto(PGCast<sabot_sql_libpgquery::PGMergeIntoStmt>(stmt));
	default:
		throw NotImplementedException(NodetypeToString(stmt.type));
	}
}

unique_ptr<QueryNode> Transformer::TransformMaterializedCTE(unique_ptr<QueryNode> root) {
	// Extract materialized CTEs from cte_map
	vector<unique_ptr<CTENode>> materialized_ctes;

	for (auto &cte : root->cte_map.map) {
		auto &cte_entry = cte.second;
		auto mat_cte = make_uniq<CTENode>();
		mat_cte->ctename = cte.first;
		mat_cte->query = TransformMaterializedCTE(cte_entry->query->node->Copy());
		mat_cte->aliases = cte_entry->aliases;
		mat_cte->materialized = cte_entry->materialized;
		materialized_ctes.push_back(std::move(mat_cte));
	}

	while (!materialized_ctes.empty()) {
		unique_ptr<CTENode> node_result;
		node_result = std::move(materialized_ctes.back());
		node_result->child = std::move(root);
		root = std::move(node_result);
		materialized_ctes.pop_back();
	}

	return root;
}

void Transformer::SetQueryLocation(ParsedExpression &expr, int query_location) {
	if (query_location < 0) {
		return;
	}
	expr.SetQueryLocation(optional_idx(static_cast<idx_t>(query_location)));
}

void Transformer::SetQueryLocation(TableRef &ref, int query_location) {
	if (query_location < 0) {
		return;
	}
	ref.query_location = optional_idx(static_cast<idx_t>(query_location));
}

} // namespace sabot_sql
