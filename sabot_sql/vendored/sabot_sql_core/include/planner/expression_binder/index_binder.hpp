//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/index_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/table_catalog_entry.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/execution/index/bound_index.hpp"
#include "sabot_sql/execution/index/unbound_index.hpp"
#include "sabot_sql/parser/parsed_data/create_index_info.hpp"
#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

class BoundColumnRefExpression;

//! The IndexBinder binds indexes and expressions within index statements.
class IndexBinder : public ExpressionBinder {
public:
	IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table = nullptr,
	            optional_ptr<CreateIndexInfo> info = nullptr);

	unique_ptr<BoundIndex> BindIndex(const UnboundIndex &index);
	unique_ptr<LogicalOperator> BindCreateIndex(ClientContext &context, unique_ptr<CreateIndexInfo> create_index_info,
	                                            TableCatalogEntry &table_entry, unique_ptr<LogicalOperator> plan,
	                                            unique_ptr<AlterTableInfo> alter_table_info);

	static void InitCreateIndexInfo(LogicalGet &get, CreateIndexInfo &info, const string &schema);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;
	string UnsupportedAggregateMessage() override;

private:
	// Only for WAL replay.
	optional_ptr<TableCatalogEntry> table;
	optional_ptr<CreateIndexInfo> info;
};

} // namespace sabot_sql
