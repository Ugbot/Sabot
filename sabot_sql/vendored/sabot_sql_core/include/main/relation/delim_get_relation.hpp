//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/delim_get_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class DelimGetRelation : public Relation {
public:
	SABOT_SQL_API DelimGetRelation(const shared_ptr<ClientContext> &context, vector<LogicalType> chunk_types);

	vector<LogicalType> chunk_types;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace sabot_sql
