//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/explain_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class ExplainRelation : public Relation {
public:
	explicit ExplainRelation(shared_ptr<Relation> child, ExplainType type = ExplainType::EXPLAIN_STANDARD,
	                         ExplainFormat format = ExplainFormat::DEFAULT);

	shared_ptr<Relation> child;
	vector<ColumnDefinition> columns;
	ExplainType type;
	ExplainFormat format;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace sabot_sql
