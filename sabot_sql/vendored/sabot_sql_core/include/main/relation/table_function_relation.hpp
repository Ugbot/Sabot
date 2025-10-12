//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/table_function_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class TableFunctionRelation : public Relation {
public:
	TableFunctionRelation(const shared_ptr<ClientContext> &context, string name, vector<Value> parameters,
	                      named_parameter_map_t named_parameters, shared_ptr<Relation> input_relation_p = nullptr,
	                      bool auto_init = true);
	TableFunctionRelation(const shared_ptr<RelationContextWrapper> &context, string name, vector<Value> parameters,
	                      named_parameter_map_t named_parameters, shared_ptr<Relation> input_relation_p = nullptr,
	                      bool auto_init = true);
	TableFunctionRelation(const shared_ptr<ClientContext> &context, string name, vector<Value> parameters,
	                      shared_ptr<Relation> input_relation_p = nullptr, bool auto_init = true);
	~TableFunctionRelation() override {
	}

	string name;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	vector<ColumnDefinition> columns;
	shared_ptr<Relation> input_relation;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
	void AddNamedParameter(const string &name, Value argument);
	void RemoveNamedParameterIfExists(const string &name);
	void SetNamedParameters(named_parameter_map_t &&named_parameters);

private:
	void InitializeColumns();

private:
	//! Whether or not to auto initialize the columns on construction
	bool auto_initialize;
};

} // namespace sabot_sql
