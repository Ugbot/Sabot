//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/query_node/select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/parser/parsed_data/sample_options.hpp"
#include "sabot_sql/parser/group_by_node.hpp"
#include "sabot_sql/common/enums/aggregate_handling.hpp"

namespace sabot_sql {

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::SELECT_NODE;

public:
	SABOT_SQL_API SelectNode();

	//! The projection list
	vector<unique_ptr<ParsedExpression>> select_list;
	//! The FROM clause
	unique_ptr<TableRef> from_table;
	//! The WHERE clause
	unique_ptr<ParsedExpression> where_clause;
	//! list of groups
	GroupByNode groups;
	//! HAVING clause
	unique_ptr<ParsedExpression> having;
	//! QUALIFY clause
	unique_ptr<ParsedExpression> qualify;
	//! Aggregate handling during binding
	AggregateHandling aggregate_handling;
	//! The SAMPLE clause
	unique_ptr<SampleOptions> sample;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return select_list;
	}

public:
	//! Convert the query node to a string
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;

	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() const override;

	//! Serializes a QueryNode to a stand-alone binary blob

	//! Deserializes a blob back into a QueryNode

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
