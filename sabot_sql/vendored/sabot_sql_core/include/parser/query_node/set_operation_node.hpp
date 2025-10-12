//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/query_node/set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/set_operation_type.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class SetOperationNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::SET_OPERATION_NODE;

public:
	SetOperationNode();

	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! whether the ALL modifier was used or not
	bool setop_all = false;
	//! The children of the set operation
	vector<unique_ptr<QueryNode>> children;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override;

public:
	//! Convert the query node to a string
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() const override;

	//! Serializes a QueryNode to a stand-alone binary blob
	//! Deserializes a blob back into a QueryNode

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);

public:
	// these methods exist for forwards/backwards compatibility of (de)serialization
	SetOperationNode(SetOperationType setop_type, unique_ptr<QueryNode> left, unique_ptr<QueryNode> right,
	                 vector<unique_ptr<QueryNode>> children, bool setop_all);

	unique_ptr<QueryNode> SerializeChildNode(Serializer &serializer, idx_t index) const;
	bool SerializeChildList(Serializer &serializer) const;
};

} // namespace sabot_sql
