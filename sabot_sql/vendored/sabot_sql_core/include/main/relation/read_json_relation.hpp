#pragma once

#include "sabot_sql/main/relation/table_function_relation.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/named_parameter_map.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class ReadJSONRelation : public TableFunctionRelation {
public:
	ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file, named_parameter_map_t options,
	                 bool auto_detect, string alias = "");
	ReadJSONRelation(const shared_ptr<ClientContext> &context, vector<string> &json_file, named_parameter_map_t options,
	                 bool auto_detect, string alias = "");
	~ReadJSONRelation() override;
	string json_file;
	string alias;

public:
	string GetAlias() override;

private:
	void InitializeAlias(const vector<string> &input);
};

} // namespace sabot_sql
