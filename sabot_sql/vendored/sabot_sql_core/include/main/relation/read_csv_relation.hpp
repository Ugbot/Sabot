//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "sabot_sql/main/relation/table_function_relation.hpp"
#include "sabot_sql/common/shared_ptr.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"

namespace sabot_sql {

class ReadCSVRelation : public TableFunctionRelation {
public:
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const vector<string> &csv_files,
	                named_parameter_map_t &&options, string alias = string());

	string alias;

protected:
	void InitializeAlias(const vector<string> &input);

public:
	string GetAlias() override;
};

} // namespace sabot_sql
