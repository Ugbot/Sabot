//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// yyjson_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "yyjson.hpp"

using namespace sabot_sql_yyjson; // NOLINT

namespace sabot_sql {

struct ConvertedJSONHolder {
public:
	~ConvertedJSONHolder() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
		if (stringified_json) {
			free(stringified_json);
		}
	}

public:
	yyjson_mut_doc *doc = nullptr;
	char *stringified_json = nullptr;
};

} // namespace sabot_sql
