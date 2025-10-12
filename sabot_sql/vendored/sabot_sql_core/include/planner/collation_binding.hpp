//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/collation_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/cast/default_casts.hpp"
#include "sabot_sql/common/enums/collation_type.hpp"

namespace sabot_sql {
struct MapCastInfo;
struct MapCastNode;
struct DBConfig;

typedef bool (*try_push_collation_t)(ClientContext &context, unique_ptr<Expression> &source,
                                     const LogicalType &sql_type, CollationType type);

struct CollationCallback {
	explicit CollationCallback(try_push_collation_t try_push_collation_p) : try_push_collation(try_push_collation_p) {
	}

	try_push_collation_t try_push_collation;
};

class CollationBinding {
public:
	CollationBinding();

public:
	SABOT_SQL_API static CollationBinding &Get(ClientContext &context);
	SABOT_SQL_API static CollationBinding &Get(DatabaseInstance &db);

	SABOT_SQL_API void RegisterCollation(CollationCallback callback);
	SABOT_SQL_API bool PushCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type,
	                              CollationType type) const;

private:
	vector<CollationCallback> collations;
};

} // namespace sabot_sql
