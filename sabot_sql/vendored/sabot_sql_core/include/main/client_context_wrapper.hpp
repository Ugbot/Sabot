//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/client_context_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/shared_ptr.hpp"
#include "sabot_sql/parser/column_definition.hpp"

namespace sabot_sql {

class ClientContext;

class Relation;

class ClientContextWrapper {
public:
	virtual ~ClientContextWrapper() = default;
	explicit ClientContextWrapper(const shared_ptr<ClientContext> &context);
	shared_ptr<ClientContext> GetContext();
	shared_ptr<ClientContext> TryGetContext();
	virtual void TryBindRelation(Relation &relation, vector<ColumnDefinition> &columns);

private:
	weak_ptr<ClientContext> client_context;
};

} // namespace sabot_sql
