//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/task_notifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {
class ClientContext;

//! The TaskNotifier notifies ClientContextState listener about started / stopped tasks
class TaskNotifier {
public:
	explicit TaskNotifier(optional_ptr<ClientContext> context_p);

	~TaskNotifier();

private:
	optional_ptr<ClientContext> context;
};

} // namespace sabot_sql
