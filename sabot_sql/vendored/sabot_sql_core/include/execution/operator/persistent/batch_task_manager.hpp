//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/persistent/batch_task_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/storage/temporary_memory_manager.hpp"
#include "sabot_sql/parallel/interrupt.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"
#include "sabot_sql/common/queue.hpp"

namespace sabot_sql {

template <class TASK>
class BatchTaskManager {
public:
	void AddTask(unique_ptr<TASK> task) {
		lock_guard<mutex> l(task_lock);
		task_queue.push(std::move(task));
	}

	unique_ptr<TASK> GetTask() {
		lock_guard<mutex> l(task_lock);
		if (task_queue.empty()) {
			return nullptr;
		}
		auto entry = std::move(task_queue.front());
		task_queue.pop();
		return entry;
	}

	idx_t TaskCount() {
		lock_guard<mutex> l(task_lock);
		return task_queue.size();
	}

private:
	mutex task_lock;
	//! The task queue for the batch copy to file
	queue<unique_ptr<TASK>> task_queue;
};

} // namespace sabot_sql
