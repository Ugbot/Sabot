#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/parallel/task_scheduler.hpp"

using sabot_sql::DatabaseWrapper;

struct CAPITaskState {
	explicit CAPITaskState(sabot_sql::DatabaseInstance &db)
	    : db(db), marker(sabot_sql::make_uniq<sabot_sql::atomic<bool>>(true)), execute_count(0) {
	}

	sabot_sql::DatabaseInstance &db;
	sabot_sql::unique_ptr<sabot_sql::atomic<bool>> marker;
	sabot_sql::atomic<idx_t> execute_count;
};

void sabot_sql_execute_tasks(sabot_sql_database database, idx_t max_tasks) {
	if (!database) {
		return;
	}
	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	auto &scheduler = sabot_sql::TaskScheduler::GetScheduler(*wrapper->database->instance);
	scheduler.ExecuteTasks(max_tasks);
}

sabot_sql_task_state sabot_sql_create_task_state(sabot_sql_database database) {
	if (!database) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	auto state = new CAPITaskState(*wrapper->database->instance);
	return state;
}

void sabot_sql_execute_tasks_state(sabot_sql_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = sabot_sql::TaskScheduler::GetScheduler(state->db);
	state->execute_count++;
	scheduler.ExecuteForever(state->marker.get());
}

idx_t sabot_sql_execute_n_tasks_state(sabot_sql_task_state state_p, idx_t max_tasks) {
	if (!state_p) {
		return 0;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = sabot_sql::TaskScheduler::GetScheduler(state->db);
	return scheduler.ExecuteTasks(state->marker.get(), max_tasks);
}

void sabot_sql_finish_execution(sabot_sql_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	*state->marker = false;
	if (state->execute_count > 0) {
		// signal to the threads to wake up
		auto &scheduler = sabot_sql::TaskScheduler::GetScheduler(state->db);
		scheduler.Signal(state->execute_count);
	}
}

bool sabot_sql_task_state_is_finished(sabot_sql_task_state state_p) {
	if (!state_p) {
		return false;
	}
	auto state = (CAPITaskState *)state_p;
	return !(*state->marker);
}

void sabot_sql_destroy_task_state(sabot_sql_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	delete state;
}

bool sabot_sql_execution_is_finished(sabot_sql_connection con) {
	if (!con) {
		return false;
	}
	sabot_sql::Connection *conn = (sabot_sql::Connection *)con;
	return conn->context->ExecutionIsFinished();
}
