//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/connection_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {
class ClientContext;
class DatabaseInstance;
class Connection;

class ConnectionManager {
public:
	ConnectionManager();

	void AddConnection(ClientContext &context);
	void RemoveConnection(ClientContext &context);

	vector<shared_ptr<ClientContext>> GetConnectionList();
	const reference_map_t<ClientContext, weak_ptr<ClientContext>> &GetConnectionListReference() const {
		return connections;
	}
	idx_t GetConnectionCount() const;

	void AssignConnectionId(Connection &connection);

	static ConnectionManager &Get(DatabaseInstance &db);
	static ConnectionManager &Get(ClientContext &context);

private:
	mutex connections_lock;
	reference_map_t<ClientContext, weak_ptr<ClientContext>> connections;
	atomic<idx_t> connection_count;
	atomic<connection_t> current_connection_id;
};

} // namespace sabot_sql
