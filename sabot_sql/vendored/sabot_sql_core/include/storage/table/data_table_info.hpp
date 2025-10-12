//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/atomic.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/storage/table/table_index_list.hpp"
#include "sabot_sql/storage/storage_lock.hpp"

namespace sabot_sql {
class DatabaseInstance;
class TableIOManager;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, string schema, string table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all, if nullptr.
	void BindIndexes(ClientContext &context, const char *index_type = nullptr);

	//! Whether or not the table is temporary
	bool IsTemporary() const;

	AttachedDatabase &GetDB() {
		return db;
	}

	TableIOManager &GetIOManager() {
		return *table_io_manager;
	}

	TableIndexList &GetIndexes() {
		return indexes;
	}
	const vector<IndexStorageInfo> &GetIndexStorageInfo() const {
		return index_storage_infos;
	}
	unique_ptr<StorageLockKey> GetSharedLock() {
		return checkpoint_lock.GetSharedLock();
	}

	string GetSchemaName();
	string GetTableName();
	void SetTableName(string name);

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The schema of the table
	string schema;
	//! The name of the table
	string table;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! Lock held while checkpointing
	StorageLock checkpoint_lock;
};

} // namespace sabot_sql
