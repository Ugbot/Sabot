//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/transaction/undo_buffer.hpp"
#include "sabot_sql/common/vector_size.hpp"

namespace sabot_sql {
class CatalogEntry;
class DataChunk;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
struct DeleteInfo;
struct UpdateInfo;

class CommitState {
public:
	explicit CommitState(DuckTransaction &transaction, transaction_t commit_id);

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);
	void RevertCommit(UndoFlags type, data_ptr_t data);

private:
	void CommitEntryDrop(CatalogEntry &entry, data_ptr_t extra_data);

private:
	DuckTransaction &transaction;
	transaction_t commit_id;
};

} // namespace sabot_sql
