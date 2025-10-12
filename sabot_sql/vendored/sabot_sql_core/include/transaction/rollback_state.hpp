//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/rollback_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/transaction/undo_buffer.hpp"

namespace sabot_sql {
class DataChunk;
class DataTable;
class DuckTransaction;
class WriteAheadLog;

class RollbackState {
public:
	explicit RollbackState(DuckTransaction &transaction);

public:
	void RollbackEntry(UndoFlags type, data_ptr_t data);

private:
	DuckTransaction &transaction;
};

} // namespace sabot_sql
