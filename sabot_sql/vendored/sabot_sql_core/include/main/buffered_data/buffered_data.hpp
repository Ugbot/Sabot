//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/interrupt.hpp"
#include "sabot_sql/common/queue.hpp"
#include "sabot_sql/common/vector_size.hpp"
#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/common/optional_idx.hpp"
#include "sabot_sql/execution/physical_operator_states.hpp"
#include "sabot_sql/common/enums/pending_execution_result.hpp"
#include "sabot_sql/common/enums/stream_execution_result.hpp"
#include "sabot_sql/common/shared_ptr.hpp"

namespace sabot_sql {

class StreamQueryResult;
class ClientContextLock;

class BufferedData {
protected:
	enum class Type { SIMPLE, BATCHED };

public:
	BufferedData(Type type, weak_ptr<ClientContext> context_p);
	virtual ~BufferedData();

public:
	StreamExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock);
	virtual StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) = 0;
	virtual unique_ptr<DataChunk> Scan() = 0;
	virtual void UnblockSinks() = 0;
	shared_ptr<ClientContext> GetContext() {
		return context.lock();
	}
	bool Closed() const {
		if (context.expired()) {
			return true;
		}
		auto c = context.lock();
		return c == nullptr;
	}
	void Close() {
		context.reset();
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast buffered data to type - buffered data type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast buffered data to type - buffered data type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	Type type;
	//! This is weak to avoid a cyclical reference
	weak_ptr<ClientContext> context;
	//! The maximum amount of memory we should keep buffered
	idx_t total_buffer_size;
	//! Protect against populate/fetch race condition
	mutex glock;
};

} // namespace sabot_sql
