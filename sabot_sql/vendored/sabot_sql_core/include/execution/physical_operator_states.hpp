//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/physical_operator_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/operator_result_type.hpp"
#include "sabot_sql/common/enums/physical_operator_type.hpp"
#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/execution/execution_context.hpp"
#include "sabot_sql/optimizer/join_order/join_node.hpp"
#include "sabot_sql/parallel/interrupt.hpp"
#include "sabot_sql/execution/partition_info.hpp"

namespace sabot_sql {
class Event;
class Executor;
class PhysicalOperator;
class Pipeline;
class PipelineBuildState;
class MetaPipeline;
class InterruptState;

// LCOV_EXCL_START
class OperatorState {
public:
	virtual ~OperatorState() {
	}

	virtual void Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalSinkState : public StateWithBlockableTasks {
public:
	GlobalSinkState() : state(SinkFinalizeType::READY) {
	}
	virtual ~GlobalSinkState() {
	}

	SinkFinalizeType state;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual idx_t MaxThreads(idx_t source_max_threads) {
		return source_max_threads;
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}

	//! Source partition info
	SourcePartitionInfo partition_info;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalSourceState : public StateWithBlockableTasks {
public:
	virtual ~GlobalSourceState() {
	}

	virtual idx_t MaxThreads() {
		return 1;
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class LocalSourceState {
public:
	virtual ~LocalSourceState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct OperatorSinkInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSourceInput {
	GlobalSourceState &global_state;
	LocalSourceState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkCombineInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkFinalizeInput {
	GlobalSinkState &global_state;
	InterruptState &interrupt_state;
};

struct OperatorFinalizeInput {
	GlobalOperatorState &global_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkNextBatchInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

// LCOV_EXCL_STOP

} // namespace sabot_sql
