//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/profiling_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/deque.hpp"
#include "sabot_sql/common/enums/profiler_format.hpp"
#include "sabot_sql/common/pair.hpp"
#include "sabot_sql/common/profiler.hpp"
#include "sabot_sql/common/reference_map.hpp"
#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/execution/expression_executor_state.hpp"
#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/main/profiling_info.hpp"

namespace sabot_sql {

//! Recursive tree mirroring the operator tree.
class ProfilingNode {
public:
	explicit ProfilingNode() {
	}
	virtual ~ProfilingNode() {};

private:
	ProfilingInfo profiling_info;

public:
	idx_t depth = 0;
	vector<unique_ptr<ProfilingNode>> children;

public:
	idx_t GetChildCount() {
		return children.size();
	}
	ProfilingInfo &GetProfilingInfo() {
		return profiling_info;
	}
	const ProfilingInfo &GetProfilingInfo() const {
		return profiling_info;
	}
	optional_ptr<ProfilingNode> GetChild(idx_t idx) {
		return children[idx].get();
	}
	optional_ptr<ProfilingNode> AddChild(unique_ptr<ProfilingNode> child) {
		children.push_back(std::move(child));
		return children.back().get();
	}
};

} // namespace sabot_sql
