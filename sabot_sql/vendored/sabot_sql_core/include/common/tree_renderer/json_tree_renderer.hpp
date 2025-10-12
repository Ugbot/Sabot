//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/json_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/main/profiling_node.hpp"
#include "sabot_sql/common/tree_renderer.hpp"
#include "sabot_sql/common/render_tree.hpp"

namespace sabot_sql {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

class JSONTreeRenderer : public TreeRenderer {
public:
	explicit JSONTreeRenderer() {
	}
	~JSONTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const ProfilingNode &op, std::ostream &ss) override;
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStreamInternal(RenderTree &root, std::ostream &ss) override;
};

} // namespace sabot_sql
