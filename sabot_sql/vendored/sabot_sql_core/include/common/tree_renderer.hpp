//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/main/profiling_node.hpp"
#include "sabot_sql/common/render_tree.hpp"

namespace sabot_sql {

class TreeRenderer {
public:
	explicit TreeRenderer() {
	}
	virtual ~TreeRenderer() {
	}

public:
	void ToStream(RenderTree &root, std::ostream &ss);
	virtual void ToStreamInternal(RenderTree &root, std::ostream &ss) = 0;
	static unique_ptr<TreeRenderer> CreateRenderer(ExplainFormat format);

	virtual bool UsesRawKeyNames() {
		return false;
	}
	virtual void Render(const ProfilingNode &op, std::ostream &ss) {
	}
};

} // namespace sabot_sql
