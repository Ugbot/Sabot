#include "sabot_sql/common/tree_renderer.hpp"

namespace sabot_sql {

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	if (!UsesRawKeyNames()) {
		root.SanitizeKeyNames();
	}
	return ToStreamInternal(root, ss);
}

} // namespace sabot_sql
