#include "sabot_sql/parser/tableref/delimgetref.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/tableref/bound_delimgetref.hpp"

namespace sabot_sql {

unique_ptr<BoundTableRef> Binder::Bind(DelimGetRef &ref) {
	// Have to add bindings
	idx_t tbl_idx = GenerateTableIndex();
	string internal_name = "__internal_delim_get_ref_" + std::to_string(tbl_idx);
	bind_context.AddGenericBinding(tbl_idx, internal_name, ref.internal_aliases, ref.types);

	return make_uniq<BoundDelimGetRef>(tbl_idx, ref.types);
}

} // namespace sabot_sql
