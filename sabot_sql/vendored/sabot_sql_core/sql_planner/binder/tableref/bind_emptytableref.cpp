#include "sabot_sql/parser/tableref/emptytableref.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/tableref/bound_dummytableref.hpp"

namespace sabot_sql {

unique_ptr<BoundTableRef> Binder::Bind(EmptyTableRef &ref) {
	return make_uniq<BoundEmptyTableRef>(GenerateTableIndex());
}

} // namespace sabot_sql
