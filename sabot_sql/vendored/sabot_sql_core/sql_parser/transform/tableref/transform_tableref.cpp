#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<TableRef> Transformer::TransformTableRefNode(sabot_sql_libpgquery::PGNode &n) {
	auto stack_checker = StackCheck();

	switch (n.type) {
	case sabot_sql_libpgquery::T_PGRangeVar:
		return TransformRangeVar(PGCast<sabot_sql_libpgquery::PGRangeVar>(n));
	case sabot_sql_libpgquery::T_PGJoinExpr:
		return TransformJoin(PGCast<sabot_sql_libpgquery::PGJoinExpr>(n));
	case sabot_sql_libpgquery::T_PGRangeSubselect:
		return TransformRangeSubselect(PGCast<sabot_sql_libpgquery::PGRangeSubselect>(n));
	case sabot_sql_libpgquery::T_PGRangeFunction:
		return TransformRangeFunction(PGCast<sabot_sql_libpgquery::PGRangeFunction>(n));
	case sabot_sql_libpgquery::T_PGPivotExpr:
		return TransformPivot(PGCast<sabot_sql_libpgquery::PGPivotExpr>(n));
	default:
		throw NotImplementedException("From Type %d not supported", n.type);
	}
}

} // namespace sabot_sql
