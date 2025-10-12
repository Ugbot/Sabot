#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/expression/positional_reference_expression.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformPositionalReference(sabot_sql_libpgquery::PGPositionalReference &node) {
	if (node.position <= 0) {
		throw ParserException("Positional reference node needs to be >= 1");
	}
	auto result = make_uniq<PositionalReferenceExpression>(NumericCast<idx_t>(node.position));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

} // namespace sabot_sql
