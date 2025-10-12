#include "sabot_sql/common/limits.hpp"
#include "sabot_sql/parser/expression/cast_expression.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/common/types/blob.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(sabot_sql_libpgquery::PGTypeCast &root) {
	// get the type to cast to
	auto type_name = root.typeName;
	LogicalType target_type = TransformTypeName(*type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (!root.tryCast && target_type == LogicalType::BLOB && root.arg->type == sabot_sql_libpgquery::T_PGAConst) {
		auto c = PGPointerCast<sabot_sql_libpgquery::PGAConst>(root.arg);
		if (c->val.type == sabot_sql_libpgquery::T_PGString) {
			CastParameters parameters;
			if (root.location >= 0) {
				parameters.query_location = NumericCast<idx_t>(root.location);
			}
			auto blob_data = Blob::ToBlob(string(c->val.val.str), parameters);
			return make_uniq<ConstantExpression>(Value::BLOB_RAW(blob_data));
		}
	}
	// transform the expression node
	auto expression = TransformExpression(root.arg);
	bool try_cast = root.tryCast;

	// now create a cast operation
	auto result = make_uniq<CastExpression>(target_type, std::move(expression), try_cast);
	SetQueryLocation(*result, root.location);
	return std::move(result);
}

} // namespace sabot_sql
