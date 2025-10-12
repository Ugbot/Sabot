#include "sabot_sql/function/scalar/string_functions.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/vector_operations/unary_executor.hpp"
#include "mbedtls_wrapper.hpp"

namespace sabot_sql {

namespace {

struct SHA1Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, sabot_sql_mbedtls::MbedTlsWrapper::SHA1_HASH_LENGTH_TEXT);

		sabot_sql_mbedtls::MbedTlsWrapper::SHA1State state;
		state.AddString(input.GetString());
		state.FinishHex(hash.GetDataWriteable());

		hash.Finalize();
		return hash;
	}
};

void SHA1Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, SHA1Operator>(input, result, args.size());
}

} // namespace

ScalarFunctionSet SHA1Fun::GetFunctions() {
	ScalarFunctionSet set("sha1");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, SHA1Function));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, SHA1Function));
	return set;
}

} // namespace sabot_sql
