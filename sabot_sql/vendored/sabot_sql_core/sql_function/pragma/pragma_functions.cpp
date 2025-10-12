#include "sabot_sql/function/pragma/pragma_functions.hpp"

#include "sabot_sql/common/enums/output_type.hpp"
#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/main/query_profiler.hpp"
#include "sabot_sql/main/secret/secret_manager.hpp"
#include "sabot_sql/parallel/task_scheduler.hpp"
#include "sabot_sql/planner/expression_binder.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"
#include "sabot_sql/storage/storage_manager.hpp"
#include "sabot_sql/common/encryption_functions.hpp"
#include "sabot_sql/logging/log_manager.hpp"

#include <cctype>

namespace sabot_sql {

static void PragmaEnableProfilingStatement(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

void RegisterEnableProfiling(BuiltinFunctions &set) {
	PragmaFunctionSet functions("");
	functions.AddFunction(PragmaFunction::PragmaStatement(string(), PragmaEnableProfilingStatement));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void PragmaDisableProfiling(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = false;
}

static void PragmaEnableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = true;
}

static void PragmaDisableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = false;
}

static void PragmaEnablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = true;
}

static void PragmaDisablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = false;
}

static void PragmaEnableVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).query_verification_enabled = true;
	ClientConfig::GetConfig(context).verify_serializer = true;
}

static void PragmaDisableVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).query_verification_enabled = false;
	ClientConfig::GetConfig(context).verify_serializer = false;
}

static void PragmaVerifySerializer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_serializer = true;
}

static void PragmaDisableVerifySerializer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_serializer = false;
}

static void PragmaEnableExternalVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_external = true;
}

static void PragmaDisableExternalVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_external = false;
}

static void PragmaEnableFetchRowVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_fetch_row = true;
}

static void PragmaDisableFetchRowVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_fetch_row = false;
}

static void PragmaEnableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = true;
}

static void PragmaForceCheckpoint(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.force_checkpoint = true;
}

static void PragmaDisableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = false;
}

static void PragmaEnableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
}

static void PragmaDisableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
}

static void PragmaEnableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = true;
}

static void PragmaDisableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = false;
}

static void PragmaEnableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_optimizer = true;
}

static void PragmaDisableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_optimizer = false;
}

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	RegisterEnableProfiling(set);

	// NOTE: use of Pragma functions is discouraged. Instead, opt for adding a regular table function to be invoked with
	// CALL.
	//       see for example the "enable_logging" function

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", PragmaDisableProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", PragmaDisableProfiling));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", PragmaEnableVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", PragmaDisableVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_external", PragmaEnableExternalVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_external", PragmaDisableExternalVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_fetch_row", PragmaEnableFetchRowVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_fetch_row", PragmaDisableFetchRowVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_serializer", PragmaVerifySerializer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_serializer", PragmaDisableVerifySerializer));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_parallelism", PragmaEnableForceParallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_parallelism", PragmaDisableForceParallelism));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_object_cache", PragmaEnableObjectCache));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_object_cache", PragmaDisableObjectCache));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_optimizer", PragmaEnableOptimizer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_optimizer", PragmaDisableOptimizer));

	set.AddFunction(PragmaFunction::PragmaStatement("force_checkpoint", PragmaForceCheckpoint));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));
}

} // namespace sabot_sql
