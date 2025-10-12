//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/secret/default_secrets.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {
class DatabaseInstance;
class ClientContext;
class BaseSecret;
struct CreateSecretInput;
class SecretManager;
struct SecretType;
class CreateSecretFunction;

struct CreateHTTPSecretFunctions {
public:
	//! Get the default secret types
	static vector<SecretType> GetDefaultSecretTypes();
	//! Get the default secret functions
	static vector<CreateSecretFunction> GetDefaultSecretFunctions();

protected:
	//! HTTP secret CONFIG provider
	static unique_ptr<BaseSecret> CreateHTTPSecretFromConfig(ClientContext &context, CreateSecretInput &input);
	//! HTTP secret ENV provider
	static unique_ptr<BaseSecret> CreateHTTPSecretFromEnv(ClientContext &context, CreateSecretInput &input);
};

} // namespace sabot_sql
