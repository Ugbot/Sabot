#include "sabot_sql/main/extension.hpp"

#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/main/capi/extension_api.hpp"
#include "sabot_sql/main/extension_helper.hpp"

namespace sabot_sql {

Extension::~Extension() {
}

static string PrettyPrintString(const string &s) {
	string res = "";
	for (auto c : s) {
		if (StringUtil::CharacterIsAlpha(c) || StringUtil::CharacterIsDigit(c) || c == '_' || c == '-' || c == ' ' ||
		    c == '.') {
			res += c;
		} else {
			auto value = UnsafeNumericCast<uint8_t>(c);
			res += "\\x";
			uint8_t first = value / 16;
			if (first < 10) {
				res.push_back((char)('0' + first));
			} else {
				res.push_back((char)('a' + first - 10));
			}
			uint8_t second = value % 16;
			if (second < 10) {
				res.push_back((char)('0' + second));
			} else {
				res.push_back((char)('a' + second - 10));
			}
		}
	}
	return res;
}

string ParsedExtensionMetaData::GetInvalidMetadataError() {
	const string engine_platform = string(SabotSQL::Platform());

	if (!AppearsValid()) {
		return "The file is not a SabotSQL extension. The metadata at the end of the file is invalid";
	}

	string result;

	// CPP or C_STRUCT_UNSTABLE ABI versioning needs to match the SabotSQL version exactly
	if (abi_type == ExtensionABIType::CPP || abi_type == ExtensionABIType::C_STRUCT_UNSTABLE) {
		const string engine_version = string(ExtensionHelper::GetVersionDirectoryName());

		if (engine_version != sabot_sql_version) {
			result += StringUtil::Format("The file was built specifically for SabotSQL version '%s' and can only be "
			                             "loaded with that version of SabotSQL. (this version of SabotSQL is '%s')",
			                             PrettyPrintString(sabot_sql_version), engine_version);
		}
		// C_STRUCT ABI versioning
	} else if (abi_type == ExtensionABIType::C_STRUCT) {
		idx_t major, minor, patch;
		if (!VersioningUtils::ParseSemver(sabot_sql_capi_version, major, minor, patch)) {
			result += StringUtil::Format("The file was built for SabotSQL C API version '%s', which failed to parse as a "
			                             "recognized version string",
			                             sabot_sql_capi_version, SABOT_SQL_EXTENSION_API_VERSION_MAJOR);
		} else if (major != SABOT_SQL_EXTENSION_API_VERSION_MAJOR) {
			// Special case where the extension is built for a completely unsupported API
			result +=
			    StringUtil::Format("The file was built for SabotSQL C API version '%s', but we can only load extensions "
			                       "built for SabotSQL C API 'v%lld.x.y'.",
			                       sabot_sql_capi_version, SABOT_SQL_EXTENSION_API_VERSION_MAJOR);
		} else if (!VersioningUtils::IsSupportedCAPIVersion(major, minor, patch)) {
			result +=
			    StringUtil::Format("The file was built for SabotSQL C API version '%s', but we can only load extensions "
			                       "built for SabotSQL C API 'v%lld.%lld.%lld' and lower.",
			                       sabot_sql_capi_version, SABOT_SQL_EXTENSION_API_VERSION_MAJOR,
			                       SABOT_SQL_EXTENSION_API_VERSION_MINOR, SABOT_SQL_EXTENSION_API_VERSION_PATCH);
		}
	} else {
		throw InternalException("Unknown ABI type for extension: '%s'", extension_abi_metadata);
	}

	if (engine_platform != platform) {
		if (!result.empty()) {
			result += " Also, t";
		} else {
			result += "T";
		}
		result += StringUtil::Format(
		    "he file was built for the platform '%s', but we can only load extensions built for platform '%s'.",
		    PrettyPrintString(platform), engine_platform);
	}

	return result;
}

bool VersioningUtils::IsSupportedCAPIVersion(string &capi_version_string) {
	idx_t major, minor, patch;
	if (!ParseSemver(capi_version_string, major, minor, patch)) {
		return false;
	}

	return IsSupportedCAPIVersion(major, minor, patch);
}

bool VersioningUtils::IsSupportedCAPIVersion(idx_t major, idx_t minor, idx_t patch) {
	if (major != SABOT_SQL_EXTENSION_API_VERSION_MAJOR) {
		return false;
	}
	if (minor > SABOT_SQL_EXTENSION_API_VERSION_MINOR) {
		return false;
	}
	if (minor < SABOT_SQL_EXTENSION_API_VERSION_MINOR) {
		return true;
	}
	if (patch > SABOT_SQL_EXTENSION_API_VERSION_PATCH) {
		return false;
	}
	return true;
}

bool VersioningUtils::ParseSemver(string &semver, idx_t &major_out, idx_t &minor_out, idx_t &patch_out) {
	if (!StringUtil::StartsWith(semver, "v")) {
		return false;
	}

	auto without_v = semver.substr(1);

	auto split = StringUtil::Split(without_v, '.');

	if (split.size() != 3) {
		return false;
	}

	idx_t major, minor, patch;
	bool succeeded = true;

	succeeded &= TryCast::Operation<string_t, idx_t>(split[0], major);
	succeeded &= TryCast::Operation<string_t, idx_t>(split[1], minor);
	succeeded &= TryCast::Operation<string_t, idx_t>(split[2], patch);

	if (!succeeded) {
		return false;
	}

	major_out = major;
	minor_out = minor;
	patch_out = patch;

	return true;
}

const char *Extension::DefaultVersion() {
	if (ExtensionHelper::IsRelease(SabotSQL::LibraryVersion())) {
		return SabotSQL::LibraryVersion();
	}
	return SabotSQL::SourceID();
}

} // namespace sabot_sql
