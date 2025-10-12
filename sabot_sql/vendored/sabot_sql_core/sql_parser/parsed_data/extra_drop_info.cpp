#include "sabot_sql/parser/parsed_data/extra_drop_info.hpp"

namespace sabot_sql {

ExtraDropSecretInfo::ExtraDropSecretInfo() : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
}

ExtraDropSecretInfo::ExtraDropSecretInfo(const ExtraDropSecretInfo &info)
    : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
	persist_mode = info.persist_mode;
	secret_storage = info.secret_storage;
}

unique_ptr<ExtraDropInfo> ExtraDropSecretInfo::Copy() const {
	return std::move(make_uniq<ExtraDropSecretInfo>(*this));
}

} // namespace sabot_sql
