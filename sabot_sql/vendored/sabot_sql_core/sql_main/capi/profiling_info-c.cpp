#include "sabot_sql/main/capi/capi_internal.hpp"

using sabot_sql::Connection;
using sabot_sql::SabotSQL;
using sabot_sql::EnumUtil;
using sabot_sql::MetricsType;
using sabot_sql::optional_ptr;
using sabot_sql::ProfilingNode;

sabot_sql_profiling_info sabot_sql_get_profiling_info(sabot_sql_connection connection) {
	if (!connection) {
		return nullptr;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	optional_ptr<ProfilingNode> profiling_node;
	try {
		profiling_node = conn->GetProfilingTree();
	} catch (std::exception &ex) {
		return nullptr;
	}

	if (!profiling_node) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_profiling_info>(profiling_node.get());
}

sabot_sql_value sabot_sql_profiling_info_get_value(sabot_sql_profiling_info info, const char *key) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<sabot_sql::ProfilingNode *>(info);
	auto &profiling_info = node.GetProfilingInfo();
	auto key_enum = EnumUtil::FromString<MetricsType>(sabot_sql::StringUtil::Upper(key));
	if (!profiling_info.Enabled(profiling_info.settings, key_enum)) {
		return nullptr;
	}

	auto str = profiling_info.GetMetricAsString(key_enum);
	return sabot_sql_create_varchar_length(str.c_str(), strlen(str.c_str()));
}

sabot_sql_value sabot_sql_profiling_info_get_metrics(sabot_sql_profiling_info info) {
	if (!info) {
		return nullptr;
	}

	auto &node = *reinterpret_cast<sabot_sql::ProfilingNode *>(info);
	auto &profiling_info = node.GetProfilingInfo();

	sabot_sql::InsertionOrderPreservingMap<sabot_sql::string> metrics_map;
	for (const auto &metric : profiling_info.metrics) {
		auto key = EnumUtil::ToString(metric.first);
		if (!profiling_info.Enabled(profiling_info.settings, metric.first)) {
			continue;
		}

		if (key == EnumUtil::ToString(MetricsType::OPERATOR_TYPE)) {
			auto type = sabot_sql::PhysicalOperatorType(metric.second.GetValue<uint8_t>());
			metrics_map[key] = EnumUtil::ToString(type);
		} else {
			metrics_map[key] = metric.second.ToString();
		}
	}

	auto map = sabot_sql::Value::MAP(metrics_map);
	return reinterpret_cast<sabot_sql_value>(new sabot_sql::Value(map));
}

idx_t sabot_sql_profiling_info_get_child_count(sabot_sql_profiling_info info) {
	if (!info) {
		return 0;
	}
	auto &node = *reinterpret_cast<sabot_sql::ProfilingNode *>(info);
	return node.GetChildCount();
}

sabot_sql_profiling_info sabot_sql_profiling_info_get_child(sabot_sql_profiling_info info, idx_t index) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<sabot_sql::ProfilingNode *>(info);
	if (index >= node.GetChildCount()) {
		return nullptr;
	}

	ProfilingNode *profiling_info_ptr = node.GetChild(index).get();
	return reinterpret_cast<sabot_sql_profiling_info>(profiling_info_ptr);
}
