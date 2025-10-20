#include "sabot_sql/streaming/source_connector.h"
#include <sstream>
#include <nlohmann/json.hpp>

namespace sabot_sql {
namespace streaming {

// ========== Offset Serialization ==========

std::string Offset::ToJson() const {
    nlohmann::json j;
    j["connector_id"] = connector_id;
    j["partition"] = partition;
    j["offset"] = offset;
    j["timestamp"] = timestamp;
    return j.dump();
}

arrow::Result<Offset> Offset::FromJson(const std::string& json) {
    try {
        auto j = nlohmann::json::parse(json);
        Offset offset;
        offset.connector_id = j["connector_id"];
        offset.partition = j["partition"];
        offset.offset = j["offset"];
        offset.timestamp = j["timestamp"];
        return offset;
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to parse offset JSON: " + std::string(e.what())
        );
    }
}

// ========== Connector Factory ==========

std::unordered_map<std::string, ConnectorFactory::ConnectorCreator>& 
ConnectorFactory::GetRegistry() {
    static std::unordered_map<std::string, ConnectorCreator> registry;
    return registry;
}

void ConnectorFactory::RegisterConnector(
    const std::string& type,
    ConnectorCreator creator
) {
    GetRegistry()[type] = std::move(creator);
}

arrow::Result<std::unique_ptr<SourceConnector>> 
ConnectorFactory::CreateConnector(const ConnectorConfig& config) {
    auto& registry = GetRegistry();
    auto it = registry.find(config.connector_type);
    
    if (it == registry.end()) {
        return arrow::Status::Invalid(
            "Unknown connector type: " + config.connector_type
        );
    }
    
    auto connector = it->second(config);
    ARROW_RETURN_NOT_OK(connector->Initialize(config));
    
    return connector;
}

} // namespace streaming
} // namespace sabot_sql

