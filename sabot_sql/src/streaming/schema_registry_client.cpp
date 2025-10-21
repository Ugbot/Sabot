#include "sabot_sql/streaming/schema_registry_client.h"
#include <simdjson.h>
#include <curl/curl.h>
#include <sstream>
#include <iostream>

namespace sabot_sql {
namespace streaming {

// CURL callback for response data
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

SchemaRegistryClient::SchemaRegistryClient(
    const std::string& url,
    int timeout_ms)
    : base_url_(url), timeout_ms_(timeout_ms) {
    
    // Initialize CURL globally
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

SchemaRegistryClient::~SchemaRegistryClient() {
    curl_global_cleanup();
}

// ========== Schema Operations ==========

arrow::Result<int> SchemaRegistryClient::RegisterSchema(
    const std::string& subject,
    const std::string& schema,
    const std::string& schema_type) {
    
    // Build request body
    std::ostringstream body_stream;
    body_stream << "{"
                << "\"schema\":\"" << schema << "\","
                << "\"schemaType\":\"" << schema_type << "\""
                << "}";
    
    std::string endpoint = "/subjects/" + subject + "/versions";
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("POST", endpoint, body_stream.str())
    );
    
    return ParseRegisterResponse(response);
}

arrow::Result<RegisteredSchema> SchemaRegistryClient::GetSchemaById(int schema_id) {
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = schema_cache_.find(schema_id);
        if (it != schema_cache_.end()) {
            return it->second;
        }
    }
    
    // Fetch from registry
    std::string endpoint = "/schemas/ids/" + std::to_string(schema_id);
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("GET", endpoint)
    );
    
    ARROW_ASSIGN_OR_RAISE(auto schema, ParseSchemaResponse(response));
    
    // Cache the schema
    CacheSchema(schema_id, schema);
    
    return schema;
}

arrow::Result<RegisteredSchema> SchemaRegistryClient::GetLatestSchema(
    const std::string& subject) {
    
    std::string endpoint = "/subjects/" + subject + "/versions/latest";
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("GET", endpoint)
    );
    
    return ParseSchemaResponse(response);
}

arrow::Result<RegisteredSchema> SchemaRegistryClient::GetSchemaByVersion(
    const std::string& subject,
    int version) {
    
    std::string endpoint = "/subjects/" + subject + "/versions/" + std::to_string(version);
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("GET", endpoint)
    );
    
    return ParseSchemaResponse(response);
}

arrow::Result<bool> SchemaRegistryClient::SubjectExists(const std::string& subject) {
    std::string endpoint = "/subjects/" + subject;
    
    auto response = MakeHTTPRequest("GET", endpoint);
    if (!response.ok()) {
        // Subject doesn't exist (404)
        return false;
    }
    
    return true;
}

arrow::Result<std::vector<int>> SchemaRegistryClient::DeleteSubject(
    const std::string& subject) {
    
    std::string endpoint = "/subjects/" + subject;
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("DELETE", endpoint)
    );
    
    // Parse array of deleted versions
    // TODO: Implement JSON array parsing
    return std::vector<int>();
}

// ========== Compatibility Operations ==========

arrow::Result<bool> SchemaRegistryClient::TestCompatibility(
    const std::string& subject,
    const std::string& schema,
    const std::string& schema_type) {
    
    // Build request body
    std::ostringstream body_stream;
    body_stream << "{"
                << "\"schema\":\"" << schema << "\","
                << "\"schemaType\":\"" << schema_type << "\""
                << "}";
    
    std::string endpoint = "/compatibility/subjects/" + subject + "/versions/latest";
    
    ARROW_ASSIGN_OR_RAISE(
        auto response,
        MakeHTTPRequest("POST", endpoint, body_stream.str())
    );
    
    // Parse {"is_compatible": true/false}
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(response);
    if (doc.error()) {
        return arrow::Status::Invalid("Failed to parse compatibility response");
    }
    
    auto is_compatible = doc.value()["is_compatible"].get_bool();
    if (is_compatible.error()) {
        return arrow::Status::Invalid("Invalid compatibility response format");
    }
    
    return is_compatible.value();
}

arrow::Status SchemaRegistryClient::SetCompatibility(
    const std::string& subject,
    CompatibilityMode mode) {
    
    // Map enum to string
    std::string mode_str;
    switch (mode) {
        case CompatibilityMode::NONE: mode_str = "NONE"; break;
        case CompatibilityMode::BACKWARD: mode_str = "BACKWARD"; break;
        case CompatibilityMode::BACKWARD_TRANSITIVE: mode_str = "BACKWARD_TRANSITIVE"; break;
        case CompatibilityMode::FORWARD: mode_str = "FORWARD"; break;
        case CompatibilityMode::FORWARD_TRANSITIVE: mode_str = "FORWARD_TRANSITIVE"; break;
        case CompatibilityMode::FULL: mode_str = "FULL"; break;
        case CompatibilityMode::FULL_TRANSITIVE: mode_str = "FULL_TRANSITIVE"; break;
    }
    
    // Build request body
    std::string body = "{\"compatibility\":\"" + mode_str + "\"}";
    
    std::string endpoint = "/config/" + subject;
    
    auto response = MakeHTTPRequest("PUT", endpoint, body);
    if (!response.ok()) {
        return response.status();
    }
    
    return arrow::Status::OK();
}

// ========== Wire Format Helpers ==========

arrow::Result<int> SchemaRegistryClient::ExtractSchemaId(
    const uint8_t* data,
    size_t len) {
    
    if (len < 5) {
        return arrow::Status::Invalid("Data too short for wire format (need at least 5 bytes)");
    }
    
    // Check magic byte
    if (data[0] != 0x00) {
        return arrow::Status::Invalid("Invalid magic byte (expected 0x00)");
    }
    
    // Extract schema ID (big-endian int32)
    int schema_id = (static_cast<int>(data[1]) << 24) |
                    (static_cast<int>(data[2]) << 16) |
                    (static_cast<int>(data[3]) << 8) |
                    static_cast<int>(data[4]);
    
    return schema_id;
}

arrow::Result<std::pair<const uint8_t*, size_t>> 
SchemaRegistryClient::ExtractPayload(const uint8_t* data, size_t len) {
    if (len < 5) {
        return arrow::Status::Invalid("Data too short for wire format");
    }
    
    // Payload starts at byte 5
    return std::make_pair(data + 5, len - 5);
}

arrow::Result<std::vector<uint8_t>> 
SchemaRegistryClient::CreateWireFormat(
    int schema_id,
    const std::vector<uint8_t>& payload) {
    
    std::vector<uint8_t> wire_format;
    wire_format.reserve(5 + payload.size());
    
    // Magic byte
    wire_format.push_back(0x00);
    
    // Schema ID (big-endian int32)
    wire_format.push_back(static_cast<uint8_t>((schema_id >> 24) & 0xFF));
    wire_format.push_back(static_cast<uint8_t>((schema_id >> 16) & 0xFF));
    wire_format.push_back(static_cast<uint8_t>((schema_id >> 8) & 0xFF));
    wire_format.push_back(static_cast<uint8_t>(schema_id & 0xFF));
    
    // Payload
    wire_format.insert(wire_format.end(), payload.begin(), payload.end());
    
    return wire_format;
}

// ========== Private Helpers ==========

arrow::Result<std::string> SchemaRegistryClient::MakeHTTPRequest(
    const std::string& method,
    const std::string& endpoint,
    const std::string& body) {
    
    CURL* curl = curl_easy_init();
    if (!curl) {
        return arrow::Status::IOError("Failed to initialize CURL");
    }
    
    std::string response_data;
    std::string url = base_url_ + endpoint;
    
    // Set URL
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    
    // Set method
    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    } else if (method == "PUT") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    } else if (method == "DELETE") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    }
    
    // Set headers
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/vnd.schemaregistry.v1+json");
    headers = curl_slist_append(headers, "Accept: application/vnd.schemaregistry.v1+json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    // Set timeout
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout_ms_));
    
    // Set callback
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_data);
    
    // Perform request
    CURLcode res = curl_easy_perform(curl);
    
    // Get HTTP response code
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    
    // Cleanup
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    
    if (res != CURLE_OK) {
        return arrow::Status::IOError(
            "CURL request failed: " + std::string(curl_easy_strerror(res))
        );
    }
    
    if (http_code >= 400) {
        return arrow::Status::IOError(
            "HTTP error " + std::to_string(http_code) + ": " + response_data
        );
    }
    
    return response_data;
}

void SchemaRegistryClient::CacheSchema(int schema_id, const RegisteredSchema& schema) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    schema_cache_[schema_id] = schema;
}

arrow::Result<RegisteredSchema> SchemaRegistryClient::GetCachedSchema(int schema_id) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = schema_cache_.find(schema_id);
    if (it == schema_cache_.end()) {
        return arrow::Status::KeyError("Schema ID not in cache: " + std::to_string(schema_id));
    }
    return it->second;
}

arrow::Result<RegisteredSchema> SchemaRegistryClient::ParseSchemaResponse(
    const std::string& json) {
    
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(json);
    if (doc.error()) {
        return arrow::Status::Invalid("Failed to parse Schema Registry response");
    }
    
    RegisteredSchema schema;
    
    // Extract fields
    auto id_result = doc.value()["id"].get_int64();
    if (!id_result.error()) {
        schema.schema_id = static_cast<int>(id_result.value());
    }
    
    auto subject_result = doc.value()["subject"].get_string();
    if (!subject_result.error()) {
        schema.subject = std::string(subject_result.value());
    }
    
    auto version_result = doc.value()["version"].get_int64();
    if (!version_result.error()) {
        schema.version = static_cast<int>(version_result.value());
    }
    
    auto schema_str_result = doc.value()["schema"].get_string();
    if (!schema_str_result.error()) {
        schema.schema_definition = std::string(schema_str_result.value());
    }
    
    auto type_result = doc.value()["schemaType"].get_string();
    if (!type_result.error()) {
        schema.schema_type = std::string(type_result.value());
    } else {
        schema.schema_type = "AVRO";  // Default
    }
    
    return schema;
}

arrow::Result<int> SchemaRegistryClient::ParseRegisterResponse(
    const std::string& json) {
    
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(json);
    if (doc.error()) {
        return arrow::Status::Invalid("Failed to parse register response");
    }
    
    auto id_result = doc.value()["id"].get_int64();
    if (id_result.error()) {
        return arrow::Status::Invalid("No 'id' field in register response");
    }
    
    return static_cast<int>(id_result.value());
}

} // namespace streaming
} // namespace sabot_sql

