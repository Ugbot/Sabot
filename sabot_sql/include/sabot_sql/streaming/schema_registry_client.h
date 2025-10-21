#pragma once

#include <arrow/api.h>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace sabot_sql {
namespace streaming {

/**
 * Schema metadata from Confluent Schema Registry
 */
struct RegisteredSchema {
    int schema_id;
    std::string subject;
    int version;
    std::string schema_type;  // AVRO, PROTOBUF, JSON
    std::string schema_definition;
    
    RegisteredSchema() : schema_id(0), version(0) {}
};

/**
 * Schema compatibility modes
 */
enum class CompatibilityMode {
    NONE,
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE
};

/**
 * Confluent Schema Registry HTTP client
 * 
 * Provides:
 * - Schema registration and retrieval
 * - Schema caching for performance
 * - Compatibility checking
 * - Subject management
 * 
 * Wire format:
 * - Byte 0: Magic byte (0x00)
 * - Bytes 1-4: Schema ID (big-endian int32)
 * - Bytes 5+: Serialized data
 */
class SchemaRegistryClient {
public:
    /**
     * Initialize Schema Registry client
     * 
     * @param url Schema Registry URL (e.g., "http://localhost:8081")
     * @param timeout_ms HTTP request timeout in milliseconds
     */
    explicit SchemaRegistryClient(
        const std::string& url,
        int timeout_ms = 5000
    );
    
    ~SchemaRegistryClient();
    
    // ========== Schema Operations ==========
    
    /**
     * Register a new schema
     * 
     * @param subject Subject name (e.g., "transactions-value")
     * @param schema Schema definition (Avro, Protobuf, or JSON Schema)
     * @param schema_type Schema type (AVRO, PROTOBUF, JSON)
     * @return Schema ID
     */
    arrow::Result<int> RegisterSchema(
        const std::string& subject,
        const std::string& schema,
        const std::string& schema_type = "AVRO"
    );
    
    /**
     * Get schema by ID
     * 
     * @param schema_id Schema ID
     * @return RegisteredSchema
     */
    arrow::Result<RegisteredSchema> GetSchemaById(int schema_id);
    
    /**
     * Get latest schema for subject
     * 
     * @param subject Subject name
     * @return RegisteredSchema
     */
    arrow::Result<RegisteredSchema> GetLatestSchema(const std::string& subject);
    
    /**
     * Get specific version of schema
     * 
     * @param subject Subject name
     * @param version Version number
     * @return RegisteredSchema
     */
    arrow::Result<RegisteredSchema> GetSchemaByVersion(
        const std::string& subject,
        int version
    );
    
    /**
     * Check if subject exists
     * 
     * @param subject Subject name
     * @return true if subject exists
     */
    arrow::Result<bool> SubjectExists(const std::string& subject);
    
    /**
     * Delete subject and all versions
     * 
     * @param subject Subject name
     * @return Deleted version numbers
     */
    arrow::Result<std::vector<int>> DeleteSubject(const std::string& subject);
    
    // ========== Compatibility Operations ==========
    
    /**
     * Test schema compatibility
     * 
     * @param subject Subject name
     * @param schema Schema definition
     * @param schema_type Schema type
     * @return true if compatible
     */
    arrow::Result<bool> TestCompatibility(
        const std::string& subject,
        const std::string& schema,
        const std::string& schema_type = "AVRO"
    );
    
    /**
     * Set compatibility mode for subject
     * 
     * @param subject Subject name
     * @param mode Compatibility mode
     * @return Status
     */
    arrow::Status SetCompatibility(
        const std::string& subject,
        CompatibilityMode mode
    );
    
    // ========== Wire Format Helpers ==========
    
    /**
     * Extract schema ID from wire format message
     * 
     * @param data Wire format data (magic byte + schema ID + payload)
     * @return Schema ID
     */
    static arrow::Result<int> ExtractSchemaId(const uint8_t* data, size_t len);
    
    /**
     * Get payload after schema ID
     * 
     * @param data Wire format data
     * @param len Data length
     * @return Pointer to payload and payload length
     */
    static arrow::Result<std::pair<const uint8_t*, size_t>> 
    ExtractPayload(const uint8_t* data, size_t len);
    
    /**
     * Create wire format message
     * 
     * @param schema_id Schema ID
     * @param payload Serialized payload
     * @return Wire format bytes (magic + schema ID + payload)
     */
    static arrow::Result<std::vector<uint8_t>> 
    CreateWireFormat(int schema_id, const std::vector<uint8_t>& payload);
    
private:
    // Configuration
    std::string base_url_;
    int timeout_ms_;
    
    // Schema cache (schema_id -> schema)
    std::unordered_map<int, RegisteredSchema> schema_cache_;
    mutable std::mutex cache_mutex_;
    
    // HTTP helper
    arrow::Result<std::string> MakeHTTPRequest(
        const std::string& method,
        const std::string& endpoint,
        const std::string& body = ""
    );
    
    // Cache helpers
    void CacheSchema(int schema_id, const RegisteredSchema& schema);
    arrow::Result<RegisteredSchema> GetCachedSchema(int schema_id);
    
    // JSON parsing
    arrow::Result<RegisteredSchema> ParseSchemaResponse(const std::string& json);
    arrow::Result<int> ParseRegisterResponse(const std::string& json);
};

} // namespace streaming
} // namespace sabot_sql

