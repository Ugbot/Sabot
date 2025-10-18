/************************************************************************
MarbleDB Column Family Implementation
**************************************************************************/

#include "marble/column_family.h"
#include "marble/lsm_tree.h"
#include "marble/memtable.h"
#include "marble/json_utils.h"
#include <sstream>

namespace marble {

// ColumnFamilySet implementation
ColumnFamilySet::ColumnFamilySet() {
    // Create default column family
    ColumnFamilyDescriptor default_descriptor("default", ColumnFamilyOptions());
    ColumnFamilyHandle* handle = nullptr;
    auto status = CreateColumnFamily(default_descriptor, &handle);
    if (status.ok()) {
        default_cf_ = handle;
    }
}

ColumnFamilySet::~ColumnFamilySet() {
    // Handles are owned by unique_ptr in families_ map
}

Status ColumnFamilySet::CreateColumnFamily(const ColumnFamilyDescriptor& descriptor,
                                          ColumnFamilyHandle** handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if CF already exists
    if (families_.find(descriptor.name) != families_.end()) {
        return Status::InvalidArgument("Column family already exists: " + descriptor.name);
    }
    
    // Create new handle
    uint32_t id = next_id_++;
    auto cf_handle = std::make_unique<ColumnFamilyHandle>(descriptor.name, id);
    cf_handle->options_ = descriptor.options;
    
    // Initialize LSM tree components
    // TODO: Create memtable and levels based on schema
    
    *handle = cf_handle.get();
    id_to_handle_[id] = cf_handle.get();
    families_[descriptor.name] = std::move(cf_handle);
    
    return Status::OK();
}

ColumnFamilyHandle* ColumnFamilySet::GetColumnFamily(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = families_.find(name);
    if (it == families_.end()) {
        return nullptr;
    }
    
    return it->second.get();
}

ColumnFamilyHandle* ColumnFamilySet::GetColumnFamily(uint32_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = id_to_handle_.find(id);
    if (it == id_to_handle_.end()) {
        return nullptr;
    }
    
    return it->second;
}

Status ColumnFamilySet::DropColumnFamily(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Cannot drop default CF
    if (name == "default") {
        return Status::InvalidArgument("Cannot drop default column family");
    }
    
    auto it = families_.find(name);
    if (it == families_.end()) {
        return Status::NotFound("Column family not found: " + name);
    }
    
    uint32_t id = it->second->id();
    id_to_handle_.erase(id);
    families_.erase(it);
    
    // TODO: Clean up LSM tree files for this CF
    
    return Status::OK();
}

Status ColumnFamilySet::DropColumnFamily(ColumnFamilyHandle* handle) {
    if (!handle) {
        return Status::InvalidArgument("Handle is null");
    }
    return DropColumnFamily(handle->name());
}

std::vector<std::string> ColumnFamilySet::ListColumnFamilies() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> names;
    names.reserve(families_.size());
    
    for (const auto& pair : families_) {
        names.push_back(pair.first);
    }
    
    std::sort(names.begin(), names.end());
    return names;
}

ColumnFamilyHandle* ColumnFamilySet::DefaultColumnFamily() {
    return default_cf_;
}

// ColumnFamilyMetadata implementation
std::string ColumnFamilyMetadata::Serialize() const {
    std::ostringstream oss;
    oss << id << "\n"
        << name << "\n"
        << schema_json << "\n"
        << options_json;
    return oss.str();
}

Status ColumnFamilyMetadata::Deserialize(const std::string& data, ColumnFamilyMetadata* metadata) {
    std::istringstream iss(data);
    
    std::string line;
    
    // Parse ID
    if (!std::getline(iss, line)) {
        return Status::Corruption("Failed to parse CF id");
    }
    metadata->id = std::stoul(line);
    
    // Parse name
    if (!std::getline(iss, metadata->name)) {
        return Status::Corruption("Failed to parse CF name");
    }
    
    // Parse schema JSON
    if (!std::getline(iss, metadata->schema_json)) {
        return Status::Corruption("Failed to parse CF schema");
    }
    
    // Parse options JSON
    if (!std::getline(iss, metadata->options_json)) {
        return Status::Corruption("Failed to parse CF options");
    }
    
    return Status::OK();
}

// ===== Capability Serialization =====

namespace CapabilitySerialization {

std::string SerializeCapabilities(const TableCapabilities& caps) {
    // Use simdjson Builder for clean JSON generation
    json::Builder builder;

    builder.StartObject();

    // MVCC
    builder.Key("enable_mvcc");
    builder.Bool(caps.enable_mvcc);

    if (caps.enable_mvcc) {
        builder.Key("mvcc_settings");
        builder.StartObject();
        builder.Key("gc_policy");
        builder.Int(static_cast<int>(caps.mvcc_settings.gc_policy));
        builder.Key("max_versions_per_key");
        builder.UInt(caps.mvcc_settings.max_versions_per_key);
        builder.Key("gc_timestamp_ms");
        builder.UInt(caps.mvcc_settings.gc_timestamp_ms);
        builder.Key("max_active_snapshots");
        builder.UInt(caps.mvcc_settings.max_active_snapshots);
        builder.Key("snapshot_ttl_ms");
        builder.UInt(caps.mvcc_settings.snapshot_ttl_ms);
        builder.EndObject();
    }

    // Temporal
    builder.Key("temporal_model");
    builder.Int(static_cast<int>(caps.temporal_model));

    if (caps.temporal_model != TableCapabilities::TemporalModel::kNone) {
        builder.Key("temporal_settings");
        builder.StartObject();
        builder.Key("max_snapshots");
        builder.UInt(caps.temporal_settings.max_snapshots);
        builder.Key("snapshot_retention_ms");
        builder.UInt(caps.temporal_settings.snapshot_retention_ms);
        builder.Key("min_valid_time_ms");
        builder.UInt(caps.temporal_settings.min_valid_time_ms);
        builder.Key("max_valid_time_ms");
        builder.UInt(caps.temporal_settings.max_valid_time_ms);
        builder.Key("auto_create_snapshots");
        builder.Bool(caps.temporal_settings.auto_create_snapshots);
        builder.Key("snapshot_interval_ms");
        builder.UInt(caps.temporal_settings.snapshot_interval_ms);
        builder.EndObject();
    }

    // Full-text search
    builder.Key("enable_full_text_search");
    builder.Bool(caps.enable_full_text_search);

    if (caps.enable_full_text_search) {
        builder.Key("search_settings");
        builder.StartObject();
        builder.Key("indexed_columns");
        builder.StartArray();
        for (const auto& col : caps.search_settings.indexed_columns) {
            builder.String(col);
        }
        builder.EndArray();
        builder.Key("analyzer");
        builder.Int(static_cast<int>(caps.search_settings.analyzer));
        builder.Key("enable_term_positions");
        builder.Bool(caps.search_settings.enable_term_positions);
        builder.Key("enable_term_offsets");
        builder.Bool(caps.search_settings.enable_term_offsets);
        builder.Key("max_terms_per_doc");
        builder.UInt(caps.search_settings.max_terms_per_doc);
        builder.Key("dictionary_cache_mb");
        builder.UInt(caps.search_settings.dictionary_cache_mb);
        builder.EndObject();
    }

    // TTL
    builder.Key("enable_ttl");
    builder.Bool(caps.enable_ttl);

    if (caps.enable_ttl) {
        builder.Key("ttl_settings");
        builder.StartObject();
        builder.Key("timestamp_column");
        builder.String(caps.ttl_settings.timestamp_column);
        builder.Key("ttl_duration_ms");
        builder.UInt(caps.ttl_settings.ttl_duration_ms);
        builder.Key("cleanup_policy");
        builder.Int(static_cast<int>(caps.ttl_settings.cleanup_policy));
        builder.Key("cleanup_interval_ms");
        builder.UInt(caps.ttl_settings.cleanup_interval_ms);
        builder.EndObject();
    }

    // Caches and zero-copy
    builder.Key("enable_zero_copy_reads");
    builder.Bool(caps.enable_zero_copy_reads);

    builder.Key("enable_hot_key_cache");
    builder.Bool(caps.enable_hot_key_cache);

    if (caps.enable_hot_key_cache) {
        builder.Key("hot_key_cache_settings");
        builder.StartObject();
        builder.Key("cache_size_mb");
        builder.UInt(caps.hot_key_cache_settings.cache_size_mb);
        builder.Key("promotion_threshold");
        builder.UInt(caps.hot_key_cache_settings.promotion_threshold);
        builder.EndObject();
    }

    builder.Key("enable_negative_cache");
    builder.Bool(caps.enable_negative_cache);

    if (caps.enable_negative_cache) {
        builder.Key("negative_cache_settings");
        builder.StartObject();
        builder.Key("max_entries");
        builder.UInt(caps.negative_cache_settings.max_entries);
        builder.EndObject();
    }

    builder.EndObject();
    return builder.ToString();
}

Status DeserializeCapabilities(const std::string& json_str, TableCapabilities* caps) {
    if (!caps) {
        return Status::InvalidArgument("Capabilities pointer is null");
    }

    // Initialize with defaults
    *caps = TableCapabilities();

    // Parse JSON using simdjson
    json::Parser parser;
    simdjson::ondemand::document doc;
    auto status = parser.Parse(json_str, doc);
    if (!status.ok()) {
        return status;
    }

    try {
        auto obj = doc.get_object();
        if (obj.error()) {
            return Status::InvalidArgument("Invalid JSON object");
        }
        auto root = obj.value_unsafe();

        // Parse MVCC
        bool enable_mvcc = false;
        auto mvcc_status = json::GetBool(root, "enable_mvcc", &enable_mvcc);
        if (mvcc_status.ok()) {
            caps->enable_mvcc = enable_mvcc;

            if (enable_mvcc) {
                auto mvcc_obj_result = root["mvcc_settings"];
                if (!mvcc_obj_result.error()) {
                    auto mvcc_obj = mvcc_obj_result.get_object();
                    if (!mvcc_obj.error()) {
                        auto settings = mvcc_obj.value_unsafe();
                        int64_t gc_policy = 0;
                        json::TryGet(settings, "gc_policy", json::GetInt64, &gc_policy);
                        caps->mvcc_settings.gc_policy =
                            static_cast<TableCapabilities::MVCCSettings::GCPolicy>(gc_policy);

                        uint64_t max_versions = 0;
                        json::TryGet(settings, "max_versions_per_key", json::GetUInt64, &max_versions);
                        if (max_versions > 0) caps->mvcc_settings.max_versions_per_key = max_versions;

                        uint64_t gc_timestamp = 0;
                        json::TryGet(settings, "gc_timestamp_ms", json::GetUInt64, &gc_timestamp);
                        caps->mvcc_settings.gc_timestamp_ms = gc_timestamp;

                        uint64_t max_snapshots = 0;
                        json::TryGet(settings, "max_active_snapshots", json::GetUInt64, &max_snapshots);
                        if (max_snapshots > 0) caps->mvcc_settings.max_active_snapshots = max_snapshots;

                        uint64_t snapshot_ttl = 0;
                        json::TryGet(settings, "snapshot_ttl_ms", json::GetUInt64, &snapshot_ttl);
                        if (snapshot_ttl > 0) caps->mvcc_settings.snapshot_ttl_ms = snapshot_ttl;
                    }
                }
            }
        }

        // Parse Temporal
        int64_t temporal_model = 0;
        auto temporal_status = json::GetInt64(root, "temporal_model", &temporal_model);
        if (temporal_status.ok()) {
            caps->temporal_model = static_cast<TableCapabilities::TemporalModel>(temporal_model);

            if (caps->temporal_model != TableCapabilities::TemporalModel::kNone) {
                auto temporal_obj_result = root["temporal_settings"];
                if (!temporal_obj_result.error()) {
                    auto temporal_obj = temporal_obj_result.get_object();
                    if (!temporal_obj.error()) {
                        auto settings = temporal_obj.value_unsafe();

                        uint64_t max_snapshots = 0;
                        json::TryGet(settings, "max_snapshots", json::GetUInt64, &max_snapshots);
                        if (max_snapshots > 0) caps->temporal_settings.max_snapshots = max_snapshots;

                        uint64_t retention = 0;
                        json::TryGet(settings, "snapshot_retention_ms", json::GetUInt64, &retention);
                        if (retention > 0) caps->temporal_settings.snapshot_retention_ms = retention;

                        uint64_t min_valid = 0;
                        json::TryGet(settings, "min_valid_time_ms", json::GetUInt64, &min_valid);
                        caps->temporal_settings.min_valid_time_ms = min_valid;

                        uint64_t max_valid = 0;
                        json::TryGet(settings, "max_valid_time_ms", json::GetUInt64, &max_valid);
                        if (max_valid > 0) caps->temporal_settings.max_valid_time_ms = max_valid;

                        bool auto_create = false;
                        json::TryGet(settings, "auto_create_snapshots", json::GetBool, &auto_create);
                        caps->temporal_settings.auto_create_snapshots = auto_create;

                        uint64_t interval = 0;
                        json::TryGet(settings, "snapshot_interval_ms", json::GetUInt64, &interval);
                        if (interval > 0) caps->temporal_settings.snapshot_interval_ms = interval;
                    }
                }
            }
        }

        // Parse Full-text search
        bool enable_search = false;
        auto search_status = json::GetBool(root, "enable_full_text_search", &enable_search);
        if (search_status.ok()) {
            caps->enable_full_text_search = enable_search;

            if (enable_search) {
                auto search_obj_result = root["search_settings"];
                if (!search_obj_result.error()) {
                    auto search_obj = search_obj_result.get_object();
                    if (!search_obj.error()) {
                        auto settings = search_obj.value_unsafe();

                        std::vector<std::string> indexed_columns;
                        json::TryGet(settings, "indexed_columns", json::GetStringArray, &indexed_columns);
                        caps->search_settings.indexed_columns = indexed_columns;

                        int64_t analyzer = 0;
                        json::TryGet(settings, "analyzer", json::GetInt64, &analyzer);
                        caps->search_settings.analyzer =
                            static_cast<TableCapabilities::SearchSettings::Analyzer>(analyzer);

                        bool term_positions = false;
                        json::TryGet(settings, "enable_term_positions", json::GetBool, &term_positions);
                        caps->search_settings.enable_term_positions = term_positions;

                        bool term_offsets = false;
                        json::TryGet(settings, "enable_term_offsets", json::GetBool, &term_offsets);
                        caps->search_settings.enable_term_offsets = term_offsets;

                        uint64_t max_terms = 0;
                        json::TryGet(settings, "max_terms_per_doc", json::GetUInt64, &max_terms);
                        if (max_terms > 0) caps->search_settings.max_terms_per_doc = max_terms;

                        uint64_t dict_cache = 0;
                        json::TryGet(settings, "dictionary_cache_mb", json::GetUInt64, &dict_cache);
                        if (dict_cache > 0) caps->search_settings.dictionary_cache_mb = dict_cache;
                    }
                }
            }
        }

        // Parse TTL
        bool enable_ttl = false;
        auto ttl_status = json::GetBool(root, "enable_ttl", &enable_ttl);
        if (ttl_status.ok()) {
            caps->enable_ttl = enable_ttl;

            if (enable_ttl) {
                auto ttl_obj_result = root["ttl_settings"];
                if (!ttl_obj_result.error()) {
                    auto ttl_obj = ttl_obj_result.get_object();
                    if (!ttl_obj.error()) {
                        auto settings = ttl_obj.value_unsafe();

                        std::string timestamp_col;
                        json::TryGet(settings, "timestamp_column", json::GetString, &timestamp_col);
                        caps->ttl_settings.timestamp_column = timestamp_col;

                        uint64_t duration = 0;
                        json::TryGet(settings, "ttl_duration_ms", json::GetUInt64, &duration);
                        if (duration > 0) caps->ttl_settings.ttl_duration_ms = duration;

                        int64_t cleanup_policy = 0;
                        json::TryGet(settings, "cleanup_policy", json::GetInt64, &cleanup_policy);
                        caps->ttl_settings.cleanup_policy =
                            static_cast<TableCapabilities::TTLSettings::CleanupPolicy>(cleanup_policy);

                        uint64_t cleanup_interval = 0;
                        json::TryGet(settings, "cleanup_interval_ms", json::GetUInt64, &cleanup_interval);
                        if (cleanup_interval > 0) caps->ttl_settings.cleanup_interval_ms = cleanup_interval;
                    }
                }
            }
        }

        // Parse cache settings
        bool zero_copy = false;
        json::TryGet(root, "enable_zero_copy_reads", json::GetBool, &zero_copy);
        caps->enable_zero_copy_reads = zero_copy;

        bool hot_key_cache = false;
        auto cache_status = json::GetBool(root, "enable_hot_key_cache", &hot_key_cache);
        if (cache_status.ok()) {
            caps->enable_hot_key_cache = hot_key_cache;

            if (hot_key_cache) {
                auto cache_obj_result = root["hot_key_cache_settings"];
                if (!cache_obj_result.error()) {
                    auto cache_obj = cache_obj_result.get_object();
                    if (!cache_obj.error()) {
                        auto settings = cache_obj.value_unsafe();

                        uint64_t cache_size = 0;
                        json::TryGet(settings, "cache_size_mb", json::GetUInt64, &cache_size);
                        if (cache_size > 0) caps->hot_key_cache_settings.cache_size_mb = cache_size;

                        uint64_t promotion = 0;
                        json::TryGet(settings, "promotion_threshold", json::GetUInt64, &promotion);
                        if (promotion > 0) caps->hot_key_cache_settings.promotion_threshold = promotion;
                    }
                }
            }
        }

        bool negative_cache = false;
        auto neg_status = json::GetBool(root, "enable_negative_cache", &negative_cache);
        if (neg_status.ok()) {
            caps->enable_negative_cache = negative_cache;

            if (negative_cache) {
                auto neg_obj_result = root["negative_cache_settings"];
                if (!neg_obj_result.error()) {
                    auto neg_obj = neg_obj_result.get_object();
                    if (!neg_obj.error()) {
                        auto settings = neg_obj.value_unsafe();

                        uint64_t max_entries = 0;
                        json::TryGet(settings, "max_entries", json::GetUInt64, &max_entries);
                        if (max_entries > 0) caps->negative_cache_settings.max_entries = max_entries;
                    }
                }
            }
        }

        return Status::OK();

    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON deserialization error: ") + e.what());
    }
}

} // namespace CapabilitySerialization

} // namespace marble

