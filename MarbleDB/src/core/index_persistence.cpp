/**
 * Index Persistence Manager Implementation
 */

#include "marble/index_persistence.h"
#include <simdjson.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>

namespace fs = std::filesystem;

namespace marble {

//==============================================================================
// IndexManifest Implementation
//==============================================================================

std::string IndexManifest::ToJSON() const {
    std::ostringstream ss;
    ss << "{\n";
    ss << "  \"version\": " << version << ",\n";

    // Column families array
    ss << "  \"column_families\": [\n";
    bool first_cf = true;
    for (const auto& [cf_id, cf_meta] : column_families) {
        if (!first_cf) ss << ",\n";
        first_cf = false;

        ss << "    {\n";
        ss << "      \"cf_id\": " << cf_meta.cf_id << ",\n";
        ss << "      \"cf_name\": \"" << cf_meta.cf_name << "\",\n";

        // Bloom filters array
        ss << "      \"bloom_filters\": [";
        for (size_t i = 0; i < cf_meta.bloom_filter_files.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "\"" << cf_meta.bloom_filter_files[i] << "\"";
        }
        ss << "],\n";

        // Skipping indexes array
        ss << "      \"skipping_indexes\": [";
        for (size_t i = 0; i < cf_meta.skipping_index_files.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "\"" << cf_meta.skipping_index_files[i] << "\"";
        }
        ss << "]\n";
        ss << "    }";
    }
    ss << "\n  ],\n";

    // Batch index map
    ss << "  \"batch_index_map\": {\n";
    bool first_entry = true;
    for (const auto& [key, entry] : batch_index_map) {
        if (!first_entry) ss << ",\n";
        first_entry = false;

        ss << "    \"" << key << "\": {\n";
        ss << "      \"cf_id\": " << entry.cf_id << ",\n";
        ss << "      \"batch_id\": " << entry.batch_id << ",\n";
        ss << "      \"bloom_filter\": \"" << entry.bloom_filter_path << "\",\n";
        ss << "      \"skipping_index\": \"" << entry.skipping_index_path << "\"\n";
        ss << "    }";
    }
    ss << "\n  }\n";
    ss << "}\n";

    return ss.str();
}

Status IndexManifest::FromJSON(const std::string& json_str, IndexManifest* manifest) {
    if (!manifest) {
        return Status::InvalidArgument("Manifest pointer is null");
    }

    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string padded_json(json_str);
        simdjson::ondemand::document doc = parser.iterate(padded_json);

        // Parse version
        uint64_t ver;
        auto version_error = doc["version"].get(ver);
        if (!version_error) {
            manifest->version = static_cast<uint32_t>(ver);
        }

        // Parse column families array
        auto cf_array_result = doc["column_families"].get_array();
        if (!cf_array_result.error()) {
            for (auto cf_element : cf_array_result.value()) {
                ColumnFamilyIndexMeta meta;

                uint64_t cf_id_val;
                auto cf_id_error = cf_element["cf_id"].get(cf_id_val);
                if (!cf_id_error) {
                    meta.cf_id = static_cast<uint32_t>(cf_id_val);
                }

                std::string_view cf_name_view;
                auto cf_name_error = cf_element["cf_name"].get(cf_name_view);
                if (!cf_name_error) {
                    meta.cf_name = std::string(cf_name_view);
                }

                // Parse bloom_filters array
                auto bloom_array_result = cf_element["bloom_filters"].get_array();
                if (!bloom_array_result.error()) {
                    for (auto bloom_elem : bloom_array_result.value()) {
                        std::string_view bloom_str;
                        auto bloom_error = bloom_elem.get(bloom_str);
                        if (!bloom_error) {
                            meta.bloom_filter_files.push_back(std::string(bloom_str));
                        }
                    }
                }

                // Parse skipping_indexes array
                auto skip_array_result = cf_element["skipping_indexes"].get_array();
                if (!skip_array_result.error()) {
                    for (auto skip_elem : skip_array_result.value()) {
                        std::string_view skip_str;
                        auto skip_error = skip_elem.get(skip_str);
                        if (!skip_error) {
                            meta.skipping_index_files.push_back(std::string(skip_str));
                        }
                    }
                }

                manifest->column_families[meta.cf_id] = meta;
            }
        }

        // Parse batch_index_map object
        auto batch_map_result = doc["batch_index_map"].get_object();
        if (!batch_map_result.error()) {
            for (auto field : batch_map_result.value()) {
                std::string key(field.unescaped_key().value());
                auto entry_obj = field.value();

                IndexManifestEntry entry;

                uint64_t cf_id_val;
                auto entry_cf_id_error = entry_obj["cf_id"].get(cf_id_val);
                if (!entry_cf_id_error) {
                    entry.cf_id = static_cast<uint32_t>(cf_id_val);
                }

                uint64_t batch_id_val;
                auto batch_id_error = entry_obj["batch_id"].get(batch_id_val);
                if (!batch_id_error) {
                    entry.batch_id = batch_id_val;
                }

                std::string_view bloom_path;
                auto bloom_path_error = entry_obj["bloom_filter"].get(bloom_path);
                if (!bloom_path_error) {
                    entry.bloom_filter_path = std::string(bloom_path);
                }

                std::string_view skip_path;
                auto skip_path_error = entry_obj["skipping_index"].get(skip_path);
                if (!skip_path_error) {
                    entry.skipping_index_path = std::string(skip_path);
                }

                manifest->batch_index_map[key] = entry;
            }
        }

        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("Failed to parse manifest JSON: ") + simdjson::error_message(e.error()));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Failed to parse manifest JSON: ") + e.what());
    }
}

//==============================================================================
// IndexPersistenceManager Implementation
//==============================================================================

IndexPersistenceManager::IndexPersistenceManager(const std::string& base_path)
    : base_path_(base_path),
      indexes_path_(base_path + "/indexes"),
      manifest_path_(base_path + "/indexes/manifest.json") {
}

Status IndexPersistenceManager::Initialize() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create indexes directory
    auto status = CreateDirectory(indexes_path_);
    if (!status.ok()) {
        return status;
    }

    // Try to load existing manifest
    status = LoadManifest();
    if (!status.ok()) {
        // Manifest doesn't exist or is invalid - create new one
        manifest_ = IndexManifest();
        return SaveManifest();
    }

    return Status::OK();
}

Status IndexPersistenceManager::SaveBloomFilter(uint32_t cf_id, uint64_t batch_id,
                                                  const std::vector<uint8_t>& serialized_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Ensure CF directories exist
    auto status = CreateColumnFamilyDirectories(cf_id);
    if (!status.ok()) {
        return status;
    }

    // Get file path
    std::string file_path = GetBloomFilterPath(cf_id, batch_id);

    // Write bloom filter to file
    try {
        std::ofstream ofs(file_path, std::ios::binary);
        if (!ofs) {
            return Status::IOError("Failed to open bloom filter file for writing: " + file_path);
        }

        ofs.write(reinterpret_cast<const char*>(serialized_data.data()), serialized_data.size());
        ofs.close();

        if (!ofs) {
            return Status::IOError("Failed to write bloom filter file: " + file_path);
        }

        // Update manifest
        std::string key = std::to_string(cf_id) + ":" + std::to_string(batch_id);
        IndexManifestEntry entry(cf_id, batch_id);
        entry.bloom_filter_path = "cf_" + std::to_string(cf_id) + "/bloom/batch_" +
                                  std::to_string(batch_id) + ".bloom";
        manifest_.batch_index_map[key] = entry;

        // Update CF metadata
        auto& cf_meta = manifest_.column_families[cf_id];
        auto it = std::find(cf_meta.bloom_filter_files.begin(),
                            cf_meta.bloom_filter_files.end(), entry.bloom_filter_path);
        if (it == cf_meta.bloom_filter_files.end()) {
            cf_meta.bloom_filter_files.push_back(entry.bloom_filter_path);
        }

        // Save manifest
        return SaveManifest();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception writing bloom filter: ") + e.what());
    }
}

Status IndexPersistenceManager::LoadBloomFilter(uint32_t cf_id, uint64_t batch_id,
                                                  std::vector<uint8_t>* data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!data) {
        return Status::InvalidArgument("Data buffer is null");
    }

    std::string file_path = GetBloomFilterPath(cf_id, batch_id);

    // Check if file exists
    if (!fs::exists(file_path)) {
        return Status::NotFound("Bloom filter file not found: " + file_path);
    }

    try {
        std::ifstream ifs(file_path, std::ios::binary | std::ios::ate);
        if (!ifs) {
            return Status::IOError("Failed to open bloom filter file: " + file_path);
        }

        std::streamsize size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);

        data->resize(size);
        if (!ifs.read(reinterpret_cast<char*>(data->data()), size)) {
            return Status::IOError("Failed to read bloom filter file: " + file_path);
        }

        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception reading bloom filter: ") + e.what());
    }
}

Status IndexPersistenceManager::LoadAllBloomFilters(
    uint32_t cf_id,
    std::unordered_map<uint64_t, std::vector<uint8_t>>* filters) {

    std::lock_guard<std::mutex> lock(mutex_);

    if (!filters) {
        return Status::InvalidArgument("Filters map is null");
    }

    // Check if CF exists in manifest
    auto cf_it = manifest_.column_families.find(cf_id);
    if (cf_it == manifest_.column_families.end()) {
        // No indexes for this CF
        return Status::OK();
    }

    // Load each bloom filter file
    for (const auto& [key, entry] : manifest_.batch_index_map) {
        if (entry.cf_id == cf_id && !entry.bloom_filter_path.empty()) {
            std::vector<uint8_t> data;
            // Unlock during I/O (LoadBloomFilter will reacquire)
            mutex_.unlock();
            auto status = LoadBloomFilter(cf_id, entry.batch_id, &data);
            mutex_.lock();

            if (status.ok()) {
                (*filters)[entry.batch_id] = std::move(data);
            } else if (!status.IsNotFound()) {
                // Log warning but continue loading others
                std::cerr << "Warning: Failed to load bloom filter for CF " << cf_id
                          << " batch " << entry.batch_id << ": " << status.ToString() << std::endl;
            }
        }
    }

    return Status::OK();
}

Status IndexPersistenceManager::DeleteBloomFilter(uint32_t cf_id, uint64_t batch_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string file_path = GetBloomFilterPath(cf_id, batch_id);

    // Delete file if it exists
    try {
        if (fs::exists(file_path)) {
            fs::remove(file_path);
        }

        // Update manifest
        std::string key = std::to_string(cf_id) + ":" + std::to_string(batch_id);
        manifest_.batch_index_map.erase(key);

        // Update CF metadata
        auto cf_it = manifest_.column_families.find(cf_id);
        if (cf_it != manifest_.column_families.end()) {
            std::string rel_path = "cf_" + std::to_string(cf_id) + "/bloom/batch_" +
                                   std::to_string(batch_id) + ".bloom";
            auto& files = cf_it->second.bloom_filter_files;
            files.erase(std::remove(files.begin(), files.end(), rel_path), files.end());
        }

        return SaveManifest();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception deleting bloom filter: ") + e.what());
    }
}

Status IndexPersistenceManager::SaveSkippingIndex(uint32_t cf_id, uint64_t batch_id,
                                                    const std::vector<uint8_t>& serialized_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Ensure CF directories exist
    auto status = CreateColumnFamilyDirectories(cf_id);
    if (!status.ok()) {
        return status;
    }

    // Get file path
    std::string file_path = GetSkippingIndexPath(cf_id, batch_id);

    // Write skipping index to file
    try {
        std::ofstream ofs(file_path, std::ios::binary);
        if (!ofs) {
            return Status::IOError("Failed to open skipping index file for writing: " + file_path);
        }

        ofs.write(reinterpret_cast<const char*>(serialized_data.data()), serialized_data.size());
        ofs.close();

        if (!ofs) {
            return Status::IOError("Failed to write skipping index file: " + file_path);
        }

        // Update manifest
        std::string key = std::to_string(cf_id) + ":" + std::to_string(batch_id);
        auto& entry = manifest_.batch_index_map[key];
        entry.cf_id = cf_id;
        entry.batch_id = batch_id;
        entry.skipping_index_path = "cf_" + std::to_string(cf_id) + "/skip/batch_" +
                                     std::to_string(batch_id) + ".skip";

        // Update CF metadata
        auto& cf_meta = manifest_.column_families[cf_id];
        auto it = std::find(cf_meta.skipping_index_files.begin(),
                            cf_meta.skipping_index_files.end(), entry.skipping_index_path);
        if (it == cf_meta.skipping_index_files.end()) {
            cf_meta.skipping_index_files.push_back(entry.skipping_index_path);
        }

        // Save manifest
        return SaveManifest();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception writing skipping index: ") + e.what());
    }
}

Status IndexPersistenceManager::LoadSkippingIndex(uint32_t cf_id, uint64_t batch_id,
                                                    std::vector<uint8_t>* data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!data) {
        return Status::InvalidArgument("Data buffer is null");
    }

    std::string file_path = GetSkippingIndexPath(cf_id, batch_id);

    // Check if file exists
    if (!fs::exists(file_path)) {
        return Status::NotFound("Skipping index file not found: " + file_path);
    }

    try {
        std::ifstream ifs(file_path, std::ios::binary | std::ios::ate);
        if (!ifs) {
            return Status::IOError("Failed to open skipping index file: " + file_path);
        }

        std::streamsize size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);

        data->resize(size);
        if (!ifs.read(reinterpret_cast<char*>(data->data()), size)) {
            return Status::IOError("Failed to read skipping index file: " + file_path);
        }

        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception reading skipping index: ") + e.what());
    }
}

Status IndexPersistenceManager::LoadAllSkippingIndexes(
    uint32_t cf_id,
    std::unordered_map<uint64_t, std::vector<uint8_t>>* indexes) {

    std::lock_guard<std::mutex> lock(mutex_);

    if (!indexes) {
        return Status::InvalidArgument("Indexes map is null");
    }

    // Check if CF exists in manifest
    auto cf_it = manifest_.column_families.find(cf_id);
    if (cf_it == manifest_.column_families.end()) {
        // No indexes for this CF
        return Status::OK();
    }

    // Load each skipping index file
    for (const auto& [key, entry] : manifest_.batch_index_map) {
        if (entry.cf_id == cf_id && !entry.skipping_index_path.empty()) {
            std::vector<uint8_t> data;
            // Unlock during I/O (LoadSkippingIndex will reacquire)
            mutex_.unlock();
            auto status = LoadSkippingIndex(cf_id, entry.batch_id, &data);
            mutex_.lock();

            if (status.ok()) {
                (*indexes)[entry.batch_id] = std::move(data);
            } else if (!status.IsNotFound()) {
                // Log warning but continue loading others
                std::cerr << "Warning: Failed to load skipping index for CF " << cf_id
                          << " batch " << entry.batch_id << ": " << status.ToString() << std::endl;
            }
        }
    }

    return Status::OK();
}

Status IndexPersistenceManager::DeleteSkippingIndex(uint32_t cf_id, uint64_t batch_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string file_path = GetSkippingIndexPath(cf_id, batch_id);

    // Delete file if it exists
    try {
        if (fs::exists(file_path)) {
            fs::remove(file_path);
        }

        // Update manifest
        std::string key = std::to_string(cf_id) + ":" + std::to_string(batch_id);
        auto& entry = manifest_.batch_index_map[key];
        entry.skipping_index_path.clear();

        // If entry has no indexes at all, remove it
        if (entry.bloom_filter_path.empty() && entry.skipping_index_path.empty()) {
            manifest_.batch_index_map.erase(key);
        }

        // Update CF metadata
        auto cf_it = manifest_.column_families.find(cf_id);
        if (cf_it != manifest_.column_families.end()) {
            std::string rel_path = "cf_" + std::to_string(cf_id) + "/skip/batch_" +
                                   std::to_string(batch_id) + ".skip";
            auto& files = cf_it->second.skipping_index_files;
            files.erase(std::remove(files.begin(), files.end(), rel_path), files.end());
        }

        return SaveManifest();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception deleting skipping index: ") + e.what());
    }
}

Status IndexPersistenceManager::RegisterColumnFamily(uint32_t cf_id, const std::string& cf_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if already registered
    if (manifest_.column_families.find(cf_id) != manifest_.column_families.end()) {
        return Status::OK();  // Already registered
    }

    // Add to manifest
    ColumnFamilyIndexMeta meta(cf_id, cf_name);
    manifest_.column_families[cf_id] = meta;

    // Create directories
    auto status = CreateColumnFamilyDirectories(cf_id);
    if (!status.ok()) {
        return status;
    }

    return SaveManifest();
}

Status IndexPersistenceManager::SaveManifest() {
    // Assumes mutex is already held by caller

    try {
        std::string json_str = manifest_.ToJSON();

        std::ofstream ofs(manifest_path_);
        if (!ofs) {
            return Status::IOError("Failed to open manifest file for writing: " + manifest_path_);
        }

        ofs << json_str;
        ofs.close();

        if (!ofs) {
            return Status::IOError("Failed to write manifest file: " + manifest_path_);
        }

        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception saving manifest: ") + e.what());
    }
}

Status IndexPersistenceManager::LoadManifest() {
    // Assumes mutex is already held by caller

    if (!fs::exists(manifest_path_)) {
        return Status::NotFound("Manifest file not found: " + manifest_path_);
    }

    try {
        std::ifstream ifs(manifest_path_);
        if (!ifs) {
            return Status::IOError("Failed to open manifest file: " + manifest_path_);
        }

        std::stringstream buffer;
        buffer << ifs.rdbuf();
        std::string json_str = buffer.str();

        return IndexManifest::FromJSON(json_str, &manifest_);

    } catch (const std::exception& e) {
        return Status::IOError(std::string("Exception loading manifest: ") + e.what());
    }
}

std::string IndexPersistenceManager::GetColumnFamilyIndexPath(uint32_t cf_id) const {
    return indexes_path_ + "/cf_" + std::to_string(cf_id);
}

std::string IndexPersistenceManager::GetBloomFilterPath(uint32_t cf_id, uint64_t batch_id) const {
    return GetColumnFamilyIndexPath(cf_id) + "/bloom/batch_" + std::to_string(batch_id) + ".bloom";
}

std::string IndexPersistenceManager::GetSkippingIndexPath(uint32_t cf_id, uint64_t batch_id) const {
    return GetColumnFamilyIndexPath(cf_id) + "/skip/batch_" + std::to_string(batch_id) + ".skip";
}

std::string IndexPersistenceManager::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    size_t total_bloom_filters = 0;
    size_t total_size_bytes = 0;

    for (const auto& [key, entry] : manifest_.batch_index_map) {
        if (!entry.bloom_filter_path.empty()) {
            total_bloom_filters++;
            std::string full_path = indexes_path_ + "/" + entry.bloom_filter_path;
            if (fs::exists(full_path)) {
                total_size_bytes += fs::file_size(full_path);
            }
        }
    }

    std::ostringstream ss;
    ss << "{\n";
    ss << "  \"total_column_families\": " << manifest_.column_families.size() << ",\n";
    ss << "  \"total_bloom_filters\": " << total_bloom_filters << ",\n";
    ss << "  \"total_size_bytes\": " << total_size_bytes << ",\n";
    ss << "  \"total_size_mb\": " << (static_cast<double>(total_size_bytes) / (1024 * 1024)) << "\n";
    ss << "}\n";

    return ss.str();
}

Status IndexPersistenceManager::CreateDirectory(const std::string& path) {
    try {
        if (!fs::exists(path)) {
            fs::create_directories(path);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to create directory ") + path + ": " + e.what());
    }
}

Status IndexPersistenceManager::CreateColumnFamilyDirectories(uint32_t cf_id) {
    // Assumes mutex is already held by caller

    std::string cf_path = GetColumnFamilyIndexPath(cf_id);
    std::string bloom_path = cf_path + "/bloom";
    std::string skip_path = cf_path + "/skip";

    auto status = CreateDirectory(cf_path);
    if (!status.ok()) return status;

    status = CreateDirectory(bloom_path);
    if (!status.ok()) return status;

    status = CreateDirectory(skip_path);
    if (!status.ok()) return status;

    return Status::OK();
}

}  // namespace marble
