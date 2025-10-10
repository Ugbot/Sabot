/************************************************************************
MarbleDB Column Family Implementation
**************************************************************************/

#include "marble/column_family.h"
#include "marble/lsm_tree.h"
#include "marble/memtable.h"
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

} // namespace marble

