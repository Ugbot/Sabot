/**
 * @file factory.cpp
 * @brief Factory functions for creating storage backends
 */

#include "interface.h"
#include "marbledb_backend.h"

namespace sabot {
namespace storage {

std::unique_ptr<StateBackend> CreateStateBackend(const std::string& backend_type) {
    if (backend_type == "marbledb" || backend_type.empty()) {
        return std::make_unique<MarbleDBStateBackend>();
    }
    return nullptr;
}

std::unique_ptr<StoreBackend> CreateStoreBackend(const std::string& backend_type) {
    if (backend_type == "marbledb" || backend_type.empty()) {
        return std::make_unique<MarbleDBStoreBackend>();
    }
    return nullptr;
}

} // namespace storage
} // namespace sabot

