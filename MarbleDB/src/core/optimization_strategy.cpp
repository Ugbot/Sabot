/**
 * OptimizationPipeline implementation
 */

#include "marble/optimization_strategy.h"
#include "marble/table_capabilities.h"
#include "marble/record.h"
#include <sstream>
#include <algorithm>

namespace marble {

//==============================================================================
// OptimizationPipeline
//==============================================================================

void OptimizationPipeline::AddStrategy(std::unique_ptr<OptimizationStrategy> strategy) {
    if (!strategy) {
        return;  // Ignore null strategies
    }

    std::string name = strategy->Name();

    // Check for duplicate
    if (strategy_index_.find(name) != strategy_index_.end()) {
        // Strategy with this name already exists, ignore
        return;
    }

    // Add to pipeline
    strategy_index_[name] = strategies_.size();
    strategies_.push_back(std::move(strategy));
}

void OptimizationPipeline::RemoveStrategy(const std::string& name) {
    auto it = strategy_index_.find(name);
    if (it == strategy_index_.end()) {
        return;  // Strategy not found
    }

    size_t index = it->second;

    // Remove strategy
    strategies_.erase(strategies_.begin() + index);
    strategy_index_.erase(it);

    // Rebuild index (indices after removed strategy have shifted)
    for (size_t i = index; i < strategies_.size(); ++i) {
        strategy_index_[strategies_[i]->Name()] = i;
    }
}

OptimizationStrategy* OptimizationPipeline::GetStrategy(const std::string& name) {
    auto it = strategy_index_.find(name);
    if (it == strategy_index_.end()) {
        return nullptr;
    }
    return strategies_[it->second].get();
}

void OptimizationPipeline::ClearStrategies() {
    strategies_.clear();
    strategy_index_.clear();
}

//==============================================================================
// Lifecycle hooks
//==============================================================================

Status OptimizationPipeline::OnTableCreate(const TableCapabilities& caps) {
    for (auto& strategy : strategies_) {
        Status s = strategy->OnTableCreate(caps);
        if (!s.ok()) {
            return s;  // Fail fast on initialization error
        }
    }
    return Status::OK();
}

Status OptimizationPipeline::OnRead(ReadContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("ReadContext is null");
    }

    // Call strategies in order, short-circuit if NotFound
    for (auto& strategy : strategies_) {
        Status s = strategy->OnRead(ctx);

        // Short-circuit if strategy definitively says key doesn't exist
        if (s.IsNotFound()) {
            ctx->definitely_not_found = true;
            return Status::NotFound("Optimization strategy short-circuited read");
        }

        // Also check if context flags indicate we can skip further work
        if (ctx->definitely_not_found) {
            return Status::NotFound("Context indicates key definitely not found");
        }

        if (ctx->definitely_found) {
            // Strategy says value is available (e.g., cache hit), no need to hit disk
            return Status::OK();
        }

        // Continue to next strategy
        if (!s.ok()) {
            // Strategy returned an error (not NotFound), fail
            return s;
        }
    }

    return Status::OK();
}

void OptimizationPipeline::OnReadComplete(const Key& key, const Record& record) {
    // Call all strategies (no short-circuiting)
    for (auto& strategy : strategies_) {
        strategy->OnReadComplete(key, record);
    }
}

Status OptimizationPipeline::OnWrite(WriteContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("WriteContext is null");
    }

    // Call all strategies (no short-circuiting)
    for (auto& strategy : strategies_) {
        Status s = strategy->OnWrite(ctx);
        if (!s.ok()) {
            return s;  // Fail on write error
        }
    }

    return Status::OK();
}

Status OptimizationPipeline::OnCompaction(CompactionContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("CompactionContext is null");
    }

    // Call all strategies (no short-circuiting)
    for (auto& strategy : strategies_) {
        Status s = strategy->OnCompaction(ctx);
        if (!s.ok()) {
            // Log error but continue (compaction should not fail due to optimization)
            // In production, we'd use a proper logger here
            continue;
        }
    }

    return Status::OK();
}

Status OptimizationPipeline::OnFlush(FlushContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("FlushContext is null");
    }

    // Call all strategies (no short-circuiting)
    for (auto& strategy : strategies_) {
        Status s = strategy->OnFlush(ctx);
        if (!s.ok()) {
            // Log error but continue (flush should not fail due to optimization)
            continue;
        }
    }

    return Status::OK();
}

//==============================================================================
// Management
//==============================================================================

size_t OptimizationPipeline::MemoryUsage() const {
    size_t total = 0;
    for (const auto& strategy : strategies_) {
        total += strategy->MemoryUsage();
    }
    return total;
}

void OptimizationPipeline::Clear() {
    for (auto& strategy : strategies_) {
        strategy->Clear();
    }
}

std::vector<uint8_t> OptimizationPipeline::Serialize() const {
    // Serialization format:
    // [num_strategies: 4 bytes]
    // For each strategy:
    //   [name_length: 4 bytes][name: variable][data_length: 4 bytes][data: variable]

    std::vector<uint8_t> result;

    // Write number of strategies
    uint32_t num_strategies = static_cast<uint32_t>(strategies_.size());
    result.insert(result.end(),
                  reinterpret_cast<const uint8_t*>(&num_strategies),
                  reinterpret_cast<const uint8_t*>(&num_strategies) + sizeof(num_strategies));

    // Write each strategy
    for (const auto& strategy : strategies_) {
        std::string name = strategy->Name();
        std::vector<uint8_t> data = strategy->Serialize();

        // Write name length
        uint32_t name_length = static_cast<uint32_t>(name.size());
        result.insert(result.end(),
                      reinterpret_cast<const uint8_t*>(&name_length),
                      reinterpret_cast<const uint8_t*>(&name_length) + sizeof(name_length));

        // Write name
        result.insert(result.end(), name.begin(), name.end());

        // Write data length
        uint32_t data_length = static_cast<uint32_t>(data.size());
        result.insert(result.end(),
                      reinterpret_cast<const uint8_t*>(&data_length),
                      reinterpret_cast<const uint8_t*>(&data_length) + sizeof(data_length));

        // Write data
        result.insert(result.end(), data.begin(), data.end());
    }

    return result;
}

Status OptimizationPipeline::Deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(uint32_t)) {
        return Status::InvalidArgument("Serialized data too short");
    }

    size_t offset = 0;

    // Read number of strategies
    uint32_t num_strategies;
    std::memcpy(&num_strategies, data.data() + offset, sizeof(num_strategies));
    offset += sizeof(num_strategies);

    // Read each strategy
    for (uint32_t i = 0; i < num_strategies; ++i) {
        // Read name length
        if (offset + sizeof(uint32_t) > data.size()) {
            return Status::InvalidArgument("Corrupted serialized data: name length");
        }
        uint32_t name_length;
        std::memcpy(&name_length, data.data() + offset, sizeof(name_length));
        offset += sizeof(name_length);

        // Read name
        if (offset + name_length > data.size()) {
            return Status::InvalidArgument("Corrupted serialized data: name");
        }
        std::string name(reinterpret_cast<const char*>(data.data() + offset), name_length);
        offset += name_length;

        // Read data length
        if (offset + sizeof(uint32_t) > data.size()) {
            return Status::InvalidArgument("Corrupted serialized data: data length");
        }
        uint32_t data_length;
        std::memcpy(&data_length, data.data() + offset, sizeof(data_length));
        offset += sizeof(data_length);

        // Read data
        if (offset + data_length > data.size()) {
            return Status::InvalidArgument("Corrupted serialized data: strategy data");
        }
        std::vector<uint8_t> strategy_data(data.begin() + offset, data.begin() + offset + data_length);
        offset += data_length;

        // Find strategy and deserialize
        OptimizationStrategy* strategy = GetStrategy(name);
        if (strategy) {
            Status s = strategy->Deserialize(strategy_data);
            if (!s.ok()) {
                return s;
            }
        }
        // If strategy not found, skip (may be removed in newer version)
    }

    return Status::OK();
}

std::string OptimizationPipeline::GetStats() const {
    if (strategies_.empty()) {
        return "{}";
    }

    std::ostringstream ss;
    ss << "{\n";

    for (size_t i = 0; i < strategies_.size(); ++i) {
        const auto& strategy = strategies_[i];
        ss << "  \"" << strategy->Name() << "\": ";

        std::string stats = strategy->GetStats();
        if (stats.empty()) {
            ss << "{}";
        } else {
            ss << stats;
        }

        if (i + 1 < strategies_.size()) {
            ss << ",";
        }
        ss << "\n";
    }

    ss << "}";
    return ss.str();
}

}  // namespace marble
