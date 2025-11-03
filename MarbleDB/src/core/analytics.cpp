/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "marble/analytics.h"
#include <arrow/compute/api.h>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace cp = arrow::compute;

namespace marble {

// ZoneMap implementation

bool ZoneMap::CanPrune(const std::string& predicate_op,
                      const std::shared_ptr<arrow::Scalar>& predicate_value) const {
    // For MVP, no pruning logic implemented yet
    // TODO: Implement proper zone map pruning
    return false;
}

// BloomFilter implementation

class BloomFilter::Impl {
public:
    Impl(size_t expected_items, double false_positive_rate)
        : expected_items_(expected_items), false_positive_rate_(false_positive_rate),
          capacity_(expected_items) {
        // Calculate optimal size and hash functions
        // m = optimal number of bits = -n * ln(p) / (ln(2))^2
        double m_real = -static_cast<double>(expected_items) * std::log(false_positive_rate) /
                        std::pow(std::log(2.0), 2.0);
        size_t m = static_cast<size_t>(std::ceil(m_real));

        // k = optimal number of hash functions = (m/n) * ln(2)
        double k_real = (static_cast<double>(m) / static_cast<double>(expected_items)) * std::log(2.0);
        size_t k = static_cast<size_t>(std::round(k_real));

        // Ensure at least 1 hash function and at least 64 bits
        if (k < 1) k = 1;
        if (m < 64) m = 64;

        num_hashes_ = k;
        size_t num_words = (m + 63) / 64;  // Round up to 64-bit words
        bits_.resize(num_words, 0);

        // For large filters (>100K items), disable key tracking to prevent memory exhaustion
        // This keeps the filter fixed-size but avoids the 47+ MB key buffer overhead
        if (expected_items > 100000) {
            tracking_keys_ = false;  // No growth, fixed-size filter
            key_buffer_.reset();      // Don't allocate key buffer
            max_tracked_keys_ = 0;
            std::cerr << "BloomFilter: Large filter (" << expected_items
                      << " items), using fixed-size mode (no growth)\n";
        } else {
            // For small filters, enable growth with memory limit
            max_tracked_keys_ = 1000000;
            tracking_keys_ = true;
            key_buffer_ = std::make_unique<std::vector<std::string>>();
            key_buffer_->reserve(std::min(expected_items, max_tracked_keys_));
        }
    }

    void Add(const std::string& item) {
        // Save key for potential rehash, but stop tracking at memory limit
        if (tracking_keys_ && key_buffer_->size() < max_tracked_keys_) {
            key_buffer_->push_back(item);
        } else if (tracking_keys_ && key_buffer_->size() >= max_tracked_keys_) {
            // Hit memory limit - stop tracking and disable future growth
            std::cerr << "BloomFilter: Memory limit reached (" << max_tracked_keys_
                      << " keys, ~" << (max_tracked_keys_ * 50 / 1024 / 1024) << " MB). "
                      << "Disabling growth, accepting higher FPR.\n";
            key_buffer_->clear();
            key_buffer_.reset();
            tracking_keys_ = false;
        }

        // Check if we need to grow (at 85% capacity) - only if still tracking
        if (tracking_keys_ && LoadFactor() > 0.85) {
            Grow();
        }

        // Add to bit array
        auto bit_indices = HashItem(item);
        for (size_t bit_index : bit_indices) {
            size_t word_index = bit_index / 64;
            size_t bit_offset = bit_index % 64;
            bits_[word_index] |= (1ULL << bit_offset);
        }

        items_added_++;
    }

    bool MightContain(const std::string& item) const {
        auto bit_indices = HashItem(item);  // Now returns bit indices directly
        for (size_t bit_index : bit_indices) {
            size_t word_index = bit_index / 64;
            size_t bit_offset = bit_index % 64;
            if ((bits_[word_index] & (1ULL << bit_offset)) == 0) {
                return false;
            }
        }
        return true;
    }

    size_t SizeBytes() const {
        return bits_.size() * sizeof(uint64_t) + sizeof(*this);
    }

    std::vector<uint8_t> Serialize() const {
        // Simple serialization - in production, use proper format
        std::vector<uint8_t> data;
        size_t num_words = bits_.size();
        data.insert(data.end(), reinterpret_cast<const uint8_t*>(&num_words),
                   reinterpret_cast<const uint8_t*>(&num_words) + sizeof(size_t));
        data.insert(data.end(), reinterpret_cast<const uint8_t*>(&num_hashes_),
                   reinterpret_cast<const uint8_t*>(&num_hashes_) + sizeof(size_t));
        data.insert(data.end(), reinterpret_cast<const uint8_t*>(bits_.data()),
                   reinterpret_cast<const uint8_t*>(bits_.data()) + bits_.size() * sizeof(uint64_t));
        return data;
    }

    static std::unique_ptr<Impl> Deserialize(const std::vector<uint8_t>& data) {
        if (data.size() < 2 * sizeof(size_t)) {
            return nullptr;
        }

        size_t offset = 0;
        size_t num_words, num_hashes;

        memcpy(&num_words, data.data() + offset, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(&num_hashes, data.data() + offset, sizeof(size_t));
        offset += sizeof(size_t);

        if (data.size() < offset + num_words * sizeof(uint64_t)) {
            return nullptr;
        }

        auto impl = std::make_unique<Impl>(0, 0.0);  // Dummy values
        impl->bits_.resize(num_words);
        impl->num_hashes_ = num_hashes;
        memcpy(impl->bits_.data(), data.data() + offset, num_words * sizeof(uint64_t));

        return impl;
    }

    // Dynamic resizing support methods
    size_t ItemsAdded() const { return items_added_; }
    size_t Capacity() const { return capacity_; }
    double LoadFactor() const {
        if (capacity_ == 0) return 0.0;
        return static_cast<double>(items_added_) / static_cast<double>(capacity_);
    }

    void StartKeyTracking() {
        if (!tracking_keys_) {
            tracking_keys_ = true;
            key_buffer_ = std::make_unique<std::vector<std::string>>();
            key_buffer_->reserve(static_cast<size_t>(capacity_ * 0.3));  // Reserve for 30% more
            std::cerr << "BloomFilter: Started key tracking at load factor " << LoadFactor() << "\n";
        }
    }

    void Grow() {
        if (!key_buffer_ || key_buffer_->empty()) {
            std::cerr << "BloomFilter: Cannot grow - no keys tracked\n";
            return;
        }

        size_t old_capacity = capacity_;
        size_t new_capacity = capacity_ * 2;

        std::cerr << "BloomFilter: Growing from " << old_capacity << " to " << new_capacity
                  << " capacity (load factor: " << LoadFactor() << ", " << items_added_ << " items)\n";

        // Create new filter with 2x capacity
        auto new_impl = std::make_unique<Impl>(new_capacity, false_positive_rate_);

        // Rehash all tracked keys
        for (const auto& key : *key_buffer_) {
            new_impl->AddInternal(key);
        }

        // Swap internal state
        bits_ = std::move(new_impl->bits_);
        capacity_ = new_capacity;
        expected_items_ = new_capacity;
        num_hashes_ = new_impl->num_hashes_;
        items_added_ = key_buffer_->size();

        // DON'T clear key buffer - we need ALL keys for future growths
        // This is the memory cost of the aggressive rehashing strategy
        size_t buffer_mb = key_buffer_->size() * 50 / 1024 / 1024;
        std::cerr << "BloomFilter: Growth complete, new load factor: " << LoadFactor()
                  << ", buffer: " << key_buffer_->size() << " keys (~" << buffer_mb << " MB)\n";
    }

    // Internal add without growth checks (used during Grow())
    void AddInternal(const std::string& item) {
        auto bit_indices = HashItem(item);
        for (size_t bit_index : bit_indices) {
            size_t word_index = bit_index / 64;
            size_t bit_offset = bit_index % 64;
            bits_[word_index] |= (1ULL << bit_offset);
        }
        items_added_++;
    }

private:
    std::vector<size_t> HashItem(const std::string& item) const {
        // Use double hashing approach inspired by RocksDB
        // Generate k hash values from two base hashes: h1 and h2
        // hash_i = h1 + i * h2 (mod m) - Kirsch-Mitzenmacher optimization
        std::vector<size_t> hashes;

        // Generate two independent hash values
        std::hash<std::string> hasher;
        size_t h1 = hasher(item);

        // For h2, use golden ratio multiplier (like RocksDB) to generate independent hash
        // 0x9e3779b9 is the 32-bit golden ratio: 2^32 / φ where φ = (1+√5)/2
        size_t h2 = h1 * 0x9e3779b9;

        size_t num_bits = bits_.size() * 64;
        for (size_t i = 0; i < num_hashes_; ++i) {
            hashes.push_back((h1 + i * h2) % num_bits);
        }

        return hashes;
    }

    size_t expected_items_;
    double false_positive_rate_;
    std::vector<uint64_t> bits_;
    size_t num_hashes_;

    // Dynamic resizing support
    size_t items_added_ = 0;
    size_t capacity_;  // Same as expected_items_, stored for quick access
    size_t max_tracked_keys_;  // Memory limit: stop tracking after this many keys
    std::unique_ptr<std::vector<std::string>> key_buffer_;  // Only allocated when needed
    bool tracking_keys_ = false;
};

BloomFilter::BloomFilter(size_t expected_items, double false_positive_rate)
    : impl_(std::make_unique<Impl>(expected_items, false_positive_rate)) {}

BloomFilter::~BloomFilter() = default;

void BloomFilter::Add(const std::string& item) {
    impl_->Add(item);
}

bool BloomFilter::MightContain(const std::string& item) const {
    return impl_->MightContain(item);
}

size_t BloomFilter::SizeBytes() const {
    return impl_->SizeBytes();
}

std::vector<uint8_t> BloomFilter::Serialize() const {
    return impl_->Serialize();
}

std::unique_ptr<BloomFilter> BloomFilter::Deserialize(const std::vector<uint8_t>& data) {
    auto impl = Impl::Deserialize(data);
    if (!impl) {
        return nullptr;
    }

    auto filter = std::make_unique<BloomFilter>(0, 0.0);  // Dummy values
    filter->impl_ = std::move(impl);
    return filter;
}

size_t BloomFilter::ItemsAdded() const {
    return impl_->ItemsAdded();
}

size_t BloomFilter::Capacity() const {
    return impl_->Capacity();
}

double BloomFilter::LoadFactor() const {
    return impl_->LoadFactor();
}

// ColumnIndex implementation

Status ColumnIndex::BuildFromArray(const arrow::Array& array) {
    // Build zone map
    zone_map.column_name = column_name;
    zone_map.column_type = array.type();
    zone_map.row_count = array.length();
    zone_map.null_count = array.null_count();

    // Calculate min/max using Arrow compute
    // For MVP, we'll skip complex min/max calculation
    // TODO: Implement proper min/max extraction from Arrow compute results
    zone_map.min_value = nullptr;
    zone_map.max_value = nullptr;

    // Build bloom filter for string/categorical columns
    if (array.type()->id() == arrow::Type::STRING ||
        array.type()->id() == arrow::Type::LARGE_STRING) {
        bloom_filter = std::make_shared<BloomFilter>(array.length(), 0.01);

        // Add all non-null values to bloom filter
        for (int64_t i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                auto scalar = array.GetScalar(i).ValueUnsafe();
                if (scalar->is_valid) {
                    bloom_filter->Add(scalar->ToString());
                }
            }
        }
    }

    return Status::OK();
}

// PartitionMetadata implementation

bool PartitionMetadata::CanPrune(const ScanSpec& spec) const {
    // Check time range pruning first
    if (!spec.time_filter.empty()) {
        // TODO: Parse time filter and check against partition time range
        // For now, assume we can't prune based on time
    }

    // Check column filters against zone maps
    for (const auto& [column, filter_expr] : spec.filters) {
        auto it = column_indexes.find(column);
        if (it != column_indexes.end()) {
            const auto& index = it->second;
            // TODO: Parse filter expression and check against zone map
            // For now, assume we can prune if zone map exists
            if (index.zone_map.min_value && index.zone_map.max_value) {
                return true;  // Can potentially prune
            }
        }
    }

    return false;  // Cannot prune
}

std::string PartitionMetadata::ToJson() const {
    json j = {
        {"partition_id", partition_id},
        {"row_count", row_count},
        {"size_bytes", size_bytes},
        {"min_timestamp", min_timestamp},
        {"max_timestamp", max_timestamp},
        {"data_files", data_files}
    };

    // Add column indexes
    json indexes = json::object();
    for (const auto& [name, index] : column_indexes) {
        json index_json = {
            {"column_name", index.column_name}
        };

        // Zone map info
        json zone_map_json = {
            {"null_count", index.zone_map.null_count},
            {"row_count", index.zone_map.row_count}
        };
        index_json["zone_map"] = zone_map_json;

        indexes[name] = index_json;
    }
    j["column_indexes"] = indexes;

    return j.dump();
}

std::unique_ptr<PartitionMetadata> PartitionMetadata::FromJson(const std::string& json_str) {
    try {
        auto j = json::parse(json_str);
        auto metadata = std::make_unique<PartitionMetadata>(j["partition_id"]);

        metadata->row_count = j.value("row_count", 0LL);
        metadata->size_bytes = j.value("size_bytes", 0LL);
        metadata->min_timestamp = j.value("min_timestamp", INT64_MAX);
        metadata->max_timestamp = j.value("max_timestamp", INT64_MIN);

        if (j.contains("data_files")) {
            for (const auto& file : j["data_files"]) {
                metadata->data_files.push_back(file);
            }
        }

        // Load column indexes (simplified)
        if (j.contains("column_indexes")) {
            for (const auto& [name, index_json] : j["column_indexes"].items()) {
                ColumnIndex index(name);
                metadata->column_indexes[name] = index;
            }
        }

        return metadata;
    } catch (const std::exception&) {
        return nullptr;
    }
}

// QueryOptimizer implementation

Status QueryOptimizer::OptimizeScan(const ScanSpec& original_spec,
                                   const std::vector<PartitionMetadata>& partitions,
                                   ScanSpec* optimized_spec,
                                   std::vector<std::string>* selected_partitions) const {
    *optimized_spec = original_spec;
    selected_partitions->clear();

    // Select partitions that cannot be pruned
    for (const auto& partition : partitions) {
        if (!partition.CanPrune(original_spec)) {
            selected_partitions->push_back(partition.partition_id);
        }
    }

    // If no partitions selected, select all (fallback)
    if (selected_partitions->empty()) {
        for (const auto& partition : partitions) {
            selected_partitions->push_back(partition.partition_id);
        }
    }

    return Status::OK();
}

Status QueryOptimizer::GetCandidateBlocks(const PartitionMetadata& partition,
                                         const std::string& column_name,
                                         const std::string& op,
                                         const std::shared_ptr<arrow::Scalar>& value,
                                         std::vector<int64_t>* candidate_blocks) const {
    candidate_blocks->clear();

    // Use skipping index if available
    if (partition.skipping_index) {
        return partition.skipping_index->GetCandidateBlocks(column_name, op, value, candidate_blocks);
    }

    // Fallback: return all blocks if no skipping index
    // In practice, this would need to know the total number of blocks
    // For now, return empty vector to indicate no optimization possible
    return Status::OK();
}

double QueryOptimizer::EstimateSelectivity(const ScanSpec& spec,
                                          const PartitionMetadata& partition) const {
    // Simple selectivity estimation
    // TODO: Implement proper selectivity estimation based on histograms/zone maps

    if (spec.filters.empty()) {
        return 1.0;  // No filters = 100% selectivity
    }

    // Assume 10% selectivity per filter (very rough estimate)
    double selectivity = 1.0;
    for (const auto& filter : spec.filters) {
        selectivity *= 0.1;  // 10% per filter
    }

    return std::max(selectivity, 0.001);  // Minimum 0.1% selectivity
}

Status QueryOptimizer::ParseFilter(const std::string& filter_expr,
                                  std::string* column,
                                  std::string* op,
                                  std::string* value) const {
    // Simple filter parsing: "column = value"
    // TODO: Implement proper expression parsing

    size_t eq_pos = filter_expr.find('=');
    if (eq_pos == std::string::npos) {
        return Status::InvalidArgument("Unsupported filter format: " + filter_expr);
    }

    *column = filter_expr.substr(0, eq_pos);
    *op = "=";
    *value = filter_expr.substr(eq_pos + 1);

    // Trim whitespace
    *column = column->substr(column->find_first_not_of(" \t"));
    *column = column->substr(0, column->find_last_not_of(" \t") + 1);
    *value = value->substr(value->find_first_not_of(" \t"));
    *value = value->substr(0, value->find_last_not_of(" \t") + 1);

    return Status::OK();
}

// AggregationEngine implementation

Status AggregationEngine::Execute(const std::vector<AggSpec>& specs,
                                 const std::shared_ptr<arrow::Table>& input_table,
                                 std::shared_ptr<arrow::Table>* output_table) const {
    if (specs.empty()) {
        *output_table = input_table;
        return Status::OK();
    }

    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    std::vector<std::shared_ptr<arrow::Field>> result_fields;

    for (const auto& spec : specs) {
        std::shared_ptr<arrow::Array> result_array;

        switch (spec.function) {
            case AggFunction::kCount:
                if (ExecuteCount(spec, input_table, &result_array).ok()) {
                    result_arrays.push_back(result_array);
                    result_fields.push_back(arrow::field(spec.output_column, arrow::int64()));
                }
                break;

            case AggFunction::kSum:
                if (ExecuteSum(spec, input_table, &result_array).ok()) {
                    result_arrays.push_back(result_array);
                    result_fields.push_back(arrow::field(spec.output_column, result_array->type()));
                }
                break;

            case AggFunction::kAvg:
                if (ExecuteAvg(spec, input_table, &result_array).ok()) {
                    result_arrays.push_back(result_array);
                    result_fields.push_back(arrow::field(spec.output_column, arrow::float64()));
                }
                break;

            default:
                return Status::NotImplemented("Aggregation function not implemented");
        }
    }

    if (result_arrays.empty()) {
        return Status::InvalidArgument("No valid aggregations specified");
    }

    // Create result table
    auto result_schema = arrow::schema(result_fields);
    // FIXME: Simplified for MVP - proper error handling needed
    *output_table = arrow::Table::Make(result_schema, result_arrays);

    return Status::OK();
}

Status AggregationEngine::ExecuteCount(const AggSpec& spec,
                                      const std::shared_ptr<arrow::Table>& table,
                                      std::shared_ptr<arrow::Array>* result) const {
    auto column = table->GetColumnByName(spec.input_column);
    if (!column) {
        return Status::InvalidArgument("Column not found: " + spec.input_column);
    }

    int64_t count = 0;
    for (int i = 0; i < table->num_columns(); ++i) {
        if (table->schema()->field(i)->name() == spec.input_column) {
            count = table->num_rows();  // Simple count - doesn't handle nulls properly
            break;
        }
    }

    // Create scalar array with single value
    arrow::Int64Builder builder;
    if (builder.Append(count).ok()) {
        auto status = builder.Finish(result);
        return status.ok() ? Status::OK() : Status::FromArrowStatus(status);
    }

    return Status::InternalError("Failed to build count result");
}

Status AggregationEngine::ExecuteSum(const AggSpec& spec,
                                    const std::shared_ptr<arrow::Table>& table,
                                    std::shared_ptr<arrow::Array>* result) const {
    auto column = table->GetColumnByName(spec.input_column);
    if (!column) {
        return Status::InvalidArgument("Column not found: " + spec.input_column);
    }

    // Use Arrow compute for sum
    auto sum_result = cp::Sum(column);
    if (!sum_result.ok()) {
        return Status::FromArrowStatus(sum_result.status());
    }

    auto sum_datum = sum_result.ValueUnsafe();
    if (!sum_datum.is_scalar()) {
        return Status::InternalError("Expected scalar result from Sum");
    }

    auto sum_scalar = sum_datum.scalar();

    // Convert scalar to array
    auto scalar_to_array_result = arrow::MakeArrayFromScalar(*sum_scalar, 1);
    if (!scalar_to_array_result.ok()) {
        return Status::FromArrowStatus(scalar_to_array_result.status());
    }

    *result = scalar_to_array_result.ValueUnsafe();
    return Status::OK();
}

Status AggregationEngine::ExecuteAvg(const AggSpec& spec,
                                    const std::shared_ptr<arrow::Table>& table,
                                    std::shared_ptr<arrow::Array>* result) const {
    // For MVP, AVG is not implemented yet
    // TODO: Implement proper AVG aggregation
    return Status::NotImplemented("AVG aggregation not implemented yet");
}

// Factory functions

std::unique_ptr<BloomFilter> CreateBloomFilter(size_t expected_items,
                                              double false_positive_rate) {
    return std::make_unique<BloomFilter>(expected_items, false_positive_rate);
}

} // namespace marble
