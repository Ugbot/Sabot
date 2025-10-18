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

#include "marble/temporal_reconstruction.h"
#include <algorithm>
#include <arrow/compute/api.h>
#include <arrow/table.h>

namespace marble {

// TemporalReconstructor implementation

// Implement proper AS OF temporal reconstruction
Status TemporalReconstructor::ReconstructAsOf(
    const SnapshotId& snapshot,
    const std::vector<arrow::RecordBatch>& version_batches,
    const std::vector<TemporalMetadata>& metadata_list,
    std::shared_ptr<arrow::RecordBatch>* result) {

    if (version_batches.empty() || metadata_list.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    // Build version chains first
    std::unordered_map<std::string, VersionChain> version_chains;
    auto status = BuildVersionChains(version_batches, metadata_list, &version_chains);
    if (!status.ok()) return status;

    // For each chain, find the active version at the snapshot time
    std::vector<std::shared_ptr<arrow::RecordBatch>> active_versions;

    for (const auto& [key, chain] : version_chains) {
        // Find the version that was active at the snapshot time
        std::shared_ptr<arrow::RecordBatch> active_version;
        status = FindActiveVersion(chain, snapshot, version_batches, &active_version);
        if (!status.ok()) continue;

        if (active_version && !chain.is_deleted) {
            active_versions.push_back(active_version);
        }
    }

    // Merge all active versions into final result
    if (active_versions.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    return MergeRecordBatches(active_versions, result);
}

// TODO: Implement proper valid time temporal reconstruction
// Currently simplified, real implementation needs temporal algebra
Status TemporalReconstructor::ReconstructValidTime(
    uint64_t valid_start,
    uint64_t valid_end,
    const std::vector<arrow::RecordBatch>& version_batches,
    const std::vector<TemporalMetadata>& metadata_list,
    std::shared_ptr<arrow::RecordBatch>* result) {

    // Implement proper valid time temporal reconstruction
    if (version_batches.empty() || metadata_list.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    // Collect all versions that were valid during the specified time range
    std::vector<std::shared_ptr<arrow::RecordBatch>> valid_versions;

    for (size_t i = 0; i < version_batches.size(); ++i) {
        const auto& metadata = metadata_list[i];

        // Check if this version's valid time range overlaps with the query range
        bool overlaps = (metadata.valid_from <= valid_end) &&
                       (metadata.valid_to >= valid_start);

        if (overlaps) {
            // Create a copy of this version batch
            auto version_copy = arrow::RecordBatch::Make(
                version_batches[i].schema(),
                version_batches[i].num_rows(),
                version_batches[i].columns());
            valid_versions.push_back(version_copy);
        }
    }

    // Merge all valid versions
    if (valid_versions.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    return MergeRecordBatches(valid_versions, result);
}

// TODO: Implement proper bitemporal reconstruction
// ArcticDB-style full bitemporal queries are complex and need careful implementation
Status TemporalReconstructor::ReconstructBitemporal(
    const TemporalQuerySpec& spec,
    const std::vector<arrow::RecordBatch>& version_batches,
    const std::vector<TemporalMetadata>& metadata_list,
    std::shared_ptr<arrow::RecordBatch>* result) {

    // Implement proper bitemporal reconstruction
    if (version_batches.empty() || metadata_list.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    // First, apply AS OF filtering (system time)
    std::shared_ptr<arrow::RecordBatch> as_of_result;
    auto status = ReconstructAsOf(spec.as_of_snapshot, version_batches, metadata_list, &as_of_result);
    if (!status.ok()) return status;

    if (!as_of_result) {
        *result = nullptr;
        return Status::OK();
    }

    // Then, apply valid time filtering if specified
    if (spec.valid_time_start != 0 || spec.valid_time_end != UINT64_MAX) {
        // Create filtered versions based on the AS OF result
        std::vector<arrow::RecordBatch> filtered_versions;
        std::vector<TemporalMetadata> filtered_metadata;

        // This is simplified - in practice, we'd need to filter the AS OF results
        // For now, just apply valid time filtering to all original batches
        status = ReconstructValidTime(spec.valid_time_start, spec.valid_time_end,
                                    version_batches, metadata_list, result);
    } else {
        *result = as_of_result;
    }

    return Status::OK();
}

Status TemporalReconstructor::ReconstructHistory(
    const std::string& primary_key,
    const std::vector<arrow::RecordBatch>& version_batches,
    const std::vector<TemporalMetadata>& metadata_list,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* history) {

    history->clear();

    if (version_batches.size() != metadata_list.size()) {
        return Status::InvalidArgument("Version batches and metadata must have same size");
    }

    if (version_batches.empty()) {
        return Status::OK();
    }

    // Build version chains to group versions by primary key
    std::unordered_map<std::string, VersionChain> chains;
    auto status = BuildVersionChains(version_batches, metadata_list, &chains);
    if (!status.ok()) {
        return status;
    }

    // Find the chain for the requested primary key
    auto chain_it = chains.find(primary_key);
    if (chain_it == chains.end()) {
        // No versions found for this primary key
        return Status::OK();
    }

    const auto& chain = chain_it->second;

    // Reconstruct history from the version chain
    // Versions are already sorted by system time (newest first) in BuildVersionChains
    for (size_t i = 0; i < chain.version_indices.size(); ++i) {
        size_t version_index = chain.version_indices[i];
        if (version_index >= version_batches.size()) {
            continue;
        }

        // Create a copy of this version
        auto version_copy = arrow::RecordBatch::Make(
            version_batches[version_index].schema(),
            version_batches[version_index].num_rows(),
            version_batches[version_index].columns());

        history->push_back(version_copy);
    }

    return Status::OK();
}

// FIXME: BuildVersionChains is a complete placeholder
// Real implementation needs proper version lineage tracking and conflict detection
Status TemporalReconstructor::BuildVersionChains(
    const std::vector<arrow::RecordBatch>& version_batches,
    const std::vector<TemporalMetadata>& metadata_list,
    std::unordered_map<std::string, VersionChain>* chains) {

    // Implement proper version chain building
    chains->clear();

    if (version_batches.size() != metadata_list.size()) {
        return Status::InvalidArgument("Version batches and metadata lists must have same size");
    }

    // Group versions by primary key
    std::unordered_map<std::string, std::vector<std::pair<size_t, TemporalMetadata>>> grouped_versions;

    for (size_t i = 0; i < version_batches.size(); ++i) {
        // Extract primary key from the batch (assume first column is primary key)
        std::string primary_key;
        auto status = ExtractPrimaryKey(version_batches[i], 0, "", &primary_key);
        if (!status.ok()) continue;

        grouped_versions[primary_key].emplace_back(i, metadata_list[i]);
    }

    // Build version chains
    for (const auto& group : grouped_versions) {
        VersionChain chain;
        chain.primary_key = group.first;

        // Sort versions by system time (most recent first)
        auto versions = group.second;  // Make a mutable copy
        std::sort(versions.begin(), versions.end(),
                 [](const std::pair<size_t, TemporalMetadata>& a, 
                    const std::pair<size_t, TemporalMetadata>& b) {
                     return a.second.system_time > b.second.system_time; // Descending order
                 });

        // Build the chain
        for (const auto& version : versions) {
            chain.version_indices.push_back(version.first);
            chain.metadata.push_back(version.second);
        }

        // Determine current state
        if (!chain.metadata.empty()) {
            const auto& latest_metadata = chain.metadata.front();
            chain.is_deleted = latest_metadata.is_deleted;
            chain.valid_from = latest_metadata.valid_from;
            chain.valid_to = latest_metadata.valid_to;
        }

        (*chains)[chain.primary_key] = chain;
    }

    return Status::OK();
}

// Resolve temporal version conflicts using temporal algebra
// Conflicts occur when multiple versions have overlapping valid time ranges
Status TemporalReconstructor::ResolveVersionConflicts(VersionChain* chain) {
    if (!chain || chain->version_indices.size() <= 1) {
        // No conflicts possible with 0 or 1 versions
        return Status::OK();
    }

    // Detect conflicts: versions with overlapping valid time ranges
    std::vector<std::pair<size_t, size_t>> conflicts;

    for (size_t i = 0; i < chain->metadata.size(); ++i) {
        for (size_t j = i + 1; j < chain->metadata.size(); ++j) {
            const auto& meta_i = chain->metadata[i];
            const auto& meta_j = chain->metadata[j];

            // Check for overlapping valid time ranges
            bool overlaps = (meta_i.valid_from < meta_j.valid_to) &&
                           (meta_j.valid_from < meta_i.valid_to);

            if (overlaps) {
                conflicts.push_back({i, j});
            }
        }
    }

    if (conflicts.empty()) {
        // No conflicts to resolve
        return Status::OK();
    }

    // Resolve conflicts using "last writer wins" based on system time
    // Keep only the version with the most recent system time for overlapping ranges
    std::set<size_t> versions_to_remove;

    for (const auto& conflict : conflicts) {
        size_t idx1 = conflict.first;
        size_t idx2 = conflict.second;

        const auto& meta1 = chain->metadata[idx1];
        const auto& meta2 = chain->metadata[idx2];

        // Compare system times - keep the more recent one
        if (meta1.system_time < meta2.system_time) {
            // Version 2 is more recent, remove version 1
            versions_to_remove.insert(idx1);
        } else {
            // Version 1 is more recent (or equal), remove version 2
            versions_to_remove.insert(idx2);
        }
    }

    // Remove conflicting versions (in reverse order to maintain indices)
    std::vector<size_t> sorted_removals(versions_to_remove.begin(), versions_to_remove.end());
    std::sort(sorted_removals.rbegin(), sorted_removals.rend());  // Reverse sort

    for (size_t idx : sorted_removals) {
        if (idx < chain->version_indices.size()) {
            chain->version_indices.erase(chain->version_indices.begin() + idx);
        }
        if (idx < chain->metadata.size()) {
            chain->metadata.erase(chain->metadata.begin() + idx);
        }
    }

    return Status::OK();
}

// Old FindActiveVersion removed - using the 4-parameter version below

Status TemporalReconstructor::ApplyValidTimeFilter(
    const VersionChain& chain,
    uint64_t valid_start,
    uint64_t valid_end,
    std::vector<size_t>* matching_versions) {

    matching_versions->clear();

    // Filter versions by valid time range
    // A version matches if its valid time range overlaps with the query range
    for (size_t i = 0; i < chain.version_indices.size(); ++i) {
        if (i >= chain.metadata.size()) {
            continue;
        }

        const auto& metadata = chain.metadata[i];

        // Check if this version's valid time range overlaps with the query range
        // Overlap condition: (valid_from <= query_end) AND (valid_to >= query_start)
        bool overlaps = (metadata.valid_from <= valid_end) &&
                       (metadata.valid_to >= valid_start);

        if (overlaps) {
            matching_versions->push_back(i);
        }
    }

    return Status::OK();
}

// FIXME: ReconstructFromChains is a complete placeholder
// Real implementation needs proper temporal reconstruction algorithms
Status TemporalReconstructor::ReconstructFromChains(
    const std::unordered_map<std::string, VersionChain>& chains,
    const std::vector<arrow::RecordBatch>& version_batches,
    const TemporalQuerySpec& spec,
    std::shared_ptr<arrow::RecordBatch>* result) {

    // FIXME: This is a placeholder - just return the first batch
    if (version_batches.empty()) {
        return Status::InvalidArgument("No version batches provided");
    }

    // FIXME: Simplified for MVP - proper error handling needed
    *result = arrow::RecordBatch::Make(
        version_batches[0].schema(), version_batches[0].num_rows(), version_batches[0].columns());
    return Status::OK();
}

Status TemporalReconstructor::ExtractPrimaryKey(
    const arrow::RecordBatch& batch,
    size_t row_index,
    const std::string& key_column,
    std::string* primary_key) {

    // Extract primary key from the specified column
    if (row_index >= batch.num_rows()) {
        return Status::InvalidArgument("Row index out of bounds");
    }

    // Use specified key column or default to first column
    std::string actual_key_column = key_column.empty() ? "" : key_column;

    int column_index = 0;
    if (!actual_key_column.empty()) {
        column_index = batch.schema()->GetFieldIndex(actual_key_column);
        if (column_index == -1) {
            return Status::InvalidArgument("Key column not found: " + actual_key_column);
        }
    }

    // Get the value from the specified row and column
    auto column = batch.column(column_index);
    auto scalar_result = column->GetScalar(row_index);

    if (!scalar_result.ok()) {
        return Status::FromArrowStatus(scalar_result.status());
    }

    // Convert to string representation
    *primary_key = (*scalar_result)->ToString();
    return Status::OK();
}

// FIXME: MergeRecordBatches is a complete placeholder
// Real implementation needs proper RecordBatch concatenation
Status TemporalReconstructor::MergeRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    std::shared_ptr<arrow::RecordBatch>* result) {

    if (batches.empty()) {
        // FIXME: Simplified for MVP - proper error handling needed
        auto empty_result = arrow::RecordBatch::MakeEmpty(arrow::schema({}));
        *result = empty_result.ValueUnsafe();
        return Status::OK();
    }

    // Implement proper batch merging using Arrow's ConcatenateTables
    if (batches.size() == 1) {
        *result = batches[0];
        return Status::OK();
    }

    // Convert shared_ptr to raw pointers for ConcatenateTables
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto& batch : batches) {
        // Convert RecordBatch to Table
        auto table_result = arrow::Table::FromRecordBatches({batch});
        if (!table_result.ok()) {
            return Status::FromArrowStatus(table_result.status());
        }
        tables.push_back(*table_result);
    }

    // Concatenate all tables
    auto concat_result = arrow::ConcatenateTables(tables);
    if (!concat_result.ok()) {
        return Status::FromArrowStatus(concat_result.status());
    }

    // Convert back to RecordBatch (combine chunks)
    auto combined_result = (*concat_result)->CombineChunksToBatch();
    if (!combined_result.ok()) {
        return Status::FromArrowStatus(combined_result.status());
    }

    *result = *combined_result;
    return Status::OK();
}

Status TemporalReconstructor::SortByPrimaryKey(
    const arrow::RecordBatch& batch,
    const std::string& key_column,
    std::shared_ptr<arrow::RecordBatch>* result) {

    if (batch.num_rows() == 0) {
        *result = arrow::RecordBatch::Make(batch.schema(), 0, batch.columns());
        return Status::OK();
    }

    // Determine which column to sort by
    int sort_column_index = 0;
    if (!key_column.empty()) {
        sort_column_index = batch.schema()->GetFieldIndex(key_column);
        if (sort_column_index == -1) {
            return Status::InvalidArgument("Key column not found: " + key_column);
        }
    }

    // Convert RecordBatch to Table for sorting
    auto table_result = arrow::Table::FromRecordBatches({std::make_shared<arrow::RecordBatch>(batch)});
    if (!table_result.ok()) {
        return Status::FromArrowStatus(table_result.status());
    }
    auto table = *table_result;

    // Create sort options
    arrow::compute::SortOptions sort_options;
    sort_options.sort_keys.push_back(
        arrow::compute::SortKey(sort_column_index, arrow::compute::SortOrder::Ascending));

    // Perform the sort using Arrow compute
    auto sort_indices_result = arrow::compute::SortIndices(table->column(sort_column_index)->chunk(0), sort_options);
    if (!sort_indices_result.ok()) {
        return Status::FromArrowStatus(sort_indices_result.status());
    }
    auto sort_indices = *sort_indices_result;

    // Apply the sort indices to reorder the batch
    std::vector<std::shared_ptr<arrow::Array>> sorted_columns;
    for (int i = 0; i < batch.num_columns(); ++i) {
        auto take_result = arrow::compute::Take(batch.column(i), sort_indices);
        if (!take_result.ok()) {
            return Status::FromArrowStatus(take_result.status());
        }
        sorted_columns.push_back((*take_result).make_array());
    }

    // Create sorted RecordBatch
    *result = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), sorted_columns);
    return Status::OK();
}

// ArcticQueryBuilder implementation

// Implement FindActiveVersion method (renamed from FindActiveVersionAtSnapshot)
Status TemporalReconstructor::FindActiveVersion(
    const VersionChain& chain,
    const SnapshotId& snapshot,
    const std::vector<arrow::RecordBatch>& version_batches,
    std::shared_ptr<arrow::RecordBatch>* result) {

    // Find the version that was active at the snapshot time
    if (chain.version_indices.empty()) {
        *result = nullptr;
        return Status::OK();
    }

    // Traverse the version chain to find the most recent version
    // that was committed before or at the snapshot time
    for (size_t i = 0; i < chain.version_indices.size(); ++i) {
        size_t version_index = chain.version_indices[i];
        if (version_index >= version_batches.size()) continue;

        const auto& metadata = chain.metadata[i];

        // Check if this version was active at the snapshot time
        // For system time, check if the version's system time <= snapshot time
        if (metadata.system_time <= snapshot.timestamp) {
            // This is the active version at the snapshot time
            *result = arrow::RecordBatch::Make(
                version_batches[version_index].schema(),
                version_batches[version_index].num_rows(),
                version_batches[version_index].columns());
            return Status::OK();
        }
    }

    // No version was active at the snapshot time
    *result = nullptr;
    return Status::OK();
}

ArcticQueryBuilder::ArcticQueryBuilder() {
    spec_.include_deleted = false;
    spec_.temporal_reconstruction = true;
}

ArcticQueryBuilder& ArcticQueryBuilder::AsOf(const SnapshotId& snapshot) {
    spec_.as_of_snapshot = snapshot;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::AsOfLatest() {
    spec_.as_of_snapshot = SnapshotId::Latest();
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::ValidFrom(uint64_t timestamp) {
    spec_.valid_time_start = timestamp;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::ValidTo(uint64_t timestamp) {
    spec_.valid_time_end = timestamp;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::ValidBetween(uint64_t start, uint64_t end) {
    spec_.valid_time_start = start;
    spec_.valid_time_end = end;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::SystemTimeFrom(uint64_t timestamp) {
    spec_.system_time_start = timestamp;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::SystemTimeTo(uint64_t timestamp) {
    spec_.system_time_end = timestamp;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::SystemTimeBetween(uint64_t start, uint64_t end) {
    spec_.system_time_start = start;
    spec_.system_time_end = end;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::IncludeDeleted(bool include) {
    spec_.include_deleted = include;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::ReconstructionEnabled(bool enabled) {
    spec_.temporal_reconstruction = enabled;
    return *this;
}

ArcticQueryBuilder& ArcticQueryBuilder::HistoryForKey(const std::string& primary_key) {
    history_key_ = primary_key;
    return *this;
}

TemporalQuerySpec ArcticQueryBuilder::Build() const {
    return spec_;
}

Status ArcticQueryBuilder::Execute(const std::shared_ptr<TemporalTable>& table,
                                  std::unique_ptr<QueryResult>* result) {
    if (history_key_.has_value()) {
        // History query
        // FIXME: GetVersionHistory not implemented in TemporalTable interface
        return Status::NotImplemented("Version history queries not yet implemented");
    } else {
        // Standard temporal query
        ScanSpec scan_spec;
        return table->TemporalScan(spec_, scan_spec, result);
    }
}

// ArcticOperations implementation

Status ArcticOperations::AppendWithValidity(
    const std::shared_ptr<TemporalTable>& table,
    const arrow::RecordBatch& data,
    uint64_t valid_from,
    uint64_t valid_to) {

    TemporalMetadata metadata;
    metadata.valid_from = valid_from;
    metadata.valid_to = valid_to;

    return table->TemporalInsert(data, metadata);
}

Status ArcticOperations::UpdateWithValidity(
    const std::shared_ptr<TemporalTable>& table,
    const std::string& primary_key,
    const arrow::RecordBatch& updates,
    uint64_t valid_from,
    uint64_t valid_to) {

    // For MVP, treat as insert with validity
    return AppendWithValidity(table, updates, valid_from, valid_to);
}

Status ArcticOperations::DeleteWithValidity(
    const std::shared_ptr<TemporalTable>& table,
    const std::string& primary_key,
    uint64_t valid_from,
    uint64_t valid_to) {

    TemporalMetadata metadata;
    metadata.valid_from = valid_from;
    metadata.valid_to = valid_to;
    metadata.is_deleted = true;

    return table->TemporalDelete(primary_key, metadata);
}

Status ArcticOperations::GetVersionHistory(
    const std::shared_ptr<TemporalTable>& table,
    const std::string& primary_key,
    std::vector<VersionInfo>* history) {

    // For MVP, return empty history
    history->clear();
    return Status::OK();
}

// Factory functions

std::unique_ptr<TemporalReconstructor> CreateTemporalReconstructor() {
    return std::make_unique<TemporalReconstructor>();
}

std::unique_ptr<ArcticQueryBuilder> CreateArcticQueryBuilder() {
    return std::make_unique<ArcticQueryBuilder>();
}

} // namespace marble
