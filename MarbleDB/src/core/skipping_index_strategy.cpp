/**
 * SkippingIndexStrategy implementation
 */

#include "marble/skipping_index_strategy.h"
#include "marble/record.h"
#include "marble/table_capabilities.h"
#include <sstream>
#include <chrono>

namespace marble {

//==============================================================================
// SkippingIndexStrategy implementation
//==============================================================================

SkippingIndexStrategy::SkippingIndexStrategy(int64_t block_size_rows)
    : block_size_rows_(block_size_rows) {
    skipping_index_ = std::make_unique<InMemorySkippingIndex>();
}

Status SkippingIndexStrategy::OnTableCreate(const TableCapabilities& caps) {
    std::lock_guard<std::mutex> lock(mutex_);

    // TableCapabilities doesn't contain schema yet
    // Schema will be inferred from data during flush
    // Nothing to initialize here

    return Status::OK();
}

Status SkippingIndexStrategy::OnRead(ReadContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("ReadContext is null");
    }

    // TODO: Integrate with actual read path
    // The current ReadContext doesn't have predicate information yet
    // This will be implemented when query execution is enhanced

    std::lock_guard<std::mutex> lock(mutex_);

    // For now, just track that a read occurred
    if (ctx->is_range_scan) {
        stats_.num_queries.fetch_add(1, std::memory_order_relaxed);
    }

    return Status::OK();
}

void SkippingIndexStrategy::OnReadComplete(const Key& key, const Record& record) {
    // No post-read action needed for skipping indexes
}

Status SkippingIndexStrategy::OnWrite(WriteContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("WriteContext is null");
    }

    // Skipping indexes are typically built on flush, not per-write
    // Individual writes don't trigger index updates
    return Status::OK();
}

Status SkippingIndexStrategy::OnCompaction(CompactionContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("CompactionContext is null");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Set flag to indicate skipping index should be rebuilt
    // The actual rebuild happens when the compacted batches are processed
    ctx->rebuild_statistics = true;

    return Status::OK();
}

Status SkippingIndexStrategy::OnFlush(FlushContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("FlushContext is null");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Build skipping index from flushed memtable batch
    // Convert RecordBatch to Table for processing
    auto table_result = arrow::Table::FromRecordBatches({ctx->memtable_batch});
    if (!table_result.ok()) {
        return Status::InvalidArgument("Failed to convert batch to table");
    }

    auto start = std::chrono::steady_clock::now();

    auto status = skipping_index_->BuildFromTable(table_result.ValueOrDie(), block_size_rows_);
    if (!status.ok()) {
        return status;
    }

    auto end = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    stats_.num_builds.fetch_add(1, std::memory_order_relaxed);
    stats_.total_build_ms.fetch_add(duration_ms, std::memory_order_relaxed);

    // Serialize skipping index and attach to SSTable metadata
    std::vector<uint8_t> serialized = skipping_index_->Serialize();
    ctx->metadata["skipping_index"] = std::move(serialized);
    ctx->include_statistics = true;

    return Status::OK();
}

size_t SkippingIndexStrategy::MemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return skipping_index_->MemoryUsage() + sizeof(*this);
}

void SkippingIndexStrategy::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create fresh skipping index
    skipping_index_ = std::make_unique<InMemorySkippingIndex>();

    // Reset statistics
    stats_.num_queries.store(0, std::memory_order_relaxed);
    stats_.num_blocks_total.store(0, std::memory_order_relaxed);
    stats_.num_blocks_pruned.store(0, std::memory_order_relaxed);
    stats_.num_blocks_scanned.store(0, std::memory_order_relaxed);
    stats_.num_builds.store(0, std::memory_order_relaxed);
    stats_.total_build_ms.store(0, std::memory_order_relaxed);
}

std::vector<uint8_t> SkippingIndexStrategy::Serialize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return skipping_index_->Serialize();
}

Status SkippingIndexStrategy::Deserialize(const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create fresh index and deserialize
    auto index = std::make_unique<InMemorySkippingIndex>();
    auto status = index->Deserialize(data);

    if (!status.ok()) {
        return Status::InvalidArgument("Failed to deserialize skipping index");
    }

    skipping_index_ = std::move(index);
    return Status::OK();
}

std::string SkippingIndexStrategy::GetStats() const {
    std::ostringstream ss;

    uint64_t num_queries = stats_.num_queries.load(std::memory_order_relaxed);
    uint64_t blocks_total = stats_.num_blocks_total.load(std::memory_order_relaxed);
    uint64_t blocks_pruned = stats_.num_blocks_pruned.load(std::memory_order_relaxed);
    uint64_t blocks_scanned = stats_.num_blocks_scanned.load(std::memory_order_relaxed);
    uint64_t num_builds = stats_.num_builds.load(std::memory_order_relaxed);
    uint64_t total_build_ms = stats_.total_build_ms.load(std::memory_order_relaxed);

    double prune_rate = blocks_total > 0 ?
        static_cast<double>(blocks_pruned) / blocks_total : 0.0;
    double avg_build_ms = num_builds > 0 ?
        static_cast<double>(total_build_ms) / num_builds : 0.0;

    ss << "{\n";
    ss << "  \"queries\": " << num_queries << ",\n";
    ss << "  \"blocks_total\": " << blocks_total << ",\n";
    ss << "  \"blocks_pruned\": " << blocks_pruned << ",\n";
    ss << "  \"blocks_scanned\": " << blocks_scanned << ",\n";
    ss << "  \"prune_rate\": " << prune_rate << ",\n";
    ss << "  \"num_builds\": " << num_builds << ",\n";
    ss << "  \"avg_build_ms\": " << avg_build_ms << ",\n";
    ss << "  \"memory_usage\": " << MemoryUsage() << "\n";
    ss << "}";

    return ss.str();
}

//==============================================================================
// Skipping-specific methods
//==============================================================================

Status SkippingIndexStrategy::BuildFromTable(const std::shared_ptr<arrow::Table>& table) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto start = std::chrono::steady_clock::now();

    auto status = skipping_index_->BuildFromTable(table, block_size_rows_);

    if (status.ok()) {
        auto end = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        stats_.num_builds.fetch_add(1, std::memory_order_relaxed);
        stats_.total_build_ms.fetch_add(duration_ms, std::memory_order_relaxed);
    }

    return status;
}

Status SkippingIndexStrategy::GetCandidateBlocks(
    const std::string& column_name,
    const std::string& op,
    const std::shared_ptr<arrow::Scalar>& value,
    std::vector<int64_t>* candidate_blocks) const {

    std::lock_guard<std::mutex> lock(mutex_);
    return skipping_index_->GetCandidateBlocks(column_name, op, value, candidate_blocks);
}

const std::vector<SkippingIndex::BlockStats>& SkippingIndexStrategy::GetAllBlocks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return skipping_index_->GetAllBlocks();
}

void SkippingIndexStrategy::SetSkippingIndex(std::unique_ptr<InMemorySkippingIndex> index) {
    std::lock_guard<std::mutex> lock(mutex_);
    skipping_index_ = std::move(index);
}

}  // namespace marble
