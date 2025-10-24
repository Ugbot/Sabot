/************************************************************************
TripleStore: Real MarbleDB-backed implementation (No in-memory caches!)
**************************************************************************/

#include <sabot_ql/storage/triple_store.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/db.h>
#include <marble/record.h>
#include <unordered_map>
#include <algorithm>

namespace sabot_ql {

// Real MarbleDB-backed triple store using Iterator API
class MarbleDBTripleStore : public TripleStore {
public:
    MarbleDBTripleStore(const std::string& db_path,
                       std::shared_ptr<marble::MarbleDB> db)
        : db_path_(db_path), db_(std::move(db)), total_triples_(0) {}

    ~MarbleDBTripleStore() override = default;

    // Initialize column families for SPO, POS, OSP indexes
    arrow::Status Initialize() {
        // Create schema for triple storage (3 int64 columns)
        auto triple_schema = arrow::schema({
            arrow::field("col1", arrow::int64()),
            arrow::field("col2", arrow::int64()),
            arrow::field("col3", arrow::int64())
        });

        // Create column family options
        marble::ColumnFamilyOptions cf_opts;
        cf_opts.schema = triple_schema;
        cf_opts.enable_bloom_filter = true;
        cf_opts.enable_sparse_index = true;

        // Create column families for each index permutation
        marble::ColumnFamilyDescriptor spo_cf("SPO", cf_opts);
        auto status = db_->CreateColumnFamily(spo_cf, &spo_handle_);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create SPO column family: " +
                                         status.ToString());
        }

        marble::ColumnFamilyDescriptor pos_cf("POS", cf_opts);
        status = db_->CreateColumnFamily(pos_cf, &pos_handle_);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create POS column family: " +
                                         status.ToString());
        }

        marble::ColumnFamilyDescriptor osp_cf("OSP", cf_opts);
        status = db_->CreateColumnFamily(osp_cf, &osp_handle_);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create OSP column family: " +
                                         status.ToString());
        }

        return arrow::Status::OK();
    }

    arrow::Status InsertTriples(const std::vector<Triple>& triples) override {
        if (triples.empty()) {
            return arrow::Status::OK();
        }

        // Convert triples to Arrow batches for each index
        ARROW_ASSIGN_OR_RAISE(auto spo_batch, CreateIndexBatch(triples, IndexType::SPO));
        ARROW_ASSIGN_OR_RAISE(auto pos_batch, CreateIndexBatch(triples, IndexType::POS));
        ARROW_ASSIGN_OR_RAISE(auto osp_batch, CreateIndexBatch(triples, IndexType::OSP));

        // Insert into each index (MarbleDB handles persistence)
        auto status = db_->InsertBatch("SPO", spo_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert SPO batch: " +
                                         status.ToString());
        }

        status = db_->InsertBatch("POS", pos_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert POS batch: " +
                                         status.ToString());
        }

        status = db_->InsertBatch("OSP", osp_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert OSP batch: " +
                                         status.ToString());
        }

        total_triples_ += triples.size();
        return arrow::Status::OK();
    }

    arrow::Status InsertArrowBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) override {
        // Convert Arrow batch to triples
        ARROW_ASSIGN_OR_RAISE(auto triples, Triple::FromArrowBatch(batch));
        return InsertTriples(triples);
    }

    arrow::Result<std::shared_ptr<arrow::Table>> ScanPattern(
        const TriplePattern& pattern) override {

        // Choose best index for this pattern
        IndexType index = SelectIndex(pattern);

        // Scan using MarbleDB Iterator API (real implementation!)
        ARROW_ASSIGN_OR_RAISE(auto scan_result, ScanIndexMarbleDB(index, pattern));

        // Project to requested columns (unbound variables)
        ARROW_ASSIGN_OR_RAISE(auto projected, ProjectResult(scan_result, pattern));

        return projected;
    }

    arrow::Result<size_t> EstimateCardinality(
        const TriplePattern& pattern) override {

        size_t bound = pattern.BoundCount();

        if (bound == 3) {
            // All bound: point lookup (0 or 1 result)
            return 1;
        } else if (bound == 2) {
            // Two bound: estimate ~1% of total triples
            return std::max(size_t(1), total_triples_ / 100);
        } else if (bound == 1) {
            // One bound: estimate ~10% of total triples
            return std::max(size_t(1), total_triples_ / 10);
        } else {
            // No bounds: full scan
            return total_triples_;
        }
    }

    IndexType SelectIndex(const TriplePattern& pattern) const override {
        // Choose index based on bound positions (leftmost principle)
        std::string bound = pattern.BoundPositions();

        if (bound.empty()) {
            // No bounds: SPO is default
            return IndexType::SPO;
        }

        if (bound[0] == 'S') {
            return IndexType::SPO;
        } else if (bound[0] == 'P') {
            return IndexType::POS;
        } else if (bound[0] == 'O') {
            return IndexType::OSP;
        }

        // Fallback
        return IndexType::SPO;
    }

    size_t TotalTriples() const override {
        return total_triples_;
    }

    arrow::Status Flush() override {
        auto status = db_->Flush();
        if (!status.ok()) {
            return arrow::Status::IOError("Flush failed: " + status.ToString());
        }
        return arrow::Status::OK();
    }

    arrow::Status Compact() override {
        // MarbleDB will eventually expose compaction API
        // For now, flush is sufficient
        return Flush();
    }

private:
    // **REAL MARBLEDB IMPLEMENTATION**: Scan using Iterator API
    arrow::Result<std::shared_ptr<arrow::Table>> ScanIndexMarbleDB(
        IndexType index, const TriplePattern& pattern) {

        // Get column family for this index
        std::string cf_name = GetColumnFamilyName(index);

        // Determine key range based on bound variables
        marble::KeyRange key_range = CreateKeyRange(index, pattern);

        // Create MarbleDB iterator for range scan
        marble::ReadOptions read_opts;
        std::unique_ptr<marble::Iterator> iter;

        auto status = db_->NewIterator(read_opts, key_range, &iter);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create iterator: " +
                                         status.ToString());
        }

        // Build result by scanning with iterator
        arrow::Int64Builder col1_builder;
        arrow::Int64Builder col2_builder;
        arrow::Int64Builder col3_builder;

        // Estimate size for better performance
        size_t estimated_size = total_triples_ / 10;
        ARROW_RETURN_NOT_OK(col1_builder.Reserve(estimated_size));
        ARROW_RETURN_NOT_OK(col2_builder.Reserve(estimated_size));
        ARROW_RETURN_NOT_OK(col3_builder.Reserve(estimated_size));

        // Scan all matching triples using MarbleDB Iterator
        while (iter->Valid()) {
            // Get record from iterator
            auto record = iter->value();

            // Extract triple components from Arrow RecordBatch
            // MarbleDB stores records as Arrow format
            auto batch = record->ToRecordBatch();
            if (!batch.ok()) {
                return batch.status();
            }

            auto arrow_batch = *batch;
            if (arrow_batch->num_rows() > 0) {
                auto col1_array = std::static_pointer_cast<arrow::Int64Array>(
                    arrow_batch->column(0));
                auto col2_array = std::static_pointer_cast<arrow::Int64Array>(
                    arrow_batch->column(1));
                auto col3_array = std::static_pointer_cast<arrow::Int64Array>(
                    arrow_batch->column(2));

                for (int64_t row = 0; row < arrow_batch->num_rows(); ++row) {
                    uint64_t col1 = col1_array->Value(row);
                    uint64_t col2 = col2_array->Value(row);
                    uint64_t col3 = col3_array->Value(row);

                    // Apply pattern filter
                    if (CheckPatternMatch(index, pattern, col1, col2, col3)) {
                        ARROW_RETURN_NOT_OK(col1_builder.Append(col1));
                        ARROW_RETURN_NOT_OK(col2_builder.Append(col2));
                        ARROW_RETURN_NOT_OK(col3_builder.Append(col3));
                    }
                }
            }

            // Move to next record
            iter->Next();
        }

        // Check iterator status
        if (!iter->status().ok()) {
            return arrow::Status::IOError("Iterator error: " +
                                         iter->status().ToString());
        }

        // Finish arrays
        std::shared_ptr<arrow::Array> col1_array;
        std::shared_ptr<arrow::Array> col2_array;
        std::shared_ptr<arrow::Array> col3_array;

        ARROW_RETURN_NOT_OK(col1_builder.Finish(&col1_array));
        ARROW_RETURN_NOT_OK(col2_builder.Finish(&col2_array));
        ARROW_RETURN_NOT_OK(col3_builder.Finish(&col3_array));

        // Create schema based on index type
        auto schema = GetIndexSchema(index);

        // Create table with real data from MarbleDB
        return arrow::Table::Make(schema, {col1_array, col2_array, col3_array});
    }

    // Create key range for MarbleDB scan
    marble::KeyRange CreateKeyRange(IndexType index, const TriplePattern& pattern) {
        // Determine start and end keys based on bound variables
        std::shared_ptr<marble::Key> start_key;
        std::shared_ptr<marble::Key> end_key;

        // Get bound values based on index permutation
        auto [bound1, bound2, bound3] = GetBoundValues(index, pattern);

        if (bound1.has_value() && bound2.has_value() && bound3.has_value()) {
            // All bound: point lookup
            start_key = std::make_shared<marble::TripleKey>(
                *bound1, *bound2, *bound3);
            end_key = start_key;  // Same key for point lookup
        } else if (bound1.has_value() && bound2.has_value()) {
            // First two bound: range on third
            start_key = std::make_shared<marble::TripleKey>(
                *bound1, *bound2, INT64_MIN);
            end_key = std::make_shared<marble::TripleKey>(
                *bound1, *bound2, INT64_MAX);
        } else if (bound1.has_value()) {
            // First bound: range on second and third
            start_key = std::make_shared<marble::TripleKey>(
                *bound1, INT64_MIN, INT64_MIN);
            end_key = std::make_shared<marble::TripleKey>(
                *bound1, INT64_MAX, INT64_MAX);
        } else {
            // No bounds: full scan
            start_key = std::make_shared<marble::TripleKey>(
                INT64_MIN, INT64_MIN, INT64_MIN);
            end_key = std::make_shared<marble::TripleKey>(
                INT64_MAX, INT64_MAX, INT64_MAX);
        }

        return marble::KeyRange(start_key, true, end_key, true);
    }

    // Get bound values based on index permutation
    std::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>>
    GetBoundValues(IndexType index, const TriplePattern& pattern) {
        switch (index) {
            case IndexType::SPO:
                return {
                    pattern.subject.has_value() ?
                        std::optional<int64_t>(*pattern.subject) : std::nullopt,
                    pattern.predicate.has_value() ?
                        std::optional<int64_t>(*pattern.predicate) : std::nullopt,
                    pattern.object.has_value() ?
                        std::optional<int64_t>(*pattern.object) : std::nullopt
                };
            case IndexType::POS:
                return {
                    pattern.predicate.has_value() ?
                        std::optional<int64_t>(*pattern.predicate) : std::nullopt,
                    pattern.object.has_value() ?
                        std::optional<int64_t>(*pattern.object) : std::nullopt,
                    pattern.subject.has_value() ?
                        std::optional<int64_t>(*pattern.subject) : std::nullopt
                };
            case IndexType::OSP:
                return {
                    pattern.object.has_value() ?
                        std::optional<int64_t>(*pattern.object) : std::nullopt,
                    pattern.subject.has_value() ?
                        std::optional<int64_t>(*pattern.subject) : std::nullopt,
                    pattern.predicate.has_value() ?
                        std::optional<int64_t>(*pattern.predicate) : std::nullopt
                };
        }
        return {std::nullopt, std::nullopt, std::nullopt};
    }

    // Helper methods (same as before)
    std::string GetColumnFamilyName(IndexType index) const {
        switch (index) {
            case IndexType::SPO: return "SPO";
            case IndexType::POS: return "POS";
            case IndexType::OSP: return "OSP";
        }
        return "SPO";
    }

    std::shared_ptr<arrow::Schema> GetIndexSchema(IndexType index) const {
        switch (index) {
            case IndexType::SPO:
                return arrow::schema({
                    arrow::field("subject", arrow::int64()),
                    arrow::field("predicate", arrow::int64()),
                    arrow::field("object", arrow::int64())
                });
            case IndexType::POS:
                return arrow::schema({
                    arrow::field("predicate", arrow::int64()),
                    arrow::field("object", arrow::int64()),
                    arrow::field("subject", arrow::int64())
                });
            case IndexType::OSP:
                return arrow::schema({
                    arrow::field("object", arrow::int64()),
                    arrow::field("subject", arrow::int64()),
                    arrow::field("predicate", arrow::int64())
                });
        }
        return Triple::Schema();
    }

    // Create Arrow batch with columns ordered for specific index
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateIndexBatch(
        const std::vector<Triple>& triples, IndexType index) {

        arrow::Int64Builder col1_builder;
        arrow::Int64Builder col2_builder;
        arrow::Int64Builder col3_builder;

        ARROW_RETURN_NOT_OK(col1_builder.Reserve(triples.size()));
        ARROW_RETURN_NOT_OK(col2_builder.Reserve(triples.size()));
        ARROW_RETURN_NOT_OK(col3_builder.Reserve(triples.size()));

        for (const auto& triple : triples) {
            switch (index) {
                case IndexType::SPO:
                    ARROW_RETURN_NOT_OK(col1_builder.Append(triple.subject.getBits()));
                    ARROW_RETURN_NOT_OK(col2_builder.Append(triple.predicate.getBits()));
                    ARROW_RETURN_NOT_OK(col3_builder.Append(triple.object.getBits()));
                    break;
                case IndexType::POS:
                    ARROW_RETURN_NOT_OK(col1_builder.Append(triple.predicate.getBits()));
                    ARROW_RETURN_NOT_OK(col2_builder.Append(triple.object.getBits()));
                    ARROW_RETURN_NOT_OK(col3_builder.Append(triple.subject.getBits()));
                    break;
                case IndexType::OSP:
                    ARROW_RETURN_NOT_OK(col1_builder.Append(triple.object.getBits()));
                    ARROW_RETURN_NOT_OK(col2_builder.Append(triple.subject.getBits()));
                    ARROW_RETURN_NOT_OK(col3_builder.Append(triple.predicate.getBits()));
                    break;
            }
        }

        std::shared_ptr<arrow::Array> col1_array;
        std::shared_ptr<arrow::Array> col2_array;
        std::shared_ptr<arrow::Array> col3_array;

        ARROW_RETURN_NOT_OK(col1_builder.Finish(&col1_array));
        ARROW_RETURN_NOT_OK(col2_builder.Finish(&col2_array));
        ARROW_RETURN_NOT_OK(col3_builder.Finish(&col3_array));

        auto schema = GetIndexSchema(index);

        return arrow::RecordBatch::Make(
            schema, triples.size(),
            {col1_array, col2_array, col3_array});
    }

    // Check if a triple matches the pattern
    bool CheckPatternMatch(IndexType index, const TriplePattern& pattern,
                          uint64_t col1, uint64_t col2, uint64_t col3) const {
        switch (index) {
            case IndexType::SPO:
                if (pattern.subject.has_value() &&
                    *pattern.subject != col1) return false;
                if (pattern.predicate.has_value() &&
                    *pattern.predicate != col2) return false;
                if (pattern.object.has_value() &&
                    *pattern.object != col3) return false;
                break;
            case IndexType::POS:
                if (pattern.predicate.has_value() &&
                    *pattern.predicate != col1) return false;
                if (pattern.object.has_value() &&
                    *pattern.object != col2) return false;
                if (pattern.subject.has_value() &&
                    *pattern.subject != col3) return false;
                break;
            case IndexType::OSP:
                if (pattern.object.has_value() &&
                    *pattern.object != col1) return false;
                if (pattern.subject.has_value() &&
                    *pattern.subject != col2) return false;
                if (pattern.predicate.has_value() &&
                    *pattern.predicate != col3) return false;
                break;
        }
        return true;
    }

    // Project scan results to requested columns
    arrow::Result<std::shared_ptr<arrow::Table>> ProjectResult(
        const std::shared_ptr<arrow::Table>& scan_result,
        const TriplePattern& pattern) {

        // Determine which columns to keep based on unbound variables
        std::vector<int> keep_columns;

        if (!pattern.subject.has_value()) keep_columns.push_back(0);
        if (!pattern.predicate.has_value()) keep_columns.push_back(1);
        if (!pattern.object.has_value()) keep_columns.push_back(2);

        if (keep_columns.empty()) {
            // All bound: return empty result (just count)
            return scan_result;
        }

        // Project columns
        std::vector<std::shared_ptr<arrow::ChunkedArray>> projected_columns;
        std::vector<std::shared_ptr<arrow::Field>> projected_fields;

        for (int col_idx : keep_columns) {
            projected_columns.push_back(scan_result->column(col_idx));
            projected_fields.push_back(scan_result->schema()->field(col_idx));
        }

        auto projected_schema = arrow::schema(projected_fields);

        return arrow::Table::Make(projected_schema, projected_columns);
    }

    // Member variables
    std::string db_path_;
    std::shared_ptr<marble::MarbleDB> db_;

    // Column family handles
    marble::ColumnFamilyHandle* spo_handle_ = nullptr;
    marble::ColumnFamilyHandle* pos_handle_ = nullptr;
    marble::ColumnFamilyHandle* osp_handle_ = nullptr;

    // Statistics
    std::atomic<size_t> total_triples_;
};

// Factory function
arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStoreMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    auto store = std::make_shared<MarbleDBTripleStore>(db_path, std::move(db));

    auto status = store->Initialize();
    if (!status.ok()) {
        return status;
    }

    return store;
}

} // namespace sabot_ql
