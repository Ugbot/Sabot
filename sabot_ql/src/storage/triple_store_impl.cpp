#include <sabot_ql/storage/triple_store.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/db.h>
#include <unordered_map>
#include <algorithm>

namespace sabot_ql {

// TripleStoreImpl: MarbleDB-backed triple store with 3 indexes
class TripleStoreImpl : public TripleStore {
public:
    TripleStoreImpl(const std::string& db_path,
                    std::shared_ptr<marble::MarbleDB> db)
        : db_path_(db_path), db_(std::move(db)) {}

    ~TripleStoreImpl() override = default;

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

        // Insert into each index
        marble::WriteOptions write_opts;

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

        // Also populate in-memory cache for scanning (MVP approach)
        // Reserve space for better performance
        spo_cache_.reserve(spo_cache_.size() + triples.size());
        pos_cache_.reserve(pos_cache_.size() + triples.size());
        osp_cache_.reserve(osp_cache_.size() + triples.size());

        for (const auto& triple : triples) {
            // SPO index: (S, P, O)
            spo_cache_.push_back({
                triple.subject.getBits(),
                triple.predicate.getBits(),
                triple.object.getBits()
            });

            // POS index: (P, O, S)
            pos_cache_.push_back({
                triple.predicate.getBits(),
                triple.object.getBits(),
                triple.subject.getBits()
            });

            // OSP index: (O, S, P)
            osp_cache_.push_back({
                triple.object.getBits(),
                triple.subject.getBits(),
                triple.predicate.getBits()
            });
        }

        total_triples_ += triples.size();
        return arrow::Status::OK();
    }

    arrow::Status InsertArrowBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) override {

        // Convert Arrow batch to vector of triples
        ARROW_ASSIGN_OR_RAISE(auto triples, Triple::FromArrowBatch(batch));
        return InsertTriples(triples);
    }

    arrow::Result<std::shared_ptr<arrow::Table>> ScanPattern(
        const TriplePattern& pattern) override {

        // Select best index for this pattern
        IndexType index = SelectIndex(pattern);

        // Build scan key based on bound variables
        ARROW_ASSIGN_OR_RAISE(auto scan_result, ScanIndex(index, pattern));

        // Project to requested columns (unbound variables)
        ARROW_ASSIGN_OR_RAISE(auto projected, ProjectResult(scan_result, pattern));

        return projected;
    }

    arrow::Result<size_t> EstimateCardinality(
        const TriplePattern& pattern) override {

        // For now, use simple heuristics based on bound count
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

        // SPO: good for S, SP, SPO
        // POS: good for P, PO, POS
        // OSP: good for O, OS, OSP

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
        // MarbleDB handles flushing internally
        // Could add explicit flush API if needed
        return arrow::Status::OK();
    }

    arrow::Status Compact() override {
        // Trigger compaction on all column families
        // MarbleDB should expose compaction API
        return arrow::Status::OK();
    }

private:
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

        // Schema depends on index type
        std::shared_ptr<arrow::Schema> schema;
        switch (index) {
            case IndexType::SPO:
                schema = arrow::schema({
                    arrow::field("subject", arrow::int64()),
                    arrow::field("predicate", arrow::int64()),
                    arrow::field("object", arrow::int64())
                });
                break;
            case IndexType::POS:
                schema = arrow::schema({
                    arrow::field("predicate", arrow::int64()),
                    arrow::field("object", arrow::int64()),
                    arrow::field("subject", arrow::int64())
                });
                break;
            case IndexType::OSP:
                schema = arrow::schema({
                    arrow::field("object", arrow::int64()),
                    arrow::field("subject", arrow::int64()),
                    arrow::field("predicate", arrow::int64())
                });
                break;
        }

        return arrow::RecordBatch::Make(schema, triples.size(),
                                       {col1_array, col2_array, col3_array});
    }

    // Scan index with pattern
    arrow::Result<std::shared_ptr<arrow::Table>> ScanIndex(
        IndexType index, const TriplePattern& pattern) {

        // MVP Implementation: Use in-memory cache for scanning
        // TODO: Once MarbleDB Iterator API is implemented, use range scans

        // Get cached triples for this index
        const auto& triples_cache = GetCacheForIndex(index);

        // Build result by filtering cached triples
        arrow::Int64Builder col1_builder;
        arrow::Int64Builder col2_builder;
        arrow::Int64Builder col3_builder;

        // Estimate size for better performance
        size_t estimated_size = total_triples_ / 10;  // Rough estimate
        ARROW_RETURN_NOT_OK(col1_builder.Reserve(estimated_size));
        ARROW_RETURN_NOT_OK(col2_builder.Reserve(estimated_size));
        ARROW_RETURN_NOT_OK(col3_builder.Reserve(estimated_size));

        // Scan and filter based on bound variables
        for (const auto& triple_data : triples_cache) {
            uint64_t col1 = triple_data.col1;
            uint64_t col2 = triple_data.col2;
            uint64_t col3 = triple_data.col3;

            // Apply pattern filter based on index type
            bool matches = CheckPatternMatch(index, pattern, col1, col2, col3);

            if (matches) {
                ARROW_RETURN_NOT_OK(col1_builder.Append(col1));
                ARROW_RETURN_NOT_OK(col2_builder.Append(col2));
                ARROW_RETURN_NOT_OK(col3_builder.Append(col3));
            }
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

        // Create table
        return arrow::Table::Make(schema, {col1_array, col2_array, col3_array});
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

        // Project columns by selecting specific column indices
        std::vector<std::shared_ptr<arrow::ChunkedArray>> projected_columns;
        for (int col_idx : keep_columns) {
            projected_columns.push_back(scan_result->column(col_idx));
        }

        // Build new schema with selected columns
        std::vector<std::shared_ptr<arrow::Field>> projected_fields;
        for (int col_idx : keep_columns) {
            projected_fields.push_back(scan_result->schema()->field(col_idx));
        }
        auto projected_schema = arrow::schema(projected_fields);

        return arrow::Table::Make(projected_schema, projected_columns);
    }

    // Simple triple storage for in-memory cache
    struct TripleData {
        uint64_t col1;
        uint64_t col2;
        uint64_t col3;
    };

    // Helper: Get cache for index
    const std::vector<TripleData>& GetCacheForIndex(IndexType index) const {
        switch (index) {
            case IndexType::SPO: return spo_cache_;
            case IndexType::POS: return pos_cache_;
            case IndexType::OSP: return osp_cache_;
            default: return spo_cache_;
        }
    }

    // Helper: Check if triple matches pattern
    bool CheckPatternMatch(IndexType index, const TriplePattern& pattern,
                          uint64_t col1, uint64_t col2, uint64_t col3) const {
        // Map columns back to S, P, O based on index type
        uint64_t s, p, o;
        switch (index) {
            case IndexType::SPO:
                s = col1; p = col2; o = col3;
                break;
            case IndexType::POS:
                p = col1; o = col2; s = col3;
                break;
            case IndexType::OSP:
                o = col1; s = col2; p = col3;
                break;
        }

        // Check each bound variable
        if (pattern.subject.has_value() && pattern.subject.value() != s) return false;
        if (pattern.predicate.has_value() && pattern.predicate.value() != p) return false;
        if (pattern.object.has_value() && pattern.object.value() != o) return false;

        return true;
    }

    // Helper: Get schema for index
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
            default:
                return arrow::schema({
                    arrow::field("subject", arrow::int64()),
                    arrow::field("predicate", arrow::int64()),
                    arrow::field("object", arrow::int64())
                });
        }
    }

    std::string db_path_;
    std::shared_ptr<marble::MarbleDB> db_;

    // Column family handles
    marble::ColumnFamilyHandle* spo_handle_ = nullptr;
    marble::ColumnFamilyHandle* pos_handle_ = nullptr;
    marble::ColumnFamilyHandle* osp_handle_ = nullptr;

    // In-memory cache for scanning (MVP approach)
    // TODO: Remove once MarbleDB Iterator API is implemented
    std::vector<TripleData> spo_cache_;
    std::vector<TripleData> pos_cache_;
    std::vector<TripleData> osp_cache_;

    // Statistics
    size_t total_triples_ = 0;
};

// Factory functions implementation

arrow::Result<std::shared_ptr<TripleStore>> TripleStore::Open(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    auto impl = std::make_shared<TripleStoreImpl>(db_path, std::move(db));
    ARROW_RETURN_NOT_OK(impl->Initialize());
    return impl;
}

arrow::Result<std::shared_ptr<TripleStore>> TripleStore::Create(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    // Same as Open for now (MarbleDB creates if not exists)
    return Open(db_path, std::move(db));
}

arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStore(
    const std::string& db_path) {

    // Open MarbleDB instance
    marble::DBOptions options;
    options.db_path = db_path;
    options.enable_sparse_index = true;
    options.enable_bloom_filter = true;
    options.index_granularity = 8192;  // Index every 8192 rows (ClickHouse-style)

    // MarbleDB::Open expects marble::Schema, pass nullptr for now
    // (schema is defined per-column-family via ColumnFamilyOptions)
    std::unique_ptr<marble::MarbleDB> db_ptr;
    auto status = marble::MarbleDB::Open(options, nullptr, &db_ptr);
    if (!status.ok()) {
        return arrow::Status::IOError("Failed to open MarbleDB: " +
                                     status.ToString());
    }

    return TripleStore::Create(db_path, std::move(db_ptr));
}

// Triple utility methods implementation

arrow::Result<std::shared_ptr<arrow::RecordBatch>> Triple::ToArrowBatch(
    const std::vector<Triple>& triples) {

    arrow::Int64Builder s_builder;
    arrow::Int64Builder p_builder;
    arrow::Int64Builder o_builder;

    ARROW_RETURN_NOT_OK(s_builder.Reserve(triples.size()));
    ARROW_RETURN_NOT_OK(p_builder.Reserve(triples.size()));
    ARROW_RETURN_NOT_OK(o_builder.Reserve(triples.size()));

    for (const auto& triple : triples) {
        ARROW_RETURN_NOT_OK(s_builder.Append(triple.subject.getBits()));
        ARROW_RETURN_NOT_OK(p_builder.Append(triple.predicate.getBits()));
        ARROW_RETURN_NOT_OK(o_builder.Append(triple.object.getBits()));
    }

    std::shared_ptr<arrow::Array> s_array;
    std::shared_ptr<arrow::Array> p_array;
    std::shared_ptr<arrow::Array> o_array;

    ARROW_RETURN_NOT_OK(s_builder.Finish(&s_array));
    ARROW_RETURN_NOT_OK(p_builder.Finish(&p_array));
    ARROW_RETURN_NOT_OK(o_builder.Finish(&o_array));

    return arrow::RecordBatch::Make(Triple::Schema(), triples.size(),
                                   {s_array, p_array, o_array});
}

arrow::Result<std::vector<Triple>> Triple::FromArrowBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    if (batch->num_columns() != 3) {
        return arrow::Status::Invalid(
            "Expected 3 columns (subject, predicate, object), got " +
            std::to_string(batch->num_columns()));
    }

    auto s_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    auto p_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
    auto o_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

    std::vector<Triple> triples;
    triples.reserve(batch->num_rows());

    for (int64_t i = 0; i < batch->num_rows(); i++) {
        triples.push_back({
            ValueId::fromBits(s_array->Value(i)),
            ValueId::fromBits(p_array->Value(i)),
            ValueId::fromBits(o_array->Value(i))
        });
    }

    return triples;
}

} // namespace sabot_ql
