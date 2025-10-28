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

        // Update statistics
        total_triples_ += triples.size();
        return arrow::Status::OK();
    }

    arrow::Status InsertArrowBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) override {

        // FAST PATH: Use Arrow compute kernels to create index batches directly
        // No conversion to/from vector<Triple> - pure columnar operations!

        // Extract columns (support both naming conventions)
        auto s_array = batch->GetColumnByName("s");
        if (!s_array) s_array = batch->GetColumnByName("subject");

        auto p_array = batch->GetColumnByName("p");
        if (!p_array) p_array = batch->GetColumnByName("predicate");

        auto o_array = batch->GetColumnByName("o");
        if (!o_array) o_array = batch->GetColumnByName("object");

        if (!s_array || !p_array || !o_array) {
            return arrow::Status::Invalid("Batch missing s/p/o columns");
        }

        // Create index batches using Arrow kernels (just reorder columns - zero copy!)
        // SPO: subject, predicate, object
        auto spo_schema = arrow::schema({
            arrow::field("s", arrow::int64()),
            arrow::field("p", arrow::int64()),
            arrow::field("o", arrow::int64())
        });
        auto spo_batch = arrow::RecordBatch::Make(
            spo_schema,
            batch->num_rows(),
            {s_array, p_array, o_array}
        );

        // POS: predicate, object, subject
        auto pos_schema = arrow::schema({
            arrow::field("p", arrow::int64()),
            arrow::field("o", arrow::int64()),
            arrow::field("s", arrow::int64())
        });
        auto pos_batch = arrow::RecordBatch::Make(
            pos_schema,
            batch->num_rows(),
            {p_array, o_array, s_array}
        );

        // OSP: object, subject, predicate
        auto osp_schema = arrow::schema({
            arrow::field("o", arrow::int64()),
            arrow::field("s", arrow::int64()),
            arrow::field("p", arrow::int64())
        });
        auto osp_batch = arrow::RecordBatch::Make(
            osp_schema,
            batch->num_rows(),
            {o_array, s_array, p_array}
        );

        // Insert into each index (3x parallel writes possible with proper MarbleDB API)
        auto status = db_->InsertBatch("SPO", spo_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert SPO batch: " + status.ToString());
        }

        status = db_->InsertBatch("POS", pos_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert POS batch: " + status.ToString());
        }

        status = db_->InsertBatch("OSP", osp_batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert OSP batch: " + status.ToString());
        }

        total_triples_ += batch->num_rows();
        return arrow::Status::OK();
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

        // Schema must match column family schema (col1, col2, col3)
        // The ordering is determined by the index type, but column names are generic
        std::shared_ptr<arrow::Schema> schema = arrow::schema({
            arrow::field("col1", arrow::int64()),
            arrow::field("col2", arrow::int64()),
            arrow::field("col3", arrow::int64())
        });

        return arrow::RecordBatch::Make(schema, triples.size(),
                                       {col1_array, col2_array, col3_array});
    }

    // Helper: Convert index type to column family name
    std::string IndexTypeToString(IndexType index) const {
        switch (index) {
            case IndexType::SPO: return "SPO";
            case IndexType::POS: return "POS";
            case IndexType::OSP: return "OSP";
            default: return "SPO";
        }
    }

    // Helper: Build MarbleDB key range from triple pattern
    // Inspired by QLever's ScanSpecification
    // Uses TripleKey for proper lexicographic ordering
    marble::KeyRange BuildKeyRange(IndexType index, const TriplePattern& pattern) {
        // Get bound values based on index type
        // SPO index: (subject, predicate, object)
        // POS index: (predicate, object, subject)
        // OSP index: (object, subject, predicate)

        std::optional<uint64_t> col1, col2, col3;
        switch (index) {
            case IndexType::SPO:
                col1 = pattern.subject;
                col2 = pattern.predicate;
                col3 = pattern.object;
                break;
            case IndexType::POS:
                col1 = pattern.predicate;
                col2 = pattern.object;
                col3 = pattern.subject;
                break;
            case IndexType::OSP:
                col1 = pattern.object;
                col2 = pattern.subject;
                col3 = pattern.predicate;
                break;
        }

        // Build key range using TripleKey
        // TripleKey compares lexicographically: first col1, then col2, then col3

        if (col1.has_value()) {
            if (col2.has_value()) {
                if (col3.has_value()) {
                    // All three bound - point lookup
                    auto key = std::make_shared<marble::TripleKey>(
                        static_cast<int64_t>(col1.value()),
                        static_cast<int64_t>(col2.value()),
                        static_cast<int64_t>(col3.value())
                    );
                    return marble::KeyRange(key, true, key, true);
                } else {
                    // col1 and col2 bound - range scan for col3
                    // Start: (col1, col2, 0)
                    // End: (col1, col2+1, 0) exclusive
                    auto start = std::make_shared<marble::TripleKey>(
                        static_cast<int64_t>(col1.value()),
                        static_cast<int64_t>(col2.value()),
                        0
                    );
                    auto end = std::make_shared<marble::TripleKey>(
                        static_cast<int64_t>(col1.value()),
                        static_cast<int64_t>(col2.value() + 1),
                        0
                    );
                    return marble::KeyRange(start, true, end, false);
                }
            } else {
                // Only col1 bound - range scan for col2 and col3
                // Start: (col1, 0, 0)
                // End: (col1+1, 0, 0) exclusive
                auto start = std::make_shared<marble::TripleKey>(
                    static_cast<int64_t>(col1.value()),
                    0,
                    0
                );
                auto end = std::make_shared<marble::TripleKey>(
                    static_cast<int64_t>(col1.value() + 1),
                    0,
                    0
                );
                return marble::KeyRange(start, true, end, false);
            }
        } else {
            // No columns bound - full scan
            return marble::KeyRange::All();
        }
    }

    // Helper: Convert MarbleDB record to triple values
    arrow::Result<std::tuple<uint64_t, uint64_t, uint64_t>>
    ConvertRecordToValues(const std::shared_ptr<marble::Record>& record) {
        // Records in triple store are Arrow RecordBatches with 3 int64 columns
        // Extract the values from the Arrow data

        // Get the Arrow RecordBatch from MarbleDB record
        ARROW_ASSIGN_OR_RAISE(auto batch, record->ToRecordBatch());
        if (!batch || batch->num_rows() == 0) {
            return arrow::Status::Invalid("Empty record batch");
        }

        // We expect exactly 3 columns (col1, col2, col3)
        if (batch->num_columns() != 3) {
            return arrow::Status::Invalid("Expected 3 columns, got " +
                                         std::to_string(batch->num_columns()));
        }

        // Extract first row (each MarbleDB record contains one triple)
        auto col1_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto col2_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        auto col3_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

        uint64_t col1 = static_cast<uint64_t>(col1_array->Value(0));
        uint64_t col2 = static_cast<uint64_t>(col2_array->Value(0));
        uint64_t col3 = static_cast<uint64_t>(col3_array->Value(0));

        return std::make_tuple(col1, col2, col3);
    }

    // Helper: Check if triple matches pattern (for post-filtering)
    bool MatchesPattern(IndexType index, const TriplePattern& pattern,
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

    // Scan index with pattern using MarbleDB Iterator API
    arrow::Result<std::shared_ptr<arrow::Table>> ScanIndex(
        IndexType index, const TriplePattern& pattern) {

        // Build key range for MarbleDB scan
        marble::KeyRange range = BuildKeyRange(index, pattern);

        // Get column family name for this index
        std::string cf_name = IndexTypeToString(index);

        // Create MarbleDB iterator for the specific column family
        marble::ReadOptions read_opts;
        read_opts.fill_cache = true;  // Cache frequently accessed data

        std::unique_ptr<marble::Iterator> it;
        auto status = db_->NewIterator(cf_name, read_opts, range, &it);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create iterator for " + cf_name +
                                         ": " + status.ToString());
        }

        // Stream results into Arrow builders
        // Batch size: 10K rows for efficient memory usage
        const size_t BATCH_SIZE = 10000;
        arrow::Int64Builder col1_builder;
        arrow::Int64Builder col2_builder;
        arrow::Int64Builder col3_builder;

        // Reserve space for first batch
        ARROW_RETURN_NOT_OK(col1_builder.Reserve(BATCH_SIZE));
        ARROW_RETURN_NOT_OK(col2_builder.Reserve(BATCH_SIZE));
        ARROW_RETURN_NOT_OK(col3_builder.Reserve(BATCH_SIZE));

        size_t rows_scanned = 0;

        // Seek to start of range
        if (range.start()) {
            it->Seek(*range.start());
        } else {
            // Full scan - start from the beginning using minimal key
            auto min_key = marble::TripleKey(0, 0, 0);
            it->Seek(min_key);
        }

        // Iterate through matching records
        while (it->Valid()) {
            // Check if we've exceeded range end
            if (range.end() && range.end_inclusive()) {
                if (it->key()->Compare(*range.end()) > 0) {
                    break;  // Past end of range
                }
            } else if (range.end() && !range.end_inclusive()) {
                if (it->key()->Compare(*range.end()) >= 0) {
                    break;  // At or past end of range
                }
            }

            // Extract (col1, col2, col3) from record
            auto record = it->value();

            // Records in triple store are Arrow RecordBatches with 3 int64 columns
            // Extract values directly from Arrow data
            auto batch_result = ConvertRecordToValues(record);
            if (!batch_result.ok()) {
                return arrow::Status::IOError("Failed to extract values from record");
            }
            auto [col1, col2, col3] = batch_result.ValueOrDie();

            // Apply additional filtering if needed
            // (MarbleDB range scan handles prefix, but we may need exact match on later columns)
            if (MatchesPattern(index, pattern, col1, col2, col3)) {
                ARROW_RETURN_NOT_OK(col1_builder.Append(col1));
                ARROW_RETURN_NOT_OK(col2_builder.Append(col2));
                ARROW_RETURN_NOT_OK(col3_builder.Append(col3));
                rows_scanned++;
            }

            it->Next();
        }

        // Check iterator status
        if (!it->status().ok()) {
            return arrow::Status::IOError("Iterator error: " + it->status().ToString());
        }

        // Finish arrays
        std::shared_ptr<arrow::Array> col1_array;
        std::shared_ptr<arrow::Array> col2_array;
        std::shared_ptr<arrow::Array> col3_array;

        ARROW_RETURN_NOT_OK(col1_builder.Finish(&col1_array));
        ARROW_RETURN_NOT_OK(col2_builder.Finish(&col2_array));
        ARROW_RETURN_NOT_OK(col3_builder.Finish(&col3_array));

        // Create table with columns in index order
        auto index_table = arrow::Table::Make(
            GetIndexSchema(index),
            {col1_array, col2_array, col3_array}
        );

        // Un-permute columns to canonical (subject, predicate, object) order
        // This ensures ProjectResult always sees data in S-P-O order
        ARROW_ASSIGN_OR_RAISE(auto canonical_table, UnpermuteToSPO(index_table, index));

        return canonical_table;
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

    // Helper: Un-permute table columns to canonical SPO order
    // Each index stores data in a different permutation:
    //   SPO: (subject, predicate, object)
    //   POS: (predicate, object, subject)
    //   OSP: (object, subject, predicate)
    // This function converts any permutation back to (subject, predicate, object)
    arrow::Result<std::shared_ptr<arrow::Table>> UnpermuteToSPO(
        const std::shared_ptr<arrow::Table>& table,
        IndexType index) const {

        // SPO schema (canonical ordering)
        auto spo_schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("predicate", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        switch (index) {
            case IndexType::SPO:
                // Already in (subject, predicate, object) order
                return table;

            case IndexType::POS: {
                // Convert from (predicate, object, subject) to (subject, predicate, object)
                // Column mapping: [2, 0, 1] -> [0, 1, 2]
                //   col0 (predicate) -> col1
                //   col1 (object) -> col2
                //   col2 (subject) -> col0
                return arrow::Table::Make(
                    spo_schema,
                    {table->column(2), table->column(0), table->column(1)}
                );
            }

            case IndexType::OSP: {
                // Convert from (object, subject, predicate) to (subject, predicate, object)
                // Column mapping: [1, 2, 0] -> [0, 1, 2]
                //   col0 (object) -> col2
                //   col1 (subject) -> col0
                //   col2 (predicate) -> col1
                return arrow::Table::Make(
                    spo_schema,
                    {table->column(1), table->column(2), table->column(0)}
                );
            }

            default:
                return arrow::Status::Invalid("Unknown index type");
        }
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
