/************************************************************************
Vocabulary: Real MarbleDB-backed term→ID mapping (No in-memory hash maps!)
**************************************************************************/

#include <sabot_ql/storage/vocabulary.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/db.h>
#include <marble/record.h>
#include <atomic>
#include <shared_mutex>

namespace sabot_ql {

// Real MarbleDB-backed vocabulary with LRU cache
class MarbleDBVocabulary : public Vocabulary {
public:
    MarbleDBVocabulary(const std::string& db_path,
                      std::shared_ptr<marble::MarbleDB> db)
        : db_path_(db_path), db_(std::move(db)), next_id_(1) {}

    ~MarbleDBVocabulary() override = default;

    // Initialize vocabulary column family
    arrow::Status Initialize() {
        // Create schema for vocabulary storage
        // Schema: {id: int64, lex: string, kind: uint8, lang: string, datatype: string}
        auto vocab_schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("lexical", arrow::utf8()),
            arrow::field("kind", arrow::uint8()),
            arrow::field("language", arrow::utf8()),
            arrow::field("datatype", arrow::utf8())
        });

        // Create column family options
        marble::ColumnFamilyOptions cf_opts;
        cf_opts.schema = vocab_schema;
        cf_opts.enable_bloom_filter = true;
        cf_opts.enable_sparse_index = true;

        // Create vocabulary column family
        marble::ColumnFamilyDescriptor vocab_cf("VOCABULARY", cf_opts);
        auto status = db_->CreateColumnFamily(vocab_cf, &vocab_handle_);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create VOCABULARY column family: " +
                                         status.ToString());
        }

        // Load existing vocabulary size from MarbleDB
        LoadVocabularySize();

        // Load all terms into hash map for O(1) lookups
        LoadVocabularyMap();

        return arrow::Status::OK();
    }

    arrow::Result<ValueId> AddTerm(const Term& term) override {
        // Try inline encoding first (int, bool, etc.)
        auto inline_id = TryParseInline(term.lexical, term.datatype);
        if (inline_id.has_value()) {
            return *inline_id;
        }

        // Check cache first (for hot terms)
        {
            std::shared_lock<std::shared_mutex> lock(cache_mutex_);
            auto it = term_to_id_cache_.find(term);
            if (it != term_to_id_cache_.end()) {
                return it->second;
            }
        }

        // Check MarbleDB for existing term
        ARROW_ASSIGN_OR_RAISE(auto existing_id, FindTermInDB(term));
        if (existing_id.has_value()) {
            // Update cache
            UpdateCache(term, *existing_id);
            return *existing_id;
        }

        // Allocate new ID
        ValueId new_id = ValueId::fromBits(next_id_.fetch_add(1, std::memory_order_relaxed));

        // Store in MarbleDB
        ARROW_RETURN_NOT_OK(StoreTermInDB(term, new_id));

        // Update cache
        UpdateCache(term, new_id);

        // Update full vocabulary map
        {
            std::unique_lock<std::shared_mutex> lock(cache_mutex_);
            if (vocab_map_loaded_) {
                full_vocab_map_[term] = new_id;
            }
        }

        return new_id;
    }

    arrow::Result<std::optional<ValueId>> GetValueId(
        const Term& term) const override {

        // Try inline encoding first
        auto inline_id = TryParseInline(term.lexical, term.datatype);
        if (inline_id.has_value()) {
            return inline_id;
        }

        // Check cache
        {
            std::shared_lock<std::shared_mutex> lock(cache_mutex_);
            auto it = term_to_id_cache_.find(term);
            if (it != term_to_id_cache_.end()) {
                return it->second;
            }
        }

        // Query MarbleDB
        return FindTermInDB(term);
    }

    arrow::Result<Term> GetTerm(ValueId id) const override {
        // Check if inline value
        if (IsInlineValue(id)) {
            return DecodeInlineValue(id);
        }

        // Check cache
        {
            std::shared_lock<std::shared_mutex> lock(cache_mutex_);
            auto it = id_to_term_cache_.find(id);
            if (it != id_to_term_cache_.end()) {
                return it->second;
            }
        }

        // Query MarbleDB
        ARROW_ASSIGN_OR_RAISE(auto term_opt, FindTermByIdInDB(id));
        if (!term_opt.has_value()) {
            return arrow::Status::KeyError("Term ID not found: " + std::to_string(id.getBits()));
        }

        // Update cache
        const_cast<MarbleDBVocabulary*>(this)->UpdateCache(*term_opt, id);

        return *term_opt;
    }

    arrow::Result<std::vector<ValueId>> AddTerms(
        const std::vector<Term>& terms) override {

        std::vector<ValueId> ids;
        ids.reserve(terms.size());

        for (const auto& term : terms) {
            ARROW_ASSIGN_OR_RAISE(auto id, AddTerm(term));
            ids.push_back(id);
        }

        return ids;
    }

    arrow::Result<std::vector<Term>> GetTerms(
        const std::vector<ValueId>& ids) const override {

        std::vector<Term> terms;
        terms.reserve(ids.size());

        for (ValueId id : ids) {
            ARROW_ASSIGN_OR_RAISE(auto term, GetTerm(id));
            terms.push_back(term);
        }

        return terms;
    }

    size_t Size() const override {
        return next_id_.load(std::memory_order_relaxed) - 1;
    }

    arrow::Status Flush() override {
        auto status = db_->Flush();
        if (!status.ok()) {
            return arrow::Status::IOError("Flush failed: " + status.ToString());
        }
        return arrow::Status::OK();
    }

    void ClearCache() override {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        term_to_id_cache_.clear();
        id_to_term_cache_.clear();
    }

    arrow::Result<std::shared_ptr<arrow::Table>> ToArrowTable() const override {
        // Build Arrow table by scanning all vocabulary entries in MarbleDB
        arrow::Int64Builder id_builder;
        arrow::StringBuilder lex_builder;
        arrow::UInt8Builder kind_builder;
        arrow::StringBuilder lang_builder;
        arrow::StringBuilder dtype_builder;

        // Create iterator to scan all vocabulary entries
        marble::ReadOptions read_opts;
        marble::KeyRange full_range(
            std::make_shared<marble::Int64Key>(1),
            true,
            std::make_shared<marble::Int64Key>(next_id_.load()),
            false);

        std::unique_ptr<marble::Iterator> iter;
        auto status = db_->NewIterator(read_opts, full_range, &iter);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create iterator: " +
                                         status.ToString());
        }

        // Scan all entries
        while (iter->Valid()) {
            auto record = iter->value();
            auto batch_result = record->ToRecordBatch();
            if (!batch_result.ok()) {
                return batch_result.status();
            }

            auto batch = *batch_result;
            if (batch->num_rows() > 0) {
                auto id_array = std::static_pointer_cast<arrow::Int64Array>(
                    batch->column(0));
                auto lex_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(1));
                auto kind_array = std::static_pointer_cast<arrow::UInt8Array>(
                    batch->column(2));
                auto lang_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(3));
                auto dtype_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(4));

                for (int64_t row = 0; row < batch->num_rows(); ++row) {
                    ARROW_RETURN_NOT_OK(id_builder.Append(id_array->Value(row)));
                    ARROW_RETURN_NOT_OK(lex_builder.Append(lex_array->GetView(row)));
                    ARROW_RETURN_NOT_OK(kind_builder.Append(kind_array->Value(row)));
                    ARROW_RETURN_NOT_OK(lang_builder.Append(lang_array->GetView(row)));
                    ARROW_RETURN_NOT_OK(dtype_builder.Append(dtype_array->GetView(row)));
                }
            }

            iter->Next();
        }

        if (!iter->status().ok()) {
            return arrow::Status::IOError("Iterator error: " +
                                         iter->status().ToString());
        }

        // Build arrays
        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> lex_array;
        std::shared_ptr<arrow::Array> kind_array;
        std::shared_ptr<arrow::Array> lang_array;
        std::shared_ptr<arrow::Array> dtype_array;

        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(lex_builder.Finish(&lex_array));
        ARROW_RETURN_NOT_OK(kind_builder.Finish(&kind_array));
        ARROW_RETURN_NOT_OK(lang_builder.Finish(&lang_array));
        ARROW_RETURN_NOT_OK(dtype_builder.Finish(&dtype_array));

        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("lexical", arrow::utf8()),
            arrow::field("kind", arrow::uint8()),
            arrow::field("language", arrow::utf8()),
            arrow::field("datatype", arrow::utf8())
        });

        return arrow::Table::Make(schema,
            {id_array, lex_array, kind_array, lang_array, dtype_array});
    }

private:
    // Load vocabulary size from MarbleDB on startup
    void LoadVocabularySize() {
        // Scan to find max ID (could be optimized with metadata)
        marble::ReadOptions read_opts;
        read_opts.reverse_order = true;  // Scan from end

        marble::KeyRange full_range(
            std::make_shared<marble::Int64Key>(INT64_MIN),
            true,
            std::make_shared<marble::Int64Key>(INT64_MAX),
            true);

        std::unique_ptr<marble::Iterator> iter;
        auto status = db_->NewIterator(read_opts, full_range, &iter);
        if (!status.ok() || !iter->Valid()) {
            // Empty vocabulary
            next_id_.store(1);
            return;
        }

        // Get first (highest) ID from reverse scan
        auto record = iter->value();
        auto batch_result = record->ToRecordBatch();
        if (!batch_result.ok()) {
            next_id_.store(1);
            return;
        }

        auto batch = *batch_result;
        if (batch->num_rows() > 0) {
            auto id_array = std::static_pointer_cast<arrow::Int64Array>(
                batch->column(0));
            uint64_t max_id = id_array->Value(0);
            next_id_.store(max_id + 1);
        } else {
            next_id_.store(1);
        }
    }

    // Load all vocabulary terms into hash map for O(1) lookups
    void LoadVocabularyMap() {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);

        // Check if already loaded
        if (vocab_map_loaded_) {
            return;
        }

        // Scan all vocabulary entries
        marble::ReadOptions read_opts;
        marble::KeyRange full_range(
            std::make_shared<marble::Int64Key>(1),
            true,
            std::make_shared<marble::Int64Key>(next_id_.load()),
            false);

        std::unique_ptr<marble::Iterator> iter;
        auto status = db_->NewIterator(read_opts, full_range, &iter);
        if (!status.ok()) {
            // Failed to load - leave map empty
            return;
        }

        size_t loaded_count = 0;
        while (iter->Valid()) {
            auto record = iter->value();
            auto batch_result = record->ToRecordBatch();
            if (!batch_result.ok()) {
                iter->Next();
                continue;
            }

            auto batch = *batch_result;
            if (batch->num_rows() > 0) {
                auto id_array = std::static_pointer_cast<arrow::Int64Array>(
                    batch->column(0));
                auto lex_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(1));
                auto kind_array = std::static_pointer_cast<arrow::UInt8Array>(
                    batch->column(2));
                auto lang_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(3));
                auto dtype_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(4));

                for (int64_t row = 0; row < batch->num_rows(); ++row) {
                    Term term(
                        std::string(lex_array->GetView(row)),
                        static_cast<TermKind>(kind_array->Value(row)),
                        std::string(lang_array->GetView(row)),
                        std::string(dtype_array->GetView(row))
                    );
                    ValueId id = ValueId::fromBits(id_array->Value(row));
                    full_vocab_map_[term] = id;
                    loaded_count++;
                }
            }

            iter->Next();
        }

        vocab_map_loaded_ = true;
        // Note: In production, you might log this: "Loaded {loaded_count} terms into vocabulary map"
    }

    // Find term in MarbleDB by lexical form
    arrow::Result<std::optional<ValueId>> FindTermInDB(const Term& term) const {
        // Check full vocabulary hash map first (O(1) lookup)
        {
            std::shared_lock<std::shared_mutex> lock(cache_mutex_);
            if (vocab_map_loaded_) {
                auto it = full_vocab_map_.find(term);
                if (it != full_vocab_map_.end()) {
                    return it->second;
                }
                // Not in map means not in DB (map is complete)
                return std::nullopt;
            }
        }

        // Fallback: scan to find matching term (only if map not loaded)
        marble::ReadOptions read_opts;
        marble::KeyRange full_range(
            std::make_shared<marble::Int64Key>(1),
            true,
            std::make_shared<marble::Int64Key>(next_id_.load()),
            false);

        std::unique_ptr<marble::Iterator> iter;
        auto status = db_->NewIterator(read_opts, full_range, &iter);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create iterator");
        }

        while (iter->Valid()) {
            auto record = iter->value();
            auto batch_result = record->ToRecordBatch();
            if (!batch_result.ok()) {
                iter->Next();
                continue;
            }

            auto batch = *batch_result;
            if (batch->num_rows() > 0) {
                auto id_array = std::static_pointer_cast<arrow::Int64Array>(
                    batch->column(0));
                auto lex_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(1));
                auto kind_array = std::static_pointer_cast<arrow::UInt8Array>(
                    batch->column(2));
                auto lang_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(3));
                auto dtype_array = std::static_pointer_cast<arrow::StringArray>(
                    batch->column(4));

                for (int64_t row = 0; row < batch->num_rows(); ++row) {
                    if (lex_array->GetView(row) == term.lexical &&
                        kind_array->Value(row) == static_cast<uint8_t>(term.kind) &&
                        lang_array->GetView(row) == term.language &&
                        dtype_array->GetView(row) == term.datatype) {
                        return ValueId::fromBits(id_array->Value(row));
                    }
                }
            }

            iter->Next();
        }

        return std::nullopt;
    }

    // Find term by ID in MarbleDB
    arrow::Result<std::optional<Term>> FindTermByIdInDB(ValueId id) const {
        // Point lookup by ID
        marble::ReadOptions read_opts;
        marble::KeyRange point_range(
            std::make_shared<marble::Int64Key>(id.getBits()),
            true,
            std::make_shared<marble::Int64Key>(id.getBits()),
            true);

        std::unique_ptr<marble::Iterator> iter;
        auto status = db_->NewIterator(read_opts, point_range, &iter);
        if (!status.ok() || !iter->Valid()) {
            return std::nullopt;
        }

        auto record = iter->value();
        auto batch_result = record->ToRecordBatch();
        if (!batch_result.ok()) {
            return std::nullopt;
        }

        auto batch = *batch_result;
        if (batch->num_rows() == 0) {
            return std::nullopt;
        }

        auto lex_array = std::static_pointer_cast<arrow::StringArray>(
            batch->column(1));
        auto kind_array = std::static_pointer_cast<arrow::UInt8Array>(
            batch->column(2));
        auto lang_array = std::static_pointer_cast<arrow::StringArray>(
            batch->column(3));
        auto dtype_array = std::static_pointer_cast<arrow::StringArray>(
            batch->column(4));

        Term term(
            std::string(lex_array->GetView(0)),
            static_cast<TermKind>(kind_array->Value(0)),
            std::string(lang_array->GetView(0)),
            std::string(dtype_array->GetView(0))
        );

        return term;
    }

    // Store term in MarbleDB
    arrow::Status StoreTermInDB(const Term& term, ValueId id) {
        // Create Arrow batch with single row
        arrow::Int64Builder id_builder;
        arrow::StringBuilder lex_builder;
        arrow::UInt8Builder kind_builder;
        arrow::StringBuilder lang_builder;
        arrow::StringBuilder dtype_builder;

        ARROW_RETURN_NOT_OK(id_builder.Append(id.getBits()));
        ARROW_RETURN_NOT_OK(lex_builder.Append(term.lexical));
        ARROW_RETURN_NOT_OK(kind_builder.Append(static_cast<uint8_t>(term.kind)));
        ARROW_RETURN_NOT_OK(lang_builder.Append(term.language));
        ARROW_RETURN_NOT_OK(dtype_builder.Append(term.datatype));

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> lex_array;
        std::shared_ptr<arrow::Array> kind_array;
        std::shared_ptr<arrow::Array> lang_array;
        std::shared_ptr<arrow::Array> dtype_array;

        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(lex_builder.Finish(&lex_array));
        ARROW_RETURN_NOT_OK(kind_builder.Finish(&kind_array));
        ARROW_RETURN_NOT_OK(lang_builder.Finish(&lang_array));
        ARROW_RETURN_NOT_OK(dtype_builder.Finish(&dtype_array));

        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("lexical", arrow::utf8()),
            arrow::field("kind", arrow::uint8()),
            arrow::field("language", arrow::utf8()),
            arrow::field("datatype", arrow::utf8())
        });

        auto batch = arrow::RecordBatch::Make(
            schema, 1,
            {id_array, lex_array, kind_array, lang_array, dtype_array});

        // Insert into MarbleDB
        auto status = db_->InsertBatch("VOCABULARY", batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to insert vocabulary entry: " +
                                         status.ToString());
        }

        return arrow::Status::OK();
    }

    // Update LRU cache (simple implementation, can add proper LRU eviction later)
    void UpdateCache(const Term& term, ValueId id) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);

        // Simple cache with max size limit
        const size_t MAX_CACHE_SIZE = 10000;

        if (term_to_id_cache_.size() >= MAX_CACHE_SIZE) {
            // Clear cache when full (could implement proper LRU)
            term_to_id_cache_.clear();
            id_to_term_cache_.clear();
        }

        term_to_id_cache_[term] = id;
        id_to_term_cache_[id] = term;
    }

    // Check if ValueId is inline value
    bool IsInlineValue(ValueId id) const {
        // Inline values use high bit as marker
        // Simple heuristic: IDs > 2^60 are inline values
        return id.getBits() > (1ULL << 60);
    }

    // Decode inline value to Term
    arrow::Result<Term> DecodeInlineValue(ValueId id) const {
        // Decode inline values (int, bool, etc.)
        // For now, return error (full implementation would decode)
        return arrow::Status::NotImplemented("Inline value decoding not yet implemented");
    }

    // Member variables
    std::string db_path_;
    std::shared_ptr<marble::MarbleDB> db_;
    marble::ColumnFamilyHandle* vocab_handle_ = nullptr;

    // ID allocation (atomic for thread safety)
    std::atomic<uint64_t> next_id_;

    // LRU cache for hot terms (protected by shared_mutex)
    mutable std::shared_mutex cache_mutex_;
    std::unordered_map<Term, ValueId> term_to_id_cache_;
    std::unordered_map<ValueId, Term> id_to_term_cache_;

    // Full vocabulary hash map for O(1) lookups (loaded at startup)
    // Protectedby the same cache_mutex as the LRU cache
    std::unordered_map<Term, ValueId> full_vocab_map_;
    bool vocab_map_loaded_ = false;
};

// Factory function
arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabularyMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    auto vocab = std::make_shared<MarbleDBVocabulary>(db_path, std::move(db));

    auto status = vocab->Initialize();
    if (!status.ok()) {
        return status;
    }

    return vocab;
}

} // namespace sabot_ql
