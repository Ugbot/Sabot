#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/util/lru_cache.h>
#include <sabot_ql/util/hash_map.h>
#include <arrow/api.h>
#include <marble/db.h>
#include <shared_mutex>
#include <regex>

namespace sabot_ql {

// Thread-safe wrapper around LRUCache
template <typename K, typename V>
class ThreadSafeLRUCache {
public:
    explicit ThreadSafeLRUCache(size_t capacity) : cache_(capacity) {}

    std::optional<V> Get(const K& key) const {
        std::shared_lock lock(mutex_);
        return cache_.Get(key);
    }

    void Put(const K& key, const V& value) {
        std::unique_lock lock(mutex_);
        cache_.Put(key, value);
    }

    void Clear() {
        std::unique_lock lock(mutex_);
        cache_.Clear();
    }

private:
    mutable LRUCache<K, V> cache_;
    mutable std::shared_mutex mutex_;
};

// VocabularyImpl: MarbleDB-backed term dictionary with LRU cache
class VocabularyImpl : public Vocabulary {
public:
    VocabularyImpl(const std::string& db_path,
                   std::shared_ptr<marble::MarbleDB> db)
        : db_path_(db_path), db_(std::move(db)),
          term_to_id_cache_(10000),  // Cache 10K hot terms
          id_to_term_cache_(10000) {}

    ~VocabularyImpl() override = default;

    arrow::Status Initialize() {
        // Create column family for vocabulary
        marble::ColumnFamilyDescriptor vocab_cf;
        vocab_cf.name = "vocabulary";
        auto status = db_->CreateColumnFamily(vocab_cf, &vocab_handle_);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to create vocabulary column family: " +
                                         status.ToString());
        }

        // Load next_id from metadata (or start at 0)
        next_id_ = 0;

        return arrow::Status::OK();
    }

    arrow::Result<ValueId> AddTerm(const Term& term) override {
        // Try inline encoding first
        auto inline_id = TryEncodeInline(term);
        if (inline_id.has_value()) {
            return *inline_id;
        }

        // Check cache
        auto cached = term_to_id_cache_.Get(term);
        if (cached.has_value()) {
            return *cached;
        }

        // Check database
        ARROW_ASSIGN_OR_RAISE(auto existing_id, LookupTermInDB(term));
        if (existing_id.has_value()) {
            term_to_id_cache_.Put(term, *existing_id);
            return *existing_id;
        }

        // Add new term
        ValueId new_id = AllocateId(term.kind);
        ARROW_RETURN_NOT_OK(StoreTermInDB(new_id, term));

        // Update caches
        term_to_id_cache_.Put(term, new_id);
        id_to_term_cache_.Put(new_id, term);

        return new_id;
    }

    arrow::Result<std::optional<ValueId>> GetValueId(
        const Term& term) const override {

        // Try inline encoding
        auto inline_id = TryEncodeInline(term);
        if (inline_id.has_value()) {
            return inline_id;
        }

        // Check cache
        auto cached = term_to_id_cache_.Get(term);
        if (cached.has_value()) {
            return cached;
        }

        // Check database
        return LookupTermInDB(term);
    }

    arrow::Result<Term> GetTerm(ValueId id) const override {
        // Check if inline value
        if (auto decoded = TryDecodeInline(id); decoded.has_value()) {
            return *decoded;
        }

        // Check cache
        auto cached = id_to_term_cache_.Get(id);
        if (cached.has_value()) {
            return *cached;
        }

        // Check database
        ARROW_ASSIGN_OR_RAISE(auto term, LookupIdInDB(id));
        if (term.has_value()) {
            id_to_term_cache_.Put(id, *term);
            return *term;
        }

        return arrow::Status::KeyError("ValueId not found: " + std::to_string(id));
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

        for (auto id : ids) {
            ARROW_ASSIGN_OR_RAISE(auto term, GetTerm(id));
            terms.push_back(term);
        }

        return terms;
    }

    size_t Size() const override {
        return next_id_;
    }

    arrow::Status Flush() override {
        // MarbleDB handles flushing
        return arrow::Status::OK();
    }

    void ClearCache() override {
        term_to_id_cache_.Clear();
        id_to_term_cache_.Clear();
    }

    arrow::Result<std::shared_ptr<arrow::Table>> ToArrowTable() const override {
        // Export vocabulary as Arrow table for inspection
        // Schema: {id: int64, lex: string, kind: uint8, lang: string, datatype: string}

        arrow::Int64Builder id_builder;
        arrow::StringBuilder lex_builder;
        arrow::UInt8Builder kind_builder;
        arrow::StringBuilder lang_builder;
        arrow::StringBuilder dtype_builder;

        // TODO: Scan all vocabulary entries from MarbleDB
        // For now, return empty table

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

        return arrow::Table::Make(schema, {id_array, lex_array, kind_array,
                                          lang_array, dtype_array});
    }

private:
    // Try to encode term as inline value
    std::optional<ValueId> TryEncodeInline(const Term& term) const {
        if (term.kind != TermKind::Literal) {
            return std::nullopt;
        }

        // Try integer
        if (term.datatype == "http://www.w3.org/2001/XMLSchema#integer" ||
            term.datatype == "xsd:integer" || term.datatype.empty()) {
            try {
                int64_t value = std::stoll(term.lexical);
                // Only inline if fits in 60 bits
                if (value >= -(1LL << 59) && value < (1LL << 59)) {
                    return value_id::EncodeInt(value);
                }
            } catch (...) {
                // Not an integer
            }
        }

        // Try boolean
        if (term.datatype == "http://www.w3.org/2001/XMLSchema#boolean" ||
            term.datatype == "xsd:boolean") {
            if (term.lexical == "true" || term.lexical == "1") {
                return value_id::EncodeBool(true);
            } else if (term.lexical == "false" || term.lexical == "0") {
                return value_id::EncodeBool(false);
            }
        }

        // Try double (with precision loss)
        if (term.datatype == "http://www.w3.org/2001/XMLSchema#double" ||
            term.datatype == "xsd:double") {
            try {
                double value = std::stod(term.lexical);
                return value_id::EncodeDouble(value);
            } catch (...) {
                // Not a double
            }
        }

        return std::nullopt;
    }

    // Try to decode inline value
    std::optional<Term> TryDecodeInline(ValueId id) const {
        auto kind = value_id::GetType(id);

        if (kind == TermKind::Int) {
            if (auto value = value_id::DecodeInt(id); value.has_value()) {
                return Term::Literal(std::to_string(*value), "",
                                    "http://www.w3.org/2001/XMLSchema#integer");
            }
        } else if (kind == TermKind::Bool) {
            if (auto value = value_id::DecodeBool(id); value.has_value()) {
                return Term::Literal(*value ? "true" : "false", "",
                                    "http://www.w3.org/2001/XMLSchema#boolean");
            }
        } else if (kind == TermKind::Double) {
            if (auto value = value_id::DecodeDouble(id); value.has_value()) {
                return Term::Literal(std::to_string(*value), "",
                                    "http://www.w3.org/2001/XMLSchema#double");
            }
        }

        return std::nullopt;
    }

    // Allocate new ID for term
    ValueId AllocateId(TermKind kind) {
        uint64_t index = next_id_++;
        return value_id::EncodeVocabIndex(index, kind);
    }

    // Lookup term in database
    arrow::Result<std::optional<ValueId>> LookupTermInDB(const Term& term) const {
        // TODO: Implement MarbleDB lookup
        // Key: hash(lexical + kind + language + datatype)
        // Value: ValueId

        return std::optional<ValueId>(std::nullopt);
    }

    // Lookup ID in database
    arrow::Result<std::optional<Term>> LookupIdInDB(ValueId id) const {
        // TODO: Implement MarbleDB lookup
        // Key: ValueId
        // Value: Term (lexical, kind, language, datatype)

        return std::optional<Term>(std::nullopt);
    }

    // Store term in database
    arrow::Status StoreTermInDB(ValueId id, const Term& term) {
        // TODO: Implement MarbleDB storage
        // Store bidirectional mappings:
        // 1. hash(term) → id
        // 2. id → term

        return arrow::Status::OK();
    }

    std::string db_path_;
    std::shared_ptr<marble::MarbleDB> db_;
    marble::ColumnFamilyHandle* vocab_handle_ = nullptr;

    // Thread-safe LRU caches
    mutable ThreadSafeLRUCache<Term, ValueId> term_to_id_cache_;
    mutable ThreadSafeLRUCache<ValueId, Term> id_to_term_cache_;

    // Next vocabulary ID
    std::atomic<uint64_t> next_id_{0};
};

// Factory functions

arrow::Result<std::shared_ptr<Vocabulary>> Vocabulary::Open(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    auto impl = std::make_shared<VocabularyImpl>(db_path, std::move(db));
    ARROW_RETURN_NOT_OK(impl->Initialize());
    return impl;
}

arrow::Result<std::shared_ptr<Vocabulary>> Vocabulary::Create(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {

    return Open(db_path, std::move(db));
}

arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabulary(
    const std::string& db_path) {

    marble::DBOptions options;
    options.db_path = db_path;
    options.enable_sparse_index = true;
    options.enable_bloom_filter = true;

    auto db_result = marble::MarbleDB::Open(options);
    if (!db_result.ok()) {
        return arrow::Status::IOError("Failed to open MarbleDB: " +
                                     db_result.status().ToString());
    }

    return Vocabulary::Create(db_path, db_result.value());
}

// Utility functions for N-Triples parsing

arrow::Result<Term> ParseNTriplesTerm(const std::string& term_str) {
    // IRI: <http://example.org/alice>
    if (term_str.size() >= 2 && term_str[0] == '<' && term_str.back() == '>') {
        std::string iri = term_str.substr(1, term_str.size() - 2);
        return Term::IRI(iri);
    }

    // Blank node: _:b1
    if (term_str.size() >= 3 && term_str[0] == '_' && term_str[1] == ':') {
        std::string id = term_str.substr(2);
        return Term::BlankNode(id);
    }

    // Literal: "Alice"@en or "42"^^<xsd:integer>
    if (term_str.size() >= 2 && term_str[0] == '"') {
        // Find closing quote
        size_t end_quote = term_str.find('"', 1);
        if (end_quote == std::string::npos) {
            return arrow::Status::Invalid("Unterminated literal: " + term_str);
        }

        std::string lexical = term_str.substr(1, end_quote - 1);
        std::string language;
        std::string datatype;

        // Check for language tag: @en
        if (end_quote + 1 < term_str.size() && term_str[end_quote + 1] == '@') {
            size_t lang_start = end_quote + 2;
            language = term_str.substr(lang_start);
        }
        // Check for datatype: ^^<xsd:integer>
        else if (end_quote + 2 < term_str.size() && term_str.substr(end_quote + 1, 2) == "^^") {
            size_t dtype_start = end_quote + 3;
            if (dtype_start < term_str.size() && term_str[dtype_start] == '<') {
                size_t dtype_end = term_str.find('>', dtype_start);
                if (dtype_end != std::string::npos) {
                    datatype = term_str.substr(dtype_start + 1, dtype_end - dtype_start - 1);
                }
            }
        }

        return Term::Literal(lexical, language, datatype);
    }

    return arrow::Status::Invalid("Invalid N-Triples term: " + term_str);
}

std::string SerializeNTriplesTerm(const Term& term) {
    switch (term.kind) {
        case TermKind::IRI:
            return "<" + term.lexical + ">";

        case TermKind::BlankNode:
            return "_:" + term.lexical;

        case TermKind::Literal: {
            std::string result = "\"" + term.lexical + "\"";
            if (!term.language.empty()) {
                result += "@" + term.language;
            } else if (!term.datatype.empty()) {
                result += "^^<" + term.datatype + ">";
            }
            return result;
        }

        default:
            return "";
    }
}

std::optional<ValueId> TryParseInline(const std::string& literal,
                                      const std::string& datatype) {
    // Try integer
    if (datatype == "http://www.w3.org/2001/XMLSchema#integer" ||
        datatype == "xsd:integer" || datatype.empty()) {
        try {
            int64_t value = std::stoll(literal);
            if (value >= -(1LL << 59) && value < (1LL << 59)) {
                return value_id::EncodeInt(value);
            }
        } catch (...) {}
    }

    // Try boolean
    if (datatype == "http://www.w3.org/2001/XMLSchema#boolean" ||
        datatype == "xsd:boolean") {
        if (literal == "true" || literal == "1") {
            return value_id::EncodeBool(true);
        } else if (literal == "false" || literal == "0") {
            return value_id::EncodeBool(false);
        }
    }

    // Try double
    if (datatype == "http://www.w3.org/2001/XMLSchema#double" ||
        datatype == "xsd:double") {
        try {
            double value = std::stod(literal);
            return value_id::EncodeDouble(value);
        } catch (...) {}
    }

    return std::nullopt;
}

} // namespace sabot_ql
