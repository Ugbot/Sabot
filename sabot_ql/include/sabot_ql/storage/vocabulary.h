#pragma once

#include <memory>
#include <string>
#include <optional>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include <marble/db.h>
#include <sabot_ql/types/value_id.h>

namespace sabot_ql {

// RDF Term: Full representation of an RDF term
struct Term {
    std::string lexical;      // Lexical form: "Alice", "http://example.org/alice"
    TermKind kind;            // IRI, Literal, or BlankNode
    std::string language;     // Language tag (for literals): "en", "de"
    std::string datatype;     // Datatype IRI (for literals): "xsd:integer"

    Term() : kind(TermKind::Undefined) {}

    Term(std::string lex, TermKind k,
         std::string lang = "", std::string dtype = "")
        : lexical(std::move(lex)), kind(k),
          language(std::move(lang)), datatype(std::move(dtype)) {}

    // Create IRI term
    static Term IRI(const std::string& iri) {
        return Term(iri, TermKind::IRI);
    }

    // Create Literal term
    static Term Literal(const std::string& value,
                       const std::string& lang = "",
                       const std::string& datatype = "") {
        return Term(value, TermKind::Literal, lang, datatype);
    }

    // Create BlankNode term
    static Term BlankNode(const std::string& id) {
        return Term(id, TermKind::BlankNode);
    }

    // Equality for hashing/comparison
    bool operator==(const Term& other) const {
        return lexical == other.lexical &&
               kind == other.kind &&
               language == other.language &&
               datatype == other.datatype;
    }
};

// Vocabulary: Maps RDF terms (strings) to ValueIds and vice versa
//
// Storage strategy:
// - Column family in MarbleDB with schema:
//   {id: int64, lex: string, kind: uint8, lang: string, datatype: string}
// - In-memory LRU cache for hot terms
// - Unique IDs assigned sequentially
//
// Inline value optimization:
// - Small integers, booleans, common values encoded directly in ValueId
// - Avoids vocabulary lookup for common cases
class Vocabulary {
public:
    virtual ~Vocabulary() = default;

    // Open existing vocabulary
    static arrow::Result<std::shared_ptr<Vocabulary>> Open(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);

    // Create new vocabulary
    static arrow::Result<std::shared_ptr<Vocabulary>> Create(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);

    // Add term to vocabulary, return ValueId
    // If term already exists, returns existing ID
    // If term can be inlined (int, bool), returns inline ValueId
    virtual arrow::Result<ValueId> AddTerm(const Term& term) = 0;

    // Get ValueId for term (if exists)
    // Returns std::nullopt if term not in vocabulary
    // Tries inline encoding first (int, bool)
    virtual arrow::Result<std::optional<ValueId>> GetValueId(
        const Term& term) const = 0;

    // Get term for ValueId
    // Handles both vocabulary IDs and inline values
    virtual arrow::Result<Term> GetTerm(ValueId id) const = 0;

    // Batch operations for efficiency
    virtual arrow::Result<std::vector<ValueId>> AddTerms(
        const std::vector<Term>& terms) = 0;

    virtual arrow::Result<std::vector<Term>> GetTerms(
        const std::vector<ValueId>& ids) const = 0;

    // Get vocabulary size (number of unique terms)
    virtual size_t Size() const = 0;

    // Flush vocabulary to disk
    virtual arrow::Status Flush() = 0;

    // Clear in-memory cache
    virtual void ClearCache() = 0;

    // Export vocabulary as Arrow table (for inspection/backup)
    virtual arrow::Result<std::shared_ptr<arrow::Table>> ToArrowTable() const = 0;
};

// Factory function to create vocabulary implementation
arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabulary(
    const std::string& db_path);

// Utility functions for term parsing/serialization

// Parse N-Triples term to Term struct
// Examples:
//   <http://example.org/alice> → IRI
//   "Alice"@en → Literal with language
//   "42"^^<xsd:integer> → Literal with datatype
//   _:b1 → BlankNode
arrow::Result<Term> ParseNTriplesTerm(const std::string& term_str);

// Serialize Term to N-Triples format
std::string SerializeNTriplesTerm(const Term& term);

// Try to parse literal as inline value (int, double, bool)
std::optional<ValueId> TryParseInline(const std::string& literal,
                                      const std::string& datatype);

} // namespace sabot_ql

// Hash function for Term (for unordered_map)
namespace std {
template <>
struct hash<sabot_ql::Term> {
    size_t operator()(const sabot_ql::Term& term) const {
        size_t h1 = std::hash<std::string>{}(term.lexical);
        size_t h2 = std::hash<uint8_t>{}(static_cast<uint8_t>(term.kind));
        size_t h3 = std::hash<std::string>{}(term.language);
        size_t h4 = std::hash<std::string>{}(term.datatype);
        return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
    }
};
} // namespace std
