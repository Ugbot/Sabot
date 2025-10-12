#pragma once

#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <arrow/api.h>
#include <arrow/result.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>

namespace sabot_ql {

// RDF format types
enum class RdfFormat {
    NTriples,   // N-Triples (.nt)
    Turtle,     // Turtle (.ttl)
    RdfXml,     // RDF/XML (.rdf, .xml)
    NQuads,     // N-Quads (.nq) - for named graphs
    TrigFormat  // TriG (.trig) - for named graphs
};

// RDF parser configuration
struct RdfParserConfig {
    // Buffer size for batch loading
    size_t batch_size = 100000;  // 100K triples per batch

    // Skip invalid triples instead of throwing errors
    bool skip_invalid_triples = false;

    // Integer overflow handling
    enum class IntegerOverflowBehavior {
        Error,                  // Throw error
        OverflowToDouble,      // Convert to double on overflow
        AllToDouble            // Always use double for numeric literals
    };
    IntegerOverflowBehavior integer_overflow = IntegerOverflowBehavior::Error;

    // Number of parallel parsing threads (0 = sequential)
    size_t num_threads = 4;
};

// Base class for RDF parsers
class RdfParser {
public:
    virtual ~RdfParser() = default;

    // Create parser for specific format
    static arrow::Result<std::shared_ptr<RdfParser>> Create(
        RdfFormat format,
        const std::string& filename,
        std::shared_ptr<Vocabulary> vocab,
        const RdfParserConfig& config = {});

    // Parse single triple from stream
    // Returns std::nullopt when end of file reached
    virtual arrow::Result<std::optional<Triple>> ParseTriple() = 0;

    // Parse batch of triples
    // Returns empty vector when end of file reached
    virtual arrow::Result<std::vector<Triple>> ParseBatch(
        size_t max_triples = 100000) = 0;

    // Load entire file into triple store
    virtual arrow::Status LoadIntoStore(
        std::shared_ptr<TripleStore> store,
        std::shared_ptr<Vocabulary> vocab) = 0;

    // Get current parse position (for error reporting)
    virtual size_t GetParsePosition() const = 0;

    // Get statistics
    virtual size_t GetTriplesProcessed() const = 0;
    virtual size_t GetTriplesSkipped() const = 0;
};

// N-Triples parser (simplest format)
// Format: <subject> <predicate> <object> .
// Example: <http://example.org/alice> <http://schema.org/name> "Alice"@en .
class NTriplesParser : public RdfParser {
public:
    NTriplesParser(const std::string& filename,
                   std::shared_ptr<Vocabulary> vocab,
                   const RdfParserConfig& config);

    arrow::Result<std::optional<Triple>> ParseTriple() override;
    arrow::Result<std::vector<Triple>> ParseBatch(size_t max_triples) override;
    arrow::Status LoadIntoStore(
        std::shared_ptr<TripleStore> store,
        std::shared_ptr<Vocabulary> vocab) override;

    size_t GetParsePosition() const override;
    size_t GetTriplesProcessed() const override { return triples_processed_; }
    size_t GetTriplesSkipped() const override { return triples_skipped_; }

private:
    arrow::Result<Triple> ParseTripleLine(const std::string& line);
    arrow::Result<std::string> ParseIri(const char*& ptr);
    arrow::Result<std::string> ParseBlankNode(const char*& ptr);
    arrow::Result<Term> ParseLiteral(const char*& ptr);
    void SkipWhitespace(const char*& ptr);

    std::ifstream file_;
    std::shared_ptr<Vocabulary> vocab_;
    RdfParserConfig config_;

    size_t line_number_ = 0;
    size_t triples_processed_ = 0;
    size_t triples_skipped_ = 0;
};

// Utility functions

// Detect RDF format from file extension
RdfFormat DetectFormat(const std::string& filename);

// Parse RDF file and load into triple store (convenience function)
arrow::Status LoadRdfFile(
    const std::string& filename,
    std::shared_ptr<TripleStore> store,
    std::shared_ptr<Vocabulary> vocab,
    const RdfParserConfig& config = {});

} // namespace sabot_ql
