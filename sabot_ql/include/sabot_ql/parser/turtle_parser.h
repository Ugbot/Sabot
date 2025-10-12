#pragma once

// Turtle parser wrapper around QLever's fast Turtle parser
// We directly use QLever's proven implementation for full Turtle support

#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <arrow/api.h>
#include <arrow/result.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/parser/rdf_parser.h>

// Forward declare QLever types
namespace qlever {
class TurtleTriple;
}

namespace ad_utility {
namespace triple_component {
class Iri;
}
}

class RdfParserBase;
template <typename T>
class TurtleParser;
class Tokenizer;
class EncodedIriManager;

namespace sabot_ql {

// Turtle parser using QLever's proven implementation
// Supports full Turtle syntax:
// - Prefixes: @prefix, PREFIX
// - Base IRIs: @base, BASE
// - Blank nodes: _:b1, [ ... ], ( ... )
// - Collections: ( item1 item2 )
// - Property lists: subject predicate1 object1 ; predicate2 object2 .
// - Object lists: subject predicate object1 , object2 .
// - Special predicate 'a' for rdf:type
// - Literals with language tags and datatypes
// - Numeric literals (integer, decimal, double)
// - Boolean literals
// - Multiline string literals (""" ... """, ''' ... ''')
// - Unicode escapes
// - Comment handling
class TurtleParserWrapper : public RdfParser {
public:
    TurtleParserWrapper(const std::string& filename,
                        std::shared_ptr<Vocabulary> vocab,
                        const RdfParserConfig& config);

    ~TurtleParserWrapper() override;

    arrow::Result<std::optional<Triple>> ParseTriple() override;
    arrow::Result<std::vector<Triple>> ParseBatch(size_t max_triples) override;
    arrow::Status LoadIntoStore(
        std::shared_ptr<TripleStore> store,
        std::shared_ptr<Vocabulary> vocab) override;

    size_t GetParsePosition() const override;
    size_t GetTriplesProcessed() const override { return triples_processed_; }
    size_t GetTriplesSkipped() const override { return triples_skipped_; }

private:
    // Convert QLever's TripleComponent to our Term
    arrow::Result<Term> ConvertTripleComponent(
        const qlever::TurtleTriple& triple,
        int component_idx);

    // Opaque pointer to QLever parser (PIMPL pattern)
    struct Impl;
    std::unique_ptr<Impl> impl_;

    std::shared_ptr<Vocabulary> vocab_;
    RdfParserConfig config_;

    size_t triples_processed_ = 0;
    size_t triples_skipped_ = 0;
};

} // namespace sabot_ql
