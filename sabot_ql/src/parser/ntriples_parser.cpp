#include <sabot_ql/parser/rdf_parser.h>
#include <arrow/api.h>
#include <regex>
#include <cctype>
#include <sstream>

namespace sabot_ql {

// N-Triples parser implementation
// Based on QLever's fast parsing logic

NTriplesParser::NTriplesParser(const std::string& filename,
                               std::shared_ptr<Vocabulary> vocab,
                               const RdfParserConfig& config)
    : vocab_(std::move(vocab)), config_(config) {

    file_.open(filename);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }
}

arrow::Result<std::optional<Triple>> NTriplesParser::ParseTriple() {
    std::string line;

    while (std::getline(file_, line)) {
        line_number_++;

        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Try to parse the line
        auto result = ParseTripleLine(line);
        if (result.ok()) {
            triples_processed_++;
            return result.ValueOrDie();
        } else {
            // Handle parse error
            if (config_.skip_invalid_triples) {
                triples_skipped_++;
                continue;
            } else {
                return result.status();
            }
        }
    }

    // End of file
    return std::nullopt;
}

arrow::Result<std::vector<Triple>> NTriplesParser::ParseBatch(size_t max_triples) {
    std::vector<Triple> batch;
    batch.reserve(max_triples);

    for (size_t i = 0; i < max_triples; i++) {
        ARROW_ASSIGN_OR_RAISE(auto triple_opt, ParseTriple());
        if (!triple_opt.has_value()) {
            break;  // End of file
        }
        batch.push_back(*triple_opt);
    }

    return batch;
}

arrow::Status NTriplesParser::LoadIntoStore(
    std::shared_ptr<TripleStore> store,
    std::shared_ptr<Vocabulary> vocab) {

    size_t batch_size = config_.batch_size;

    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto batch, ParseBatch(batch_size));
        if (batch.empty()) {
            break;  // End of file
        }

        ARROW_RETURN_NOT_OK(store->InsertTriples(batch));
    }

    return arrow::Status::OK();
}

size_t NTriplesParser::GetParsePosition() const {
    return line_number_;
}

// Parse a single N-Triples line
arrow::Result<Triple> NTriplesParser::ParseTripleLine(const std::string& line) {
    const char* ptr = line.c_str();
    SkipWhitespace(ptr);

    Triple triple;

    // Parse subject (IRI or blank node)
    if (*ptr == '<') {
        ARROW_ASSIGN_OR_RAISE(auto iri, ParseIri(ptr));
        Term subject_term = Term::IRI(iri);
        ARROW_ASSIGN_OR_RAISE(triple.subject, vocab_->AddTerm(subject_term));
    } else if (*ptr == '_' && *(ptr+1) == ':') {
        ARROW_ASSIGN_OR_RAISE(auto blank_id, ParseBlankNode(ptr));
        Term blank_term = Term::BlankNode(blank_id);
        ARROW_ASSIGN_OR_RAISE(triple.subject, vocab_->AddTerm(blank_term));
    } else {
        return arrow::Status::Invalid("Expected IRI or blank node for subject at line " +
                                     std::to_string(line_number_));
    }

    SkipWhitespace(ptr);

    // Parse predicate (must be IRI)
    if (*ptr != '<') {
        return arrow::Status::Invalid("Expected IRI for predicate at line " +
                                     std::to_string(line_number_));
    }
    ARROW_ASSIGN_OR_RAISE(auto pred_iri, ParseIri(ptr));
    Term pred_term = Term::IRI(pred_iri);
    ARROW_ASSIGN_OR_RAISE(triple.predicate, vocab_->AddTerm(pred_term));

    SkipWhitespace(ptr);

    // Parse object (IRI, blank node, or literal)
    if (*ptr == '<') {
        ARROW_ASSIGN_OR_RAISE(auto iri, ParseIri(ptr));
        Term object_term = Term::IRI(iri);
        ARROW_ASSIGN_OR_RAISE(triple.object, vocab_->AddTerm(object_term));
    } else if (*ptr == '_' && *(ptr+1) == ':') {
        ARROW_ASSIGN_OR_RAISE(auto blank_id, ParseBlankNode(ptr));
        Term blank_term = Term::BlankNode(blank_id);
        ARROW_ASSIGN_OR_RAISE(triple.object, vocab_->AddTerm(blank_term));
    } else if (*ptr == '"') {
        ARROW_ASSIGN_OR_RAISE(auto literal_term, ParseLiteral(ptr));
        ARROW_ASSIGN_OR_RAISE(triple.object, vocab_->AddTerm(literal_term));
    } else {
        return arrow::Status::Invalid("Expected IRI, blank node, or literal for object at line " +
                                     std::to_string(line_number_));
    }

    SkipWhitespace(ptr);

    // Expect '.' at end
    if (*ptr != '.') {
        return arrow::Status::Invalid("Expected '.' at end of triple at line " +
                                     std::to_string(line_number_));
    }

    return triple;
}

// Parse IRI: <http://example.org/resource>
arrow::Result<std::string> NTriplesParser::ParseIri(const char*& ptr) {
    if (*ptr != '<') {
        return arrow::Status::Invalid("Expected '<' at start of IRI");
    }
    ptr++;  // Skip '<'

    const char* start = ptr;
    while (*ptr && *ptr != '>') {
        ptr++;
    }

    if (*ptr != '>') {
        return arrow::Status::Invalid("Unterminated IRI");
    }

    std::string iri(start, ptr - start);
    ptr++;  // Skip '>'

    return iri;
}

// Parse blank node: _:b1
arrow::Result<std::string> NTriplesParser::ParseBlankNode(const char*& ptr) {
    if (*ptr != '_' || *(ptr+1) != ':') {
        return arrow::Status::Invalid("Expected '_:' at start of blank node");
    }
    ptr += 2;  // Skip '_:'

    const char* start = ptr;
    while (*ptr && (std::isalnum(*ptr) || *ptr == '_' || *ptr == '-')) {
        ptr++;
    }

    if (ptr == start) {
        return arrow::Status::Invalid("Empty blank node identifier");
    }

    return std::string(start, ptr - start);
}

// Parse literal: "value"@lang or "value"^^<datatype>
arrow::Result<Term> NTriplesParser::ParseLiteral(const char*& ptr) {
    if (*ptr != '"') {
        return arrow::Status::Invalid("Expected '\"' at start of literal");
    }
    ptr++;  // Skip opening '"'

    std::string value;

    // Parse literal content (with escape sequences)
    while (*ptr && *ptr != '"') {
        if (*ptr == '\\') {
            ptr++;
            if (!*ptr) {
                return arrow::Status::Invalid("Unterminated escape sequence");
            }

            // Handle escape sequences
            switch (*ptr) {
                case 't': value += '\t'; break;
                case 'n': value += '\n'; break;
                case 'r': value += '\r'; break;
                case '"': value += '"'; break;
                case '\\': value += '\\'; break;
                case 'u': {
                    // Unicode escape \uXXXX
                    ptr++;
                    // TODO: Implement full Unicode escape handling
                    // For now, just skip
                    ptr += 3;
                    break;
                }
                case 'U': {
                    // Unicode escape \UXXXXXXXX
                    ptr++;
                    // TODO: Implement full Unicode escape handling
                    ptr += 7;
                    break;
                }
                default:
                    return arrow::Status::Invalid("Invalid escape sequence");
            }
            ptr++;
        } else {
            value += *ptr;
            ptr++;
        }
    }

    if (*ptr != '"') {
        return arrow::Status::Invalid("Unterminated literal");
    }
    ptr++;  // Skip closing '"'

    std::string language;
    std::string datatype;

    // Check for language tag or datatype
    if (*ptr == '@') {
        ptr++;  // Skip '@'
        const char* start = ptr;
        while (*ptr && (std::isalnum(*ptr) || *ptr == '-')) {
            ptr++;
        }
        language = std::string(start, ptr - start);
    } else if (*ptr == '^' && *(ptr+1) == '^') {
        ptr += 2;  // Skip '^^'
        if (*ptr != '<') {
            return arrow::Status::Invalid("Expected '<' after '^^'");
        }
        ARROW_ASSIGN_OR_RAISE(datatype, ParseIri(ptr));
    }

    return Term::Literal(value, language, datatype);
}

void NTriplesParser::SkipWhitespace(const char*& ptr) {
    while (*ptr && (*ptr == ' ' || *ptr == '\t' || *ptr == '\r')) {
        ptr++;
    }
}

// Factory function
arrow::Result<std::shared_ptr<RdfParser>> RdfParser::Create(
    RdfFormat format,
    const std::string& filename,
    std::shared_ptr<Vocabulary> vocab,
    const RdfParserConfig& config) {

    switch (format) {
        case RdfFormat::NTriples:
            return std::make_shared<NTriplesParser>(filename, vocab, config);
        case RdfFormat::Turtle:
            return arrow::Status::NotImplemented("Turtle parser not yet implemented");
        case RdfFormat::RdfXml:
            return arrow::Status::NotImplemented("RDF/XML parser not yet implemented");
        case RdfFormat::NQuads:
            return arrow::Status::NotImplemented("N-Quads parser not yet implemented");
        case RdfFormat::TrigFormat:
            return arrow::Status::NotImplemented("TriG parser not yet implemented");
        default:
            return arrow::Status::Invalid("Unknown RDF format");
    }
}

// Detect format from filename
RdfFormat DetectFormat(const std::string& filename) {
    if (filename.ends_with(".nt")) {
        return RdfFormat::NTriples;
    } else if (filename.ends_with(".ttl")) {
        return RdfFormat::Turtle;
    } else if (filename.ends_with(".rdf") || filename.ends_with(".xml")) {
        return RdfFormat::RdfXml;
    } else if (filename.ends_with(".nq")) {
        return RdfFormat::NQuads;
    } else if (filename.ends_with(".trig")) {
        return RdfFormat::TrigFormat;
    }

    // Default to N-Triples
    return RdfFormat::NTriples;
}

// Convenience function
arrow::Status LoadRdfFile(
    const std::string& filename,
    std::shared_ptr<TripleStore> store,
    std::shared_ptr<Vocabulary> vocab,
    const RdfParserConfig& config) {

    RdfFormat format = DetectFormat(filename);
    ARROW_ASSIGN_OR_RAISE(auto parser, RdfParser::Create(format, filename, vocab, config));
    ARROW_RETURN_NOT_OK(parser->LoadIntoStore(store, vocab));

    return arrow::Status::OK();
}

} // namespace sabot_ql
