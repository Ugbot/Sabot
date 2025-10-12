#pragma once

#include <memory>
#include <string>
#include <optional>
#include <vector>
#include <arrow/api.h>
#include <arrow/result.h>
#include <marble/db.h>
#include <sabot_ql/types/value_id.h>
#include <sabot_ql/operators/operator.h>  // For TriplePattern definition

namespace sabot_ql {

// RDF Triple: (Subject, Predicate, Object)
// Each component is a ValueId (64-bit encoded value)
struct Triple {
    ValueId subject;
    ValueId predicate;
    ValueId object;

    // Create Arrow schema for triples
    static std::shared_ptr<arrow::Schema> Schema() {
        return arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("predicate", arrow::int64()),
            arrow::field("object", arrow::int64())
        });
    }

    // Convert vector of triples to Arrow RecordBatch
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToArrowBatch(
        const std::vector<Triple>& triples);

    // Convert Arrow RecordBatch to vector of triples
    static arrow::Result<std::vector<Triple>> FromArrowBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch);
};

// TriplePattern is defined in operator.h and included above
// Using uint64_t internally (ValueId is typedef'd to uint64_t)

// Index type for triple storage
// SPO: Subject-Predicate-Object (default)
// POS: Predicate-Object-Subject (for ?x <predicate> ?y)
// OSP: Object-Subject-Predicate (for ?x ?y <object>)
enum class IndexType {
    SPO,
    POS,
    OSP
};

// Triple Store: RDF triple storage backed by MarbleDB
//
// Storage strategy:
// - 3 column families (SPO, POS, OSP) for different access patterns
// - Each triple stored as Arrow RecordBatch {s: int64, p: int64, o: int64}
// - MarbleDB provides LSM tree, sparse indexing, bloom filters
//
// Query strategy:
// - Choose index based on bound variables in pattern
// - Use MarbleDB range scans for efficient lookups
// - Return results as Arrow tables for zero-copy integration
class TripleStore {
public:
    virtual ~TripleStore() = default;

    // Open existing triple store
    static arrow::Result<std::shared_ptr<TripleStore>> Open(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);

    // Create new triple store
    static arrow::Result<std::shared_ptr<TripleStore>> Create(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);

    // Insert triples (batch)
    virtual arrow::Status InsertTriples(const std::vector<Triple>& triples) = 0;

    // Insert from Arrow batch
    virtual arrow::Status InsertArrowBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) = 0;

    // Scan with pattern (? = wildcard)
    // Example: (s, ?, ?) → all triples with subject s
    // Returns Arrow table with columns based on unbound variables
    virtual arrow::Result<std::shared_ptr<arrow::Table>> ScanPattern(
        const TriplePattern& pattern) = 0;

    // Estimate cardinality for query planning
    virtual arrow::Result<size_t> EstimateCardinality(
        const TriplePattern& pattern) = 0;

    // Get statistics for query optimization
    virtual size_t TotalTriples() const = 0;

    // Choose best index for pattern
    virtual IndexType SelectIndex(const TriplePattern& pattern) const = 0;

    // Flush in-memory data to disk
    virtual arrow::Status Flush() = 0;

    // Compact storage
    virtual arrow::Status Compact() = 0;
};

// Factory function to create triple store implementation
arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStore(
    const std::string& db_path);

} // namespace sabot_ql
