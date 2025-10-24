#pragma once

#include <sabot_ql/operators/operator.h>
#include <sabot_ql/storage/triple_store.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <optional>

namespace sabot_ql {

/**
 * @brief Scan operator for SPARQL triple pattern matching
 *
 * This is the leaf operator in SPARQL query execution trees.
 * It wraps TripleStore::ScanPattern() and returns matching triples.
 *
 * The operator uses MarbleDB's generic Iterator API underneath,
 * so the same pattern works for any column family.
 *
 * Example:
 *   // Scan for: ?subject <knows> ?object
 *   TriplePattern pattern{
 *       .subject = std::nullopt,
 *       .predicate = 42,  // ID for <knows>
 *       .object = std::nullopt
 *   };
 *   ScanOperator scan(store, pattern);
 *   auto results = scan.GetAllResults();
 */
class ScanOperator : public Operator {
public:
    /**
     * @brief Construct a scan operator
     * @param store Triple store to scan (uses generic MarbleDB iterators)
     * @param pattern Triple pattern to match
     * @param description Optional human-readable description
     * @param var_bindings Optional SPARQL variable bindings for metadata
     *                     Map from column name (subject/predicate/object) to variable name
     */
    ScanOperator(
        std::shared_ptr<TripleStore> store,
        const TriplePattern& pattern,
        const std::string& description = "",
        const std::unordered_map<std::string, std::string>& var_bindings = {}
    );

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;

    /**
     * @brief Estimate number of results
     * Uses triple store statistics (works with any column family)
     */
    size_t EstimateCardinality() const override;

    /**
     * @brief Get the triple pattern being scanned
     */
    const TriplePattern& GetPattern() const { return pattern_; }

private:
    std::shared_ptr<TripleStore> store_;
    TriplePattern pattern_;
    std::string description_;
    std::unordered_map<std::string, std::string> var_bindings_;
    bool executed_;
};

} // namespace sabot_ql
