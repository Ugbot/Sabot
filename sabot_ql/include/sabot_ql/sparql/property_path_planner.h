/**
 * Property Path Planner
 *
 * Plans and executes SPARQL 1.1 property path queries using Arrow-native graph operators.
 */

#pragma once

#include <sabot_ql/sparql/ast.h>
#include <sabot_ql/sparql/planner.h>
#include <arrow/api.h>
#include <memory>

namespace sabot_ql {
namespace sparql {

// Forward declaration
struct PlanningContext;

/**
 * Property Path Planning Result
 *
 * Contains the RecordBatch with subject-object bindings and metadata
 */
struct PropertyPathResult {
    std::shared_ptr<arrow::RecordBatch> bindings;  // Columns: (subject, object)
    std::string subject_column = "subject";
    std::string object_column = "object";
};

/**
 * Main property path planner entry point
 *
 * Analyzes a triple pattern with property path predicate and dispatches
 * to appropriate specialized planner.
 *
 * @param pattern The triple pattern containing the property path
 * @param path The property path specification
 * @param ctx Planning context with vocabulary and storage access
 * @return RecordBatch with (subject, object) bindings
 */
arrow::Result<PropertyPathResult> PlanPropertyPath(
    const TriplePattern& pattern,
    const PropertyPath& path,
    PlanningContext& ctx
);

/**
 * Plan transitive path (p+, p*, p{m,n})
 *
 * Uses transitive closure algorithm for quantified paths.
 *
 * Examples:
 *   ?person foaf:knows+ ?friend      → min_dist=1, max_dist=unbounded
 *   ?person foaf:knows* ?friend      → min_dist=0, max_dist=unbounded
 *   ?person foaf:knows{2,5} ?friend  → min_dist=2, max_dist=5
 *
 * @param subject Subject term (variable or bound value)
 * @param predicate_iri The predicate IRI for the path
 * @param object Object term (variable or bound value)
 * @param quantifier Type of quantification (ZeroOrMore, OneOrMore, etc.)
 * @param min_count Minimum path length
 * @param max_count Maximum path length (-1 for unbounded)
 * @param ctx Planning context
 * @return RecordBatch with (subject, object) bindings
 */
arrow::Result<PropertyPathResult> PlanTransitivePath(
    const RDFTerm& subject,
    const IRI& predicate_iri,
    const RDFTerm& object,
    PropertyPathQuantifier quantifier,
    int min_count,
    int max_count,
    PlanningContext& ctx
);

/**
 * Plan sequence path (p/q)
 *
 * Composes two paths by joining on intermediate nodes.
 *
 * Example:
 *   ?person foaf:knows/foaf:name ?name
 *
 * Algorithm:
 *   1. Plan first path: ?person foaf:knows ?intermediate
 *   2. Plan second path: ?intermediate foaf:name ?name
 *   3. Join results on intermediate variable
 *
 * @param subject Subject term
 * @param path_elements Elements of the sequence path
 * @param object Object term
 * @param ctx Planning context
 * @return RecordBatch with (subject, object) bindings
 */
arrow::Result<PropertyPathResult> PlanSequencePath(
    const RDFTerm& subject,
    const std::vector<PropertyPathElement>& path_elements,
    const RDFTerm& object,
    PlanningContext& ctx
);

/**
 * Plan alternative path (p|q)
 *
 * Takes union of multiple alternative paths.
 *
 * Example:
 *   ?person (foaf:knows | foaf:worksWith) ?contact
 *
 * Algorithm:
 *   1. Plan first alternative: ?person foaf:knows ?contact
 *   2. Plan second alternative: ?person foaf:worksWith ?contact
 *   3. Union and deduplicate results
 *
 * @param subject Subject term
 * @param path_elements Alternative path elements
 * @param object Object term
 * @param ctx Planning context
 * @return RecordBatch with (subject, object) bindings
 */
arrow::Result<PropertyPathResult> PlanAlternativePath(
    const RDFTerm& subject,
    const std::vector<PropertyPathElement>& path_elements,
    const RDFTerm& object,
    PlanningContext& ctx
);

/**
 * Plan inverse path (^p)
 *
 * Reverses the direction of traversal by building a reverse CSR graph.
 *
 * Example:
 *   ?person ^foaf:knows ?knower  # who knows person?
 *
 * Algorithm:
 *   1. Load edges for predicate
 *   2. Build reverse CSR (swap subject/object)
 *   3. Traverse from object to subject
 *
 * @param subject Subject term (becomes target in reversed graph)
 * @param predicate_iri The predicate IRI to reverse
 * @param object Object term (becomes source in reversed graph)
 * @param ctx Planning context
 * @return RecordBatch with (subject, object) bindings (in original orientation)
 */
arrow::Result<PropertyPathResult> PlanInversePath(
    const RDFTerm& subject,
    const IRI& predicate_iri,
    const RDFTerm& object,
    PlanningContext& ctx
);

/**
 * Plan negated path (!p)
 *
 * Filters edges to exclude specific predicates.
 *
 * Example:
 *   ?person !foaf:knows ?other  # any relationship except knows
 *
 * Algorithm:
 *   1. Load all edges from subject
 *   2. Filter to exclude negated predicates
 *   3. Return matching (subject, object) pairs
 *
 * @param subject Subject term
 * @param excluded_predicates List of predicate IRIs to exclude
 * @param object Object term
 * @param ctx Planning context
 * @return RecordBatch with (subject, object) bindings
 */
arrow::Result<PropertyPathResult> PlanNegatedPath(
    const RDFTerm& subject,
    const std::vector<IRI>& excluded_predicates,
    const RDFTerm& object,
    PlanningContext& ctx
);

}  // namespace sparql
}  // namespace sabot_ql
