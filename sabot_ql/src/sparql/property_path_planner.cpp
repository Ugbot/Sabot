/**
 * Property Path Planner Implementation
 *
 * Plans and executes SPARQL 1.1 property path queries using Arrow-native graph operators.
 * Implements property path expansion approach: converts SPARQL paths into graph operations.
 */

#include <sabot_ql/sparql/property_path_planner.h>
#include <sabot_ql/graph/csr_graph.h>
#include <sabot_ql/graph/transitive_closure.h>
#include <sabot_ql/graph/property_paths.h>
#include <arrow/compute/api.h>
#include <arrow/table.h>
#include <unordered_set>
#include <iostream>

namespace sabot_ql {
namespace sparql {

namespace {

/**
 * Extract all bound values for a term, or return all values if unbound
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> GetStartNodes(
    const RDFTerm& term,
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::string& column_name,
    PlanningContext& ctx) {

    // If term is a variable, return all unique values from the column
    if (std::holds_alternative<Variable>(term)) {
        auto column = edges->GetColumnByName(column_name);
        if (!column) {
            return arrow::Status::Invalid("Column not found: " + column_name);
        }

        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(column);

        // Get unique values using hash set
        std::unordered_set<int64_t> unique_values;
        for (int64_t i = 0; i < int64_array->length(); ++i) {
            if (int64_array->IsValid(i)) {
                unique_values.insert(int64_array->Value(i));
            }
        }

        // Build result array
        arrow::Int64Builder builder;
        ARROW_RETURN_NOT_OK(builder.Reserve(unique_values.size()));
        for (int64_t val : unique_values) {
            ARROW_RETURN_NOT_OK(builder.Append(val));
        }

        std::shared_ptr<arrow::Int64Array> result;
        ARROW_RETURN_NOT_OK(builder.Finish(&result));
        return result;
    }

    // Term is bound - convert to ValueId
    // First convert RDFTerm to storage Term
    Term storage_term;
    if (auto* iri = std::get_if<IRI>(&term)) {
        storage_term = Term::IRI(iri->iri);
    } else if (auto* lit = std::get_if<Literal>(&term)) {
        storage_term = Term::Literal(lit->value, lit->language, lit->datatype);
    } else if (auto* bnode = std::get_if<BlankNode>(&term)) {
        storage_term = Term::BlankNode(bnode->id);
    } else {
        return arrow::Status::Invalid("Invalid term type");
    }

    ARROW_ASSIGN_OR_RAISE(auto value_id_opt, ctx.vocab->GetValueId(storage_term));
    if (!value_id_opt.has_value()) {
        // Term not in vocabulary - return empty array
        arrow::Int64Builder builder;
        std::shared_ptr<arrow::Int64Array> result;
        ARROW_RETURN_NOT_OK(builder.Finish(&result));
        return result;
    }

    // Return single-element array with the bound value
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(value_id_opt->getBits()));
    std::shared_ptr<arrow::Int64Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

/**
 * Extract IRI from PropertyPathElement (must be a simple term, not a recursive path)
 */
arrow::Result<IRI> ExtractIRI(const PropertyPathElement& element) {
    // Try simple case: element is an RDFTerm
    if (auto* term_ptr = std::get_if<RDFTerm>(&element.element)) {
        auto* iri_ptr = std::get_if<IRI>(term_ptr);
        if (!iri_ptr) {
            return arrow::Status::Invalid("Property path element is not an IRI");
        }
        return *iri_ptr;
    }

    // Nested case: element is a PropertyPath (for inverse/alternative paths)
    if (auto* path_ptr = std::get_if<std::shared_ptr<PropertyPath>>(&element.element)) {
        // For inverse paths like ^p, the nested path contains the actual predicate
        if ((*path_ptr)->elements.size() == 1) {
            // Recursively extract IRI from nested element
            return ExtractIRI((*path_ptr)->elements[0]);
        } else {
            return arrow::Status::Invalid("Cannot extract single IRI from multi-element nested path");
        }
    }

    return arrow::Status::Invalid("Property path element is neither a term nor a path");
}

} // anonymous namespace

/**
 * Main property path planner entry point
 *
 * Analyzes property path structure and dispatches to appropriate specialized planner.
 */
arrow::Result<PropertyPathResult> PlanPropertyPath(
    const TriplePattern& pattern,
    const PropertyPath& path,
    PlanningContext& ctx) {

    // Simple case: Single element path
    if (path.elements.size() == 1) {
        const auto& element = path.elements[0];

        // Extract the predicate IRI
        ARROW_ASSIGN_OR_RAISE(auto predicate_iri, ExtractIRI(element));

        // Handle quantifiers (p+, p*, p{n,m})
        if (element.quantifier != PropertyPathQuantifier::None) {
            int min_count = element.min_count;
            int max_count = element.max_count;

            // Set defaults based on quantifier type
            switch (element.quantifier) {
                case PropertyPathQuantifier::ZeroOrMore:
                    min_count = 0;
                    max_count = -1;  // unbounded
                    break;
                case PropertyPathQuantifier::OneOrMore:
                    min_count = 1;
                    max_count = -1;  // unbounded
                    break;
                case PropertyPathQuantifier::ZeroOrOne:
                    min_count = 0;
                    max_count = 1;
                    break;
                case PropertyPathQuantifier::ExactCount:
                    max_count = min_count;
                    break;
                case PropertyPathQuantifier::MinCount:
                    max_count = -1;  // unbounded
                    break;
                case PropertyPathQuantifier::RangeCount:
                    // Use provided min_count and max_count
                    break;
                default:
                    break;
            }

            return PlanTransitivePath(
                pattern.subject,
                predicate_iri,
                pattern.object,
                element.quantifier,
                min_count,
                max_count,
                ctx
            );
        }

        // Handle inverse path (^p)
        if (path.modifier == PropertyPathModifier::Inverse) {
            return PlanInversePath(pattern.subject, predicate_iri, pattern.object, ctx);
        }

        // Handle negated path (!p)
        if (path.modifier == PropertyPathModifier::Negated) {
            return PlanNegatedPath(pattern.subject, {predicate_iri}, pattern.object, ctx);
        }

        // Simple path without quantifiers - just a regular triple pattern
        // This case should not normally reach here, as simple predicates are handled by PlanTriplePattern
        return arrow::Status::NotImplemented("Simple path without quantifiers should be handled by PlanTriplePattern");
    }

    // Multiple elements: Handle sequence (p/q) or alternative (p|q)
    switch (path.modifier) {
        case PropertyPathModifier::Sequence:
            return PlanSequencePath(pattern.subject, path.elements, pattern.object, ctx);

        case PropertyPathModifier::Alternative:
            return PlanAlternativePath(pattern.subject, path.elements, pattern.object, ctx);

        case PropertyPathModifier::Negated: {
            // Negated property set: extract all IRIs
            std::vector<IRI> excluded_predicates;
            for (const auto& elem : path.elements) {
                ARROW_ASSIGN_OR_RAISE(auto iri, ExtractIRI(elem));
                excluded_predicates.push_back(iri);
            }
            return PlanNegatedPath(pattern.subject, excluded_predicates, pattern.object, ctx);
        }

        default:
            return arrow::Status::NotImplemented("Property path modifier not yet implemented");
    }
}

/**
 * Plan transitive path (p+, p*, p{m,n})
 *
 * Uses transitive closure algorithm for quantified paths.
 *
 * Examples:
 *   ?person foaf:knows+ ?friend      → min_dist=1, max_dist=unbounded
 *   ?person foaf:knows* ?friend      → min_dist=0, max_dist=unbounded
 *   ?person foaf:knows{2,5} ?friend  → min_dist=2, max_dist=5
 */
arrow::Result<PropertyPathResult> PlanTransitivePath(
    const RDFTerm& subject,
    const IRI& predicate_iri,
    const RDFTerm& object,
    PropertyPathQuantifier quantifier,
    int min_count,
    int max_count,
    PlanningContext& ctx) {

    std::cerr << "[DEBUG PlanTransitivePath] Predicate IRI: " << predicate_iri.iri << "\n";

    // 1. Resolve predicate IRI to value ID
    Term pred_term = Term::IRI(predicate_iri.iri);
    ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
    if (!pred_id_opt.has_value()) {
        std::cerr << "[DEBUG PlanTransitivePath] Predicate NOT found in vocabulary\n";
        // Predicate not in vocabulary - return empty result
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        arrow::Int64Builder subject_builder;
        arrow::Int64Builder object_builder;

        std::shared_ptr<arrow::Int64Array> subject_array;
        std::shared_ptr<arrow::Int64Array> object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));

        auto result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
        return PropertyPathResult{result, "subject", "object"};
    }

    std::cerr << "[DEBUG PlanTransitivePath] Predicate found in vocabulary, ID: " << pred_id_opt->getBits() << "\n";

    // 2. Load edges for this predicate from triple store
    // Create pattern: (?, predicate, ?) to get all edges with this predicate
    sabot_ql::TriplePattern pattern;
    pattern.subject = std::nullopt;  // unbound
    pattern.predicate = pred_id_opt->getBits();  // bound to this predicate
    pattern.object = std::nullopt;  // unbound

    std::cerr << "[DEBUG PlanTransitivePath] Calling ScanPattern with predicate ID: " << pred_id_opt->getBits() << "\n";

    ARROW_ASSIGN_OR_RAISE(auto edges_table, ctx.store->ScanPattern(pattern));

    std::cerr << "[DEBUG PlanTransitivePath] ScanPattern returned table with " << edges_table->num_rows() << " rows\n";

    // Convert Table to RecordBatch (combine all chunks)
    ARROW_ASSIGN_OR_RAISE(auto edges_combined, edges_table->CombineChunksToBatch());
    auto edges = edges_combined;

    std::cerr << "[DEBUG PlanTransitivePath] Combined batch has " << edges->num_rows() << " rows\n";

    if (edges->num_rows() == 0) {
        std::cerr << "[DEBUG PlanTransitivePath] No edges found for predicate, returning empty result\n";
        // No edges for this predicate - return empty result
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        arrow::Int64Builder subject_builder;
        arrow::Int64Builder object_builder;

        std::shared_ptr<arrow::Int64Array> subject_array;
        std::shared_ptr<arrow::Int64Array> object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));

        auto result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
        return PropertyPathResult{result, "subject", "object"};
    }

    // 2.5. Remap vocabulary IDs to dense node IDs (0, 1, 2, ...)
    // CSR graphs use node IDs as array indices, so sparse IDs cause huge allocations
    std::cerr << "[DEBUG PlanTransitivePath] Remapping vocabulary IDs to dense node IDs\n";

    auto subject_col = std::static_pointer_cast<arrow::Int64Array>(edges->column(0));
    auto object_col = std::static_pointer_cast<arrow::Int64Array>(edges->column(1));

    // Build vocabulary ID -> dense ID mapping
    std::unordered_map<int64_t, int64_t> vocab_to_dense;
    int64_t next_dense_id = 0;

    for (int64_t i = 0; i < edges->num_rows(); i++) {
        int64_t subject_id = subject_col->Value(i);
        int64_t object_id = object_col->Value(i);

        if (vocab_to_dense.find(subject_id) == vocab_to_dense.end()) {
            vocab_to_dense[subject_id] = next_dense_id++;
        }
        if (vocab_to_dense.find(object_id) == vocab_to_dense.end()) {
            vocab_to_dense[object_id] = next_dense_id++;
        }
    }

    std::cerr << "[DEBUG PlanTransitivePath] Mapped " << vocab_to_dense.size()
              << " vocabulary IDs to dense range [0, " << next_dense_id << ")\n";

    // Create remapped edge RecordBatch
    arrow::Int64Builder remapped_subject_builder;
    arrow::Int64Builder remapped_object_builder;

    for (int64_t i = 0; i < edges->num_rows(); i++) {
        int64_t subject_id = subject_col->Value(i);
        int64_t object_id = object_col->Value(i);

        ARROW_RETURN_NOT_OK(remapped_subject_builder.Append(vocab_to_dense[subject_id]));
        ARROW_RETURN_NOT_OK(remapped_object_builder.Append(vocab_to_dense[object_id]));
    }

    std::shared_ptr<arrow::Int64Array> remapped_subject_array;
    std::shared_ptr<arrow::Int64Array> remapped_object_array;
    ARROW_RETURN_NOT_OK(remapped_subject_builder.Finish(&remapped_subject_array));
    ARROW_RETURN_NOT_OK(remapped_object_builder.Finish(&remapped_object_array));

    auto remapped_schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    auto remapped_edges = arrow::RecordBatch::Make(
        remapped_schema, edges->num_rows(),
        {remapped_subject_array, remapped_object_array}
    );

    // 3. Build CSR graph from remapped edges
    std::cerr << "[DEBUG PlanTransitivePath] Building CSR graph from remapped edges\n";
    ARROW_ASSIGN_OR_RAISE(auto csr, sabot::graph::BuildCSRGraph(
        remapped_edges, "subject", "object", arrow::default_memory_pool()
    ));

    // 4. Determine start nodes (bound subjects or all subjects) and remap to dense IDs
    ARROW_ASSIGN_OR_RAISE(auto start_nodes_vocab, GetStartNodes(subject, edges, "subject", ctx));

    if (start_nodes_vocab->length() == 0) {
        // No start nodes - return empty result
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        arrow::Int64Builder subject_builder;
        arrow::Int64Builder object_builder;

        std::shared_ptr<arrow::Int64Array> subject_array;
        std::shared_ptr<arrow::Int64Array> object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));

        auto result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
        return PropertyPathResult{result, "subject", "object"};
    }

    // Remap start nodes from vocabulary IDs to dense IDs
    arrow::Int64Builder dense_start_builder;
    auto start_nodes_int64 = std::static_pointer_cast<arrow::Int64Array>(start_nodes_vocab);
    for (int64_t i = 0; i < start_nodes_int64->length(); i++) {
        int64_t vocab_id = start_nodes_int64->Value(i);
        if (vocab_to_dense.find(vocab_id) != vocab_to_dense.end()) {
            ARROW_RETURN_NOT_OK(dense_start_builder.Append(vocab_to_dense[vocab_id]));
        }
    }
    std::shared_ptr<arrow::Int64Array> start_nodes_dense;
    ARROW_RETURN_NOT_OK(dense_start_builder.Finish(&start_nodes_dense));

    std::cerr << "[DEBUG PlanTransitivePath] Remapped " << start_nodes_dense->length()
              << " start nodes to dense IDs\n";

    // 5. Determine min/max distance from quantifier and counts
    int64_t min_dist = min_count;
    int64_t max_dist = (max_count < 0) ? INT64_MAX : max_count;

    // 6. Run transitive closure on remapped graph
    std::cerr << "[DEBUG PlanTransitivePath] Running transitive closure (min=" << min_dist
              << ", max=" << max_dist << ")\n";
    ARROW_ASSIGN_OR_RAISE(auto tc_result, sabot::graph::TransitiveClosure(
        csr, start_nodes_dense, min_dist, max_dist, arrow::default_memory_pool()
    ));

    std::cerr << "[DEBUG PlanTransitivePath] Transitive closure returned " << tc_result.result->num_rows()
              << " rows\n";

    // 7. Remap (start, end) back to vocabulary IDs
    auto result_batch = tc_result.result;

    auto start_column = std::static_pointer_cast<arrow::Int64Array>(result_batch->GetColumnByName("start"));
    auto end_column = std::static_pointer_cast<arrow::Int64Array>(result_batch->GetColumnByName("end"));

    if (!start_column || !end_column) {
        return arrow::Status::Invalid("Transitive closure result missing start/end columns");
    }

    // Build reverse mapping: dense ID -> vocabulary ID
    std::vector<int64_t> dense_to_vocab(next_dense_id);
    for (const auto& [vocab_id, dense_id] : vocab_to_dense) {
        dense_to_vocab[dense_id] = vocab_id;
    }

    // Remap result columns back to vocabulary IDs
    arrow::Int64Builder vocab_subject_builder;
    arrow::Int64Builder vocab_object_builder;

    for (int64_t i = 0; i < result_batch->num_rows(); i++) {
        int64_t dense_start = start_column->Value(i);
        int64_t dense_end = end_column->Value(i);

        ARROW_RETURN_NOT_OK(vocab_subject_builder.Append(dense_to_vocab[dense_start]));
        ARROW_RETURN_NOT_OK(vocab_object_builder.Append(dense_to_vocab[dense_end]));
    }

    std::shared_ptr<arrow::Int64Array> vocab_subject_array;
    std::shared_ptr<arrow::Int64Array> vocab_object_array;
    ARROW_RETURN_NOT_OK(vocab_subject_builder.Finish(&vocab_subject_array));
    ARROW_RETURN_NOT_OK(vocab_object_builder.Finish(&vocab_object_array));

    // Create new RecordBatch with vocabulary IDs
    auto schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    auto result = arrow::RecordBatch::Make(
        schema,
        result_batch->num_rows(),
        {vocab_subject_array, vocab_object_array}
    );

    std::cerr << "[DEBUG PlanTransitivePath] Final result has " << result->num_rows() << " rows\n";

    return PropertyPathResult{result, "subject", "object"};
}

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
 *   3. Join results on intermediate variable using SequencePath operator
 */
arrow::Result<PropertyPathResult> PlanSequencePath(
    const RDFTerm& subject,
    const std::vector<PropertyPathElement>& path_elements,
    const RDFTerm& object,
    PlanningContext& ctx) {

    if (path_elements.size() < 2) {
        return arrow::Status::Invalid("Sequence path requires at least 2 elements");
    }

    // Plan first path element
    const auto& first_elem = path_elements[0];
    ARROW_ASSIGN_OR_RAISE(auto first_iri, ExtractIRI(first_elem));

    // Create intermediate variable (not bound)
    Variable intermediate_var;
    intermediate_var.name = "_intermediate_" + std::to_string(reinterpret_cast<uintptr_t>(&path_elements));
    RDFTerm intermediate_term = intermediate_var;

    // Plan first sub-path
    PropertyPathResult path_p_result;
    if (first_elem.quantifier != PropertyPathQuantifier::None) {
        // Handle quantified first element
        ARROW_ASSIGN_OR_RAISE(path_p_result, PlanTransitivePath(
            subject, first_iri, intermediate_term,
            first_elem.quantifier,
            first_elem.min_count,
            first_elem.max_count,
            ctx
        ));
    } else {
        // Simple predicate - load all edges
        Term pred_term = Term::IRI(first_iri.iri);
        ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
        if (!pred_id_opt.has_value()) {
            // Return empty result
            auto schema = arrow::schema({
                arrow::field("subject", arrow::int64()),
                arrow::field("object", arrow::int64())
            });
            arrow::Int64Builder subject_builder, object_builder;
            std::shared_ptr<arrow::Int64Array> subject_array, object_array;
            ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
            ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));
            return PropertyPathResult{
                arrow::RecordBatch::Make(schema, 0, {subject_array, object_array}),
                "subject", "object"
            };
        }

        sabot_ql::TriplePattern pattern;
        pattern.subject = std::nullopt;
        pattern.predicate = pred_id_opt->getBits();
        pattern.object = std::nullopt;
        ARROW_ASSIGN_OR_RAISE(auto edges_table, ctx.store->ScanPattern(pattern));
        ARROW_ASSIGN_OR_RAISE(auto edges_batch, edges_table->CombineChunksToBatch());
        path_p_result = PropertyPathResult{edges_batch, "subject", "object"};
    }

    // For simplicity, handle only 2-element sequences for now
    // Multi-element sequences can be chained recursively in future work
    if (path_elements.size() > 2) {
        return arrow::Status::NotImplemented("Sequence paths with >2 elements not yet implemented");
    }

    // Plan second path element
    const auto& second_elem = path_elements[1];
    ARROW_ASSIGN_OR_RAISE(auto second_iri, ExtractIRI(second_elem));

    PropertyPathResult path_q_result;
    if (second_elem.quantifier != PropertyPathQuantifier::None) {
        // Handle quantified second element
        ARROW_ASSIGN_OR_RAISE(path_q_result, PlanTransitivePath(
            intermediate_term, second_iri, object,
            second_elem.quantifier,
            second_elem.min_count,
            second_elem.max_count,
            ctx
        ));
    } else {
        // Simple predicate - load all edges
        Term pred_term = Term::IRI(second_iri.iri);
        ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
        if (!pred_id_opt.has_value()) {
            // Return empty result
            auto schema = arrow::schema({
                arrow::field("subject", arrow::int64()),
                arrow::field("object", arrow::int64())
            });
            arrow::Int64Builder subject_builder, object_builder;
            std::shared_ptr<arrow::Int64Array> subject_array, object_array;
            ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
            ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));
            return PropertyPathResult{
                arrow::RecordBatch::Make(schema, 0, {subject_array, object_array}),
                "subject", "object"
            };
        }

        sabot_ql::TriplePattern pattern;
        pattern.subject = std::nullopt;
        pattern.predicate = pred_id_opt->getBits();
        pattern.object = std::nullopt;
        ARROW_ASSIGN_OR_RAISE(auto edges_table, ctx.store->ScanPattern(pattern));
        ARROW_ASSIGN_OR_RAISE(auto edges_batch, edges_table->CombineChunksToBatch());
        path_q_result = PropertyPathResult{edges_batch, "subject", "object"};
    }

    // Join the two paths using SequencePath operator
    // SequencePath expects RecordBatches with columns (start, end)
    // Our results have (subject, object), which is the same structure

    // Rename columns to (start, end) for SequencePath
    auto path_p_batch = path_p_result.bindings;
    auto path_q_batch = path_q_result.bindings;

    // Build renamed batches
    auto start_end_schema = arrow::schema({
        arrow::field("start", arrow::int64()),
        arrow::field("end", arrow::int64())
    });

    auto path_p_renamed = arrow::RecordBatch::Make(
        start_end_schema,
        path_p_batch->num_rows(),
        {path_p_batch->column(0), path_p_batch->column(1)}
    );

    auto path_q_renamed = arrow::RecordBatch::Make(
        start_end_schema,
        path_q_batch->num_rows(),
        {path_q_batch->column(0), path_q_batch->column(1)}
    );

    // Call SequencePath operator
    ARROW_ASSIGN_OR_RAISE(auto result_batch, sabot::graph::SequencePath(
        path_p_renamed, path_q_renamed, arrow::default_memory_pool()
    ));

    // Rename back to (subject, object)
    auto subject_object_schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    auto final_batch = arrow::RecordBatch::Make(
        subject_object_schema,
        result_batch->num_rows(),
        {result_batch->column(0), result_batch->column(1)}
    );

    return PropertyPathResult{final_batch, "subject", "object"};
}

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
 *   3. Union and deduplicate using AlternativePath operator
 */
arrow::Result<PropertyPathResult> PlanAlternativePath(
    const RDFTerm& subject,
    const std::vector<PropertyPathElement>& path_elements,
    const RDFTerm& object,
    PlanningContext& ctx) {

    if (path_elements.size() < 2) {
        return arrow::Status::Invalid("Alternative path requires at least 2 elements");
    }

    // Plan all alternatives and union them pairwise
    std::shared_ptr<arrow::RecordBatch> accumulated_result;

    for (size_t i = 0; i < path_elements.size(); ++i) {
        const auto& elem = path_elements[i];
        ARROW_ASSIGN_OR_RAISE(auto elem_iri, ExtractIRI(elem));

        // Plan this alternative path
        PropertyPathResult path_result;
        if (elem.quantifier != PropertyPathQuantifier::None) {
            // Handle quantified element
            ARROW_ASSIGN_OR_RAISE(path_result, PlanTransitivePath(
                subject, elem_iri, object,
                elem.quantifier,
                elem.min_count,
                elem.max_count,
                ctx
            ));
        } else {
            // Simple predicate - load all edges
            Term pred_term = Term::IRI(elem_iri.iri);
            ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
            if (!pred_id_opt.has_value()) {
                // This alternative has no results, skip it
                continue;
            }

            sabot_ql::TriplePattern pattern;
            pattern.subject = std::nullopt;
            pattern.predicate = pred_id_opt->getBits();
            pattern.object = std::nullopt;
            ARROW_ASSIGN_OR_RAISE(auto edges_table, ctx.store->ScanPattern(pattern));
            ARROW_ASSIGN_OR_RAISE(auto edges_batch, edges_table->CombineChunksToBatch());
            path_result = PropertyPathResult{edges_batch, "subject", "object"};
        }

        // Union with accumulated results
        if (i == 0) {
            // First alternative becomes the initial result
            accumulated_result = path_result.bindings;
        } else {
            // Rename columns to (start, end) for AlternativePath operator
            auto start_end_schema = arrow::schema({
                arrow::field("start", arrow::int64()),
                arrow::field("end", arrow::int64())
            });

            auto prev_renamed = arrow::RecordBatch::Make(
                start_end_schema,
                accumulated_result->num_rows(),
                {accumulated_result->column(0), accumulated_result->column(1)}
            );

            auto curr_renamed = arrow::RecordBatch::Make(
                start_end_schema,
                path_result.bindings->num_rows(),
                {path_result.bindings->column(0), path_result.bindings->column(1)}
            );

            // Call AlternativePath operator (union with dedup)
            ARROW_ASSIGN_OR_RAISE(auto union_batch, sabot::graph::AlternativePath(
                prev_renamed, curr_renamed, arrow::default_memory_pool()
            ));

            // Rename back to (subject, object)
            auto subject_object_schema = arrow::schema({
                arrow::field("subject", arrow::int64()),
                arrow::field("object", arrow::int64())
            });

            accumulated_result = arrow::RecordBatch::Make(
                subject_object_schema,
                union_batch->num_rows(),
                {union_batch->column(0), union_batch->column(1)}
            );
        }
    }

    // If no results at all, return empty batch
    if (!accumulated_result) {
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });
        arrow::Int64Builder subject_builder, object_builder;
        std::shared_ptr<arrow::Int64Array> subject_array, object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));
        accumulated_result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
    }

    return PropertyPathResult{accumulated_result, "subject", "object"};
}

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
 *   3. Traverse from object to subject (in reversed graph)
 */
arrow::Result<PropertyPathResult> PlanInversePath(
    const RDFTerm& subject,
    const IRI& predicate_iri,
    const RDFTerm& object,
    PlanningContext& ctx) {

    // 1. Resolve predicate IRI to value ID
    Term pred_term = Term::IRI(predicate_iri.iri);
    ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
    if (!pred_id_opt.has_value()) {
        // Predicate not in vocabulary - return empty result
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        arrow::Int64Builder subject_builder;
        arrow::Int64Builder object_builder;

        std::shared_ptr<arrow::Int64Array> subject_array;
        std::shared_ptr<arrow::Int64Array> object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));

        auto result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
        return PropertyPathResult{result, "subject", "object"};
    }

    // 2. Load edges for this predicate
    sabot_ql::TriplePattern pattern;
    pattern.subject = std::nullopt;  // unbound
    pattern.predicate = pred_id_opt->getBits();  // bound to this predicate
    pattern.object = std::nullopt;  // unbound

    ARROW_ASSIGN_OR_RAISE(auto edges_table, ctx.store->ScanPattern(pattern));
    ARROW_ASSIGN_OR_RAISE(auto edges_combined, edges_table->CombineChunksToBatch());
    auto edges = edges_combined;

    if (edges->num_rows() == 0) {
        // No edges - return empty result
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        arrow::Int64Builder subject_builder;
        arrow::Int64Builder object_builder;

        std::shared_ptr<arrow::Int64Array> subject_array;
        std::shared_ptr<arrow::Int64Array> object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));

        auto result = arrow::RecordBatch::Make(schema, 0, {subject_array, object_array});
        return PropertyPathResult{result, "subject", "object"};
    }

    // 3. For inverse path, we simply swap subject and object columns
    // This gives us the reverse edges
    auto subject_column = edges->GetColumnByName("subject");
    auto object_column = edges->GetColumnByName("object");

    if (!subject_column || !object_column) {
        return arrow::Status::Invalid("Edges missing subject/object columns");
    }

    // Create new RecordBatch with swapped columns
    // Original: (subject, object) = (A, B) means A→B
    // Inverse: (subject, object) = (B, A) means B←A (i.e., who points to B)
    auto schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    auto result = arrow::RecordBatch::Make(
        schema,
        edges->num_rows(),
        {object_column, subject_column}  // Swapped!
    );

    return PropertyPathResult{result, "subject", "object"};
}

/**
 * Plan negated path (!p)
 *
 * Filters edges to exclude specific predicates.
 *
 * Example:
 *   ?person !foaf:knows ?other  # any relationship except knows
 *
 * Algorithm:
 *   1. Load all edges from triple store
 *   2. Filter to exclude negated predicates using FilterByPredicate operator
 *   3. Return matching (subject, object) pairs
 */
arrow::Result<PropertyPathResult> PlanNegatedPath(
    const RDFTerm& subject,
    const std::vector<IRI>& excluded_predicates,
    const RDFTerm& object,
    PlanningContext& ctx) {

    // 1. Convert excluded predicate IRIs to ValueIds
    std::vector<int64_t> excluded_predicate_ids;
    for (const auto& pred_iri : excluded_predicates) {
        Term pred_term = Term::IRI(pred_iri.iri);
        ARROW_ASSIGN_OR_RAISE(auto pred_id_opt, ctx.vocab->GetValueId(pred_term));
        if (pred_id_opt.has_value()) {
            excluded_predicate_ids.push_back(pred_id_opt->getBits());
        }
        // If predicate not in vocabulary, skip it (can't exclude what doesn't exist)
    }

    // If no excluded predicates found in vocabulary, return empty result
    if (excluded_predicate_ids.empty()) {
        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("object", arrow::int64())
        });
        arrow::Int64Builder subject_builder, object_builder;
        std::shared_ptr<arrow::Int64Array> subject_array, object_array;
        ARROW_RETURN_NOT_OK(subject_builder.Finish(&subject_array));
        ARROW_RETURN_NOT_OK(object_builder.Finish(&object_array));
        return PropertyPathResult{
            arrow::RecordBatch::Make(schema, 0, {subject_array, object_array}),
            "subject", "object"
        };
    }

    // 2. Load all edges (?, ?, ?)
    sabot_ql::TriplePattern pattern;
    pattern.subject = std::nullopt;
    pattern.predicate = std::nullopt;
    pattern.object = std::nullopt;

    ARROW_ASSIGN_OR_RAISE(auto all_edges_table, ctx.store->ScanPattern(pattern));
    ARROW_ASSIGN_OR_RAISE(auto all_edges_batch, all_edges_table->CombineChunksToBatch());

    // 3. Filter to exclude negated predicates
    // The batch should have columns: (subject, predicate, object)
    ARROW_ASSIGN_OR_RAISE(auto filtered_batch, sabot::graph::FilterByPredicate(
        all_edges_batch, excluded_predicate_ids, "predicate", arrow::default_memory_pool()
    ));

    // 4. Extract only (subject, object) columns from filtered result
    auto subject_column = filtered_batch->GetColumnByName("subject");
    auto object_column = filtered_batch->GetColumnByName("object");

    if (!subject_column || !object_column) {
        return arrow::Status::Invalid("Filtered batch missing subject/object columns");
    }

    auto result_schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    auto result_batch = arrow::RecordBatch::Make(
        result_schema,
        filtered_batch->num_rows(),
        {subject_column, object_column}
    );

    return PropertyPathResult{result_batch, "subject", "object"};
}

}  // namespace sparql
}  // namespace sabot_ql
