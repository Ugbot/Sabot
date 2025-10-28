#include <sabot_ql/storage/bulk_loader.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>

namespace sabot_ql {

arrow::Status BulkLoadFromArrow(
    std::shared_ptr<Vocabulary> vocab,
    std::shared_ptr<TripleStore> triple_store,
    const std::shared_ptr<arrow::Table>& terms_table,
    const std::shared_ptr<arrow::Table>& triples_table) {

    // Step 1: Build vocabulary using Arrow kernels
    std::cerr << "[BULK_LOAD] Loading " << terms_table->num_rows()
              << " terms into vocabulary..." << std::endl;

    // Extract columns from terms table
    auto id_col = terms_table->GetColumnByName("id");
    auto lex_col = terms_table->GetColumnByName("lex");
    auto kind_col = terms_table->GetColumnByName("kind");
    auto lang_col = terms_table->GetColumnByName("lang");
    auto datatype_col = terms_table->GetColumnByName("datatype");

    if (!id_col || !lex_col || !kind_col || !lang_col || !datatype_col) {
        return arrow::Status::Invalid("Terms table missing required columns");
    }

    // Combine chunks into single arrays for fast access (Arrow kernel)
    ARROW_ASSIGN_OR_RAISE(auto id_array,
        arrow::Concatenate(id_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto lex_array,
        arrow::Concatenate(lex_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto kind_array,
        arrow::Concatenate(kind_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto lang_array,
        arrow::Concatenate(lang_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto datatype_array,
        arrow::Concatenate(datatype_col->chunks()));

    // Cast to concrete types for SIMD access
    auto id_data = std::static_pointer_cast<arrow::Int64Array>(id_array);
    auto lex_data = std::static_pointer_cast<arrow::StringArray>(lex_array);
    auto kind_data = std::static_pointer_cast<arrow::UInt8Array>(kind_array);
    auto lang_data = std::static_pointer_cast<arrow::StringArray>(lang_array);
    auto datatype_data = std::static_pointer_cast<arrow::StringArray>(datatype_array);

    // Build terms vector (TODO: can optimize with SIMD when all same kind)
    std::vector<Term> terms;
    terms.reserve(terms_table->num_rows());

    // Fast path: use raw pointers for SIMD-friendly access
    const int64_t* old_ids_ptr = id_data->raw_values();
    const uint8_t* kinds_ptr = kind_data->raw_values();

    for (int64_t i = 0; i < terms_table->num_rows(); ++i) {
        std::string lexical = lex_data->GetString(i);
        uint8_t kind = kinds_ptr[i];
        std::string lang = lang_data->IsNull(i) ? "" : lang_data->GetString(i);
        std::string dtype = datatype_data->IsNull(i) ? "" : datatype_data->GetString(i);

        // Create term based on kind
        Term term;
        if (kind == 0) {  // IRI
            term = Term::IRI(lexical);
        } else if (kind == 1) {  // Literal
            term = Term::Literal(lexical, lang, dtype);
        } else if (kind == 2) {  // BlankNode
            term = Term::BlankNode(lexical);
        } else {
            return arrow::Status::Invalid("Invalid term kind: " + std::to_string(kind));
        }

        terms.push_back(std::move(term));
    }

    // Batch add terms to vocabulary (lock-free atomic allocation)
    std::cerr << "[BULK_LOAD] Adding " << terms.size() << " terms to vocabulary..." << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto new_ids, vocab->AddTerms(terms));

    // Build new_ids Arrow array for ID remapping kernel
    arrow::Int64Builder new_ids_builder;
    ARROW_RETURN_NOT_OK(new_ids_builder.Reserve(new_ids.size()));
    for (const auto& id : new_ids) {
        ARROW_RETURN_NOT_OK(new_ids_builder.Append(id.getBits()));
    }
    ARROW_ASSIGN_OR_RAISE(auto new_ids_array, new_ids_builder.Finish());

    std::cerr << "[BULK_LOAD] Built ID mapping for " << new_ids.size() << " terms" << std::endl;

    // Step 2: Remap triple IDs using Arrow compute kernels
    std::cerr << "[BULK_LOAD] Remapping " << triples_table->num_rows() << " triples using Arrow kernels..." << std::endl;

    // Get triple columns (support both naming conventions)
    auto s_col = triples_table->GetColumnByName("subject");
    if (!s_col) s_col = triples_table->GetColumnByName("s");

    auto p_col = triples_table->GetColumnByName("predicate");
    if (!p_col) p_col = triples_table->GetColumnByName("p");

    auto o_col = triples_table->GetColumnByName("object");
    if (!o_col) o_col = triples_table->GetColumnByName("o");

    if (!s_col || !p_col || !o_col) {
        return arrow::Status::Invalid("Triples table missing subject/predicate/object columns");
    }

    // Combine chunks using Arrow kernel
    ARROW_ASSIGN_OR_RAISE(auto s_array, arrow::Concatenate(s_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto p_array, arrow::Concatenate(p_col->chunks()));
    ARROW_ASSIGN_OR_RAISE(auto o_array, arrow::Concatenate(o_col->chunks()));

    // Use Arrow's index_in + take kernels for remapping (vectorized!)
    // Build dictionary: old_id_array -> new_id_array
    // Then use index_in to find positions, take to get new values

    // Create lookup table as Arrow Table (old_id, new_id)
    auto lookup_table = arrow::Table::Make(
        arrow::schema({
            arrow::field("old_id", arrow::int64()),
            arrow::field("new_id", arrow::int64())
        }),
        {id_array, new_ids_array}
    );

    // Use Arrow compute's index_in kernel to find matching indices
    // Then use take kernel to extract new IDs
    // This is SIMD-accelerated and way faster than hash map lookups

    auto s_remapped = s_array;  // If old_id == new_id (sequential case), skip remapping
    auto p_remapped = p_array;
    auto o_remapped = o_array;

    // Check if remapping is actually needed (optimization for sequential IDs)
    bool needs_remap = false;
    auto id_i64 = std::static_pointer_cast<arrow::Int64Array>(id_array);
    auto new_i64 = std::static_pointer_cast<arrow::Int64Array>(new_ids_array);
    for (int64_t i = 0; i < std::min<int64_t>(100, id_array->length()); ++i) {
        if (id_i64->Value(i) != new_i64->Value(i)) {
            needs_remap = true;
            break;
        }
    }

    if (needs_remap) {
        // Use Arrow compute's index_in + take for vectorized remapping
        arrow::compute::ExecContext ctx;

        // For each triple column, find index of old_id in id_array, then take from new_ids_array
        ARROW_ASSIGN_OR_RAISE(auto s_indices,
            arrow::compute::IndexIn(s_array, arrow::compute::SetLookupOptions(id_array)));
        ARROW_ASSIGN_OR_RAISE(auto s_remap_datum,
            arrow::compute::Take(new_ids_array, s_indices.array()));
        s_remapped = s_remap_datum.make_array();

        ARROW_ASSIGN_OR_RAISE(auto p_indices,
            arrow::compute::IndexIn(p_array, arrow::compute::SetLookupOptions(id_array)));
        ARROW_ASSIGN_OR_RAISE(auto p_remap_datum,
            arrow::compute::Take(new_ids_array, p_indices.array()));
        p_remapped = p_remap_datum.make_array();

        ARROW_ASSIGN_OR_RAISE(auto o_indices,
            arrow::compute::IndexIn(o_array, arrow::compute::SetLookupOptions(id_array)));
        ARROW_ASSIGN_OR_RAISE(auto o_remap_datum,
            arrow::compute::Take(new_ids_array, o_indices.array()));
        o_remapped = o_remap_datum.make_array();
    }

    // Create remapped table
    auto remapped_table = arrow::Table::Make(
        arrow::schema({
            arrow::field("s", arrow::int64()),
            arrow::field("p", arrow::int64()),
            arrow::field("o", arrow::int64())
        }),
        {s_remapped, p_remapped, o_remapped}
    );

    std::cerr << "[BULK_LOAD] Remapping complete, inserting into triple store..." << std::endl;

    // Step 3: Insert remapped triples
    ARROW_ASSIGN_OR_RAISE(auto batch, remapped_table->CombineChunksToBatch());
    ARROW_RETURN_NOT_OK(triple_store->InsertArrowBatch(batch));

    std::cerr << "[BULK_LOAD] Successfully loaded " << triples_table->num_rows()
              << " triples" << std::endl;

    return arrow::Status::OK();
}

} // namespace sabot_ql
