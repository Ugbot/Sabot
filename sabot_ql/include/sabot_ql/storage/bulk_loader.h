#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/compute/api.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/storage/triple_store.h>
#include <unordered_map>

namespace sabot_ql {

/**
 * Bulk load RDF data from Arrow tables into triple store.
 *
 * This function handles:
 * 1. Building vocabulary from terms table
 * 2. Creating ID mapping (Python IDs → C++ IDs)
 * 3. Remapping triple IDs using Arrow compute
 * 4. Inserting remapped triples into store
 *
 * All operations done in C++ for maximum performance.
 *
 * @param vocab Vocabulary to load terms into
 * @param triple_store Triple store to insert triples into
 * @param terms_table Arrow table with columns: id (int64), lex (string),
 *                    kind (uint8), lang (string), datatype (string)
 * @param triples_table Arrow table with columns: subject/s (int64),
 *                      predicate/p (int64), object/o (int64)
 * @return Status indicating success or failure
 */
arrow::Status BulkLoadFromArrow(
    std::shared_ptr<Vocabulary> vocab,
    std::shared_ptr<TripleStore> triple_store,
    const std::shared_ptr<arrow::Table>& terms_table,
    const std::shared_ptr<arrow::Table>& triples_table);

} // namespace sabot_ql
