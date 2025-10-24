/************************************************************************
SabotQL Python Bindings Interface
**************************************************************************/

#pragma once

#include "sabot_ql/storage/triple_store.h"
#include "sabot_ql/storage/vocabulary.h"
#include "marble/db.h"
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>
#include <string>

namespace sabot_ql {
namespace bindings {

/**
 * @brief Create MarbleDB-backed triple store
 *
 * Opens or creates a MarbleDB database and returns a triple store instance.
 *
 * @param db_path Path to MarbleDB database directory
 * @return Result containing shared_ptr to TripleStore or error status
 */
arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStoreMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db);

/**
 * @brief Create MarbleDB-backed vocabulary
 *
 * Opens or creates a MarbleDB database and returns a vocabulary instance.
 *
 * @param db_path Path to MarbleDB database directory
 * @return Result containing shared_ptr to Vocabulary or error status
 */
arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabularyMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db);

/**
 * @brief Open MarbleDB database for RDF storage
 *
 * Opens or creates a MarbleDB instance suitable for RDF triple storage.
 *
 * @param db_path Path to database directory
 * @param create_if_missing Create database if it doesn't exist
 * @return Result containing shared_ptr to MarbleDB or error status
 */
arrow::Result<std::shared_ptr<marble::MarbleDB>> OpenMarbleDB(
    const std::string& db_path,
    bool create_if_missing = true);

} // namespace bindings
} // namespace sabot_ql
