/************************************************************************
SabotQL Python Bindings Implementation
**************************************************************************/

#include "sabot_ql/bindings.h"
#include "sabot_ql/storage/triple_store.h"
#include "sabot_ql/storage/vocabulary.h"
#include "marble/db.h"
#include <arrow/result.h>
#include <memory>

// Forward declare the factory functions that are defined in implementation files
// They are in sabot_ql namespace, not sabot_ql::bindings
namespace sabot_ql {
    extern arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStoreMarbleDB(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);

    extern arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabularyMarbleDB(
        const std::string& db_path,
        std::shared_ptr<marble::MarbleDB> db);
}

namespace sabot_ql {
namespace bindings {

// Open MarbleDB database
arrow::Result<std::shared_ptr<marble::MarbleDB>> OpenMarbleDB(
    const std::string& db_path,
    bool create_if_missing) {

    // Configure database options
    marble::DBOptions options;
    options.db_path = db_path;
    options.memtable_size_threshold = 64 * 1024 * 1024;  // 64MB
    options.compression = marble::DBOptions::CompressionType::kLZ4;
    options.enable_bloom_filter = true;
    options.enable_sparse_index = true;

    // Open the database with nullptr schema (schema-less mode)
    std::unique_ptr<marble::MarbleDB> db_unique;
    auto status = marble::MarbleDB::Open(options, nullptr, &db_unique);

    if (!status.ok()) {
        return arrow::Status::IOError("Failed to open MarbleDB: " + status.ToString());
    }

    // Convert unique_ptr to shared_ptr
    return std::shared_ptr<marble::MarbleDB>(std::move(db_unique));
}

// Wrapper functions that delegate to the sabot_ql namespace implementations
arrow::Result<std::shared_ptr<TripleStore>> CreateTripleStoreMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {
    return ::sabot_ql::CreateTripleStoreMarbleDB(db_path, db);
}

arrow::Result<std::shared_ptr<Vocabulary>> CreateVocabularyMarbleDB(
    const std::string& db_path,
    std::shared_ptr<marble::MarbleDB> db) {
    return ::sabot_ql::CreateVocabularyMarbleDB(db_path, db);
}

// Convert Query to SelectQuery
arrow::Result<sparql::SelectQuery> QueryToSelectQuery(const sparql::Query& query) {
    if (std::holds_alternative<sparql::SelectQuery>(query.query_body)) {
        return std::get<sparql::SelectQuery>(query.query_body);
    } else if (std::holds_alternative<sparql::AskQuery>(query.query_body)) {
        return arrow::Status::Invalid("Query is ASK type, not SELECT");
    } else if (std::holds_alternative<sparql::ConstructQuery>(query.query_body)) {
        return arrow::Status::Invalid("Query is CONSTRUCT type, not SELECT");
    } else if (std::holds_alternative<sparql::DescribeQuery>(query.query_body)) {
        return arrow::Status::Invalid("Query is DESCRIBE type, not SELECT");
    } else {
        return arrow::Status::Invalid("Unknown query type");
    }
}

} // namespace bindings
} // namespace sabot_ql
