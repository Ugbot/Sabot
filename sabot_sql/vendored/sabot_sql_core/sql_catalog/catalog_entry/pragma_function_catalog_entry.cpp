#include "sabot_sql/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "sabot_sql/parser/parsed_data/create_pragma_function_info.hpp"

namespace sabot_sql {

PragmaFunctionCatalogEntry::PragmaFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreatePragmaFunctionInfo &info)
    : FunctionEntry(CatalogType::PRAGMA_FUNCTION_ENTRY, catalog, schema, info), functions(std::move(info.functions)) {
}

} // namespace sabot_sql
