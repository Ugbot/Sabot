//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/extension/generated_extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/main/database.hpp"

#if defined(GENERATED_EXTENSION_HEADERS) && !defined(SABOT_SQL_AMALGAMATION)
#include "sabot_sql/common/common.hpp"
#include "generated_extension_headers.hpp"

namespace sabot_sql {

//! Looks through the CMake-generated list of extensions that are linked into SabotSQL currently to try load <extension>
bool TryLoadLinkedExtension(SabotSQL &db, const string &extension);

vector<string> LinkedExtensions();
vector<string> LoadedExtensionTestPaths();

} // namespace sabot_sql
#endif
