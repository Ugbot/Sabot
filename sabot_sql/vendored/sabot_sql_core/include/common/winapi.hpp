//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/winapi.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef SABOT_SQL_API
#if defined(_WIN32) && !defined(__MINGW32__)
#ifdef SABOT_SQL_STATIC_BUILD
#define SABOT_SQL_API
#else
#if defined(SABOT_SQL_BUILD_LIBRARY) && !defined(SABOT_SQL_BUILD_LOADABLE_EXTENSION)
#define SABOT_SQL_API __declspec(dllexport)
#else
#define SABOT_SQL_API __declspec(dllimport)
#endif
#endif
#else
#define SABOT_SQL_API
#endif
#endif

#ifndef SABOT_SQL_EXTENSION_API
#ifdef _WIN32
#ifdef SABOT_SQL_STATIC_BUILD
#define SABOT_SQL_EXTENSION_API
#else
#define SABOT_SQL_EXTENSION_API __declspec(dllexport)
#endif
#else
#define SABOT_SQL_EXTENSION_API __attribute__((visibility("default")))
#endif
#endif
