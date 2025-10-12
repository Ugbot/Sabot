//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/platform.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

// duplicated from string_util.h to avoid linking issues
#ifndef SABOT_SQL_QUOTE_DEFINE
#define SABOT_SQL_QUOTE_DEFINE_IMPL(x) #x
#define SABOT_SQL_QUOTE_DEFINE(x)      SABOT_SQL_QUOTE_DEFINE_IMPL(x)
#endif

#if defined(_WIN32) || defined(__APPLE__) || defined(__FreeBSD__)
#else
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#include <features.h>
#ifndef __USE_GNU
#define __MUSL__
#endif
#undef _GNU_SOURCE /* don't contaminate other includes unnecessarily */
#else
#include <features.h>
#ifndef __USE_GNU
#define __MUSL__
#endif
#endif
#endif

namespace sabot_sql {

std::string SabotSQLPlatform() { // NOLINT: allow definition in header
#if defined(SABOT_SQL_CUSTOM_PLATFORM)
	return SABOT_SQL_QUOTE_DEFINE(SABOT_SQL_CUSTOM_PLATFORM);
#else
#if defined(SABOT_SQL_WASM_VERSION)
	// SabotSQL-Wasm requires CUSTOM_PLATFORM to be defined
	static_assert(0, "SABOT_SQL_WASM_VERSION should rely on CUSTOM_PLATFORM being provided");
#endif
	std::string os = "linux";
#if INTPTR_MAX == INT64_MAX
	std::string arch = "amd64";
#elif INTPTR_MAX == INT32_MAX
	std::string arch = "i686";
#else
#error Unknown pointer size or missing size macros!
#endif
	std::string postfix = "";

#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#elif defined(__FreeBSD__)
	os = "freebsd";
#endif
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
	arch = "arm64";
#endif

#if defined(__MUSL__)
	if (os == "linux") {
		postfix = "_musl";
	}
#elif (!defined(__clang__) && defined(__GNUC__) && __GNUC__ < 5) ||                                                    \
    (defined(_GLIBCXX_USE_CXX11_ABI) && _GLIBCXX_USE_CXX11_ABI == 0)
#error                                                                                                                 \
    "SabotSQL does not provide extensions for this (legacy) CXX ABI - Explicitly set SABOT_SQL_PLATFORM (Makefile) / SABOT_SQL_EXPLICIT_PLATFORM (CMake) to build anyway. "
#endif

#if defined(__ANDROID__)
	postfix = "_android";
#endif
#ifdef __MINGW32__
	postfix = "_mingw";
#endif
// this is used for the windows R builds which use `mingw` equivalent extensions
#ifdef SABOT_SQL_PLATFORM_RTOOLS
	postfix = "_mingw";
#endif
	return os + "_" + arch + postfix;
#endif
}

} // namespace sabot_sql
