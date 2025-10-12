#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/likely.hpp"
#include "sabot_sql/common/memory_safety.hpp"
#include "sabot_sql/original/std/memory.hpp"

#include <type_traits>

namespace sabot_sql {

template <class DATA_TYPE, class DELETER = std::default_delete<DATA_TYPE>, bool SAFE = true>
class unique_ptr : public sabot_sql_base_std::unique_ptr<DATA_TYPE, DELETER> { // NOLINT: naming
public:
	using original = sabot_sql_base_std::unique_ptr<DATA_TYPE, DELETER>;
	using original::original; // NOLINT
	using pointer = typename original::pointer;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(SABOT_SQL_DEBUG_NO_SAFETY) || defined(SABOT_SQL_CLANG_TIDY)
		return;
#else
		if (SABOT_SQL_UNLIKELY(null)) {
			throw sabot_sql::InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}

public:
	typename std::add_lvalue_reference<DATA_TYPE>::type operator*() const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AssertNotNull(!ptr);
		}
		return *ptr;
	}

	typename original::pointer operator->() const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AssertNotNull(!ptr);
		}
		return ptr;
	}

#ifdef SABOT_SQL_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	reset(typename original::pointer ptr = typename original::pointer()) noexcept { // NOLINT: hiding on purpose
		original::reset(ptr);
	}
};

template <class DATA_TYPE, class DELETER>
class unique_ptr<DATA_TYPE[], DELETER, true> : public sabot_sql_base_std::unique_ptr<DATA_TYPE[], DELETER> {
public:
	using original = sabot_sql_base_std::unique_ptr<DATA_TYPE[], DELETER>;
	using original::original;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(SABOT_SQL_DEBUG_NO_SAFETY) || defined(SABOT_SQL_CLANG_TIDY)
		return;
#else
		if (SABOT_SQL_UNLIKELY(null)) {
			throw sabot_sql::InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}

public:
	typename std::add_lvalue_reference<DATA_TYPE>::type operator[](size_t __i) const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<true>::ENABLED) {
			AssertNotNull(!ptr);
		}
		return ptr[__i];
	}
};

template <class DATA_TYPE, class DELETER, bool SAFE>
class unique_ptr<DATA_TYPE[], DELETER, SAFE> : public sabot_sql_base_std::unique_ptr<DATA_TYPE[], DELETER> {
public:
	using original = sabot_sql_base_std::unique_ptr<DATA_TYPE[], DELETER>;
	using original::original;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(SABOT_SQL_DEBUG_NO_SAFETY) || defined(SABOT_SQL_CLANG_TIDY)
		return;
#else
		if (SABOT_SQL_UNLIKELY(null)) {
			throw sabot_sql::InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}

public:
	typename std::add_lvalue_reference<DATA_TYPE>::type operator[](size_t __i) const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AssertNotNull(!ptr);
		}
		return ptr[__i];
	}
};

template <typename T>
using unique_array = unique_ptr<T[], std::default_delete<T[]>, true>;

template <typename T>
using unsafe_unique_array = unique_ptr<T[], std::default_delete<T[]>, false>;

template <typename T>
using unsafe_unique_ptr = unique_ptr<T, std::default_delete<T>, false>;

} // namespace sabot_sql
