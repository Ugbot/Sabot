//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/shared_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unique_ptr.hpp"
#include "sabot_sql/common/likely.hpp"
#include "sabot_sql/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace sabot_sql {

// This implementation is taken from the llvm-project, at this commit hash:
// https://github.com/llvm/llvm-project/blob/08bb121835be432ac52372f92845950628ce9a4a/libcxx/include/__memory/shared_ptr.h#353
// originally named '__compatible_with'

#if _LIBCPP_STD_VER >= 17
template <class U, class T>
struct __bounded_convertible_to_unbounded : std::false_type {};

template <class _Up, std::size_t _Np, class T>
struct __bounded_convertible_to_unbounded<_Up[_Np], T> : std::is_same<std::remove_cv<T>, _Up[]> {};

template <class U, class T>
struct compatible_with_t : std::_Or<std::is_convertible<U *, T *>, __bounded_convertible_to_unbounded<U, T>> {};
#else
template <class U, class T>
struct compatible_with_t : std::is_convertible<U *, T *> {}; // NOLINT: invalid case style
#endif // _LIBCPP_STD_VER >= 17

} // namespace sabot_sql

#include "sabot_sql/common/shared_ptr_ipp.hpp"
#include "sabot_sql/common/weak_ptr_ipp.hpp"
#include "sabot_sql/common/enable_shared_from_this_ipp.hpp"

namespace sabot_sql {

template <typename T>
using unsafe_shared_ptr = shared_ptr<T, false>;

template <typename T>
using unsafe_weak_ptr = weak_ptr<T, false>;

} // namespace sabot_sql
