//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/original/std/sstream.hpp"
#include <string>
#include <locale>

namespace sabot_sql {
using std::string;
} // namespace sabot_sql

namespace sabot_sql {

template <class charT, class traits = std::char_traits<charT>, class Allocator = std::allocator<charT>>
class basic_stringstream : public sabot_sql_base_std::basic_stringstream<charT, traits, Allocator> {
public:
	using original = sabot_sql_base_std::basic_stringstream<charT, traits, Allocator>;

	explicit basic_stringstream(std::ios_base::openmode which = std::ios_base::out | std::ios_base::in)
	    : original(which) {
		this->imbue(std::locale::classic());
	}
	explicit basic_stringstream(const std::basic_string<charT, traits, Allocator> &s,
	                            std::ios_base::openmode which = std::ios_base::out | std::ios_base::in)
	    : original(s, which) {
		this->imbue(std::locale::classic());
	}
	basic_stringstream(const basic_stringstream &) = delete;
	basic_stringstream(basic_stringstream &&rhs) noexcept;
};

typedef basic_stringstream<char> stringstream;
} // namespace sabot_sql
