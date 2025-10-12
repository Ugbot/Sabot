//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

enum class OutputStream : uint8_t { STREAM_STDOUT = 1, STREAM_STDERR = 2 };

typedef void (*line_printer_f)(OutputStream stream, const string &str);

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to the stream
	SABOT_SQL_API static void Print(OutputStream stream, const string &str);
	//! Print the object to stderr
	SABOT_SQL_API static void Print(const string &str);
	//! Print the formatted object to the stream
	template <typename... ARGS>
	static void PrintF(OutputStream stream, const string &str, ARGS... params) {
		Printer::Print(stream, StringUtil::Format(str, params...));
	}
	//! Print the formatted object to stderr
	template <typename... ARGS>
	static void PrintF(const string &str, ARGS... params) {
		Printer::PrintF(OutputStream::STREAM_STDERR, str, std::forward<ARGS>(params)...);
	}
	//! Directly prints the string to stdout without a newline
	SABOT_SQL_API static void RawPrint(OutputStream stream, const string &str);
	//! Flush an output stream
	SABOT_SQL_API static void Flush(OutputStream stream);
	//! Whether or not we are printing to a terminal
	SABOT_SQL_API static bool IsTerminal(OutputStream stream);
	//! The terminal width
	SABOT_SQL_API static idx_t TerminalWidth();

	// hook to allow capturing the output and routing it somewhere else / reformat it};
	static line_printer_f line_printer;

private:
	static void DefaultLinePrint(OutputStream stream, const string &str);
};
} // namespace sabot_sql
