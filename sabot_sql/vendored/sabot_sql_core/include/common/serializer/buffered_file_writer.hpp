//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/serializer/write_stream.hpp"
#include "sabot_sql/common/file_system.hpp"

namespace sabot_sql {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public WriteStream {
public:
	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;

	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	SABOT_SQL_API BufferedFileWriter(FileSystem &fs, const string &path, FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS);

	FileSystem &fs;
	string path;
	unsafe_unique_array<data_t> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

public:
	SABOT_SQL_API void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	//! Flush all changes to the file and then close the file
	SABOT_SQL_API void Close();
	//! Flush all changes and fsync the file to disk
	SABOT_SQL_API void Sync();
	//! Flush the buffer to the file (without sync)
	SABOT_SQL_API void Flush();
	//! Returns the current size of the file
	SABOT_SQL_API idx_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	SABOT_SQL_API void Truncate(idx_t size);

	SABOT_SQL_API idx_t GetTotalWritten() const;
};

} // namespace sabot_sql
