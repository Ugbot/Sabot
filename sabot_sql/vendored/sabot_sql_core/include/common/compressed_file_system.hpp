//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/compressed_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/file_system.hpp"

namespace sabot_sql {
class CompressedFile;

struct StreamData {
	// various buffers & pointers
	bool write = false;
	bool refresh = false;
	unsafe_unique_array<data_t> in_buff;
	unsafe_unique_array<data_t> out_buff;
	data_ptr_t out_buff_start = nullptr;
	data_ptr_t out_buff_end = nullptr;
	data_ptr_t in_buff_start = nullptr;
	data_ptr_t in_buff_end = nullptr;

	idx_t in_buf_size = 0;
	idx_t out_buf_size = 0;
};

struct StreamWrapper {
	SABOT_SQL_API virtual ~StreamWrapper();

	SABOT_SQL_API virtual void Initialize(QueryContext context, CompressedFile &file, bool write) = 0;
	SABOT_SQL_API virtual bool Read(StreamData &stream_data) = 0;
	SABOT_SQL_API virtual void Write(CompressedFile &file, StreamData &stream_data, data_ptr_t buffer,
	                              int64_t nr_bytes) = 0;
	SABOT_SQL_API virtual void Close() = 0;
};

class CompressedFileSystem : public FileSystem {
public:
	SABOT_SQL_API int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	SABOT_SQL_API int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	SABOT_SQL_API void Reset(FileHandle &handle) override;

	SABOT_SQL_API int64_t GetFileSize(FileHandle &handle) override;

	SABOT_SQL_API bool OnDiskFile(FileHandle &handle) override;
	SABOT_SQL_API bool CanSeek() override;

	SABOT_SQL_API virtual unique_ptr<StreamWrapper> CreateStream() = 0;
	SABOT_SQL_API virtual idx_t InBufferSize() = 0;
	SABOT_SQL_API virtual idx_t OutBufferSize() = 0;
};

class CompressedFile : public FileHandle {
public:
	SABOT_SQL_API CompressedFile(CompressedFileSystem &fs, unique_ptr<FileHandle> child_handle_p, const string &path);
	SABOT_SQL_API ~CompressedFile() override;

	SABOT_SQL_API idx_t GetProgress() override;

	CompressedFileSystem &compressed_fs;
	unique_ptr<FileHandle> child_handle;
	//! Whether the file is opened for reading or for writing
	bool write = false;
	StreamData stream_data;

public:
	SABOT_SQL_API void Initialize(QueryContext context, bool write);
	SABOT_SQL_API int64_t ReadData(void *buffer, int64_t nr_bytes);
	SABOT_SQL_API int64_t WriteData(data_ptr_t buffer, int64_t nr_bytes);
	SABOT_SQL_API void Close() override;

private:
	idx_t current_position = 0;
	unique_ptr<StreamWrapper> stream_wrapper;
};

} // namespace sabot_sql
