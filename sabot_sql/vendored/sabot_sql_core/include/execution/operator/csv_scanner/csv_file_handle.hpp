//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/csv_scanner/csv_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/file_system.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/common/helper.hpp"
#include "sabot_sql/common/allocator.hpp"
#include "sabot_sql/execution/operator/csv_scanner/encode/csv_encoder.hpp"
#include "sabot_sql/main/client_context.hpp"

namespace sabot_sql {
class Allocator;
class FileSystem;
struct CSVReaderOptions;

class CSVFileHandle {
public:
	CSVFileHandle(ClientContext &context, unique_ptr<FileHandle> file_handle_p, const OpenFileInfo &file,
	              const CSVReaderOptions &options);

	mutex main_mutex;

	bool CanSeek() const;
	void Seek(idx_t position) const;
	bool OnDiskFile() const;
	bool IsPipe() const;

	void Reset();

	idx_t FileSize() const;

	bool FinishedReading() const;

	idx_t Read(void *buffer, idx_t nr_bytes);

	string ReadLine();

	string GetFilePath();

	static unique_ptr<FileHandle> OpenFileHandle(FileSystem &fs, Allocator &allocator, const OpenFileInfo &file,
	                                             FileCompressionType compression);
	static unique_ptr<CSVFileHandle> OpenFile(ClientContext &context, const OpenFileInfo &file,
	                                          const CSVReaderOptions &options);
	FileCompressionType compression_type;

	double GetProgress() const;

private:
	QueryContext context;
	unique_ptr<FileHandle> file_handle;
	CSVEncoder encoder;
	const OpenFileInfo file;
	bool can_seek = false;
	bool on_disk_file = false;
	bool is_pipe = false;
	idx_t uncompressed_bytes_read = 0;

	idx_t file_size = 0;

	idx_t requested_bytes = 0;
	//! If we finished reading the file
	bool finished = false;
};

} // namespace sabot_sql
