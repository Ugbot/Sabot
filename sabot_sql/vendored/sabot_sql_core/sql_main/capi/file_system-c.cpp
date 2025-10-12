#include "sabot_sql/main/capi/capi_internal.hpp"

namespace sabot_sql {
namespace {
struct CFileSystem {

	FileSystem &fs;
	ErrorData error_data;

	explicit CFileSystem(FileSystem &fs_p) : fs(fs_p) {
	}

	void SetError(const char *message) {
		error_data = ErrorData(ExceptionType::IO, message);
	}
	void SetError(const std::exception &ex) {
		error_data = ErrorData(ex);
	}
};

struct CFileOpenOptions {
	sabot_sql::FileOpenFlags flags;
};

struct CFileHandle {
	ErrorData error_data;
	unique_ptr<FileHandle> handle;

	void SetError(const char *message) {
		error_data = ErrorData(ExceptionType::IO, message);
	}
	void SetError(const std::exception &ex) {
		error_data = ErrorData(ex);
	}
};

} // namespace
} // namespace sabot_sql

sabot_sql_file_system sabot_sql_client_context_get_file_system(sabot_sql_client_context context) {
	if (!context) {
		return nullptr;
	}
	auto ctx = reinterpret_cast<sabot_sql::CClientContextWrapper *>(context);
	auto wrapper = new sabot_sql::CFileSystem(sabot_sql::FileSystem::GetFileSystem(ctx->context));
	return reinterpret_cast<sabot_sql_file_system>(wrapper);
}

void sabot_sql_destroy_file_system(sabot_sql_file_system *file_system) {
	if (!file_system || !*file_system) {
		return;
	}
	const auto fs = reinterpret_cast<sabot_sql::CFileSystem *>(*file_system);
	delete fs;
	*file_system = nullptr;
}

sabot_sql_file_open_options sabot_sql_create_file_open_options() {
	auto options = new sabot_sql::CFileOpenOptions();
	return reinterpret_cast<sabot_sql_file_open_options>(options);
}

sabot_sql_state sabot_sql_file_open_options_set_flag(sabot_sql_file_open_options options, sabot_sql_file_flag flag, bool value) {
	if (!options) {
		return SabotSQLError;
	}
	auto coptions = reinterpret_cast<sabot_sql::CFileOpenOptions *>(options);

	switch (flag) {
	case SABOT_SQL_FILE_FLAG_READ:
		coptions->flags |= sabot_sql::FileOpenFlags::FILE_FLAGS_READ;
		break;
	case SABOT_SQL_FILE_FLAG_WRITE:
		coptions->flags |= sabot_sql::FileOpenFlags::FILE_FLAGS_WRITE;
		break;
	case SABOT_SQL_FILE_FLAG_APPEND:
		coptions->flags |= sabot_sql::FileOpenFlags::FILE_FLAGS_APPEND;
		break;
	case SABOT_SQL_FILE_FLAG_CREATE:
		coptions->flags |= sabot_sql::FileOpenFlags::FILE_FLAGS_FILE_CREATE;
		break;
	case SABOT_SQL_FILE_FLAG_CREATE_NEW:
		coptions->flags |= sabot_sql::FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
		break;
	default:
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

void sabot_sql_destroy_file_open_options(sabot_sql_file_open_options *options) {
	if (!options || !*options) {
		return;
	}
	auto coptions = reinterpret_cast<sabot_sql::CFileOpenOptions *>(*options);
	delete coptions;
	*options = nullptr;
}

sabot_sql_state sabot_sql_file_system_open(sabot_sql_file_system fs, const char *path, sabot_sql_file_open_options options,
                                     sabot_sql_file_handle *out_file) {
	if (!fs) {
		*out_file = nullptr;
		return SabotSQLError;
	}
	auto cfs = reinterpret_cast<sabot_sql::CFileSystem *>(fs);
	if (!path || !options || !out_file) {
		cfs->SetError("Invalid input to sabot_sql_file_system_open");
		*out_file = nullptr;
		return SabotSQLError;
	}

	try {
		auto coptions = reinterpret_cast<sabot_sql::CFileOpenOptions *>(options);
		auto handle = cfs->fs.OpenFile(sabot_sql::string(path), coptions->flags);
		auto wrapper = new sabot_sql::CFileHandle();
		wrapper->handle = std::move(handle);
		*out_file = reinterpret_cast<sabot_sql_file_handle>(wrapper);
		return SabotSQLSuccess;
	} catch (const std::exception &ex) {
		cfs->SetError(ex);
		*out_file = nullptr;
		return SabotSQLError;
	} catch (...) {
		cfs->SetError("Unknown error occurred during file open");
		*out_file = nullptr;
		return SabotSQLError;
	}
}

sabot_sql_error_data sabot_sql_file_system_error_data(sabot_sql_file_system fs) {
	auto wrapper = new sabot_sql::ErrorDataWrapper();
	if (!fs) {
		return reinterpret_cast<sabot_sql_error_data>(wrapper);
	}
	auto cfs = reinterpret_cast<sabot_sql::CFileSystem *>(fs);
	wrapper->error_data = cfs->error_data;
	return reinterpret_cast<sabot_sql_error_data>(wrapper);
}

void sabot_sql_destroy_file_handle(sabot_sql_file_handle *file) {
	if (!file || !*file) {
		return;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(*file);
	cfile->handle->Close(); // Ensure the file is closed before destroying
	delete cfile;
	*file = nullptr;
}

sabot_sql_error_data sabot_sql_file_handle_error_data(sabot_sql_file_handle file) {
	auto wrapper = new sabot_sql::ErrorDataWrapper();
	if (!file) {
		return reinterpret_cast<sabot_sql_error_data>(wrapper);
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	wrapper->error_data = cfile->error_data;
	return reinterpret_cast<sabot_sql_error_data>(wrapper);
}

int64_t sabot_sql_file_handle_read(sabot_sql_file_handle file, void *buffer, int64_t size) {
	if (!file || !buffer || size < 0) {
		return -1;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		return cfile->handle->Read(buffer, static_cast<idx_t>(size));
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return -1;
	} catch (...) {
		cfile->SetError("Unknown error occurred during file read");
		return -1;
	}
}

int64_t sabot_sql_file_handle_write(sabot_sql_file_handle file, const void *buffer, int64_t size) {
	if (!file || !buffer || size < 0) {
		return -1;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		return cfile->handle->Write(const_cast<void *>(buffer), static_cast<idx_t>(size));
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return -1;
	} catch (...) {
		cfile->SetError("Unknown error occurred during file write");
		return -1;
	}
}

int64_t sabot_sql_file_handle_tell(sabot_sql_file_handle file) {
	if (!file) {
		return -1;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		return static_cast<int64_t>(cfile->handle->SeekPosition());
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return -1;
	} catch (...) {
		cfile->SetError("Unknown error occurred when getting file position");
		return -1;
	}
}

int64_t sabot_sql_file_handle_size(sabot_sql_file_handle file) {
	if (!file) {
		return -1;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		return static_cast<int64_t>(cfile->handle->GetFileSize());
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return -1;
	} catch (...) {
		cfile->SetError("Unknown error occurred when getting file size");
		return -1;
	}
}

sabot_sql_state sabot_sql_file_handle_seek(sabot_sql_file_handle file, int64_t position) {
	if (!file || position < 0) {
		return SabotSQLError;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		cfile->handle->Seek(static_cast<idx_t>(position));
		return SabotSQLSuccess;
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return SabotSQLError;
	} catch (...) {
		cfile->SetError("Unknown error occurred when seeking in file");
		return SabotSQLError;
	}
}

sabot_sql_state sabot_sql_file_handle_sync(sabot_sql_file_handle file) {
	if (!file) {
		return SabotSQLError;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		cfile->handle->Sync();
		return SabotSQLSuccess;
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return SabotSQLError;
	} catch (...) {
		cfile->SetError("Unknown error occurred when syncing file");
		return SabotSQLError;
	}
}

sabot_sql_state sabot_sql_file_handle_close(sabot_sql_file_handle file) {
	if (!file) {
		return SabotSQLError;
	}
	auto cfile = reinterpret_cast<sabot_sql::CFileHandle *>(file);
	try {
		cfile->handle->Close();
		return SabotSQLSuccess;
	} catch (std::exception &ex) {
		cfile->SetError(ex);
		return SabotSQLError;
	} catch (...) {
		cfile->SetError("Unknown error occurred when closing file");
		return SabotSQLError;
	}
}
