//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/file_compression_type.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/file_buffer.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/enums/file_glob_options.hpp"
#include "sabot_sql/common/optional_ptr.hpp"
#include "sabot_sql/common/optional_idx.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/file_open_flags.hpp"
#include "sabot_sql/common/open_file_info.hpp"
#include <functional>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace sabot_sql {

class AttachedDatabase;
class DatabaseInstance;
class FileOpener;
class FileSystem;
class Logger;
class ClientContext;
class QueryContext;

enum class FileType {
	//! Regular file
	FILE_TYPE_REGULAR,
	//! Directory
	FILE_TYPE_DIR,
	//! FIFO named pipe
	FILE_TYPE_FIFO,
	//! Socket
	FILE_TYPE_SOCKET,
	//! Symbolic link
	FILE_TYPE_LINK,
	//! Block device
	FILE_TYPE_BLOCKDEV,
	//! Character device
	FILE_TYPE_CHARDEV,
	//! Unknown or invalid file handle
	FILE_TYPE_INVALID,
};

struct FileHandle {
public:
	SABOT_SQL_API FileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	FileHandle(const FileHandle &) = delete;
	SABOT_SQL_API virtual ~FileHandle();

	// Read at [nr_bytes] bytes into [buffer], and return the bytes actually read.
	// File offset will be changed, which advances for number of bytes read.
	SABOT_SQL_API int64_t Read(void *buffer, idx_t nr_bytes);
	SABOT_SQL_API int64_t Read(QueryContext context, void *buffer, idx_t nr_bytes);
	SABOT_SQL_API int64_t Write(void *buffer, idx_t nr_bytes);
	// Read at [nr_bytes] bytes into [buffer].
	// File offset will not be changed.
	SABOT_SQL_API void Read(void *buffer, idx_t nr_bytes, idx_t location);
	SABOT_SQL_API void Read(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location);
	SABOT_SQL_API void Write(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location);
	SABOT_SQL_API void Seek(idx_t location);
	SABOT_SQL_API void Reset();
	SABOT_SQL_API idx_t SeekPosition();
	SABOT_SQL_API void Sync();
	SABOT_SQL_API void Truncate(int64_t new_size);
	SABOT_SQL_API string ReadLine();
	SABOT_SQL_API string ReadLine(QueryContext context);
	SABOT_SQL_API bool Trim(idx_t offset_bytes, idx_t length_bytes);
	SABOT_SQL_API virtual idx_t GetProgress();
	SABOT_SQL_API virtual FileCompressionType GetFileCompressionType();

	SABOT_SQL_API bool CanSeek();
	SABOT_SQL_API bool IsPipe();
	SABOT_SQL_API bool OnDiskFile();
	SABOT_SQL_API idx_t GetFileSize();
	SABOT_SQL_API FileType GetType();

	SABOT_SQL_API void TryAddLogger(FileOpener &opener);

	//! Closes the file handle.
	SABOT_SQL_API virtual void Close() = 0;

	string GetPath() const {
		return path;
	}

	FileOpenFlags GetFlags() const {
		return flags;
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	FileSystem &file_system;
	string path;
	FileOpenFlags flags;

	shared_ptr<Logger> logger;
};

class FileSystem {
public:
	SABOT_SQL_API virtual ~FileSystem();

public:
	SABOT_SQL_API static FileSystem &GetFileSystem(ClientContext &context);
	SABOT_SQL_API static FileSystem &GetFileSystem(DatabaseInstance &db);
	SABOT_SQL_API static FileSystem &Get(AttachedDatabase &db);

	SABOT_SQL_API virtual unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                                   optional_ptr<FileOpener> opener = nullptr);
	SABOT_SQL_API unique_ptr<FileHandle> OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                           optional_ptr<FileOpener> opener = nullptr);

	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	SABOT_SQL_API virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be written. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	SABOT_SQL_API virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	SABOT_SQL_API virtual int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	SABOT_SQL_API virtual int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Excise a range of the file. The OS can drop pages from the page-cache, and the file-system is free to deallocate
	//! this range (sparse file support). Reads to the range will succeed but will return undefined data.
	SABOT_SQL_API virtual bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes);

	//! Returns the file size of a file handle, returns -1 on error
	SABOT_SQL_API virtual int64_t GetFileSize(FileHandle &handle);
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	SABOT_SQL_API virtual timestamp_t GetLastModifiedTime(FileHandle &handle);
	//! Returns a tag that uniquely identifies the version of the file,
	//! used for checking cache invalidation for CachingFileSystem httpfs files
	SABOT_SQL_API virtual string GetVersionTag(FileHandle &handle);
	//! Returns the file type of the attached handle
	SABOT_SQL_API virtual FileType GetFileType(FileHandle &handle);
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	SABOT_SQL_API virtual void Truncate(FileHandle &handle, int64_t new_size);

	//! Check if a directory exists
	SABOT_SQL_API virtual bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr);
	//! Create a directory if it does not exist
	SABOT_SQL_API virtual void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr);
	//! Helper function that uses DirectoryExists and CreateDirectory to ensure all directories in path are created
	SABOT_SQL_API virtual void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr);
	//! Recursively remove a directory and all files in it
	SABOT_SQL_API virtual void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr);

	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	SABOT_SQL_API virtual bool ListFiles(const string &directory,
	                                  const std::function<void(const string &, bool)> &callback,
	                                  FileOpener *opener = nullptr);
	SABOT_SQL_API bool ListFiles(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                          optional_ptr<FileOpener> opener = nullptr);

	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	SABOT_SQL_API virtual void MoveFile(const string &source, const string &target,
	                                 optional_ptr<FileOpener> opener = nullptr);
	//! Check if a file exists
	SABOT_SQL_API virtual bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Check if path is pipe
	SABOT_SQL_API virtual bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Remove a file from disk
	SABOT_SQL_API virtual void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Remvoe a file from disk if it exists - if it does not exist, return false
	SABOT_SQL_API virtual bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Sync a file handle to disk
	SABOT_SQL_API virtual void FileSync(FileHandle &handle);
	//! Sets the working directory
	SABOT_SQL_API static void SetWorkingDirectory(const string &path);
	//! Gets the working directory
	SABOT_SQL_API static string GetWorkingDirectory();
	//! Gets the users home directory
	SABOT_SQL_API static string GetHomeDirectory(optional_ptr<FileOpener> opener);
	//! Gets the users home directory
	SABOT_SQL_API virtual string GetHomeDirectory();
	//! Expands a given path, including e.g. expanding the home directory of the user
	SABOT_SQL_API static string ExpandPath(const string &path, optional_ptr<FileOpener> opener);
	//! Expands a given path, including e.g. expanding the home directory of the user
	SABOT_SQL_API virtual string ExpandPath(const string &path);
	//! Returns the system-available memory in bytes. Returns DConstants::INVALID_INDEX if the system function fails.
	SABOT_SQL_API static optional_idx GetAvailableMemory();
	//! Returns the space available on the disk. Returns DConstants::INVALID_INDEX if the information was not available.
	SABOT_SQL_API static optional_idx GetAvailableDiskSpace(const string &path);
	//! Path separator for path
	SABOT_SQL_API virtual string PathSeparator(const string &path);
	//! Checks if path is starts with separator (i.e., '/' on UNIX '\\' on Windows)
	SABOT_SQL_API bool IsPathAbsolute(const string &path);
	//! Normalize an absolute path - the goal of normalizing is converting "\test.db" and "C:/test.db" into "C:\test.db"
	//! so that the database system cache can correctly
	SABOT_SQL_API string NormalizeAbsolutePath(const string &path);
	//! Join two paths together
	SABOT_SQL_API string JoinPath(const string &a, const string &path);
	//! Convert separators in a path to the local separators (e.g. convert "/" into \\ on windows)
	SABOT_SQL_API string ConvertSeparators(const string &path);
	//! Extract the base name of a file (e.g. if the input is lib/example.dll the base name is 'example')
	SABOT_SQL_API string ExtractBaseName(const string &path);
	//! Extract the extension of a file (e.g. if the input is lib/example.dll the extension is 'dll')
	SABOT_SQL_API string ExtractExtension(const string &path);
	//! Extract the name of a file (e.g if the input is lib/example.dll the name is 'example.dll')
	SABOT_SQL_API string ExtractName(const string &path);

	//! Returns the value of an environment variable - or the empty string if it is not set
	SABOT_SQL_API static string GetEnvVariable(const string &name);

	//! Whether there is a glob in the string
	SABOT_SQL_API static bool HasGlob(const string &str);
	//! Runs a glob on the file system, returning a list of matching files
	SABOT_SQL_API virtual vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr);
	SABOT_SQL_API vector<OpenFileInfo> GlobFiles(const string &path, ClientContext &context,
	                                          const FileGlobInput &input = FileGlobOptions::DISALLOW_EMPTY);

	//! registers a sub-file system to handle certain file name prefixes, e.g. http:// etc.
	SABOT_SQL_API virtual void RegisterSubSystem(unique_ptr<FileSystem> sub_fs);
	SABOT_SQL_API virtual void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs);

	//! Unregister a sub-filesystem by name
	SABOT_SQL_API virtual void UnregisterSubSystem(const string &name);

	// !Extract a sub-filesystem by name, with ownership transfered, return nullptr if not registered or the subsystem
	// has been disabled.
	SABOT_SQL_API virtual unique_ptr<FileSystem> ExtractSubSystem(const string &name);

	//! List registered sub-filesystems, including builtin ones
	SABOT_SQL_API virtual vector<string> ListSubSystems();

	//! Whether or not a sub-system can handle a specific file path
	SABOT_SQL_API virtual bool CanHandleFile(const string &fpath);

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	SABOT_SQL_API virtual void Seek(FileHandle &handle, idx_t location);
	//! Reset a file to the beginning (equivalent to Seek(handle, 0) for simple files)
	SABOT_SQL_API virtual void Reset(FileHandle &handle);
	SABOT_SQL_API virtual idx_t SeekPosition(FileHandle &handle);

	//! If FS was manually set by the user
	SABOT_SQL_API virtual bool IsManuallySet();
	//! Whether or not we can seek into the file
	SABOT_SQL_API virtual bool CanSeek();
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	SABOT_SQL_API virtual bool OnDiskFile(FileHandle &handle);

	SABOT_SQL_API virtual unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
	                                                             bool write);

	//! Create a LocalFileSystem.
	SABOT_SQL_API static unique_ptr<FileSystem> CreateLocal();

	//! Return the name of the filesytem. Used for forming diagnosis messages.
	SABOT_SQL_API virtual std::string GetName() const = 0;

	//! Whether or not a file is remote or local, based only on file path
	SABOT_SQL_API static bool IsRemoteFile(const string &path);
	SABOT_SQL_API static bool IsRemoteFile(const string &path, string &extension);

	SABOT_SQL_API virtual void SetDisabledFileSystems(const vector<string> &names);
	SABOT_SQL_API virtual bool SubSystemIsDisabled(const string &name);

	SABOT_SQL_API static bool IsDirectory(const OpenFileInfo &info);

protected:
	SABOT_SQL_API virtual unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                                           optional_ptr<FileOpener> opener);
	SABOT_SQL_API virtual bool SupportsOpenFileExtended() const;

	SABOT_SQL_API virtual bool ListFilesExtended(const string &directory,
	                                          const std::function<void(OpenFileInfo &info)> &callback,
	                                          optional_ptr<FileOpener> opener);
	SABOT_SQL_API virtual bool SupportsListFilesExtended() const;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace sabot_sql
