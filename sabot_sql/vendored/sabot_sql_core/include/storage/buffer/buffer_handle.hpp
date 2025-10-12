//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/buffer/buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/storage_info.hpp"
#include "sabot_sql/common/file_buffer.hpp"

namespace sabot_sql {
class BlockHandle;
class FileBuffer;

class BufferHandle {
public:
	SABOT_SQL_API BufferHandle();
	SABOT_SQL_API explicit BufferHandle(shared_ptr<BlockHandle> handle, optional_ptr<FileBuffer> node);
	SABOT_SQL_API ~BufferHandle();
	// disable copy constructors
	BufferHandle(const BufferHandle &other) = delete;
	BufferHandle &operator=(const BufferHandle &) = delete;
	//! enable move constructors
	SABOT_SQL_API BufferHandle(BufferHandle &&other) noexcept;
	SABOT_SQL_API BufferHandle &operator=(BufferHandle &&) noexcept;

public:
	//! Returns whether or not the BufferHandle is valid.
	SABOT_SQL_API bool IsValid() const;
	//! Returns a pointer to the buffer data. Handle must be valid.
	inline data_ptr_t Ptr() const {
		D_ASSERT(IsValid());
		return node->buffer;
	}
	//! Returns a pointer to the buffer data. Handle must be valid.
	inline data_ptr_t Ptr() {
		D_ASSERT(IsValid());
		return node->buffer;
	}
	//! Gets the underlying file buffer. Handle must be valid.
	SABOT_SQL_API FileBuffer &GetFileBuffer();
	//! Destroys the buffer handle
	SABOT_SQL_API void Destroy();

	const shared_ptr<BlockHandle> &GetBlockHandle() const {
		return handle;
	}

private:
	//! The block handle
	shared_ptr<BlockHandle> handle;
	//! The managed buffer node
	optional_ptr<FileBuffer> node;
};

} // namespace sabot_sql
