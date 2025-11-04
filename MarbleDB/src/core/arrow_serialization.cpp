#include "marble/arrow_serialization.h"

namespace marble {

// Serialize Arrow RecordBatch to bytes - simplified implementation
Status SerializeArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                          std::string* serialized_data) {
    if (!batch) {
        return Status::InvalidArgument("Null RecordBatch provided");
    }
    if (!serialized_data) {
        return Status::InvalidArgument("Null output string provided");
    }

    // Create output stream backed by string
    auto output_stream = arrow::io::BufferOutputStream::Create();
    if (!output_stream.ok()) {
        return Status::IOError("Failed to create output stream: " + output_stream.status().ToString());
    }

    // Create IPC writer with default options
    auto writer_result = arrow::ipc::MakeStreamWriter(
        output_stream.ValueOrDie(),
        batch->schema());
    if (!writer_result.ok()) {
        return Status::IOError("Failed to create IPC writer: " + writer_result.status().ToString());
    }

    auto writer = writer_result.ValueOrDie();

    // Write the batch
    auto write_status = writer->WriteRecordBatch(*batch);
    if (!write_status.ok()) {
        return Status::IOError("Failed to write RecordBatch: " + write_status.ToString());
    }

    // Close writer to flush data
    auto close_status = writer->Close();
    if (!close_status.ok()) {
        return Status::IOError("Failed to close writer: " + close_status.ToString());
    }

    // Get the buffer
    auto finish_result = output_stream.ValueOrDie()->Finish();
    if (!finish_result.ok()) {
        return Status::IOError("Failed to finish stream: " + finish_result.status().ToString());
    }

    auto buffer = finish_result.ValueOrDie();

    // Copy to output string
    serialized_data->assign(reinterpret_cast<const char*>(buffer->data()), buffer->size());

    return Status::OK();
}

// Deserialize Arrow RecordBatch from bytes - simplified implementation
Status DeserializeArrowBatch(const void* data, size_t size,
                           std::shared_ptr<arrow::RecordBatch>* batch) {
    if (!data) {
        return Status::InvalidArgument("Null data provided");
    }
    if (size == 0) {
        return Status::InvalidArgument("Zero-size data provided");
    }
    if (!batch) {
        return Status::InvalidArgument("Null output batch pointer provided");
    }

    // Create input stream from buffer
    // IMPORTANT: Copy the data to ensure buffer lifetime extends beyond input data
    // Wrap() creates a zero-copy view which becomes invalid when input goes out of scope
    auto buffer_result = arrow::AllocateBuffer(size);
    if (!buffer_result.ok()) {
        return Status::IOError("Failed to allocate buffer: " + buffer_result.status().ToString());
    }
    auto buffer = std::move(buffer_result).ValueOrDie();
    std::memcpy(buffer->mutable_data(), data, size);

    // BufferReader needs a shared_ptr, convert from unique_ptr
    std::shared_ptr<arrow::Buffer> shared_buffer = std::move(buffer);
    auto input_stream = std::make_shared<arrow::io::BufferReader>(shared_buffer);

    // Create IPC reader
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream);
    if (!reader_result.ok()) {
        return Status::IOError("Failed to create IPC reader: " + reader_result.status().ToString());
    }

    auto reader = reader_result.ValueOrDie();

    // Read the next batch (should be the only one in stream format)
    auto read_result = reader->Next();
    if (!read_result.ok()) {
        return Status::IOError("Failed to read RecordBatch: " + read_result.status().ToString());
    }

    *batch = read_result.ValueOrDie();

    if (!*batch) {
        return Status::IOError("No RecordBatch found in stream");
    }

    return Status::OK();
}

// String overload for DeserializeArrowBatch
Status DeserializeArrowBatch(const std::string& data,
                           std::shared_ptr<arrow::RecordBatch>* batch) {
    return DeserializeArrowBatch(data.data(), data.size(), batch);
}

} // namespace marble
