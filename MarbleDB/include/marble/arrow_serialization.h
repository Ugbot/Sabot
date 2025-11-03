/**
 * Arrow Serialization Utilities
 *
 * Provides serialization and deserialization functions for Arrow RecordBatches
 * using Arrow IPC (Inter-Process Communication) format.
 *
 * These utilities enable:
 * - Efficient binary serialization of RecordBatch data
 * - Network transmission of columnar data
 * - Persistent storage of Arrow data
 * - Zero-copy data exchange between processes
 */

#pragma once

#include <memory>
#include <string>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include "marble/status.h"

namespace marble {

/**
 * @brief Serialize an Arrow RecordBatch to bytes using Arrow IPC format
 *
 * @param batch RecordBatch to serialize (must not be null)
 * @param serialized_data Output string to receive serialized bytes
 * @return Status OK if successful, error otherwise
 */
Status SerializeArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                          std::string* serialized_data);

/**
 * @brief Deserialize an Arrow RecordBatch from bytes using Arrow IPC format
 *
 * @param data Pointer to serialized data
 * @param size Size of serialized data in bytes
 * @param batch Output pointer to receive deserialized RecordBatch
 * @return Status OK if successful, error otherwise
 */
Status DeserializeArrowBatch(const void* data, size_t size,
                           std::shared_ptr<arrow::RecordBatch>* batch);

/**
 * @brief Deserialize an Arrow RecordBatch from string using Arrow IPC format
 *
 * @param data String containing serialized data
 * @param batch Output pointer to receive deserialized RecordBatch
 * @return Status OK if successful, error otherwise
 */
Status DeserializeArrowBatch(const std::string& data,
                           std::shared_ptr<arrow::RecordBatch>* batch);

} // namespace marble
