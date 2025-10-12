#pragma once

#include "sabot_sql/main/chunk_scan_state.hpp"
#include "sabot_sql/common/types/batched_data_collection.hpp"
#include "sabot_sql/common/error_data.hpp"

namespace sabot_sql {

class BatchCollectionChunkScanState : public ChunkScanState {
public:
	BatchCollectionChunkScanState(BatchedDataCollection &collection, BatchedChunkIteratorRange &range,
	                              ClientContext &context);
	~BatchCollectionChunkScanState() override;

public:
	BatchCollectionChunkScanState(const BatchCollectionChunkScanState &other) = delete;
	BatchCollectionChunkScanState &operator=(const BatchCollectionChunkScanState &other) = delete;
	BatchCollectionChunkScanState(BatchCollectionChunkScanState &&other) = default;

public:
	bool LoadNextChunk(ErrorData &error) override;
	bool HasError() const override;
	ErrorData &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<string> &Names() const override;

private:
	void InternalLoad(ErrorData &error);

private:
	BatchedDataCollection &collection;
	BatchedChunkScanState state;
};

} // namespace sabot_sql
