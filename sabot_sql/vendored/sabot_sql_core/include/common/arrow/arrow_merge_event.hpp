//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/arrow_merge_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/base_pipeline_event.hpp"
#include "sabot_sql/parallel/task.hpp"
#include "sabot_sql/common/types/batched_data_collection.hpp"
#include "sabot_sql/parallel/pipeline.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/shared_ptr.hpp"
#include "sabot_sql/common/arrow/arrow_converter.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"
#include "sabot_sql/main/chunk_scan_state/batched_data_collection.hpp"
#include "sabot_sql/execution/executor.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/unique_ptr.hpp"
#include "sabot_sql/common/helper.hpp"
#include "sabot_sql/common/arrow/arrow_query_result.hpp"

namespace sabot_sql {

// Task to create one RecordBatch by (partially) scanning a BatchedDataCollection
class ArrowBatchTask : public ExecutorTask {
public:
	ArrowBatchTask(ArrowQueryResult &result, vector<idx_t> record_batch_indices, Executor &executor,
	               shared_ptr<Event> event_p, BatchCollectionChunkScanState scan_state, vector<string> names,
	               idx_t batch_size);
	void ProduceRecordBatches();
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "ArrowBatchTask";
	}

private:
	ArrowQueryResult &result;
	vector<idx_t> record_batch_indices;
	shared_ptr<Event> event;
	idx_t batch_size;
	vector<string> names;
	BatchCollectionChunkScanState scan_state;
};

class ArrowMergeEvent : public BasePipelineEvent {
public:
	ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p);

public:
	void Schedule() override;

public:
	ArrowQueryResult &result;
	BatchedDataCollection &batches;

private:
	//! The max size of a record batch to output
	idx_t record_batch_size;
};

} // namespace sabot_sql
