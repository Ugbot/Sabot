//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/pipeline_finish_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/base_pipeline_event.hpp"

namespace sabot_sql {

class Executor;

class PipelineInitializeEvent : public BasePipelineEvent {
public:
	explicit PipelineInitializeEvent(shared_ptr<Pipeline> pipeline);

public:
	void Schedule() override;
	void FinishEvent() override;
};

} // namespace sabot_sql
