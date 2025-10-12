//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/base_pipeline_event.hpp"

namespace sabot_sql {

//! A PipelineEvent is responsible for scheduling a pipeline
class PipelineEvent : public BasePipelineEvent {
public:
	explicit PipelineEvent(shared_ptr<Pipeline> pipeline);

public:
	void Schedule() override;
	void FinishEvent() override;
};

} // namespace sabot_sql
