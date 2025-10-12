//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/pipeline_pre_finish_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/base_pipeline_event.hpp"

namespace sabot_sql {
class Executor;

class PipelinePrepareFinishEvent : public BasePipelineEvent {
public:
	explicit PipelinePrepareFinishEvent(shared_ptr<Pipeline> pipeline);

public:
	void Schedule() override;
	void FinishEvent() override;
};

} // namespace sabot_sql
