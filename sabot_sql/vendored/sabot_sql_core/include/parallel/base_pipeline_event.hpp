//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/base_pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/event.hpp"
#include "sabot_sql/parallel/pipeline.hpp"

namespace sabot_sql {

//! A BasePipelineEvent is used as the basis of any event that belongs to a specific pipeline
class BasePipelineEvent : public Event {
public:
	explicit BasePipelineEvent(shared_ptr<Pipeline> pipeline);
	explicit BasePipelineEvent(Pipeline &pipeline);

	void PrintPipeline() override {
		pipeline->Print();
	}

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
};

} // namespace sabot_sql
