//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parallel/pipeline_complete_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parallel/event.hpp"

namespace sabot_sql {
class Executor;

class PipelineCompleteEvent : public Event {
public:
	PipelineCompleteEvent(Executor &executor, bool complete_pipeline_p);

	bool complete_pipeline;

public:
	void Schedule() override;
	void FinalizeFinish() override;
};

} // namespace sabot_sql
