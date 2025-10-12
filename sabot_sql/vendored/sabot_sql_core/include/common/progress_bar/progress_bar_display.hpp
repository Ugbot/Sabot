//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/progress_bar/progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace sabot_sql {

class ProgressBarDisplay {
public:
	ProgressBarDisplay() {
	}
	virtual ~ProgressBarDisplay() {
	}

public:
	virtual void Update(double percentage) = 0;
	virtual void Finish() = 0;
};

} // namespace sabot_sql
