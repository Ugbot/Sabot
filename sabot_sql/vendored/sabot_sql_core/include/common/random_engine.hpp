//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/random_engine.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/limits.hpp"
#include "sabot_sql/common/mutex.hpp"

#include <random>

namespace sabot_sql {
class ClientContext;
struct RandomState;

class RandomEngine {
public:
	explicit RandomEngine(int64_t seed = -1);
	~RandomEngine();

	//! Generate a random number between min and max
	double NextRandom(double min, double max);

	//! Generate a random number between 0 and 1
	double NextRandom();
	//! Generate a random number between 0 and 1, using 32-bits as a base
	double NextRandom32();
	double NextRandom32(double min, double max);
	uint32_t NextRandomInteger32(uint32_t min, uint32_t max);
	uint32_t NextRandomInteger();
	uint32_t NextRandomInteger(uint32_t min, uint32_t max);
	uint64_t NextRandomInteger64();

	void SetSeed(uint64_t seed);

	static RandomEngine &Get(ClientContext &context);

	mutex lock;

private:
	unique_ptr<RandomState> random_state;
};

} // namespace sabot_sql
