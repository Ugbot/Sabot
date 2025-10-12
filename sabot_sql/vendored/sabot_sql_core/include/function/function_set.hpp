//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/aggregate_function.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/function/pragma_function.hpp"

namespace sabot_sql {

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(std::move(name)) {
	}

	//! The name of the function set
	string name;
	//! The set of functions.
	vector<T> functions;

public:
	void AddFunction(T function) {
		functions.push_back(std::move(function));
	}
	idx_t Size() {
		return functions.size();
	}
	T GetFunctionByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}
	T &GetFunctionReferenceByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}
	bool MergeFunctionSet(FunctionSet<T> new_functions, bool override = false) {
		D_ASSERT(!new_functions.functions.empty());
		for (auto &new_func : new_functions.functions) {
			bool overwritten = false;
			for (auto &func : functions) {
				if (new_func.Equal(func)) {
					// function overload already exists
					if (override) {
						// override it
						overwritten = true;
						func = new_func;
					} else {
						// throw an error
						return false;
					}
					break;
				}
			}
			if (!overwritten) {
				functions.push_back(new_func);
			}
		}
		return true;
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	SABOT_SQL_API explicit ScalarFunctionSet();
	SABOT_SQL_API explicit ScalarFunctionSet(string name);
	SABOT_SQL_API explicit ScalarFunctionSet(ScalarFunction fun);

	SABOT_SQL_API ScalarFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	SABOT_SQL_API explicit AggregateFunctionSet();
	SABOT_SQL_API explicit AggregateFunctionSet(string name);
	SABOT_SQL_API explicit AggregateFunctionSet(AggregateFunction fun);

	SABOT_SQL_API AggregateFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	SABOT_SQL_API explicit TableFunctionSet(string name);
	SABOT_SQL_API explicit TableFunctionSet(TableFunction fun);

	TableFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	SABOT_SQL_API explicit PragmaFunctionSet(string name);
	SABOT_SQL_API explicit PragmaFunctionSet(PragmaFunction fun);
};

} // namespace sabot_sql
