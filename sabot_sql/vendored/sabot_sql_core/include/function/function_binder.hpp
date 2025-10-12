//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/function.hpp"
#include "sabot_sql/function/cast/cast_function_set.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/aggregate_function.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/common/exception/binder_exception.hpp"
#include "sabot_sql/common/error_data.hpp"

namespace sabot_sql {

//! The FunctionBinder class is responsible for binding functions
class FunctionBinder {
public:
	SABOT_SQL_API explicit FunctionBinder(Binder &binder);
	SABOT_SQL_API explicit FunctionBinder(ClientContext &context);

	optional_ptr<Binder> binder;
	ClientContext &context;

public:
	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! returns optional_idx() and sets error if none could be found
	SABOT_SQL_API optional_idx BindFunction(const string &name, ScalarFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	SABOT_SQL_API optional_idx BindFunction(const string &name, ScalarFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	SABOT_SQL_API optional_idx BindFunction(const string &name, AggregateFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	SABOT_SQL_API optional_idx BindFunction(const string &name, AggregateFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	SABOT_SQL_API optional_idx BindFunction(const string &name, TableFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	SABOT_SQL_API optional_idx BindFunction(const string &name, TableFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind a pragma function from the set of functions and input arguments
	SABOT_SQL_API optional_idx BindFunction(const string &name, PragmaFunctionSet &functions, vector<Value> &parameters,
	                                     ErrorData &error);

	SABOT_SQL_API unique_ptr<Expression> BindScalarFunction(const string &schema, const string &name,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);
	SABOT_SQL_API unique_ptr<Expression> BindScalarFunction(ScalarFunctionCatalogEntry &function,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	SABOT_SQL_API unique_ptr<Expression> BindScalarFunction(ScalarFunction bound_function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	SABOT_SQL_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(AggregateFunction bound_function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	SABOT_SQL_API static void BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
	                                           const vector<unique_ptr<Expression>> &groups);
	SABOT_SQL_API static void BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr);

	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children);

	void ResolveTemplateTypes(BaseScalarFunction &bound_function, const vector<unique_ptr<Expression>> &children);
	void CheckTemplateTypesResolved(const BaseScalarFunction &bound_function);

private:
	optional_idx BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);
	optional_idx BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);

	template <class T>
	vector<idx_t> BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
	                                         const vector<LogicalType> &arguments, ErrorData &error);

	template <class T>
	optional_idx MultipleCandidateException(const string &catalog_name, const string &schema_name, const string &name,
	                                        FunctionSet<T> &functions, vector<idx_t> &candidate_functions,
	                                        const vector<LogicalType> &arguments, ErrorData &error);

	template <class T>
	optional_idx BindFunctionFromArguments(const string &name, FunctionSet<T> &functions,
	                                       const vector<LogicalType> &arguments, ErrorData &error);

	vector<LogicalType> GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments);
};

} // namespace sabot_sql
