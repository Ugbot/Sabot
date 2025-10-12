//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/join_type.hpp"
#include "sabot_sql/common/enums/relation_type.hpp"
#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/common/enums/joinref_type.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/common/named_parameter_map.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/client_context_wrapper.hpp"
#include "sabot_sql/main/external_dependencies.hpp"
#include "sabot_sql/parser/statement/explain_statement.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/result_modifier.hpp"
#include "sabot_sql/common/unique_ptr.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/helper.hpp"

namespace sabot_sql {
struct BoundStatement;

class Binder;
class LogicalOperator;
class QueryNode;
class TableRef;

static string CreateRelationAlias(RelationType type, const string &alias) {
	if (!alias.empty()) {
		return alias;
	}
	return StringUtil::Format("%s_%s", EnumUtil::ToString(type), StringUtil::GenerateRandomName());
}

class RelationContextWrapper : public ClientContextWrapper {
public:
	~RelationContextWrapper() override = default;
	explicit RelationContextWrapper(const shared_ptr<ClientContext> &context) : ClientContextWrapper(context) {};

	explicit RelationContextWrapper(const ClientContextWrapper &context) : ClientContextWrapper(context) {};

	void TryBindRelation(Relation &relation, vector<ColumnDefinition> &columns) override {
		GetContext()->TryBindRelation(relation, columns);
	}

private:
	weak_ptr<ClientContext> client_context;
};

class Relation : public enable_shared_from_this<Relation> {
public:
	Relation(const shared_ptr<ClientContext> &context_p, const RelationType type) : type(type) {
		context = make_shared_ptr<ClientContextWrapper>(context_p);
	}
	Relation(const shared_ptr<ClientContextWrapper> &context, RelationType type, const string &alias_p = "")
	    : context(context), type(type), alias(CreateRelationAlias(type, alias_p)) {
	}

	Relation(const shared_ptr<RelationContextWrapper> &context, RelationType type, const string &alias_p = "")
	    : context(context), type(type), alias(CreateRelationAlias(type, alias_p)) {
	}

	virtual ~Relation() = default;

	shared_ptr<ClientContextWrapper> context;
	RelationType type;
	const string alias;
	vector<shared_ptr<ExternalDependency>> external_dependencies;

public:
	SABOT_SQL_API virtual const vector<ColumnDefinition> &Columns() = 0;
	SABOT_SQL_API virtual unique_ptr<QueryNode> GetQueryNode();
	SABOT_SQL_API virtual BoundStatement Bind(Binder &binder);
	SABOT_SQL_API virtual string GetAlias();

	SABOT_SQL_API unique_ptr<QueryResult> ExecuteOrThrow();
	SABOT_SQL_API unique_ptr<QueryResult> Execute();
	SABOT_SQL_API string ToString();
	SABOT_SQL_API virtual string ToString(idx_t depth) = 0;

	SABOT_SQL_API void Print();
	SABOT_SQL_API void Head(idx_t limit = 10);

	SABOT_SQL_API shared_ptr<Relation> CreateView(const string &name, bool replace = true, bool temporary = false);
	SABOT_SQL_API shared_ptr<Relation> CreateView(const string &schema_name, const string &name, bool replace = true,
	                                           bool temporary = false);
	SABOT_SQL_API unique_ptr<QueryResult> Query(const string &sql) const;
	SABOT_SQL_API unique_ptr<QueryResult> Query(const string &name, const string &sql);

	//! Explain the query plan of this relation
	SABOT_SQL_API unique_ptr<QueryResult> Explain(ExplainType type = ExplainType::EXPLAIN_STANDARD,
	                                           ExplainFormat explain_format = ExplainFormat::DEFAULT);

	SABOT_SQL_API virtual unique_ptr<TableRef> GetTableRef();
	virtual bool IsReadOnly() {
		return true;
	}
	SABOT_SQL_API void TryBindRelation(vector<ColumnDefinition> &columns);

public:
	// PROJECT
	SABOT_SQL_API shared_ptr<Relation> Project(const string &select_list);
	SABOT_SQL_API shared_ptr<Relation> Project(const string &expression, const string &alias);
	SABOT_SQL_API shared_ptr<Relation> Project(const string &select_list, const vector<string> &aliases);
	SABOT_SQL_API shared_ptr<Relation> Project(const vector<string> &expressions);
	SABOT_SQL_API shared_ptr<Relation> Project(const vector<string> &expressions, const vector<string> &aliases);
	SABOT_SQL_API shared_ptr<Relation> Project(vector<unique_ptr<ParsedExpression>> expressions,
	                                        const vector<string> &aliases);

	// FILTER
	SABOT_SQL_API shared_ptr<Relation> Filter(const string &expression);
	SABOT_SQL_API shared_ptr<Relation> Filter(unique_ptr<ParsedExpression> expression);
	SABOT_SQL_API shared_ptr<Relation> Filter(const vector<string> &expressions);

	// LIMIT
	SABOT_SQL_API shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	SABOT_SQL_API shared_ptr<Relation> Order(const string &expression);
	SABOT_SQL_API shared_ptr<Relation> Order(const vector<string> &expressions);
	SABOT_SQL_API shared_ptr<Relation> Order(vector<OrderByNode> expressions);

	// JOIN operation
	SABOT_SQL_API shared_ptr<Relation> Join(const shared_ptr<Relation> &other, const string &condition,
	                                     JoinType type = JoinType::INNER, JoinRefType ref_type = JoinRefType::REGULAR);
	shared_ptr<Relation> Join(const shared_ptr<Relation> &other, vector<unique_ptr<ParsedExpression>> condition,
	                          JoinType type = JoinType::INNER, JoinRefType ref_type = JoinRefType::REGULAR);

	// CROSS PRODUCT operation
	SABOT_SQL_API shared_ptr<Relation> CrossProduct(const shared_ptr<Relation> &other,
	                                             JoinRefType join_ref_type = JoinRefType::CROSS);

	// SET operations
	SABOT_SQL_API shared_ptr<Relation> Union(const shared_ptr<Relation> &other);
	SABOT_SQL_API shared_ptr<Relation> Except(const shared_ptr<Relation> &other);
	SABOT_SQL_API shared_ptr<Relation> Intersect(const shared_ptr<Relation> &other);

	// DISTINCT operation
	SABOT_SQL_API shared_ptr<Relation> Distinct();

	// AGGREGATES
	SABOT_SQL_API shared_ptr<Relation> Aggregate(const string &aggregate_list);
	SABOT_SQL_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates);
	SABOT_SQL_API shared_ptr<Relation> Aggregate(vector<unique_ptr<ParsedExpression>> expressions);
	SABOT_SQL_API shared_ptr<Relation> Aggregate(const string &aggregate_list, const string &group_list);
	SABOT_SQL_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates, const vector<string> &groups);
	SABOT_SQL_API shared_ptr<Relation> Aggregate(vector<unique_ptr<ParsedExpression>> expressions,
	                                          const string &group_list);

	// ALIAS
	SABOT_SQL_API shared_ptr<Relation> Alias(const string &alias);

	//! Insert the data from this relation into a table
	SABOT_SQL_API shared_ptr<Relation> InsertRel(const string &schema_name, const string &table_name);
	SABOT_SQL_API void Insert(const string &table_name);
	SABOT_SQL_API void Insert(const string &schema_name, const string &table_name);
	//! Insert a row (i.e.,list of values) into a table
	SABOT_SQL_API void Insert(const vector<vector<Value>> &values);
	SABOT_SQL_API void Insert(vector<vector<unique_ptr<ParsedExpression>>> &&expressions);
	//! Create a table and insert the data from this relation into that table
	SABOT_SQL_API shared_ptr<Relation> CreateRel(const string &schema_name, const string &table_name,
	                                          bool temporary = false,
	                                          OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT);
	SABOT_SQL_API void Create(const string &table_name, bool temporary = false,
	                       OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT);
	SABOT_SQL_API void Create(const string &schema_name, const string &table_name, bool temporary = false,
	                       OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT);

	//! Write a relation to a CSV file
	SABOT_SQL_API shared_ptr<Relation>
	WriteCSVRel(const string &csv_file,
	            case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	SABOT_SQL_API void WriteCSV(const string &csv_file,
	                         case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	//! Write a relation to a Parquet file
	SABOT_SQL_API shared_ptr<Relation>
	WriteParquetRel(const string &parquet_file,
	                case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	SABOT_SQL_API void
	WriteParquet(const string &parquet_file,
	             case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());

	//! Update a table, can only be used on a TableRelation
	SABOT_SQL_API virtual void Update(const string &update, const string &condition = string());
	SABOT_SQL_API virtual void Update(vector<string> column_names, vector<unique_ptr<ParsedExpression>> &&update,
	                               unique_ptr<ParsedExpression> condition = nullptr);
	//! Delete from a table, can only be used on a TableRelation
	SABOT_SQL_API virtual void Delete(const string &condition = string());
	//! Create a relation from calling a table in/out function on the input relation
	//! Create a relation from calling a table in/out function on the input relation
	SABOT_SQL_API shared_ptr<Relation> TableFunction(const std::string &fname, const vector<Value> &values);
	SABOT_SQL_API shared_ptr<Relation> TableFunction(const std::string &fname, const vector<Value> &values,
	                                              const named_parameter_map_t &named_parameters);

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	virtual bool InheritsColumnBindings() {
		return false;
	}
	virtual Relation *ChildRelation() {
		return nullptr;
	}
	void AddExternalDependency(shared_ptr<ExternalDependency> dependency);
	SABOT_SQL_API vector<shared_ptr<ExternalDependency>> GetAllDependencies();

protected:
	SABOT_SQL_API static string RenderWhitespace(idx_t depth);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace sabot_sql
