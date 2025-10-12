//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/enums/expression_type.hpp"
#include "sabot_sql/common/stack_checker.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/parser/group_by_node.hpp"
#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/parser/parsed_data/create_secret_info.hpp"
#include "sabot_sql/parser/qualified_name.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/query_node/cte_node.hpp"
#include "sabot_sql/parser/tokens.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/primnodes.hpp"
#include "pg_definitions.hpp"
#include "sabot_sql/parser/expression/parameter_expression.hpp"
#include "sabot_sql/common/enums/on_entry_not_found.hpp"

namespace sabot_sql {

class ColumnDefinition;
struct OrderByNode;
struct CopyInfo;
struct CommonTableExpressionInfo;
struct GroupingExpressionMap;
class OnConflictInfo;
class UpdateSetInfo;
class MacroFunction;
struct ParserOptions;
struct PivotColumn;
struct PivotColumnEntry;

//! The transformer class is responsible for transforming the internal Postgres
//! parser representation into the SabotSQL representation
class Transformer {
	friend class StackChecker<Transformer>;

	struct CreatePivotEntry {
		string enum_name;
		unique_ptr<SelectNode> base;
		unique_ptr<ParsedExpression> column;
		unique_ptr<QueryNode> subquery;
		bool has_parameters;
	};

public:
	explicit Transformer(ParserOptions &options);
	Transformer(Transformer &parent);
	~Transformer();

	//! Transforms a Postgres parse tree into a set of SQL Statements
	bool TransformParseTree(sabot_sql_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements);
	string NodetypeToString(sabot_sql_libpgquery::PGNodeTag type);

	idx_t ParamCount() const;

private:
	optional_ptr<Transformer> parent;
	//! Parser options
	ParserOptions &options;
	//! The current prepared statement parameter index
	idx_t prepared_statement_parameter_index = 0;
	//! Map from named parameter to parameter index;
	case_insensitive_map_t<idx_t> named_param_map;
	//! Last parameter type
	PreparedParamType last_param_type = PreparedParamType::INVALID;
	//! Holds window expressions defined by name. We need those when transforming the expressions referring to them.
	case_insensitive_map_t<sabot_sql_libpgquery::PGWindowDef *> window_clauses;
	//! The set of pivot entries to create
	vector<unique_ptr<CreatePivotEntry>> pivot_entries;
	//! Sets of stored CTEs, if any
	vector<CommonTableExpressionMap *> stored_cte_map;
	//! Whether or not we are currently binding a window definition
	bool in_window_definition = false;

	void Clear();
	bool InWindowDefinition();

	Transformer &RootTransformer();
	const Transformer &RootTransformer() const;
	void SetParamCount(idx_t new_count);
	void ClearParameters();
	void SetParam(const string &name, idx_t index, PreparedParamType type);
	bool GetParam(const string &name, idx_t &index, PreparedParamType type);

	void AddPivotEntry(string enum_name, unique_ptr<SelectNode> source, unique_ptr<ParsedExpression> column,
	                   unique_ptr<QueryNode> subquery, bool has_parameters);
	unique_ptr<SQLStatement> GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry);
	bool HasPivotEntries();
	idx_t PivotEntryCount();
	vector<unique_ptr<CreatePivotEntry>> &GetPivotEntries();
	void PivotEntryCheck(const string &type);
	void ExtractCTEsRecursive(CommonTableExpressionMap &cte_map);

private:
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatement(sabot_sql_libpgquery::PGNode &stmt);
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatementInternal(sabot_sql_libpgquery::PGNode &stmt);
	//===--------------------------------------------------------------------===//
	// Statement transformation
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres sabot_sql_libpgquery::T_PGSelectStmt node into a SelectStatement
	unique_ptr<SelectStatement> TransformSelectStmt(sabot_sql_libpgquery::PGSelectStmt &select, bool is_select = true);
	unique_ptr<SelectStatement> TransformSelectStmt(sabot_sql_libpgquery::PGNode &node, bool is_select = true);
	//! Transform a Postgres T_AlterStmt node into a AlterStatement
	unique_ptr<AlterStatement> TransformAlter(sabot_sql_libpgquery::PGAlterTableStmt &stmt);
	//! Transform a Postgres T_AlterDatabaseStmt node into a AlterStatement
	unique_ptr<AlterStatement> TransformAlterDatabase(sabot_sql_libpgquery::PGAlterDatabaseStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGRenameStmt node into a RenameStatement
	unique_ptr<AlterStatement> TransformRename(sabot_sql_libpgquery::PGRenameStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateTable(sabot_sql_libpgquery::PGCreateStmt &node);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateTableAs(sabot_sql_libpgquery::PGCreateTableAsStmt &stmt);
	//! Transform a Postgres node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateSchema(sabot_sql_libpgquery::PGCreateSchemaStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateSeqStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateSequence(sabot_sql_libpgquery::PGCreateSeqStmt &node);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGViewStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateView(sabot_sql_libpgquery::PGViewStmt &node);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGIndexStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateIndex(sabot_sql_libpgquery::PGIndexStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateFunctionStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateFunction(sabot_sql_libpgquery::PGCreateFunctionStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateTypeStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateType(sabot_sql_libpgquery::PGCreateTypeStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCreateTypeStmt node into CreateStatement
	unique_ptr<AlterStatement> TransformCommentOn(sabot_sql_libpgquery::PGCommentOnStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGAlterSeqStmt node into CreateStatement
	unique_ptr<AlterStatement> TransformAlterSequence(sabot_sql_libpgquery::PGAlterSeqStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGDropStmt node into a Drop[Table,Schema]Statement
	unique_ptr<SQLStatement> TransformDrop(sabot_sql_libpgquery::PGDropStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGInsertStmt node into a InsertStatement
	unique_ptr<InsertStatement> TransformInsert(sabot_sql_libpgquery::PGInsertStmt &stmt);
	InsertColumnOrder TransformColumnOrder(sabot_sql_libpgquery::PGInsertColumnOrder insert_column_order);

	vector<string> TransformInsertColumns(sabot_sql_libpgquery::PGList &cols);

	//! Transform a Postgres sabot_sql_libpgquery::T_PGOnConflictClause node into a OnConflictInfo
	unique_ptr<OnConflictInfo> TransformOnConflictClause(sabot_sql_libpgquery::PGOnConflictClause *node,
	                                                     const string &relname);
	//! Transform a ON CONFLICT shorthand into a OnConflictInfo
	unique_ptr<OnConflictInfo> DummyOnConflictClause(sabot_sql_libpgquery::PGOnConflictActionAlias type,
	                                                 const string &relname);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGCopyStmt node into a CopyStatement
	unique_ptr<CopyStatement> TransformCopy(sabot_sql_libpgquery::PGCopyStmt &stmt);
	void TransformCopyOptions(CopyInfo &info, optional_ptr<sabot_sql_libpgquery::PGList> options);
	void TransformCreateSecretOptions(CreateSecretInfo &info, optional_ptr<sabot_sql_libpgquery::PGList> options);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGTransactionStmt node into a TransactionStatement
	unique_ptr<TransactionStatement> TransformTransaction(sabot_sql_libpgquery::PGTransactionStmt &stmt);
	//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
	unique_ptr<DeleteStatement> TransformDelete(sabot_sql_libpgquery::PGDeleteStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGUpdateStmt node into a UpdateStatement
	unique_ptr<UpdateStatement> TransformUpdate(sabot_sql_libpgquery::PGUpdateStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGUpdateExtensionsStmt node into a UpdateExtensionsStatement
	unique_ptr<UpdateExtensionsStatement> TransformUpdateExtensions(sabot_sql_libpgquery::PGUpdateExtensionsStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGPragmaStmt node into a PragmaStatement
	unique_ptr<SQLStatement> TransformPragma(sabot_sql_libpgquery::PGPragmaStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGExportStmt node into a ExportStatement
	unique_ptr<ExportStatement> TransformExport(sabot_sql_libpgquery::PGExportStmt &stmt);
	//! Transform a Postgres sabot_sql_libpgquery::T_PGImportStmt node into a PragmaStatement
	unique_ptr<PragmaStatement> TransformImport(sabot_sql_libpgquery::PGImportStmt &stmt);
	unique_ptr<ExplainStatement> TransformExplain(sabot_sql_libpgquery::PGExplainStmt &stmt);
	unique_ptr<SQLStatement> TransformVacuum(sabot_sql_libpgquery::PGVacuumStmt &stmt);
	unique_ptr<QueryNode> TransformShow(sabot_sql_libpgquery::PGVariableShowStmt &stmt);
	unique_ptr<SelectStatement> TransformShowStmt(sabot_sql_libpgquery::PGVariableShowStmt &stmt);
	unique_ptr<QueryNode> TransformShowSelect(sabot_sql_libpgquery::PGVariableShowSelectStmt &stmt);
	unique_ptr<SelectStatement> TransformShowSelectStmt(sabot_sql_libpgquery::PGVariableShowSelectStmt &stmt);
	unique_ptr<AttachStatement> TransformAttach(sabot_sql_libpgquery::PGAttachStmt &stmt);
	unique_ptr<DetachStatement> TransformDetach(sabot_sql_libpgquery::PGDetachStmt &stmt);
	unique_ptr<SetStatement> TransformUse(sabot_sql_libpgquery::PGUseStmt &stmt);
	unique_ptr<SQLStatement> TransformCopyDatabase(sabot_sql_libpgquery::PGCopyDatabaseStmt &stmt);
	unique_ptr<CreateStatement> TransformSecret(sabot_sql_libpgquery::PGCreateSecretStmt &stmt);
	unique_ptr<DropStatement> TransformDropSecret(sabot_sql_libpgquery::PGDropSecretStmt &stmt);

	unique_ptr<PrepareStatement> TransformPrepare(sabot_sql_libpgquery::PGPrepareStmt &stmt);
	unique_ptr<ExecuteStatement> TransformExecute(sabot_sql_libpgquery::PGExecuteStmt &stmt);
	unique_ptr<CallStatement> TransformCall(sabot_sql_libpgquery::PGCallStmt &stmt);
	unique_ptr<DropStatement> TransformDeallocate(sabot_sql_libpgquery::PGDeallocateStmt &stmt);
	unique_ptr<QueryNode> TransformPivotStatement(sabot_sql_libpgquery::PGSelectStmt &select);
	unique_ptr<SQLStatement> CreatePivotStatement(unique_ptr<SQLStatement> statement);
	PivotColumn TransformPivotColumn(sabot_sql_libpgquery::PGPivot &pivot, bool is_pivot);
	vector<PivotColumn> TransformPivotList(sabot_sql_libpgquery::PGList &list, bool is_pivot);
	static bool TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry,
	                                 bool root_entry = true);

	unique_ptr<SQLStatement> TransformMergeInto(sabot_sql_libpgquery::PGMergeIntoStmt &stmt);
	unique_ptr<MergeIntoAction> TransformMergeIntoAction(sabot_sql_libpgquery::PGMatchAction &action);

	//===--------------------------------------------------------------------===//
	// SetStatement Transform
	//===--------------------------------------------------------------------===//
	unique_ptr<SetStatement> TransformSet(sabot_sql_libpgquery::PGVariableSetStmt &set);
	unique_ptr<SetStatement> TransformSetVariable(sabot_sql_libpgquery::PGVariableSetStmt &stmt);
	unique_ptr<SetStatement> TransformResetVariable(sabot_sql_libpgquery::PGVariableSetStmt &stmt);

	unique_ptr<SQLStatement> TransformCheckpoint(sabot_sql_libpgquery::PGCheckPointStmt &stmt);
	unique_ptr<LoadStatement> TransformLoad(sabot_sql_libpgquery::PGLoadStmt &stmt);

	//===--------------------------------------------------------------------===//
	// Query Node Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres sabot_sql_libpgquery::T_PGSelectStmt node into a QueryNode
	unique_ptr<QueryNode> TransformSelectNode(sabot_sql_libpgquery::PGNode &select, bool is_select = true);
	unique_ptr<QueryNode> TransformSelectNodeInternal(sabot_sql_libpgquery::PGSelectStmt &select, bool is_select = true);
	unique_ptr<QueryNode> TransformSelectInternal(sabot_sql_libpgquery::PGSelectStmt &select);
	void TransformModifiers(sabot_sql_libpgquery::PGSelectStmt &stmt, QueryNode &node);
	bool SetOperationsMatch(sabot_sql_libpgquery::PGSelectStmt &root, sabot_sql_libpgquery::PGNode &node);
	void TransformSetOperationChildren(sabot_sql_libpgquery::PGSelectStmt &stmt, SetOperationNode &result);

	//===--------------------------------------------------------------------===//
	// Expression Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<ParsedExpression> TransformBoolExpr(sabot_sql_libpgquery::PGBoolExpr &root);
	//! Transform a Postgres case expression into an Expression
	unique_ptr<ParsedExpression> TransformCase(sabot_sql_libpgquery::PGCaseExpr &root);
	//! Transform a Postgres type cast into an Expression
	unique_ptr<ParsedExpression> TransformTypeCast(sabot_sql_libpgquery::PGTypeCast &root);
	//! Transform a Postgres coalesce into an Expression
	unique_ptr<ParsedExpression> TransformCoalesce(sabot_sql_libpgquery::PGAExpr &root);
	//! Transform a Postgres column reference into an Expression
	unique_ptr<ParsedExpression> TransformColumnRef(sabot_sql_libpgquery::PGColumnRef &root);
	//! Transform a Postgres constant value into an Expression
	unique_ptr<ConstantExpression> TransformValue(sabot_sql_libpgquery::PGValue val);
	//! Transform a Postgres operator into an Expression
	unique_ptr<ParsedExpression> TransformAExpr(sabot_sql_libpgquery::PGAExpr &root);
	unique_ptr<ParsedExpression> TransformAExprInternal(sabot_sql_libpgquery::PGAExpr &root);
	//! Transform a Postgres abstract expression into an Expression
	unique_ptr<ParsedExpression> TransformExpression(optional_ptr<sabot_sql_libpgquery::PGNode> node);
	unique_ptr<ParsedExpression> TransformExpression(sabot_sql_libpgquery::PGNode &node);
	//! Transform a Postgres function call into an Expression
	unique_ptr<ParsedExpression> TransformFuncCall(sabot_sql_libpgquery::PGFuncCall &root);
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<ParsedExpression> TransformInterval(sabot_sql_libpgquery::PGIntervalConstant &root);
	//! Transform a LAMBDA node (e.g., lambda x, y: x + y) into a lambda expression.
	unique_ptr<ParsedExpression> TransformLambda(sabot_sql_libpgquery::PGLambdaFunction &node);
	//! Transform a single arrow operator (e.g., (x, y) -> x + y) into a lambda expression.
	unique_ptr<ParsedExpression> TransformSingleArrow(sabot_sql_libpgquery::PGSingleArrowFunction &node);
	//! Transform a Postgres array access node (e.g. x[1] or x[1:3])
	unique_ptr<ParsedExpression> TransformArrayAccess(sabot_sql_libpgquery::PGAIndirection &node);
	//! Transform a positional reference (e.g. #1)
	unique_ptr<ParsedExpression> TransformPositionalReference(sabot_sql_libpgquery::PGPositionalReference &node);
	unique_ptr<ParsedExpression> TransformStarExpression(sabot_sql_libpgquery::PGAStar &node);
	unique_ptr<ParsedExpression> TransformBooleanTest(sabot_sql_libpgquery::PGBooleanTest &node);

	//! Transform a Postgres constant value into an Expression
	unique_ptr<ParsedExpression> TransformConstant(sabot_sql_libpgquery::PGAConst &c);
	unique_ptr<ParsedExpression> TransformGroupingFunction(sabot_sql_libpgquery::PGGroupingFunc &n);
	unique_ptr<ParsedExpression> TransformResTarget(sabot_sql_libpgquery::PGResTarget &root);
	unique_ptr<ParsedExpression> TransformNullTest(sabot_sql_libpgquery::PGNullTest &root);
	unique_ptr<ParsedExpression> TransformParamRef(sabot_sql_libpgquery::PGParamRef &node);
	unique_ptr<ParsedExpression> TransformNamedArg(sabot_sql_libpgquery::PGNamedArgExpr &root);

	//! Transform multi assignment reference into an Expression
	unique_ptr<ParsedExpression> TransformMultiAssignRef(sabot_sql_libpgquery::PGMultiAssignRef &root);

	unique_ptr<ParsedExpression> TransformSQLValueFunction(sabot_sql_libpgquery::PGSQLValueFunction &node);

	unique_ptr<ParsedExpression> TransformSubquery(sabot_sql_libpgquery::PGSubLink &root);
	//===--------------------------------------------------------------------===//
	// Constraints transform
	//===--------------------------------------------------------------------===//
	unique_ptr<Constraint> TransformConstraint(sabot_sql_libpgquery::PGConstraint &constraint);
	unique_ptr<Constraint> TransformConstraint(sabot_sql_libpgquery::PGConstraint &constraint, ColumnDefinition &column,
	                                           idx_t index);

	//===--------------------------------------------------------------------===//
	// Update transform
	//===--------------------------------------------------------------------===//
	unique_ptr<UpdateSetInfo> TransformUpdateSetInfo(sabot_sql_libpgquery::PGList *target_list,
	                                                 sabot_sql_libpgquery::PGNode *where_clause);

	//===--------------------------------------------------------------------===//
	// Index transform
	//===--------------------------------------------------------------------===//
	vector<unique_ptr<ParsedExpression>> TransformIndexParameters(sabot_sql_libpgquery::PGList &list,
	                                                              const string &relation_name);

	//===--------------------------------------------------------------------===//
	// Collation transform
	//===--------------------------------------------------------------------===//
	unique_ptr<ParsedExpression> TransformCollateExpr(sabot_sql_libpgquery::PGCollateClause &collate);

	string TransformCollation(optional_ptr<sabot_sql_libpgquery::PGCollateClause> collate);

	ColumnDefinition TransformColumnDefinition(sabot_sql_libpgquery::PGColumnDef &cdef);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	OnCreateConflict TransformOnConflict(sabot_sql_libpgquery::PGOnCreateConflict conflict);
	string TransformAlias(sabot_sql_libpgquery::PGAlias *root, vector<string> &column_name_alias);
	vector<string> TransformStringList(sabot_sql_libpgquery::PGList *list);
	void TransformCTE(sabot_sql_libpgquery::PGWithClause &de_with_clause, CommonTableExpressionMap &cte_map);
	static unique_ptr<QueryNode> TransformMaterializedCTE(unique_ptr<QueryNode> root);
	unique_ptr<SelectStatement> TransformRecursiveCTE(sabot_sql_libpgquery::PGCommonTableExpr &node,
	                                                  CommonTableExpressionInfo &info);

	unique_ptr<ParsedExpression> TransformUnaryOperator(const string &op, unique_ptr<ParsedExpression> child);
	unique_ptr<ParsedExpression> TransformBinaryOperator(string op, unique_ptr<ParsedExpression> left,
	                                                     unique_ptr<ParsedExpression> right);
	static bool ConstructConstantFromExpression(const ParsedExpression &expr, Value &value);
	//===--------------------------------------------------------------------===//
	// TableRef transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres node into a TableRef
	unique_ptr<TableRef> TransformTableRefNode(sabot_sql_libpgquery::PGNode &n);
	//! Transform a Postgres FROM clause into a TableRef
	unique_ptr<TableRef> TransformFrom(optional_ptr<sabot_sql_libpgquery::PGList> root);
	//! Transform a Postgres table reference into a TableRef
	unique_ptr<TableRef> TransformRangeVar(sabot_sql_libpgquery::PGRangeVar &root);
	//! Transform a Postgres table-producing function into a TableRef
	unique_ptr<TableRef> TransformRangeFunction(sabot_sql_libpgquery::PGRangeFunction &root);
	//! Transform a Postgres join node into a TableRef
	unique_ptr<TableRef> TransformJoin(sabot_sql_libpgquery::PGJoinExpr &root);
	//! Transform a Postgres pivot node into a TableRef
	unique_ptr<TableRef> TransformPivot(sabot_sql_libpgquery::PGPivotExpr &root);
	//! Transform a table producing subquery into a TableRef
	unique_ptr<TableRef> TransformRangeSubselect(sabot_sql_libpgquery::PGRangeSubselect &root);
	//! Transform a VALUES list into a set of expressions
	unique_ptr<TableRef> TransformValuesList(sabot_sql_libpgquery::PGList *list);

	//! Transform using clause
	vector<string> TransformUsingClause(sabot_sql_libpgquery::PGList &usingClause);

	//! Transform a range var into a (schema) qualified name
	QualifiedName TransformQualifiedName(sabot_sql_libpgquery::PGRangeVar &root);

	//! Transform a Postgres TypeName string into a LogicalType (non-LIST types)
	LogicalType TransformTypeNameInternal(sabot_sql_libpgquery::PGTypeName &name);
	//! Transform a Postgres TypeName string into a LogicalType
	LogicalType TransformTypeName(sabot_sql_libpgquery::PGTypeName &name);

	//! Transform a list of type modifiers into a list of values
	vector<Value> TransformTypeModifiers(sabot_sql_libpgquery::PGTypeName &name);

	//! Transform a Postgres GROUP BY expression into a list of Expression
	bool TransformGroupBy(optional_ptr<sabot_sql_libpgquery::PGList> group, SelectNode &result);
	void TransformGroupByNode(sabot_sql_libpgquery::PGNode &n, GroupingExpressionMap &map, SelectNode &result,
	                          vector<GroupingSet> &result_sets);
	void AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map, GroupByNode &result,
	                          vector<idx_t> &result_set);
	void TransformGroupByExpression(sabot_sql_libpgquery::PGNode &n, GroupingExpressionMap &map, GroupByNode &result,
	                                vector<idx_t> &result_set);
	//! Transform a Postgres ORDER BY expression into an OrderByDescription
	bool TransformOrderBy(sabot_sql_libpgquery::PGList *order, vector<OrderByNode> &result);

	//! Transform to a IN or NOT IN expression
	unique_ptr<ParsedExpression> TransformInExpression(const string &name, sabot_sql_libpgquery::PGAExpr &root);

	//! Transform a Postgres SELECT clause into a list of Expressions
	void TransformExpressionList(sabot_sql_libpgquery::PGList &list, vector<unique_ptr<ParsedExpression>> &result);

	//! Transform a Postgres PARTITION BY/ORDER BY specification into lists of expressions
	void TransformWindowDef(sabot_sql_libpgquery::PGWindowDef &window_spec, WindowExpression &expr,
	                        const char *window_name = nullptr);
	//! Transform a Postgres window frame specification into frame expressions
	void TransformWindowFrame(sabot_sql_libpgquery::PGWindowDef &window_spec, WindowExpression &expr);

	unique_ptr<SampleOptions> TransformSampleOptions(optional_ptr<sabot_sql_libpgquery::PGNode> options);
	//! Returns true if an expression is only a star (i.e. "*", without any other decorators)
	bool ExpressionIsEmptyStar(ParsedExpression &expr);

	OnEntryNotFound TransformOnEntryNotFound(bool missing_ok);

	Vector PGListToVector(optional_ptr<sabot_sql_libpgquery::PGList> column_list, idx_t &size);
	vector<string> TransformConflictTarget(sabot_sql_libpgquery::PGList &list);

	unique_ptr<MacroFunction> TransformMacroFunction(sabot_sql_libpgquery::PGFunctionDefinition &function);

	vector<string> TransformNameList(sabot_sql_libpgquery::PGList &list);

public:
	static void SetQueryLocation(ParsedExpression &expr, int query_location);
	static void SetQueryLocation(TableRef &ref, int query_location);

private:
	//! Current stack depth
	idx_t stack_depth;

	void InitializeStackCheck();
	StackChecker<Transformer> StackCheck(idx_t extra_stack = 1);

public:
	template <class T>
	static T &PGCast(sabot_sql_libpgquery::PGNode &node) {
		return reinterpret_cast<T &>(node);
	}
	template <class T>
	static optional_ptr<T> PGPointerCast(void *ptr) {
		return optional_ptr<T>(reinterpret_cast<T *>(ptr));
	}
};

vector<string> ReadPgListToString(sabot_sql_libpgquery::PGList *column_list);

} // namespace sabot_sql
