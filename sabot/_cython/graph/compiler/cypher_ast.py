"""
Cypher AST Node Definitions

Abstract Syntax Tree nodes for Cypher queries.
Adapted from KuzuDB parser/query/graph_pattern/*.h
(MIT License, Copyright 2022-2025 Kùzu Inc.)

These Python classes mirror KuzuDB's C++ AST structure but are
simplified for Sabot's use case.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


# ============================================================================
# Enums (adapted from KuzuDB)
# ============================================================================

class ArrowDirection(Enum):
    """
    Edge direction in pattern.
    From kuzu/src/include/parser/query/graph_pattern/rel_pattern.h
    """
    LEFT = "left"    # <-
    RIGHT = "right"  # ->
    BOTH = "both"    # -


class ComparisonOp(Enum):
    """Comparison operators."""
    EQ = "="      # Equal
    NEQ = "<>"    # Not equal
    LT = "<"      # Less than
    LTE = "<="    # Less than or equal
    GT = ">"      # Greater than
    GTE = ">="    # Greater than or equal


class BinaryOpType(Enum):
    """Binary operators."""
    AND = "AND"
    OR = "OR"
    XOR = "XOR"
    PLUS = "+"
    MINUS = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    MODULO = "%"


# ============================================================================
# Expression Nodes
# ============================================================================

@dataclass
class Expression:
    """Base class for all expressions."""
    pass


@dataclass
class Literal(Expression):
    """
    Literal value expression.
    From kuzu/src/include/parser/expression/parsed_literal_expression.h
    """
    value: Any  # int, float, str, bool, None


@dataclass
class Variable(Expression):
    """
    Variable reference (e.g., 'a', 'b', 'r').
    From kuzu/src/include/parser/expression/parsed_variable_expression.h
    """
    name: str


@dataclass
class PropertyAccess(Expression):
    """
    Property access expression (e.g., a.name, b.age).
    From kuzu/src/include/parser/expression/parsed_property_expression.h
    """
    variable: str
    property_name: str


@dataclass
class Comparison(Expression):
    """Comparison expression (e.g., a.age > 18)."""
    left: Expression
    op: ComparisonOp
    right: Expression


@dataclass
class BinaryOp(Expression):
    """Binary operation (e.g., a.age + 10, x AND y)."""
    left: Expression
    op: BinaryOpType
    right: Expression


@dataclass
class FunctionCall(Expression):
    """Function call (e.g., count(*), sum(a.age))."""
    name: str
    args: List[Expression] = field(default_factory=list)
    distinct: bool = False


# ============================================================================
# Pattern Nodes (adapted from KuzuDB)
# ============================================================================

@dataclass
class NodePattern:
    """
    Node pattern in MATCH clause.
    Adapted from kuzu/src/include/parser/query/graph_pattern/node_pattern.h

    Examples:
        (a)                  → variable='a', labels=[]
        (a:Person)           → variable='a', labels=['Person']
        (a:Person {age: 25}) → variable='a', labels=['Person'], properties={'age': 25}
        (:Person)            → variable=None, labels=['Person']
    """
    variable: Optional[str] = None
    labels: List[str] = field(default_factory=list)  # KuzuDB: tableNames
    properties: Dict[str, Expression] = field(default_factory=dict)  # KuzuDB: propertyKeyVals


@dataclass
class RecursiveInfo:
    """
    Variable-length path information.
    Adapted from kuzu/src/include/parser/query/graph_pattern/rel_pattern.h

    Examples:
        *        → min_hops=1, max_hops=unbounded
        *2       → min_hops=2, max_hops=2
        *1..3    → min_hops=1, max_hops=3
        *..5     → min_hops=1, max_hops=5
        *3..     → min_hops=3, max_hops=unbounded
    """
    min_hops: int = 1
    max_hops: Optional[int] = None  # None = unbounded
    where_filter: Optional[Expression] = None


@dataclass
class RelPattern:
    """
    Relationship pattern in MATCH clause.
    Adapted from kuzu/src/include/parser/query/graph_pattern/rel_pattern.h

    Examples:
        -[r]->                   → variable='r', types=[], direction=RIGHT
        -[r:KNOWS]->             → variable='r', types=['KNOWS'], direction=RIGHT
        <-[r:FOLLOWS]-           → variable='r', types=['FOLLOWS'], direction=LEFT
        -[r:KNOWS|FOLLOWS]->     → variable='r', types=['KNOWS', 'FOLLOWS']
        -[r:KNOWS {since: 2020}]-> → variable='r', types=['KNOWS'], properties={'since': 2020}
        -[r:KNOWS*1..3]->        → variable='r', types=['KNOWS'], recursive=RecursiveInfo(1, 3)
    """
    variable: Optional[str] = None
    types: List[str] = field(default_factory=list)  # KuzuDB: tableNames
    direction: ArrowDirection = ArrowDirection.RIGHT
    properties: Dict[str, Expression] = field(default_factory=dict)
    recursive: Optional[RecursiveInfo] = None  # For variable-length paths


@dataclass
class PatternElement:
    """
    Complete pattern element: node-edge-node chain.
    Adapted from kuzu/src/include/parser/query/graph_pattern/pattern_element.h

    Example:
        (a:Person)-[r:KNOWS]->(b:Person)
        → nodes = [NodePattern(a, Person), NodePattern(b, Person)]
        → edges = [RelPattern(r, KNOWS, RIGHT)]
    """
    nodes: List[NodePattern] = field(default_factory=list)
    edges: List[RelPattern] = field(default_factory=list)


@dataclass
class Pattern:
    """
    Complete MATCH pattern (can have multiple elements).

    Example:
        MATCH (a)-[r1]->(b), (c)-[r2]->(d)
        → elements = [PatternElement(...), PatternElement(...)]
    """
    elements: List[PatternElement] = field(default_factory=list)


# ============================================================================
# Query Clauses
# ============================================================================

@dataclass
class MatchClause:
    """
    MATCH clause.
    Adapted from kuzu/src/include/parser/query/reading_clause/match_clause.h

    Example:
        MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 18
    """
    pattern: Pattern
    where: Optional[Expression] = None
    optional: bool = False  # For OPTIONAL MATCH


@dataclass
class WhereClause:
    """WHERE clause with filter expression."""
    expression: Expression


@dataclass
class ProjectionItem:
    """
    Single item in RETURN/WITH clause.

    Examples:
        a           → expression=Variable('a'), alias=None
        a.name      → expression=PropertyAccess('a', 'name'), alias=None
        a.name AS n → expression=PropertyAccess('a', 'name'), alias='n'
    """
    expression: Expression
    alias: Optional[str] = None


@dataclass
class OrderBy:
    """ORDER BY specification."""
    expression: Expression
    ascending: bool = True


@dataclass
class ReturnClause:
    """
    RETURN clause.
    Adapted from kuzu/src/include/parser/query/return_with_clause/return_clause.h

    Example:
        RETURN a.name, b.age ORDER BY b.age DESC LIMIT 10
    """
    items: List[ProjectionItem] = field(default_factory=list)
    distinct: bool = False
    order_by: List[OrderBy] = field(default_factory=list)
    skip: Optional[int] = None
    limit: Optional[int] = None


@dataclass
class WithClause:
    """
    WITH clause for multi-stage query pipelines.
    Adapted from kuzu/src/include/parser/query/return_with_clause/with_clause.h

    Examples:
        WITH person, count(follower.id) as numFollowers

        WITH person WHERE person.age > 18

        WITH person, avg(person.age) as avgAge
        ORDER BY avgAge DESC LIMIT 10

    The WITH clause creates an intermediate result that can be:
    1. Projected (select specific columns/expressions)
    2. Filtered (WHERE clause after projection)
    3. Ordered and limited (ORDER BY, SKIP, LIMIT)
    4. Used as input for the next MATCH or WITH clause
    """
    items: List[ProjectionItem] = field(default_factory=list)
    distinct: bool = False
    where: Optional[Expression] = None  # WHERE after WITH
    order_by: List[OrderBy] = field(default_factory=list)
    skip: Optional[int] = None
    limit: Optional[int] = None


# ============================================================================
# Top-Level Query
# ============================================================================

@dataclass
class CypherQuery:
    """
    Complete Cypher query with optional WITH clauses.
    Adapted from kuzu/src/include/parser/query/regular_query.h

    Simple query example:
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 18
        RETURN a.name, b.name
        LIMIT 10

    Multi-stage WITH example:
        MATCH (follower:Person)-[:Follows]->(person:Person)
        WITH person, count(follower.id) as numFollowers
        ORDER BY numFollowers DESC LIMIT 1
        MATCH (person)-[:LivesIn]->(city:City)
        RETURN person.name, numFollowers, city.city

    The WITH clause creates a pipeline where:
    - First MATCH generates intermediate results
    - WITH projects/aggregates/filters those results
    - Second MATCH joins with the WITH results
    - RETURN produces final output
    """
    match_clauses: List[MatchClause] = field(default_factory=list)
    with_clauses: List[WithClause] = field(default_factory=list)  # NEW: multi-stage pipeline
    where_clause: Optional[WhereClause] = None
    return_clause: Optional[ReturnClause] = None

    def __repr__(self):
        parts = []
        if self.match_clauses:
            parts.append(f"MATCH ({len(self.match_clauses)} patterns)")
        if self.with_clauses:
            parts.append(f"WITH ({len(self.with_clauses)} stages)")
        if self.where_clause:
            parts.append("WHERE")
        if self.return_clause:
            parts.append(f"RETURN ({len(self.return_clause.items)} items)")
        return f"CypherQuery({', '.join(parts)})"


# ============================================================================
# Utility Functions
# ============================================================================

def pattern_is_2hop(pattern: Pattern) -> bool:
    """
    Check if pattern is a simple 2-hop pattern: (a)-[r]->(b).

    Returns True if:
    - Single pattern element
    - Exactly 2 nodes and 1 edge
    - No variable-length paths
    """
    if len(pattern.elements) != 1:
        return False

    element = pattern.elements[0]
    if len(element.nodes) != 2 or len(element.edges) != 1:
        return False

    # Check no recursive edges
    edge = element.edges[0]
    if edge.recursive is not None:
        return False

    return True


def pattern_is_3hop(pattern: Pattern) -> bool:
    """
    Check if pattern is a 3-hop pattern: (a)-[r1]->(b)-[r2]->(c).
    """
    if len(pattern.elements) != 1:
        return False

    element = pattern.elements[0]
    if len(element.nodes) != 3 or len(element.edges) != 2:
        return False

    # Check no recursive edges
    for edge in element.edges:
        if edge.recursive is not None:
            return False

    return True


def pattern_is_variable_length(pattern: Pattern) -> bool:
    """Check if pattern contains variable-length edges."""
    for element in pattern.elements:
        for edge in element.edges:
            if edge.recursive is not None:
                return True
    return False
