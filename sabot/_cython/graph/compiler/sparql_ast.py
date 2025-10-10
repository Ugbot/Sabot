"""
SPARQL AST Node Definitions

Abstract Syntax Tree nodes for SPARQL queries.
Inspired by QLever (https://github.com/ad-freiburg/qlever)
and Apache Jena ARQ query engine.

Reference:
- QLever: Efficient SPARQL query engine
- W3C SPARQL 1.1 Specification
- Apache Jena ARQ

This implementation supports a subset of SPARQL 1.1:
- Basic Graph Patterns (BGP)
- FILTER expressions
- SELECT with projection
- LIMIT and OFFSET
- Triple patterns with variables
"""

from dataclasses import dataclass, field
from typing import List, Optional, Union, Any
from enum import Enum


# ============================================================================
# RDF Terms
# ============================================================================

class TermType(Enum):
    """RDF term types."""
    IRI = "iri"           # <http://example.org/person/Alice>
    LITERAL = "literal"   # "Alice"
    VARIABLE = "variable" # ?name
    BLANK = "blank"       # _:b1


@dataclass
class RDFTerm:
    """Base class for RDF terms."""
    pass


@dataclass
class IRI(RDFTerm):
    """
    IRI (Internationalized Resource Identifier).

    Examples:
        <http://example.org/person/Alice>
        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
    """
    value: str  # Full IRI without < >


@dataclass
class Literal(RDFTerm):
    """
    RDF Literal with optional language tag or datatype.

    Examples:
        "Alice"
        "Alice"@en
        "42"^^<http://www.w3.org/2001/XMLSchema#integer>
    """
    value: str
    language: Optional[str] = None  # Language tag (e.g., "en")
    datatype: Optional[str] = None  # Datatype IRI


@dataclass
class Variable(RDFTerm):
    """
    SPARQL variable.

    Examples:
        ?person
        ?name
        $x (alternative syntax)
    """
    name: str  # Variable name without ? or $


@dataclass
class BlankNode(RDFTerm):
    """
    Blank node.

    Examples:
        _:b1
        []
    """
    id: Optional[str] = None  # Blank node ID or None for anonymous


# ============================================================================
# Triple Patterns
# ============================================================================

@dataclass
class TriplePattern:
    """
    Single triple pattern in a Basic Graph Pattern.

    Example:
        ?person rdf:type foaf:Person
        → subject=Variable("person")
        → predicate=IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        → object=IRI("http://xmlns.com/foaf/0.1/Person")
    """
    subject: Union[IRI, Variable, BlankNode]
    predicate: Union[IRI, Variable]
    object: Union[IRI, Literal, Variable, BlankNode]


@dataclass
class BasicGraphPattern:
    """
    Basic Graph Pattern (BGP) - a set of triple patterns.

    Example:
        { ?person rdf:type foaf:Person .
          ?person foaf:name ?name . }

        → triples=[
            TriplePattern(?person, rdf:type, foaf:Person),
            TriplePattern(?person, foaf:name, ?name)
          ]
    """
    triples: List[TriplePattern] = field(default_factory=list)


# ============================================================================
# Filters and Expressions
# ============================================================================

class ComparisonOp(Enum):
    """Comparison operators for FILTER expressions."""
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="


class BooleanOp(Enum):
    """Boolean operators."""
    AND = "&&"
    OR = "||"
    NOT = "!"


@dataclass
class Expression:
    """Base class for filter expressions."""
    pass


@dataclass
class VariableExpr(Expression):
    """Variable in expression."""
    name: str


@dataclass
class LiteralExpr(Expression):
    """Literal value in expression."""
    value: Any


@dataclass
class Comparison(Expression):
    """Comparison expression."""
    left: Expression
    op: ComparisonOp
    right: Expression


@dataclass
class BooleanExpr(Expression):
    """Boolean expression (AND, OR, NOT)."""
    op: BooleanOp
    operands: List[Expression]


@dataclass
class FunctionCall(Expression):
    """
    Built-in SPARQL function call.

    Examples:
        BOUND(?x)
        REGEX(?name, "^A")
        STR(?x)
    """
    name: str
    args: List[Expression] = field(default_factory=list)


@dataclass
class Filter:
    """
    FILTER constraint on graph pattern.

    Example:
        FILTER (?age > 18)
        FILTER (REGEX(?name, "^A"))
    """
    expression: Expression


# ============================================================================
# Query Clauses
# ============================================================================

@dataclass
class WhereClause:
    """
    WHERE clause containing graph patterns and filters.

    Example:
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:age ?age .
            FILTER (?age > 18)
        }
    """
    bgp: BasicGraphPattern
    filters: List[Filter] = field(default_factory=list)


@dataclass
class SelectClause:
    """
    SELECT clause with projection variables.

    Examples:
        SELECT *
        SELECT ?person ?name
        SELECT DISTINCT ?person
    """
    variables: List[str]  # Empty list means SELECT *
    distinct: bool = False


@dataclass
class SolutionModifier:
    """
    Solution modifiers: ORDER BY, LIMIT, OFFSET.

    Example:
        ORDER BY DESC(?age)
        LIMIT 10
        OFFSET 5
    """
    order_by: List[tuple[str, bool]] = field(default_factory=list)  # (var, ascending)
    limit: Optional[int] = None
    offset: Optional[int] = None


# ============================================================================
# Top-Level Query
# ============================================================================

@dataclass
class SPARQLQuery:
    """
    Complete SPARQL SELECT query.

    Example:
        SELECT ?person ?name
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
            FILTER (?name != "")
        }
        ORDER BY ?name
        LIMIT 10
    """
    select_clause: SelectClause
    where_clause: WhereClause
    modifiers: Optional[SolutionModifier] = None

    def __repr__(self):
        parts = []
        if self.select_clause.distinct:
            parts.append("SELECT DISTINCT")
        else:
            parts.append("SELECT")

        if not self.select_clause.variables:
            parts.append("*")
        else:
            parts.append(f"{len(self.select_clause.variables)} vars")

        parts.append(f"WHERE ({len(self.where_clause.bgp.triples)} triples")
        if self.where_clause.filters:
            parts.append(f", {len(self.where_clause.filters)} filters")
        parts.append(")")

        if self.modifiers and self.modifiers.limit:
            parts.append(f"LIMIT {self.modifiers.limit}")

        return f"SPARQLQuery({' '.join(parts)})"


# ============================================================================
# Utility Functions
# ============================================================================

def is_variable(term: RDFTerm) -> bool:
    """Check if term is a variable."""
    return isinstance(term, Variable)


def is_bound(term: RDFTerm) -> bool:
    """Check if term is bound (not a variable)."""
    return not isinstance(term, Variable)


def get_variables_from_bgp(bgp: BasicGraphPattern) -> List[str]:
    """Extract all variables from a BGP."""
    variables = set()

    for triple in bgp.triples:
        if isinstance(triple.subject, Variable):
            variables.add(triple.subject.name)
        if isinstance(triple.predicate, Variable):
            variables.add(triple.predicate.name)
        if isinstance(triple.object, Variable):
            variables.add(triple.object.name)

    return sorted(list(variables))
