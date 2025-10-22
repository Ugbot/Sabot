"""
SPARQL Query Suite for Sabot Benchmarks

Collection of SPARQL queries for testing and benchmarking Sabot's SPARQL engine.
Queries range from simple single-pattern matches to complex multi-pattern joins.

Query Categories:
1. Basic pattern matching (Q1-Q2)
2. Filtered queries (Q3)
3. Multi-pattern joins (Q4-Q5)
4. Complex filters and aggregations (Q6-Q10)
"""

# Query 1: Simple Pattern Match
# Description: Find all persons (type matching)
# Pattern: Single triple pattern
# Expected: Fast (<1ms on small datasets)
QUERY_1_SIMPLE_PATTERN = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person
WHERE {
    ?person rdf:type foaf:Person .
}
"""

# Query 2: Property Access
# Description: Find all person names
# Pattern: Two triple patterns (type + property)
# Expected: Join on ?person variable
QUERY_2_PROPERTY_ACCESS = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
}
"""

# Query 3: Filtered Query
# Description: Find persons older than 30
# Pattern: Three triple patterns + FILTER
# Expected: Post-join filtering
QUERY_3_FILTERED = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?age
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    ?person foaf:age ?age .
    FILTER (?age > 30)
}
"""

# Query 4: Relationship Query
# Description: Find friend pairs (who knows whom)
# Pattern: Three triple patterns (2 persons + relationship)
# Expected: Multi-way join
QUERY_4_RELATIONSHIPS = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person1 ?name1 ?person2 ?name2
WHERE {
    ?person1 rdf:type foaf:Person .
    ?person1 foaf:name ?name1 .
    ?person1 foaf:knows ?person2 .
    ?person2 foaf:name ?name2 .
}
"""

# Query 5: 3-Pattern Join
# Description: Find persons with names and ages
# Pattern: Three triple patterns joined on common variable
# Expected: Hash join on ?person
QUERY_5_THREE_PATTERN_JOIN = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?age
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    ?person foaf:age ?age .
}
"""

# Query 6: String Filter
# Description: Find persons with names starting with 'A'
# Pattern: Type + property + string filter
# Expected: Post-filter on string comparison
QUERY_6_STRING_FILTER = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    FILTER (STRSTARTS(?name, "A"))
}
"""

# Query 7: Range Filter
# Description: Find persons aged 25-35
# Pattern: Type + age property + range filter
# Expected: Numeric comparison on literal values
QUERY_7_RANGE_FILTER = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?age
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    ?person foaf:age ?age .
    FILTER (?age >= 25 && ?age <= 35)
}
"""

# Query 8: Transitive Friendship (2-hop)
# Description: Find friends of friends
# Pattern: 4 triple patterns (2 persons + 2 knows relationships)
# Expected: Complex multi-way join
QUERY_8_TRANSITIVE_2HOP = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person1 ?name1 ?person3 ?name3
WHERE {
    ?person1 rdf:type foaf:Person .
    ?person1 foaf:name ?name1 .
    ?person1 foaf:knows ?person2 .
    ?person2 foaf:knows ?person3 .
    ?person3 foaf:name ?name3 .
}
"""

# Query 9: Optional Pattern
# Description: Find persons, optionally with age
# Pattern: Required type + name, optional age
# Expected: LEFT JOIN semantics
QUERY_9_OPTIONAL = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?age
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    OPTIONAL { ?person foaf:age ?age . }
}
"""

# Query 10: Complex Boolean Filter
# Description: Find young (<30) OR old (>35) persons
# Pattern: Type + age + complex OR filter
# Expected: Post-filter with boolean OR
QUERY_10_COMPLEX_FILTER = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?age
WHERE {
    ?person rdf:type foaf:Person .
    ?person foaf:name ?name .
    ?person foaf:age ?age .
    FILTER (?age < 30 || ?age > 35)
}
"""

# Query Suite Dictionary
QUERY_SUITE = {
    'Q1_simple_pattern': {
        'query': QUERY_1_SIMPLE_PATTERN,
        'description': 'Simple pattern match (find all persons)',
        'complexity': 'low',
        'expected_speedup': 'baseline'
    },
    'Q2_property_access': {
        'query': QUERY_2_PROPERTY_ACCESS,
        'description': 'Property access with 2-pattern join',
        'complexity': 'low',
        'expected_speedup': '1x'
    },
    'Q3_filtered': {
        'query': QUERY_3_FILTERED,
        'description': 'Filtered query (age > 30)',
        'complexity': 'medium',
        'expected_speedup': '1-2x (filter optimization)'
    },
    'Q4_relationships': {
        'query': QUERY_4_RELATIONSHIPS,
        'description': 'Relationship query (who knows whom)',
        'complexity': 'medium',
        'expected_speedup': '2-3x (join reordering)'
    },
    'Q5_three_pattern_join': {
        'query': QUERY_5_THREE_PATTERN_JOIN,
        'description': '3-pattern join (person + name + age)',
        'complexity': 'medium',
        'expected_speedup': '2-3x (join reordering)'
    },
    'Q6_string_filter': {
        'query': QUERY_6_STRING_FILTER,
        'description': 'String filter (names starting with A)',
        'complexity': 'medium',
        'expected_speedup': '1-2x (filter pushdown)'
    },
    'Q7_range_filter': {
        'query': QUERY_7_RANGE_FILTER,
        'description': 'Range filter (age 25-35)',
        'complexity': 'medium',
        'expected_speedup': '1-2x (filter pushdown)'
    },
    'Q8_transitive_2hop': {
        'query': QUERY_8_TRANSITIVE_2HOP,
        'description': 'Transitive friendship (friends of friends)',
        'complexity': 'high',
        'expected_speedup': '3-5x (join reordering)'
    },
    'Q9_optional': {
        'query': QUERY_9_OPTIONAL,
        'description': 'Optional pattern (LEFT JOIN)',
        'complexity': 'high',
        'expected_speedup': 'TBD (OPTIONAL not yet implemented)'
    },
    'Q10_complex_filter': {
        'query': QUERY_10_COMPLEX_FILTER,
        'description': 'Complex boolean filter (young OR old)',
        'complexity': 'medium',
        'expected_speedup': '1-2x (filter optimization)'
    }
}


def get_basic_queries():
    """Return basic query suite (Q1-Q5) for quick testing."""
    return {k: v for k, v in QUERY_SUITE.items() if k in [
        'Q1_simple_pattern',
        'Q2_property_access',
        'Q3_filtered',
        'Q4_relationships',
        'Q5_three_pattern_join'
    ]}


def get_all_queries():
    """Return full query suite (Q1-Q10)."""
    return QUERY_SUITE


def get_queries_by_complexity(complexity: str):
    """
    Return queries by complexity level.

    Args:
        complexity: 'low', 'medium', or 'high'

    Returns:
        Dict of queries matching the complexity
    """
    return {k: v for k, v in QUERY_SUITE.items() if v['complexity'] == complexity}


if __name__ == '__main__':
    print("=" * 60)
    print("SPARQL Query Suite")
    print("=" * 60)

    print(f"\nTotal queries: {len(QUERY_SUITE)}")

    print("\nQuery Breakdown by Complexity:")
    for complexity in ['low', 'medium', 'high']:
        queries = get_queries_by_complexity(complexity)
        print(f"  {complexity.capitalize()}: {len(queries)} queries")

    print("\nBasic Query Suite (Q1-Q5):")
    for query_id, query_info in get_basic_queries().items():
        print(f"  {query_id}: {query_info['description']}")

    print("\n✅ Query suite loaded successfully!")
