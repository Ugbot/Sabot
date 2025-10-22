#!/usr/bin/env python3
"""Test parser with all 9 benchmark queries."""

import sys
from pathlib import Path

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot._cython.graph.compiler.cypher_parser import CypherParser

# All 9 benchmark queries
BENCHMARK_QUERIES = [
    # Query 1: Top 3 most-followed persons
    """MATCH (follower:Person)-[:Follows]->(person:Person)
       RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
       ORDER BY numFollowers DESC LIMIT 3""",

    # Query 2: City where most-followed person lives (WITH clause)
    """MATCH (follower:Person)-[:Follows]->(person:Person)
       WITH person, count(follower.id) as numFollowers
       ORDER BY numFollowers DESC LIMIT 1
       MATCH (person)-[:LivesIn]->(city:City)
       RETURN person.name, numFollowers, city.city, city.state, city.country""",

    # Query 3: 5 cities with lowest average age
    """MATCH (p:Person)-[:LivesIn]->(c:City)-[*1..2]->(co:Country)
       WHERE co.country = 'United States'
       RETURN c.city AS city, avg(p.age) AS averageAge
       ORDER BY averageAge LIMIT 5""",

    # Query 4: Persons between ages 30-40 per country
    """MATCH (p:Person)-[:LivesIn]->(ci:City)-[*1..2]->(country:Country)
       WHERE p.age >= 30 AND p.age <= 40
       RETURN country.country AS countries, count(country) AS personCounts
       ORDER BY personCounts DESC LIMIT 3""",

    # Query 5: Men in London UK interested in fine dining (WITH clause)
    """MATCH (p:Person)-[:HasInterest]->(i:Interest)
       WHERE lower(i.interest) = lower('fine dining')
       AND lower(p.gender) = lower('male')
       WITH p, i
       MATCH (p)-[:LivesIn]->(c:City)
       WHERE c.city = 'London' AND c.country = 'United Kingdom'
       RETURN count(p) AS numPersons""",

    # Query 6: City with most women interested in tennis (WITH clause)
    """MATCH (p:Person)-[:HasInterest]->(i:Interest)
       WHERE lower(i.interest) = lower('tennis')
       AND lower(p.gender) = lower('female')
       WITH p, i
       MATCH (p)-[:LivesIn]->(c:City)
       RETURN count(p.id) AS numPersons, c.city AS city, c.country AS country
       ORDER BY numPersons DESC LIMIT 5""",

    # Query 7: US state with most 23-30yo interested in photography (WITH clause)
    """MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
       WHERE p.age >= 23 AND p.age <= 30 AND s.country = 'United States'
       WITH p, s
       MATCH (p)-[:HasInterest]->(i:Interest)
       WHERE lower(i.interest) = lower('photography')
       RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
       ORDER BY numPersons DESC LIMIT 1""",

    # Query 8: Count of 2-hop paths
    """MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
       RETURN count(*) AS numPaths""",

    # Query 9: Paths through persons <50yo to persons >25yo
    """MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
       WHERE b.age < 50 AND c.age > 25
       RETURN count(*) as numPaths"""
]

print("Testing Cypher Parser with All 9 Benchmark Queries")
print("=" * 70)

parser = CypherParser()
passed = 0
failed = 0

for i, query in enumerate(BENCHMARK_QUERIES, 1):
    print(f"\n{'=' * 70}")
    print(f"Query {i}:")
    print(f"{query.strip()[:100]}..." if len(query.strip()) > 100 else query.strip())
    print()

    try:
        ast = parser.parse(query)

        # Verify it's a CypherQuery
        if not hasattr(ast, 'match_clauses'):
            print(f"❌ FAILED: Not a CypherQuery object")
            failed += 1
            continue

        print(f"✅ PARSED SUCCESSFULLY!")
        print(f"   MATCH clauses: {len(ast.match_clauses)}")
        print(f"   WITH clauses: {len(ast.with_clauses)}")
        print(f"   Has RETURN: {ast.return_clause is not None}")

        # Show pattern details
        if ast.match_clauses:
            match = ast.match_clauses[0]
            if match.pattern and match.pattern.elements:
                elem = match.pattern.elements[0]
                print(f"   Pattern: {len(elem.nodes)} nodes, {len(elem.edges)} edges")

        # Highlight WITH clause queries
        if ast.with_clauses:
            print(f"   🎯 WITH CLAUSE QUERY (critical for benchmark)")

        passed += 1

    except Exception as e:
        print(f"❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        failed += 1

print(f"\n{'=' * 70}")
print(f"RESULTS: {passed}/9 queries passed, {failed}/9 failed")
print()

if passed == 9:
    print("🎉 ALL 9 QUERIES PARSED SUCCESSFULLY!")
    print("✅ Parser is ready for benchmark integration")
else:
    print(f"⚠️  {failed} queries failed - debugging needed")

sys.exit(0 if failed == 0 else 1)
