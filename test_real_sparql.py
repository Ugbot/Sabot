#!/usr/bin/env python3
"""
Test Real SPARQL Queries (from QLever examples)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sabot import cyarrow as pa

print("=" * 70)
print("Real SPARQL Query Test (QLever Style)")
print("=" * 70)

# Create a simple RDF dataset matching QLever olympics style
print("\n[1] Creating RDF dataset...")

from sabot._cython.graph.storage.graph_storage import PyRDFTripleStore

# Create term dictionary
# IRI terms (0-99): entities
# IRI terms (100-199): predicates
# Literal terms (200+): values

terms_data = {
    'id': [],
    'lex': [],
    'kind': [],  # 0=IRI, 1=Literal
    'lang': [],
    'datatype': []
}

# Entities (people)
entities = {
    0: 'http://example.org/Alice',
    1: 'http://example.org/Bob',
    2: 'http://example.org/Charlie',
}

# Predicates
predicates = {
    100: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    101: 'http://www.w3.org/2000/01/rdf-schema#label',
    102: 'http://example.org/knows',
}

# Types
types = {
    200: 'http://xmlns.com/foaf/0.1/Person',
}

# Literals
literals = {
    300: 'Alice',
    301: 'Bob',
    302: 'Charlie',
}

# Build term dictionary
all_terms = {}
all_terms.update(entities)
all_terms.update(predicates)
all_terms.update(types)
all_terms.update(literals)

for term_id, lex in all_terms.items():
    terms_data['id'].append(term_id)
    terms_data['lex'].append(lex)
    # 0-199 are IRIs, 200+ are literals
    terms_data['kind'].append(0 if term_id < 200 else 1)
    terms_data['lang'].append('')
    terms_data['datatype'].append('')

terms_table = pa.Table.from_pydict({
    'id': pa.array(terms_data['id'], type=pa.int64()),
    'lex': pa.array(terms_data['lex'], type=pa.string()),
    'kind': pa.array(terms_data['kind'], type=pa.uint8()),
    'lang': pa.array(terms_data['lang'], type=pa.string()),
    'datatype': pa.array(terms_data['datatype'], type=pa.string())
})

# Create triples
# Alice rdf:type foaf:Person
# Alice rdfs:label "Alice"
# Alice knows Bob
# Bob rdf:type foaf:Person
# Bob rdfs:label "Bob"
# Charlie rdf:type foaf:Person
# Charlie rdfs:label "Charlie"

triples_data = {
    's': [0, 0, 0, 1, 1, 2, 2],
    'p': [100, 101, 102, 100, 101, 100, 101],
    'o': [200, 300, 1, 200, 301, 200, 302]
}

triples_table = pa.Table.from_pydict({
    's': pa.array(triples_data['s'], type=pa.int64()),
    'p': pa.array(triples_data['p'], type=pa.int64()),
    'o': pa.array(triples_data['o'], type=pa.int64())
})

# Create triple store
store = PyRDFTripleStore(triples_table, terms_table)
print(f"✅ Created RDF store with {store.num_triples()} triples, {store.num_terms()} terms")

# Show sample data
print("\nSample triples:")
for i in range(min(5, store.num_triples())):
    s_id = triples_data['s'][i]
    p_id = triples_data['p'][i]
    o_id = triples_data['o'][i]

    s_lex = all_terms[s_id].split('/')[-1]
    p_lex = all_terms[p_id].split('/')[-1] if '#' not in all_terms[p_id] else all_terms[p_id].split('#')[-1]
    o_lex = all_terms[o_id].split('/')[-1] if o_id in entities or o_id in types else all_terms[o_id]

    print(f"  {s_lex} {p_lex} {o_lex}")

# Test simple pattern matching
print("\n[2] Testing pattern matching (not SPARQL, just direct)...")

# Find all triples with predicate rdf:type
type_predicate_id = 100
matches = store.filter_triples(-1, type_predicate_id, -1)  # -1 = wildcard

print(f"Query: ?s rdf:type ?o")
print(f"✅ Found {matches.num_rows} matches")
for i in range(matches.num_rows):
    s = matches.column('s')[i].as_py()
    o = matches.column('o')[i].as_py()
    print(f"  {all_terms[s].split('/')[-1]} rdf:type {all_terms[o].split('/')[-1]}")

# Find all labels
print(f"\nQuery: ?s rdfs:label ?label")
label_predicate_id = 101
matches = store.filter_triples(-1, label_predicate_id, -1)
print(f"✅ Found {matches.num_rows} matches")
for i in range(matches.num_rows):
    s = matches.column('s')[i].as_py()
    o = matches.column('o')[i].as_py()
    print(f"  {all_terms[s].split('/')[-1]} rdfs:label \"{all_terms[o]}\"")

print("\n" + "=" * 70)
print("Summary:")
print("=" * 70)
print("\n✅ What Works:")
print("  - PyRDFTripleStore with Arrow storage")
print("  - Term dictionary encoding")
print("  - Triple pattern matching (S/P/O with wildcards)")
print("  - filter_triples() method")
print("\n⚠️ What Doesn't Work:")
print("  - Full SPARQL parsing (parser has issues)")
print("  - PREFIX declarations")
print("  - Complex queries (GROUP BY, ORDER BY, aggregates)")
print("  - FILTER expressions")
print("\n📊 The Workaround:")
print("  - Use PyRDFTripleStore.filter_triples(s, p, o) directly")
print("  - Build SPARQL-style queries programmatically")
print("  - Process results with Arrow/pandas")
print("\nExample:")
print("  # Find all people (rdf:type foaf:Person)")
print("  people = store.filter_triples(-1, rdf_type_id, person_type_id)")
print("  # Then get their labels")
print("  for person_id in people.column('s'):")
print("      labels = store.filter_triples(person_id, label_id, -1)")
