# RDF and SPARQL Examples

This guide provides comprehensive examples of using Sabot's RDF triple store and SPARQL query engine.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Basic Triple Storage](#basic-triple-storage)
3. [SPARQL Queries](#sparql-queries)
4. [Working with Vocabularies](#working-with-vocabularies)
5. [Real-World Examples](#real-world-examples)
6. [Performance Tips](#performance-tips)

## Getting Started

### Import and Setup

```python
from sabot.rdf import RDFStore, SPARQLEngine, create_rdf_store

# Create a new RDF store
store = RDFStore()

# Or use convenience function
store = create_rdf_store()
```

### Check Store Status

```python
# Get statistics
print(store)
# Output:
# RDFStore:
#   Triples: 0
#   Terms: 0
#   Prefixes: 6

# Detailed stats
stats = store.stats()
print(stats)
# {'num_triples': 0, 'num_terms': 0, 'num_prefixes': 6, 'has_store': False}
```

## Basic Triple Storage

### Adding Single Triples

```python
from sabot.rdf import RDFStore

store = RDFStore()

# Add a triple with IRI object
store.add(
    subject="http://example.org/Alice",
    predicate="http://xmlns.com/foaf/0.1/knows",
    obj="http://example.org/Bob"
)

# Add a triple with literal object
store.add(
    subject="http://example.org/Alice",
    predicate="http://xmlns.com/foaf/0.1/name",
    obj="Alice",
    obj_is_literal=True
)

# Add typed literal with datatype
store.add(
    subject="http://example.org/Alice",
    predicate="http://xmlns.com/foaf/0.1/age",
    obj="25",
    obj_is_literal=True,
    datatype="http://www.w3.org/2001/XMLSchema#integer"
)

# Add literal with language tag
store.add(
    subject="http://example.org/Alice",
    predicate="http://www.w3.org/2000/01/rdf-schema#label",
    obj="Alice",
    obj_is_literal=True,
    lang="en"
)

print(f"Store contains {store.count()} triples")
```

### Batch Adding Triples

```python
# Prepare multiple triples
triples = [
    # (subject, predicate, object, is_literal)
    ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice", True),
    ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/age", "25", True),
    ("http://example.org/Bob", "http://xmlns.com/foaf/0.1/name", "Bob", True),
    ("http://example.org/Bob", "http://xmlns.com/foaf/0.1/age", "30", True),
    ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/knows", "http://example.org/Bob", False),
]

# Add all at once
store.add_many(triples)
print(f"Added {len(triples)} triples")
```

## SPARQL Queries

### Simple Pattern Queries

```python
from sabot.rdf import RDFStore

store = RDFStore()

# Add test data
store.add("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice", True)
store.add("http://example.org/Bob", "http://xmlns.com/foaf/0.1/name", "Bob", True)

# Query all names
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?name
    WHERE {
        ?person foaf:name ?name .
    }
"""

results = store.query(query)
print(f"Found {results.num_rows} people")

# Convert to pandas for display
df = results.to_pandas()
print(df)
```

### Multi-Pattern Queries (Joins)

```python
# Add more data
store.add("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person", False)
store.add("http://example.org/Alice", "http://xmlns.com/foaf/0.1/age", "25", True)

# Query with multiple patterns (automatic join)
query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person ?name ?age
    WHERE {
        ?person rdf:type foaf:Person .
        ?person foaf:name ?name .
        ?person foaf:age ?age .
    }
"""

results = store.query(query)
for i in range(results.num_rows):
    person = results.column('person')[i].as_py()
    name = results.column('name')[i].as_py()
    age = results.column('age')[i].as_py()
    print(f"{name} (age {age})")
```

### Relationship Queries

```python
# Find friends
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person ?friend
    WHERE {
        ?person foaf:knows ?friend .
    }
"""

results = store.query(query)
for i in range(results.num_rows):
    person = results.column('person')[i].as_py().split('/')[-1]
    friend = results.column('friend')[i].as_py().split('/')[-1]
    print(f"{person} knows {friend}")
```

### Friend-of-Friend Queries

```python
# Find friends and their names
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person_name ?friend_name
    WHERE {
        ?person foaf:name ?person_name .
        ?person foaf:knows ?friend .
        ?friend foaf:name ?friend_name .
    }
"""

results = store.query(query)
df = results.to_pandas()
print(df)
```

### Using LIMIT and DISTINCT

```python
# LIMIT results
query = """
    SELECT ?s ?p ?o
    WHERE {
        ?s ?p ?o .
    }
    LIMIT 5
"""

results = store.query(query)
print(f"Limited to {results.num_rows} rows")

# DISTINCT values
query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT DISTINCT ?type
    WHERE {
        ?entity rdf:type ?type .
    }
"""

results = store.query(query)
print("Distinct types:")
for i in range(results.num_rows):
    type_uri = results.column('type')[i].as_py()
    print(f"  - {type_uri}")
```

### Specific IRI Queries

```python
# Query for specific resource
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?name ?age
    WHERE {
        <http://example.org/Alice> foaf:name ?name .
        <http://example.org/Alice> foaf:age ?age .
    }
"""

results = store.query(query)
if results.num_rows > 0:
    name = results.column('name')[0].as_py()
    age = results.column('age')[0].as_py()
    print(f"Alice: name={name}, age={age}")
```

## Working with Vocabularies

### Registering Custom Prefixes

```python
store = RDFStore()

# Add custom namespace
store.add_prefix('myapp', 'http://myapp.example.org/')
store.add_prefix('schema', 'https://schema.org/')

# Now use in queries
query = """
    PREFIX myapp: <http://myapp.example.org/>

    SELECT ?resource
    WHERE {
        ?resource myapp:customProperty ?value .
    }
"""
```

### Default Prefixes

RDFStore comes with common prefixes:

```python
store = RDFStore()
print(store.prefixes)

# Output:
# {
#     'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
#     'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
#     'xsd': 'http://www.w3.org/2001/XMLSchema#',
#     'foaf': 'http://xmlns.com/foaf/0.1/',
#     'dc': 'http://purl.org/dc/elements/1.1/',
#     'owl': 'http://www.w3.org/2002/07/owl#',
# }
```

## Real-World Examples

### Example 1: Social Network

```python
from sabot.rdf import RDFStore

# Create store
store = RDFStore()

# Define people
people = [
    ("Alice", "25", "alice@example.com"),
    ("Bob", "30", "bob@example.com"),
    ("Charlie", "28", "charlie@example.com"),
]

# Add people to store
for name, age, email in people:
    person_uri = f"http://example.org/{name}"

    store.add(person_uri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person", False)
    store.add(person_uri, "http://xmlns.com/foaf/0.1/name", name, True)
    store.add(person_uri, "http://xmlns.com/foaf/0.1/age", age, True)
    store.add(person_uri, "http://xmlns.com/foaf/0.1/mbox", email, True)

# Add relationships
store.add("http://example.org/Alice", "http://xmlns.com/foaf/0.1/knows",
          "http://example.org/Bob", False)
store.add("http://example.org/Bob", "http://xmlns.com/foaf/0.1/knows",
          "http://example.org/Charlie", False)

# Query: Find all people over 25
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person ?name ?age
    WHERE {
        ?person foaf:name ?name .
        ?person foaf:age ?age .
    }
"""

results = store.query(query)
df = results.to_pandas()

# Filter in pandas (until FILTER works)
older_people = df[df['age'].astype(int) > 25]
print("People over 25:")
print(older_people)
```

### Example 2: Product Catalog

```python
from sabot.rdf import RDFStore

store = RDFStore()
store.add_prefix('product', 'http://example.org/product/')
store.add_prefix('schema', 'https://schema.org/')

# Add products
products = [
    ("laptop-x1", "Laptop X1", "999.99", "Electronics"),
    ("phone-y2", "Phone Y2", "699.99", "Electronics"),
    ("desk-z3", "Desk Z3", "299.99", "Furniture"),
]

for product_id, name, price, category in products:
    product_uri = f"http://example.org/product/{product_id}"

    store.add(product_uri, "https://schema.org/name", name, True)
    store.add(product_uri, "https://schema.org/price", price, True)
    store.add(product_uri, "https://schema.org/category", category, True)

# Query: Find all electronics
query = """
    PREFIX schema: <https://schema.org/>

    SELECT ?product ?name ?price
    WHERE {
        ?product schema:name ?name .
        ?product schema:price ?price .
        ?product schema:category ?category .
    }
"""

results = store.query(query)
df = results.to_pandas()

# Filter by category
electronics = df[df['category'] == 'Electronics']
print("Electronics:")
print(electronics)
```

### Example 3: Organization Chart

```python
from sabot.rdf import RDFStore

store = RDFStore()
store.add_prefix('org', 'http://example.org/org/')

# Add employees and reporting structure
employees = [
    ("alice", "Alice Smith", "CEO", None),
    ("bob", "Bob Jones", "CTO", "alice"),
    ("charlie", "Charlie Brown", "Engineer", "bob"),
    ("diana", "Diana Prince", "Engineer", "bob"),
]

for emp_id, name, title, manager_id in employees:
    emp_uri = f"http://example.org/org/{emp_id}"

    store.add(emp_uri, "http://xmlns.com/foaf/0.1/name", name, True)
    store.add(emp_uri, "http://example.org/org/title", title, True)

    if manager_id:
        manager_uri = f"http://example.org/org/{manager_id}"
        store.add(emp_uri, "http://example.org/org/reportsTo", manager_uri, False)

# Query: Find all engineers and their managers
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX org: <http://example.org/org/>

    SELECT ?emp_name ?manager_name
    WHERE {
        ?emp foaf:name ?emp_name .
        ?emp org:title ?title .
        ?emp org:reportsTo ?manager .
        ?manager foaf:name ?manager_name .
    }
"""

results = store.query(query)
df = results.to_pandas()

# Filter for engineers
engineers = df[df['emp_name'].str.contains('Charlie|Diana')]
print("Engineers and their managers:")
print(engineers)
```

## Direct Pattern Matching

For simple queries, bypass SPARQL parser:

```python
from sabot.rdf import RDFStore

store = RDFStore()

# Add data
store.add("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice", True)
store.add("http://example.org/Bob", "http://xmlns.com/foaf/0.1/name", "Bob", True)

# Direct pattern matching (faster than SPARQL for simple cases)
results = store.filter_triples(
    predicate="http://xmlns.com/foaf/0.1/name"
)

print(f"Found {results.num_rows} name triples")

# Wildcard all fields
all_triples = store.filter_triples()
print(f"Total triples: {all_triples.num_rows}")

# Specific subject
alice_triples = store.filter_triples(
    subject="http://example.org/Alice"
)
print(f"Alice has {alice_triples.num_rows} triples")
```

## Performance Tips

### 1. Batch Insertions

```python
# ❌ Slow - rebuilds store each time
for i in range(1000):
    store.add(f"http://example.org/Person{i}",
              "http://xmlns.com/foaf/0.1/name",
              f"Person{i}", True)

# ✅ Fast - single rebuild
triples = [
    (f"http://example.org/Person{i}",
     "http://xmlns.com/foaf/0.1/name",
     f"Person{i}", True)
    for i in range(1000)
]
store.add_many(triples)
```

### 2. Specific Patterns

```python
# ❌ Slow - wildcard pattern
query = """
    SELECT ?s ?p ?o
    WHERE { ?s ?p ?o . }
"""

# ✅ Fast - specific predicate
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?name
    WHERE { ?person foaf:name ?name . }
"""
```

### 3. Use Direct Matching

```python
# For simple pattern matching, bypass SPARQL
results = store.filter_triples(predicate="http://xmlns.com/foaf/0.1/name")

# Instead of:
results = store.query("""
    SELECT ?s ?o
    WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?o . }
""")
```

### 4. Limit Result Sets

```python
# Always use LIMIT for exploration
query = """
    SELECT ?s ?p ?o
    WHERE { ?s ?p ?o . }
    LIMIT 100
"""
```

## Working with Arrow Tables

Results are Apache Arrow tables with zero-copy access:

```python
from sabot.rdf import RDFStore

store = RDFStore()
# ... add data ...

results = store.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o . }")

# Arrow table operations
print(f"Schema: {results.schema}")
print(f"Rows: {results.num_rows}")
print(f"Columns: {results.num_columns}")

# Convert to pandas
df = results.to_pandas()

# Access columns directly
subjects = results.column('s')
predicates = results.column('p')
objects = results.column('o')

# Iterate rows
for i in range(results.num_rows):
    s = subjects[i].as_py()
    p = predicates[i].as_py()
    o = objects[i].as_py()
    print(f"{s} {p} {o}")

# Save to file
import pyarrow.parquet as pq
pq.write_table(results, 'query_results.parquet')
```

## Error Handling

```python
from sabot.rdf import RDFStore

store = RDFStore()

# Empty store query
try:
    results = store.query("SELECT ?s WHERE { ?s ?p ?o . }")
except ValueError as e:
    print(f"Error: {e}")
    # Output: Error: Store is empty - add triples before querying

# Invalid SPARQL
store.add("http://example.org/test", "http://example.org/pred", "value", True)

try:
    results = store.query("INVALID SPARQL")
except ValueError as e:
    print(f"Parse error: {e}")
```

## See Also

- [RDF/SPARQL Feature Documentation](../docs/features/rdf_sparql.md)
- [API Reference](../docs/features/rdf_sparql.md#api-reference)
- [Performance Benchmarks](../docs/features/rdf_sparql.md#performance)
- [SPARQL 1.1 Specification](https://www.w3.org/TR/sparql11-query/)
