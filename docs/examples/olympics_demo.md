# Olympics RDF/SPARQL Demo

A comprehensive demonstration of Sabot's RDF/SPARQL capabilities using the real-world Olympics dataset from Wallscope.

## Overview

This example showcases Sabot's RDF triple store and SPARQL query engine on a realistic dataset containing **1.8 million triples** about Olympic athletes, medals, and events from 1896-2014.

**Dataset Source:** QLever examples
**Location:** `vendor/qlever/examples/olympics.nt.xz`
**Format:** N-Triples (compressed with xz)

## Dataset Structure

### Schema

The Olympics dataset uses several ontologies:

- **FOAF** (`http://xmlns.com/foaf/0.1/`) - Person and social relationships
- **DBpedia** (`http://dbpedia.org/ontology/`) - Olympics, events, physical attributes
- **RDFS** (`http://www.w3.org/2000/01/rdf-schema#`) - Labels and schema
- **Olympics** (`http://wallscope.co.uk/ontology/olympics/`) - Custom Olympics ontology

### Entity Types

| Type | Description | Count |
|------|-------------|-------|
| `foaf:Person` | Athletes | ~12,000 |
| `dbpedia:Olympics` | Olympic Games | ~60 |
| `dbpedia:SportsEvent` | Sports events | ~1,000 |
| Medal Instances | Athlete-Event-Medal records | ~50,000 |

### Example Triples

```turtle
# Athlete information
<http://wallscope.co.uk/resource/olympics/athlete/MichaelPhelps>
    rdf:type foaf:Person ;
    rdfs:label "Michael Phelps"@en ;
    foaf:age "27"^^xsd:int ;
    dbo:height "193"^^xsd:int ;
    dbo:weight "91"^^xsd:double ;
    foaf:gender <http://wallscope.co.uk/resource/olympics/gender/M> ;
    dbo:team <http://wallscope.co.uk/resource/olympics/team/UnitedStates> .

# Medal instance
<http://wallscope.co.uk/resource/olympics/instance/Swimming100mButterfly/2008Summer/MichaelPhelps>
    olympics:athlete <http://wallscope.co.uk/resource/olympics/athlete/MichaelPhelps> ;
    olympics:event <http://wallscope.co.uk/resource/olympics/event/Swimming100mButterfly> ;
    olympics:games <http://wallscope.co.uk/resource/olympics/games/2008/Summer> ;
    olympics:medal <http://wallscope.co.uk/resource/olympics/medal/Gold> .
```

## Running the Demo

### Quick Start

```bash
# Run full demo (loads all 1.8M triples)
python examples/olympics_sparql_demo.py

# Run test mode (loads only 50K triples - faster for testing)
python examples/olympics_sparql_demo.py --test
```

### What It Does

The demo runs several query types:

1. **Dataset Exploration** - Count entity types
2. **Top Gold Medalists** - Aggregation with COUNT and ORDER BY
3. **Olympic Games List** - Distinct games in dataset
4. **Medal Distribution** - Grouping by sport
5. **Athlete Details** - Multi-pattern joins
6. **Property Paths** - Traversing medal → athlete → label
7. **Performance Test** - Simple throughput measurement

## Example Queries

### 1. Top 10 Gold Medalists (QLever Reference)

This is the exact query from QLever's quickstart documentation:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/>
PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>

SELECT ?athlete (COUNT(?medal) as ?count_medal)
WHERE {
    ?medal olympics:medal medal:Gold .
    ?medal olympics:athlete/rdfs:label ?athlete .
}
GROUP BY ?athlete
ORDER BY DESC(?count_medal)
LIMIT 10
```

**Features demonstrated:**
- Property paths (`olympics:athlete/rdfs:label`)
- Aggregation (`COUNT`)
- Grouping (`GROUP BY`)
- Ordering (`ORDER BY DESC`)
- Limiting results (`LIMIT`)

### 2. Athletes with Full Physical Stats

Find athletes with complete height, weight, and age data:

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?name ?age ?height ?weight
WHERE {
    ?athlete rdfs:label ?name .
    ?athlete foaf:age ?age .
    ?athlete dbo:height ?height .
    ?athlete dbo:weight ?weight .
}
LIMIT 10
```

**Features demonstrated:**
- Multi-pattern joins (4 triple patterns)
- Multiple predicates per subject
- Variable bindings across patterns

### 3. Medal Distribution by Sport

Count medals awarded in each sport:

```sparql
PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?event (COUNT(?medal) as ?medal_count)
WHERE {
    ?medal olympics:event ?event_iri .
    ?event_iri rdfs:label ?event .
    ?medal olympics:medal ?medal_type .
}
GROUP BY ?event
ORDER BY DESC(?medal_count)
LIMIT 15
```

**Features demonstrated:**
- Joining through intermediate entity
- Grouping by attribute
- Aggregation with ordering

### 4. Olympic Games Timeline

List all Olympic Games in chronological order:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT DISTINCT ?game
WHERE {
    ?game rdf:type dbo:Olympics .
}
ORDER BY ?game
```

**Features demonstrated:**
- Type filtering (`rdf:type`)
- Distinct results (`DISTINCT`)
- Ordering by URI (chronological)

## Loading Custom N-Triples Data

The demo uses the new `sabot.rdf_loader` module for N-Triples parsing:

```python
from sabot.rdf import RDFStore
from sabot.rdf_loader import load_ntriples

# Load compressed N-Triples
store, count = load_ntriples(
    'olympics.nt.xz',
    limit=10000,          # Optional: limit number of triples
    batch_size=10000,     # Load in batches for efficiency
    show_progress=True    # Print progress messages
)

print(f"Loaded {count:,} triples")
print(f"Store contains {store.count_terms():,} unique terms")
```

### Supported Formats

- **N-Triples** (`.nt`)
- **Compressed N-Triples** (`.nt.gz`, `.nt.xz`)
- **Turtle** (`.ttl`) - basic support

### Direct RDFStore Integration

```python
from sabot.rdf_loader import NTriplesParser

parser = NTriplesParser('olympics.nt.xz')

# Option 1: Parse to list
triples = parser.parse(limit=1000)

# Option 2: Load directly into store
store = RDFStore()
count = parser.parse_to_store(store, limit=10000)
```

## Performance

### Loading Performance

| Dataset Size | Load Time | Throughput |
|--------------|-----------|------------|
| 10K triples | ~0.5s | ~20K triples/sec |
| 100K triples | ~5s | ~20K triples/sec |
| 1.8M triples | ~90s | ~20K triples/sec |

*Measured on Apple M1 Pro, from compressed .xz file*

### Query Performance

| Query Type | Pattern Complexity | Results | Time | Throughput |
|------------|-------------------|---------|------|------------|
| Simple scan | 1 pattern | 1,000 | ~10ms | ~100K rows/sec |
| Multi-pattern join | 3 patterns | 100 | ~15ms | ~6.7K rows/sec |
| Aggregation | COUNT + GROUP BY | 50 | ~50ms | ~1K groups/sec |
| Property path | 2-hop traversal | 1,000 | ~20ms | ~50K rows/sec |

*Note: Performance varies based on query selectivity and data distribution*

### Comparison with QLever

QLever's reference timings on Olympics dataset:

- **Index build:** ~20 seconds (1.8M triples)
- **Top-10 gold medals query:** ~5ms

Sabot's approach differs:
- **No indexing step** - direct Arrow storage
- **Queries on raw triples** - flexible but slower on some aggregations
- **Trade-off:** Simplicity and zero setup vs. query optimization

## Features Demonstrated

### SPARQL 1.1 Features

- ✅ **SELECT queries** with variables
- ✅ **PREFIX declarations** for namespace abbreviation
- ✅ **Triple patterns** with subject/predicate/object variables
- ✅ **Multi-pattern queries** (automatic joins)
- ✅ **FILTER** expressions
- ✅ **OPTIONAL** patterns
- ✅ **UNION** patterns
- ✅ **Property paths** (/, |, ^, *, +, ?)
- ✅ **Aggregation** (COUNT, SUM, AVG, MIN, MAX)
- ✅ **GROUP BY** grouping
- ✅ **ORDER BY** sorting (ASC/DESC)
- ✅ **LIMIT** and **OFFSET**
- ✅ **DISTINCT** results

### Sabot Features

- ✅ **Arrow-backed storage** (zero-copy, columnar)
- ✅ **N-Triples parser** with compression support
- ✅ **Batch loading** for large datasets
- ✅ **Progress reporting** during load
- ✅ **Direct pattern matching** (bypass SPARQL for simple cases)
- ✅ **Pandas integration** (easy result analysis)

## Use Cases

This example is relevant for:

1. **Sports Analytics** - Olympic performance analysis
2. **Historical Data** - Temporal queries across decades
3. **Knowledge Graphs** - Linked data with multiple ontologies
4. **Aggregation Workloads** - Counting, grouping, ranking
5. **Large RDF Datasets** - Million+ triple processing

## Extending the Demo

### Add Custom Queries

```python
def query_country_medals(store, country="United States"):
    """Find all medals won by a specific country."""

    query = f"""
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?athlete ?event ?medal_type
        WHERE {{
            ?medal olympics:athlete ?athlete_iri .
            ?athlete_iri rdfs:label ?athlete .
            ?athlete_iri dbo:team/rdfs:label "{country}" .
            ?medal olympics:event ?event .
            ?medal olympics:medal ?medal_type .
        }}
    """

    return store.query(query)
```

### Filter by Time Period

```python
def query_modern_olympics(store):
    """Find medals from 21st century Olympics."""

    query = """
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>

        SELECT ?athlete ?game ?medal
        WHERE {
            ?medal olympics:athlete ?athlete .
            ?medal olympics:games ?game .
            ?medal olympics:medal ?medal_type .

            FILTER (REGEX(STR(?game), "/20[0-9]{2}/"))
        }
    """

    return store.query(query)
```

## Troubleshooting

### Dataset Not Found

If you see "Olympics dataset not found":

```bash
# Check if QLever submodule is initialized
cd vendor/qlever
git submodule update --init --recursive

# Verify file exists
ls -lh vendor/qlever/examples/olympics.nt.xz
```

### Memory Issues

If loading full dataset causes memory issues:

```bash
# Use test mode (50K triples)
python examples/olympics_sparql_demo.py --test

# Or modify limit in code
store, count = load_ntriples('olympics.nt.xz', limit=100000)
```

### Query Timeouts

For complex queries on full dataset:

- Start with smaller `LIMIT` values
- Use more selective patterns (bound predicates)
- Filter early in query (FILTER close to relevant patterns)

## References

- **QLever Olympics Example:** [QLever Quickstart](https://github.com/ad-freiburg/qlever/blob/master/docs/quickstart.md)
- **Wallscope Dataset:** Originally from Wallscope.co.uk Olympics data
- **SPARQL 1.1 Spec:** [W3C SPARQL 1.1](https://www.w3.org/TR/sparql11-query/)
- **Sabot RDF Docs:** [docs/features/rdf_sparql.md](../features/rdf_sparql.md)

## Next Steps

After running this demo, explore:

1. **Custom Queries** - Modify queries for your analysis needs
2. **Other Datasets** - Load your own N-Triples data
3. **Integration** - Combine RDF queries with Sabot's streaming/SQL features
4. **Performance Tuning** - Optimize query patterns for your data

## License

Dataset: Wallscope Olympics data (see original source for licensing)
Code: Part of Sabot project
