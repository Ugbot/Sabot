# SabotCypher Quick Start Guide

**Status:** Skeleton complete - Implementation in progress

---

## Installation (Once Complete)

### Build from Source

```bash
# Clone repository
git clone https://github.com/yourusername/sabot.git
cd sabot/sabot_cypher

# Build C++ library
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Install Python module
pip install -e .
```

### Verify Installation

```python
import sabot_cypher

# Check if native extension loaded
if sabot_cypher.is_native_available():
    print("✅ SabotCypher ready!")
else:
    print(f"⚠️ Native extension not loaded: {sabot_cypher.get_import_error()}")
```

---

## Basic Usage

### 1. Create Bridge and Register Graph

```python
import sabot_cypher
import pyarrow as pa

# Create bridge
bridge = sabot_cypher.SabotCypherBridge.create()

# Prepare graph data as Arrow tables
vertices = pa.table({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    'label': pa.array(['Person', 'Person', 'Person', 'City', 'Interest']),
    'name': pa.array(['Alice', 'Bob', 'Charlie', 'NYC', 'Tennis']),
    'age': pa.array([25, 30, 35, None, None], type=pa.int32()),
})

edges = pa.table({
    'source': pa.array([1, 1, 2, 3, 1], type=pa.int64()),
    'target': pa.array([2, 3, 3, 4, 5], type=pa.int64()),
    'type': pa.array(['KNOWS', 'KNOWS', 'KNOWS', 'LIVES_IN', 'INTERESTED_IN']),
})

# Register graph
bridge.register_graph(vertices, edges)
```

### 2. Execute Cypher Queries

```python
# Simple query
result = bridge.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

print(f"Found {result.num_rows} matches")
print(f"Execution time: {result.execution_time_ms:.2f}ms")
print(result.table)

# Convert to pandas
df = result.table.to_pandas()
print(df)
```

### 3. Aggregation Queries

```python
# Count query
result = bridge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    RETURN p.name, count(friend) AS num_friends
""")

print(result.table)
```

### 4. Filtering

```python
# WHERE clause
result = bridge.execute("""
    MATCH (p:Person)
    WHERE p.age > 25
    RETURN p.name, p.age
    ORDER BY p.age DESC
""")

print(result.table)
```

### 5. Variable-Length Paths

```python
# Find paths of length 1-3
result = bridge.execute("""
    MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE a.name = 'Alice'
    RETURN b.name, count(*) AS num_paths
""")

print(result.table)
```

---

## Advanced Usage

### Parameterized Queries

```python
# Use parameters
result = bridge.execute(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
    params={'min_age': '25'}
)
```

### Explain Query Plan

```python
# Get execution plan
plan = bridge.explain("""
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WHERE b.age > 30
    RETURN a.name, b.name
""")

print(plan)
```

### Convenience Function

```python
# Execute without creating bridge explicitly
result = sabot_cypher.execute(
    "MATCH (a)-[r]->(b) RETURN count(*) AS edge_count",
    vertices,
    edges
)

print(f"Total edges: {result.table.column('edge_count')[0]}")
```

---

## Loading Real Data

### From CSV Files

```python
import pyarrow.csv as pcsv

# Load vertices
persons = pcsv.read_csv('persons.csv')
cities = pcsv.read_csv('cities.csv')

# Combine and add labels
vertices = pa.concat_tables([
    persons.append_column('label', pa.array(['Person'] * len(persons))),
    cities.append_column('label', pa.array(['City'] * len(cities))),
], promote=True)

# Load edges
edges = pcsv.read_csv('relationships.csv')

# Execute queries
bridge.register_graph(vertices, edges)
result = bridge.execute("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name")
```

### From Parquet Files

```python
import pyarrow.parquet as pq

# Load from parquet
vertices = pq.read_table('graph_vertices.parquet')
edges = pq.read_table('graph_edges.parquet')

bridge.register_graph(vertices, edges)
```

### From Pandas

```python
import pandas as pd

# Create DataFrames
persons_df = pd.DataFrame({
    'id': [1, 2, 3],
    'label': ['Person', 'Person', 'Person'],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

edges_df = pd.DataFrame({
    'source': [1, 1, 2],
    'target': [2, 3, 3],
    'type': ['KNOWS', 'KNOWS', 'KNOWS']
})

# Convert to Arrow
vertices = pa.Table.from_pandas(persons_df)
edges = pa.Table.from_pandas(edges_df)

bridge.register_graph(vertices, edges)
```

---

## Performance Tips

### 1. Batch Queries

```python
# Execute multiple queries efficiently
queries = [
    "MATCH (p:Person) RETURN count(*)",
    "MATCH ()-[r]->() RETURN count(*)",
    "MATCH (p:Person) RETURN avg(p.age)",
]

results = [bridge.execute(q) for q in queries]

for i, result in enumerate(results):
    print(f"Query {i+1}: {result.execution_time_ms:.2f}ms")
```

### 2. Index Your Data

```python
# Ensure ID columns are sorted for better join performance
vertices = vertices.sort_by('id')
edges = edges.sort_by([('source', 'ascending'), ('target', 'ascending')])

bridge.register_graph(vertices, edges)
```

### 3. Filter Early

```python
# Push filters into WHERE clause
# GOOD: Filter before aggregation
result = bridge.execute("""
    MATCH (p:Person)-[:KNOWS]->(f:Person)
    WHERE p.age > 25
    RETURN p.name, count(f) AS friends
""")

# AVOID: Filter after aggregation
# This processes more data unnecessarily
```

---

## Benchmarking

### Run Kuzu Study Benchmarks

```bash
cd benchmarks
python run_benchmark_sabot_cypher.py
```

### Custom Benchmarks

```python
import time

# Load graph
bridge = sabot_cypher.SabotCypherBridge.create()
bridge.register_graph(vertices, edges)

# Warmup
for _ in range(3):
    bridge.execute("MATCH (a)-[r]->(b) RETURN count(*)")

# Benchmark
queries = [
    "MATCH (a)-[r]->(b) RETURN count(*)",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*)",
    # ... more queries
]

for query in queries:
    start = time.time()
    result = bridge.execute(query)
    elapsed = time.time() - start
    
    print(f"Query: {query[:50]}...")
    print(f"  Rows: {result.num_rows}")
    print(f"  Time: {elapsed*1000:.2f}ms")
    print()
```

---

## Examples

### Example 1: Social Network

```python
# Build social network graph
vertices = pa.table({
    'id': [1, 2, 3, 4, 5],
    'label': ['Person'] * 5,
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, 35, 28, 32],
})

edges = pa.table({
    'source': [1, 1, 2, 2, 3, 4],
    'target': [2, 3, 3, 4, 5, 5],
    'type': ['KNOWS'] * 6,
})

bridge = sabot_cypher.SabotCypherBridge.create()
bridge.register_graph(vertices, edges)

# Find mutual friends
result = bridge.execute("""
    MATCH (a:Person)-[:KNOWS]->(mutual:Person)<-[:KNOWS]-(b:Person)
    WHERE a.name = 'Alice' AND b.name = 'Bob'
    RETURN mutual.name
""")

print("Mutual friends:", result.table)
```

### Example 2: Recommendation System

```python
# Recommend people Alice might know
result = bridge.execute("""
    MATCH (alice:Person {name: 'Alice'})-[:KNOWS]->(friend)-[:KNOWS]->(recommendation)
    WHERE NOT (alice)-[:KNOWS]->(recommendation) AND alice <> recommendation
    RETURN DISTINCT recommendation.name, count(*) AS common_friends
    ORDER BY common_friends DESC
    LIMIT 5
""")

print("People Alice might know:")
print(result.table.to_pandas())
```

### Example 3: Path Finding

```python
# Find shortest paths
result = bridge.execute("""
    MATCH path = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(b:Person {name: 'Eve'}))
    RETURN length(path) AS path_length
""")

print(f"Shortest path length: {result.table.column('path_length')[0]}")
```

---

## Troubleshooting

### Native Extension Not Found

```python
import sabot_cypher

if not sabot_cypher.is_native_available():
    print(sabot_cypher.get_import_error())
    # Solution: Build the C++ extension
    # cd sabot_cypher && cmake --build build
```

### Query Syntax Errors

```python
try:
    result = bridge.execute("INVALID QUERY")
except RuntimeError as e:
    print(f"Query error: {e}")
```

### Performance Issues

```python
# Use EXPLAIN to understand query plan
plan = bridge.explain("MATCH (a)-[:KNOWS*1..5]->(b) RETURN count(*)")
print(plan)

# Check for:
# - Missing indexes
# - Large Cartesian products
# - Unnecessary variable-length paths
```

---

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for technical details
- See [README.md](README.md) for project overview
- Check [benchmarks/](benchmarks/) for performance examples
- Review [examples/](examples/) for more code samples

---

## Getting Help

- GitHub Issues: https://github.com/yourusername/sabot/issues
- Documentation: https://github.com/yourusername/sabot/tree/main/sabot_cypher

---

**Note:** SabotCypher is currently in development. Some features shown here are planned but not yet implemented. See [STATUS.md](STATUS.md) for current implementation status.

