# PuppyGraph & Stardog Analysis for Sabot

**Author:** Research Agent
**Date:** October 9, 2025
**Purpose:** Comprehensive analysis of PuppyGraph and Stardog for informing Sabot's graph capabilities

---

## Research Progress
- [x] PuppyGraph overview
- [x] Stardog overview
- [x] RDF support comparison
- [x] Property graph support
- [x] Bi-temporal features
- [x] Query languages (SPARQL, Cypher, Gremlin)
- [x] Schema & reasoning
- [x] Key takeaways

---

## Part A: PuppyGraph

### 1. Executive Summary

**What is PuppyGraph?**
PuppyGraph is the world's first real-time, zero-ETL graph query engine that enables graph analytics directly on existing data stores without data migration. It positions itself as a "graph analytic engine" rather than a traditional graph database.

**Key Innovation:**
The platform's core innovation is its ability to create a virtual graph layer over existing relational databases, data warehouses, data lakes, and NoSQL stores, allowing organizations to query their data as a graph in under 10 minutes without any ETL pipeline.

**Value Proposition:**
- Deploy to query in 10 minutes
- No data duplication or migration
- Query petabyte-scale data across multiple sources
- Maintain data governance and security in original systems
- Support both SQL and graph query languages simultaneously

**Market Position:**
First and only graph analytics engine supporting both openCypher and Gremlin query languages on top of existing data infrastructure.

---

### 2. Architecture

**Core Design Principles:**
1. **Compute-Storage Separation:** PuppyGraph decouples computation from storage, allowing it to scale independently
2. **Zero-ETL:** No data movement or duplication required
3. **Distributed Processing:** Auto-sharded, distributed computation across cluster nodes
4. **Vectorized Execution:** Column-oriented, vectorized query engine for OLAP performance

**Architecture Components:**

**Query Layer:**
- Supports openCypher (v9) and Gremlin (TinkerPop-compatible)
- Query parser and optimizer
- Vectorized query execution engine

**Graph Schema Layer:**
- JSON-based schema definitions
- Maps relational structures to property graph model
- Virtual graph projection (no physical graph storage)

**Data Access Layer:**
- Database drivers (JDBC, native connectors)
- Direct connection to source systems
- Leverages existing data store permissions and security

**Compute Layer:**
- 4-node cluster configuration (typical deployment)
- Leader node (t3.xlarge) for coordination
- Compute nodes (m6i.4x) for parallel processing
- Patent-pending parallel processing technology

**Supported Deployment:**
- Cloud-native (AWS, GCP, Azure)
- On-premises
- Hybrid environments

---

### 3. Virtual Graph Layer

**Concept:**
The virtual graph layer is PuppyGraph's signature feature - a logical graph model that exists as a view over physical data sources without creating a separate graph database.

**How It Works:**

1. **Schema Definition:** Users define a graph schema using JSON that maps:
   - Tables → Vertices (nodes)
   - Foreign key relationships → Edges (relationships)
   - Columns → Properties (attributes)

2. **Query Translation:** Graph queries (Cypher/Gremlin) are translated to:
   - SQL queries for relational sources
   - Native queries for NoSQL sources
   - Federated queries across multiple sources

3. **Real-Time Execution:** Queries execute directly against source data:
   - No intermediate graph materialization
   - Results reflect current state of source systems
   - Leverages source system indexes and optimizations

**Benefits:**
- **Always Current:** No stale data from ETL processes
- **Single Source of Truth:** Data remains in authoritative systems
- **Governance Preserved:** Access controls managed by source systems
- **Version Control:** Graph schemas are JSON files (easy to version, update, switch)

**Trade-offs:**
- Query performance depends on source system capabilities
- Complex multi-hop queries may require multiple source queries
- Network latency between PuppyGraph and sources can impact performance

---

### 4. Property Graph Support

**Property Graph Model:**
PuppyGraph implements the labeled property graph model, which consists of:

1. **Vertices (Nodes):**
   - Have labels (types)
   - Contain properties (key-value pairs)
   - Have unique IDs

2. **Edges (Relationships):**
   - Have labels (relationship types)
   - Contain properties
   - Are directed (from → to)
   - Reference source and destination vertices

**Schema Mapping:**

**Standard Nodes (One-to-One Mapping):**
```json
{
  "vertices": [
    {
      "label": "Person",
      "oneToOne": {
        "tableSource": {
          "catalog": "db",
          "schema": "public",
          "table": "persons"
        },
        "id": "person_id",
        "attributes": [
          {"name": "name", "type": "String"},
          {"name": "age", "type": "Int"},
          {"name": "created_at", "type": "Date"}
        ]
      }
    }
  ]
}
```

**Flexible Nodes (Many-to-One Mapping):**
Allows multiple tables to map to a single vertex type - useful for heterogeneous data sources.

**Edge Definitions:**
```json
{
  "edges": [
    {
      "label": "KNOWS",
      "fromVertex": "Person",
      "toVertex": "Person",
      "tableSource": {
        "catalog": "db",
        "schema": "public",
        "table": "friendships"
      },
      "id": "friendship_id",
      "fromId": "person1_id",
      "toId": "person2_id",
      "attributes": [
        {"name": "since", "type": "Date"},
        {"name": "weight", "type": "Float"}
      ]
    }
  ]
}
```

**Supported Property Types:**
- String
- Int
- Long
- Float
- Double
- Boolean
- Date
- DateTime
- Arrays of above types

---

### 5. Query Languages

**openCypher Support (v9):**

PuppyGraph provides full support for openCypher, the open-source graph query language.

**Key Cypher Capabilities:**
- Pattern matching: `MATCH (p:Person)-[:KNOWS]->(friend)`
- Filtering: `WHERE p.age > 25`
- Aggregations: `COUNT()`, `SUM()`, `AVG()`, etc.
- Path queries: `MATCH path = (a)-[*1..5]->(b)`
- Subqueries and `WITH` clauses

**Example Cypher Query:**
```cypher
MATCH (person:Person)-[:KNOWS*2..4]->(friend)
WHERE person.name = 'Alice'
RETURN friend.name, friend.age
ORDER BY friend.age DESC
LIMIT 10
```

**Gremlin Support (TinkerPop-Compatible):**

Full TinkerPop-compatible Gremlin traversal support.

**Key Gremlin Capabilities:**
- Traversal steps: `g.V().has().out().values()`
- Filtering: `has()`, `where()`, `is()`
- Path traversals: `repeat().times()`, `until()`
- Aggregations: `count()`, `sum()`, `mean()`
- Graph algorithms integration

**Example Gremlin Query:**
```groovy
g.V().has('Person', 'name', 'Alice')
  .repeat(out('KNOWS')).times(3)
  .dedup()
  .values('name')
  .limit(10)
```

**Dual Query Support:**
Users can query the same data using SQL, Cypher, AND Gremlin simultaneously - providing maximum flexibility for different use cases and team preferences.

---

### 6. Data Sources & Integration

**Supported Data Sources:**

**Cloud Databases:**
- Google AlloyDB
- Google Cloud Spanner
- Amazon RDS (PostgreSQL, MySQL)

**Data Warehouses:**
- Snowflake
- Google BigQuery
- Databricks SQL Warehouse
- Redshift

**Data Lakes:**
- Apache Iceberg (S3, GCS, Azure Blob)
- Apache Hudi
- Delta Lake
- Parquet files on S3/GCS

**Relational Databases:**
- PostgreSQL
- MySQL
- SQL Server
- Oracle

**NoSQL Databases:**
- MongoDB (via MongoDB Atlas SQL JDBC driver)

**Connection Methods:**
- JDBC drivers for SQL sources
- Native connectors for cloud services
- SQL interface for MongoDB and other NoSQL

**Multi-Source Queries:**
PuppyGraph supports federated queries across multiple data sources simultaneously:
- Join data from PostgreSQL + BigQuery
- Traverse relationships spanning Iceberg + Snowflake
- Single schema can reference multiple catalogs

**Data Governance:**
- Queries inherit source system permissions
- No data copies means single point of governance
- Audit logs from source systems apply
- Row-level security preserved from sources

---

### 7. Performance Characteristics

**Benchmark Results:**

**10-Hop Neighbor Query:**
- **Time:** 2.26 seconds
- **Scale:** Billions of edges
- **Cluster:** 4-node cluster (1 leader + 3 compute)
- **Technology:** Parallel processing + vectorized evaluation

**5-Hop Query (Production):**
- **Time:** Under 3 seconds
- **Scale:** 700 million edges
- **Customer:** Financial services industry

**3-Hop Query vs Neo4j:**
- **Performance:** 20-70x faster than Neo4j
- **Dataset:** Twitter data (50M nodes, 2B edges)
- **Advantage:** High-degree node handling

**10-Hop Query vs Neo4j:**
- **PuppyGraph:** Completes in 2.26 seconds
- **Neo4j:** Unable to complete (timeout/failure)

**Performance Technologies:**

1. **Vectorized Execution:**
   - Column-oriented query processing
   - SIMD operations for batch processing
   - Reduced memory footprint

2. **Parallel Processing:**
   - Auto-sharding across compute nodes
   - Patent-pending parallel query execution
   - Efficient resource utilization

3. **Query Optimization:**
   - Push-down predicates to source systems
   - Leverage source indexes
   - Minimize data transfer

4. **Graph Algorithms:**
   - Label Propagation
   - PageRank
   - Connected Components
   - Shortest Path
   - Community Detection

**Scalability:**
- Petabyte-scale data support
- Hundreds of millions to billions of edges
- Linear scaling with cluster size
- Separation of compute/storage enables independent scaling

---

## Part B: Stardog

### 1. Executive Summary

**What is Stardog?**
Stardog is an enterprise knowledge graph platform built on the RDF (Resource Description Framework) open standard. It combines graph database, inference, and virtualization technologies to unify data based on meaning rather than location.

**Key Innovation:**
Stardog's core innovation is its unified platform that combines:
1. RDF-native graph database with ACID transactions
2. OWL 2 reasoning and inference engine
3. Virtual graph technology for query federation
4. BARQ vectorized query execution engine (9-90x performance improvement)

**Value Proposition:**
- Standards-based (RDF, SPARQL, OWL, SHACL, R2RML)
- Enterprise-grade knowledge graph with reasoning
- Query federation across distributed data sources
- Data validation and quality constraints
- Semantic data integration

**Market Position:**
Leading enterprise knowledge graph platform with strong focus on semantic web standards, reasoning, and data governance. Positioned as a complete knowledge graph solution rather than just a graph database.

---

### 2. RDF-Native Architecture

**RDF Graph Data Model:**

Stardog is built on a directed semantic graph composed of RDF triples.

**Triple Structure:**
Each triple consists of:
- **Subject:** IRI or Blank Node
- **Predicate:** IRI (defines the relationship)
- **Object:** IRI, Blank Node, or Literal

**Node Types:**

1. **IRI (Internationalized Resource Identifier):**
   - Globally unique identifiers
   - Example: `http://example.org/Person/Alice`
   - Can use prefixed names for brevity: `ex:Alice`

2. **Blank Nodes:**
   - Unnamed resources (anonymous nodes)
   - Local identifiers within a graph
   - Used for intermediate structures

3. **Literals:**
   - Concrete values: strings, numbers, dates, booleans
   - Can have datatypes: `"42"^^xsd:integer`
   - Can have language tags: `"Hello"@en`

**Named Graphs:**

Stardog supports the RDF dataset model with named graphs:
- **Default Graph:** Unnamed graph containing base triples
- **Named Graphs:** Named collections of triples
- Enables metadata attachment and data organization
- Graph URIs used as fourth element in quads

**Example RDF (Turtle Syntax):**
```turtle
@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Alice a foaf:Person ;
    foaf:name "Alice Smith" ;
    foaf:age 30 ;
    foaf:knows ex:Bob .

ex:Bob a foaf:Person ;
    foaf:name "Bob Jones" .
```

**Schema Flexibility:**
- Schema is part of the graph itself (data and metadata unified)
- Supports both implicit and explicit schemas
- RDF Schema (RDFS) defines classes and properties
- OWL provides richer ontology capabilities
- Fluid distinction between data and metadata

**Storage:**
- Native RDF triple/quad store
- Physical storage optimized for triple patterns
- Indexes for efficient SPARQL query evaluation
- ACID transaction support

---

### 3. Knowledge Graph Features

**Core Knowledge Graph Capabilities:**

1. **Semantic Data Integration:**
   - Unify data based on meaning (not location)
   - Schema-on-read approach
   - Flexible schema evolution
   - Connect disparate data sources

2. **Entity Resolution:**
   - Identify duplicate entities across sources
   - Merge entity representations
   - Owl:sameAs reasoning
   - Custom similarity functions

3. **Data Quality & Validation:**
   - SHACL shape constraints
   - Integrity constraint validation
   - Data cleansing workflows
   - Validation reports

4. **Graph Exploration:**
   - Stardog Explorer (visual graph browser)
   - Path queries to find connections
   - Graph pattern discovery
   - Interactive visualization

5. **Search Capabilities:**
   - Full-text search on RDF literals
   - Geospatial queries (point, polygon, distance)
   - Semantic search with reasoning
   - Hybrid search (structured + unstructured)

6. **Machine Learning Integration:**
   - Feature extraction from knowledge graphs
   - Graph embeddings
   - Link prediction
   - Classification and clustering

**Tools & Interfaces:**

- **Stardog Studio:** Web-based IDE for SPARQL, GraphQL, and visual modeling
- **Stardog Designer:** Visual schema design and ontology management
- **Stardog Explorer:** Graph visualization and navigation
- **APIs:** Java, Python, JavaScript, .NET, REST

**Deployment Options:**
- Cloud (AWS, GCP, Azure)
- On-premises
- Kubernetes and Docker
- High availability clusters
- Multi-datacenter replication

---

### 4. SPARQL Support

**SPARQL 1.1 Implementation:**

Stardog provides full SPARQL 1.1 support with extensions.

**Query Types:**
1. **SELECT:** Retrieve variable bindings (like SQL SELECT)
2. **CONSTRUCT:** Build new RDF graphs from patterns
3. **ASK:** Boolean queries (true/false)
4. **DESCRIBE:** Get information about resources
5. **UPDATE:** Insert, delete, modify triples

**Example SPARQL SELECT:**
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?friend ?friendOfFriend
WHERE {
  ?person foaf:name "Alice" ;
          foaf:knows ?friend .
  ?friend foaf:knows ?friendOfFriend .
  FILTER(?friendOfFriend != ?person)
}
LIMIT 10
```

**Advanced SPARQL Features:**

1. **Path Queries:**
   - Find connections between nodes
   - Property paths: `?x foaf:knows+ ?y` (1 or more hops)
   - Shortest path queries
   - All paths within N hops

2. **Full-Text Search:**
   - Search RDF literals: `?doc text:query "knowledge graph"`
   - Relevance scoring
   - Highlighting
   - Tokenization and stemming

3. **Geospatial Queries:**
   - Point-in-polygon tests
   - Distance queries
   - Spatial joins
   - GeoSPARQL standard support

4. **Aggregations:**
   - `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
   - `GROUP_CONCAT()`, `SAMPLE()`
   - `GROUP BY` and `HAVING` clauses

5. **Subqueries and Nesting:**
   - Nested SELECT queries
   - `EXISTS` and `NOT EXISTS` filters
   - Complex filtering logic

**BARQ Execution Engine (2025):**

**What is BARQ?**
Batch-Based Accelerated Query Engine - Stardog's vectorized SPARQL executor.

**Key Characteristics:**
- **Batch Processing:** Operates on batches of tuples instead of row-by-row
- **Vectorized Execution:** SIMD operations on columnar data
- **Hybrid Execution:** Switches between batch and row-based as needed

**Performance Improvements:**
- **9-90x faster** on CPU-bound queries
- **2 orders of magnitude** improvement for merge joins
- No performance sacrifice on disk-bound or OLTP queries

**Supported Operators (v10.2+):**
- Hash joins and merge joins
- Filters and projections
- Aggregations
- Anti-joins (MINUS)
- DISTINCT operations

**Inspiration:**
Based on systems like MonetDB/VectorWise/Actian and Velox.

**Research:**
Published paper on arXiv (April 2025) describing BARQ's development and integration.

**Stardog Extensions:**

1. **RDF List Functions:**
   - `list:length()`, `list:member()`, `list:get()`
   - Operations on RDF collections

2. **Custom Functions:**
   - User-defined SPARQL functions
   - Extend query capabilities

3. **Named Graph Aliases:**
   - Abstract graph names for data virtualization
   - Switch data sources without query changes

4. **Graph Exclusion:**
   - Exclude specific named graphs from query datasets
   - Fine-grained query control

5. **UNNEST Operator:**
   - Process arrays and nested structures
   - Flatten hierarchical data

**Query Optimization:**
- Query plan examination and explain
- Query hints for performance tuning
- Statistics collection and sampling
- Cost-based query optimization
- Automatic index selection

---

### 5. Virtual Graph & Query Federation

**Virtual Graph Technology:**

Stardog's virtual graphs enable querying external data sources as if they were RDF graphs without data migration.

**How Virtual Graphs Work:**

1. **Mapping Definition:** Define how relational/NoSQL data maps to RDF
2. **Query Rewriting:** SPARQL queries rewritten to native queries (SQL, MongoDB, etc.)
3. **Federated Execution:** Native queries sent to source systems
4. **Result Translation:** Native results translated back to SPARQL results

**Mapping Languages:**

**1. R2RML (W3C Standard):**
```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .

<#PersonMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "persons" ] ;
    rr:subjectMap [
        rr:template "http://example.org/Person/{person_id}" ;
        rr:class foaf:Person
    ] ;
    rr:predicateObjectMap [
        rr:predicate foaf:name ;
        rr:objectMap [ rr:column "name" ]
    ] .
```

**2. SMS (Stardog Mapping Syntax):**
Simplified syntax for common mapping patterns.

**3. SMS2 (Stardog Mapping Syntax 2):**
- Broader source support (JSON, MongoDB, Elasticsearch, SQL)
- Semi-structured data sources
- Bidirectional translation with R2RML

**Automatic Mapping:**
For relational databases with schemas, Stardog can auto-generate R2RML direct mappings.

**Supported Data Sources:**
- Relational databases (MySQL, PostgreSQL, Oracle, SQL Server)
- MongoDB
- Elasticsearch
- JSON files
- CSV files
- Other SPARQL endpoints (SPARQL federation)

**Query Federation:**

**SPARQL SERVICE Keyword:**
```sparql
SELECT ?person ?friend
WHERE {
  ?person foaf:knows ?friend .
  SERVICE <http://remote-endpoint.org/sparql> {
    ?friend foaf:age ?age .
    FILTER(?age > 25)
  }
}
```

**Multi-Source Queries:**
- Join data across local RDF + virtual graphs
- Join data from multiple virtual graphs
- Reasoning works across federated sources
- Transparent to query writer

**Query Optimization:**
- Push-down predicates to source systems
- Minimize data transfer
- Leverage source indexes
- Parallel execution across sources

**Benefits:**
- **No ETL Required:** Query data in place
- **Always Current:** Real-time access to source data
- **Unified View:** Single SPARQL interface for all sources
- **Reasoning Integration:** OWL reasoning works with virtual data

**Trade-offs:**
- Performance depends on source system capabilities
- Network latency impacts query time
- Complex queries may require multiple round-trips

---

### 6. Reasoning & Inference

**Stardog Inference Engine:**

Stardog provides OWL 2 reasoning through query-time rewriting, enabling automatic inference of new knowledge from existing data.

**Reasoning Profiles:**

1. **RDFS:** Basic schema reasoning
   - `rdfs:subClassOf`, `rdfs:subPropertyOf`
   - Domain and range inference
   - Transitive hierarchies

2. **OWL 2 Profiles:**
   - **OWL 2 QL:** Query-optimized (polynomial complexity)
   - **OWL 2 RL:** Rule-based (forward-chaining compatible)
   - **OWL 2 EL:** Expressive subset for large ontologies
   - **OWL 2 DL:** Full description logic (not supported by Stardog)

3. **SL (Stardog Default):**
   - Combination of RDFS + QL + RL + EL
   - Plus user-defined rules (SWRL, SRS)
   - Covers most practical use cases

**Reasoning Methods:**

**Query-Time Rewriting:**
- Queries rewritten to include inferred triples
- No materialization required
- Always reflects current data
- Works with virtual graphs

**Example:**
```turtle
# Data
ex:Alice a ex:Employee .

# Schema
ex:Employee rdfs:subClassOf ex:Person .

# Query with reasoning
SELECT ?person WHERE { ?person a ex:Person }
# Returns: ex:Alice (inferred)
```

**Two Reasoner Implementations:**

**1. Blackout (Mature):**
- More complete RDFS/OWL support
- Limited user-defined rule expressivity
- Production-ready
- Default reasoner

**2. Stride (Alpha):**
- Next-generation reasoner
- More expressive user-defined rules
- Supports negation and aggregation in rules
- Smaller RDFS/OWL subset
- Under active development

**User-Defined Rules:**

**SWRL (Semantic Web Rule Language):**
```turtle
# Example SWRL rule: If X has parent Y, and Y has parent Z, then X has grandparent Z
[rule1: (?x ex:hasParent ?y) (?y ex:hasParent ?z) -> (?x ex:hasGrandparent ?z)]
```

**SRS (Stardog Rules Syntax):**
More expressive than SWRL, supports:
- Negation (NOT)
- Aggregation (COUNT, SUM, etc.)
- Complex rule logic

**Example SRS:**
```
IF {
  ?person a :Employee .
  ?person :salary ?salary .
  FILTER(?salary > 100000)
} THEN {
  ?person a :HighEarner .
}
```

**Rule Translation:**
Stardog can translate between SRS and SWRL where possible.

**Reasoning with Virtual Graphs:**
- Reasoning works transparently with virtual graphs
- Inferences computed across federated data
- Same performance characteristics as local reasoning

**Explaining Inferences:**
- Stardog can explain why a triple was inferred
- Provides proof trees showing derivation steps
- Debugging support for complex ontologies

**Reasoning Performance:**
- Query-time rewriting (no pre-materialization)
- Optimized for query workloads
- Selective reasoning on specific graphs
- Can be enabled/disabled per query

---

### 7. Schema Support (SHACL, OWL)

**Schema Technologies:**

**1. OWL 2 (Web Ontology Language):**

**Purpose:** Define rich ontologies and domain models.

**Key Constructs:**
- **Classes:** Categories of entities
  - `owl:Class`, subclass hierarchies
  - Disjoint classes, equivalent classes

- **Properties:**
  - Object properties (relate entities)
  - Datatype properties (entity to literal)
  - Inverse, transitive, symmetric properties
  - Property domains and ranges

- **Restrictions:**
  - Cardinality constraints
  - Value restrictions
  - Existential and universal quantification

- **Class Expressions:**
  - Union, intersection, complement
  - Enumeration
  - Property restrictions

**Example OWL:**
```turtle
ex:Person a owl:Class .
ex:Employee rdfs:subClassOf ex:Person .

ex:hasManager a owl:ObjectProperty ;
    rdfs:domain ex:Employee ;
    rdfs:range ex:Manager .

ex:Manager a owl:Class ;
    rdfs:subClassOf ex:Employee ;
    owl:equivalentClass [
        a owl:Restriction ;
        owl:onProperty ex:manages ;
        owl:minCardinality 1
    ] .
```

**2. SHACL (Shapes Constraint Language):**

**Purpose:** Validate RDF data against shape constraints.

**Key Concepts:**
- **Shapes:** Describe expected structure of data
- **Targets:** Define which nodes to validate
- **Constraints:** Rules that data must satisfy
- **Validation Reports:** Results of constraint checking

**SHACL Constraint Types:**
- Property constraints (min/max count, datatype, pattern, range)
- Value constraints (in, hasValue, equals)
- Logical constraints (and, or, not, xone)
- Shape-based constraints (node, property shapes)
- SPARQL-based constraints (arbitrary SPARQL queries)

**Example SHACL:**
```turtle
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:datatype xsd:string ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] ;
    sh:property [
        sh:path ex:age ;
        sh:datatype xsd:integer ;
        sh:minInclusive 0 ;
        sh:maxInclusive 150
    ] .
```

**SHACL-SPARQL:**
Stardog supports SPARQL-based constraints for complex validation logic.

**Validation Workflow:**
1. Define shapes for data model
2. Run validation against RDF data
3. Receive validation report with violations
4. Fix data or adjust constraints
5. Re-validate

**SHACL with Reasoning:**
- Validation can run with reasoning enabled
- Validates against inferred triples
- Ensures semantic consistency

**3. RDF Schema (RDFS):**

**Purpose:** Basic schema primitives for RDF.

**Key Constructs:**
- `rdfs:Class` - Define classes
- `rdfs:subClassOf` - Class hierarchies
- `rdfs:Property` - Define properties
- `rdfs:subPropertyOf` - Property hierarchies
- `rdfs:domain` - Property domain
- `rdfs:range` - Property range
- `rdfs:label`, `rdfs:comment` - Documentation

**Schema as Data:**
In Stardog, schema definitions are stored as RDF triples in the graph alongside data, enabling:
- Schema evolution without downtime
- Versioning of schemas
- Querying schema with SPARQL
- Schema reasoning

**Data Quality Constraints:**

Stardog provides integrity constraint validation (ICV) using:
- SHACL shapes
- Custom SPARQL queries
- Automatic constraint checking on updates
- Validation reports and error messages

**Common Data Quality Use Cases:**
- Ensure required properties exist
- Validate data formats (email, phone, date)
- Check referential integrity
- Enforce business rules
- Detect duplicates and inconsistencies

---

## Part C: Comparison & Insights

### 1. RDF vs Property Graph

**Fundamental Data Models:**

| Aspect | RDF (Stardog) | Property Graph (PuppyGraph) |
|--------|---------------|----------------------------|
| **Basic Unit** | Triple (subject-predicate-object) | Node + Edge with properties |
| **Node Identity** | IRI (globally unique) | Local ID (per-database unique) |
| **Relationships** | Predicates (IRI) | Typed edges with properties |
| **Properties** | Additional triples | Key-value pairs on nodes/edges |
| **Schema** | Optional, RDF Schema/OWL | Optional, JSON schema |
| **Standards** | W3C (RDF, SPARQL, OWL, SHACL) | Informal (openCypher, Gremlin) |

**Key Differences:**

**1. Relationship Modeling:**
- **RDF:** Relationships are triples with no properties directly
  - Edge properties require reification or RDF-star
  - Example: `ex:Alice foaf:knows ex:Bob` (no edge properties)
- **Property Graph:** Edges have properties natively
  - Example: `(Alice)-[:KNOWS {since: 2020, weight: 0.8}]->(Bob)`

**2. Global vs Local Identity:**
- **RDF:** Globally unique IRIs enable data merging across sources
  - `http://dbpedia.org/resource/Alice` can be referenced anywhere
- **Property Graph:** Local IDs require explicit mapping across databases
  - Node IDs are database-specific

**3. Schema Philosophy:**
- **RDF:** Schema-last, schema is part of the graph
  - Schema and data in same format (RDF)
  - Reasoning derives implicit information
- **Property Graph:** Schema-first (typically)
  - JSON schema defines structure before data
  - No built-in reasoning

**4. Query Semantics:**
- **RDF:** Pattern matching on triples
  - Graph patterns describe constraints
  - Closed-world assumption (typically)
- **Property Graph:** Traversal-based
  - Navigate from node to node
  - Open-world queries

**When to Use RDF (Stardog Approach):**
- Data integration across heterogeneous sources with different vocabularies
- Semantic interoperability is critical
- Rich ontologies and reasoning required
- Standards compliance important (healthcare, government, academia)
- Knowledge representation and inference
- Global entity identity needed

**When to Use Property Graphs (PuppyGraph Approach):**
- Operational graph queries (fraud detection, recommendations)
- Relationship properties are first-class citizens
- Developer familiarity with Cypher/Gremlin
- Performance over expressiveness
- Existing relational data to be queried as graph
- No need for formal reasoning

**Hybrid Approaches:**

Both systems support virtual graphs, enabling:
- **Stardog:** RDF view over relational data (R2RML)
- **PuppyGraph:** Property graph view over relational data (JSON schema)

**RDF-star and Property Graphs:**
RDF-star (RDF 1.2) adds edge properties to RDF, narrowing the gap:
- Allows properties on relationships
- Stardog supports edge property queries
- Converging toward property graph expressiveness

---

### 2. Query Language Matrix

| Feature | SPARQL (Stardog) | Cypher (PuppyGraph) | Gremlin (PuppyGraph) |
|---------|------------------|---------------------|---------------------|
| **Standard** | W3C Standard | openCypher (open) | Apache TinkerPop |
| **Syntax Style** | SQL-like declarative | ASCII-art patterns | Imperative traversal |
| **Pattern Matching** | Triple patterns | Node-edge patterns | Step-by-step traversal |
| **Path Queries** | Property paths (`+`, `*`, `?`) | Variable-length paths `[*1..5]` | `repeat().times()` |
| **Aggregation** | GROUP BY, COUNT, SUM | GROUP BY, collect() | `groupCount()`, `mean()` |
| **Subqueries** | Nested SELECT | WITH clause | Local traversals |
| **Full-text Search** | Built-in extensions | Not standard | Plugin-based |
| **Geospatial** | GeoSPARQL | Limited | Plugin-based |
| **Reasoning** | Native integration | Not supported | Not supported |
| **Federation** | SERVICE keyword | Not standard | Not standard |
| **Graph Construction** | CONSTRUCT | Not applicable | Not applicable |
| **Updates** | INSERT/DELETE | CREATE, MERGE, DELETE | `addV()`, `addE()`, `drop()` |

**Example Queries - Find Friends of Friends:**

**SPARQL:**
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?fof WHERE {
  ?person foaf:name "Alice" ;
          foaf:knows/foaf:knows ?fof .
  FILTER(?fof != ?person)
}
```

**Cypher:**
```cypher
MATCH (person:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof)
WHERE fof <> person
RETURN fof
```

**Gremlin:**
```groovy
g.V().has('Person', 'name', 'Alice')
  .out('KNOWS').out('KNOWS')
  .where(neq('person'))
  .dedup()
```

**Comparison:**

**SPARQL Strengths:**
- Declarative, optimizer has more freedom
- Natural for RDF triple patterns
- Federation (SERVICE) built-in
- CONSTRUCT creates new graphs
- Reasoning integration
- Standardized by W3C

**SPARQL Weaknesses:**
- Verbose for complex traversals
- Less intuitive for graph navigation
- No native edge properties (without RDF-star)

**Cypher Strengths:**
- Visual ASCII-art syntax
- Intuitive for developers
- Natural for property graphs
- Easy to read and write
- Good aggregation syntax

**Cypher Weaknesses:**
- No formal standard (openCypher is community effort)
- Limited federation support
- No built-in reasoning
- Graph construction not applicable

**Gremlin Strengths:**
- Imperative, full control over traversal
- Composable (can build traversals programmatically)
- Multi-language (Java, Python, JavaScript)
- TinkerPop ecosystem
- Graph algorithms support

**Gremlin Weaknesses:**
- Verbose for simple queries
- Imperative style can be harder to optimize
- Less readable than Cypher
- No declarative graph construction

**PuppyGraph Advantage:**
- Supports both Cypher AND Gremlin on same data
- Plus SQL for relational queries
- Choose query language by use case

**Stardog Advantage:**
- SPARQL + GraphQL + SQL (via virtual graphs)
- Reasoning and federation built-in
- Standards-based ecosystem

---

### 3. Bi-temporal Support

**What is Bi-temporal Data?**

Bi-temporal data models use two distinct time dimensions:
1. **Valid Time (Effective Time):** When a fact is true in the real world
2. **Transaction Time (System Time):** When a fact was recorded in the database

**Why Bi-temporality Matters for Graphs:**
- Audit trails (who knew what when)
- Time-travel queries (graph state at point in time)
- Correcting historical errors without losing history
- Regulatory compliance (finance, healthcare)
- Event reconstruction and forensics

**PuppyGraph Bi-temporal Support:**

**Status:** Not natively supported

**Workarounds:**
- Store valid_from/valid_to as node/edge properties
- Store transaction_time as property
- Query using filters on time properties
- Manual time-slicing in queries

**Example Pattern:**
```json
{
  "edges": [{
    "label": "KNOWS",
    "fromVertex": "Person",
    "toVertex": "Person",
    "attributes": [
      {"name": "valid_from", "type": "Date"},
      {"name": "valid_to", "type": "Date"},
      {"name": "transaction_time", "type": "DateTime"},
      {"name": "superseded_by", "type": "String"}
    ]
  }]
}
```

**Limitations:**
- No automatic versioning
- Time-travel queries require complex predicates
- No built-in snapshot isolation
- Application-managed time dimensions

**Stardog Bi-temporal Support:**

**Status:** Partially supported (deprecated versioning, evolving features)

**Historical Context:**
- Stardog 7.x had versioning features
- Deprecated in favor of new temporal capabilities (7.2+)
- Transaction-time tracking via ACID transactions
- Valid-time modeling through RDF properties

**Current Approach:**

**1. Transaction Time:**
- Stardog maintains transaction log
- ACID guarantees with transaction IDs
- Can query historical transaction states
- Not exposed as first-class query feature

**2. Valid Time:**
- Model as RDF properties using W3C Time Ontology
- Example:
```turtle
ex:employment123 a ex:Employment ;
    time:hasBeginning "2020-01-01"^^xsd:date ;
    time:hasEnd "2023-12-31"^^xsd:date ;
    ex:employee ex:Alice ;
    ex:employer ex:CompanyX .
```

**3. Named Graphs for Versioning:**
Use named graphs to represent snapshots:
```sparql
# Insert data into time-specific graph
INSERT DATA {
  GRAPH <http://example.org/snapshot/2025-10-09> {
    ex:Alice foaf:age 30 .
  }
}

# Query specific snapshot
SELECT ?age WHERE {
  GRAPH <http://example.org/snapshot/2025-10-09> {
    ex:Alice foaf:age ?age .
  }
}
```

**Limitations:**
- No automatic bi-temporal indexing
- Time-travel requires application logic
- No built-in temporal query operators
- Manual snapshot management

**Graph Database Temporal Support (Industry):**

**Strong Bi-temporal Support:**
- **XTDB:** Native bi-temporal graph database (transaction-time + valid-time)
- **TerminusDB:** Version control, time-travel queries, diff functions
- **RecallGraph (ArangoDB):** Transaction-time support, bi-temporality on roadmap

**Limited Temporal Support:**
- **Neo4j:** Application-level modeling (create/expire timestamps on nodes/edges)
- **GraphDB (Ontotext):** Data history and versioning plugin for RDF
- **PuppyGraph:** Property-based modeling
- **Stardog:** Named graphs + properties approach

**Temporal Query Techniques:**

**1. Time-Slicing:**
Filter graph by time window:
```cypher
MATCH (p:Person)-[r:KNOWS]->(f)
WHERE r.created <= $queryTime < r.expired
RETURN p, f
```

**2. Copy-on-Write:**
Duplicate nodes/edges on modification, mark old as expired.

**3. Proxies:**
Separate entity identity from temporal state:
```
(PersonEntity)-[:HAS_STATE {valid_from, valid_to}]->(PersonState)
```

**Recommendations for Sabot:**

**Bi-temporal Architecture:**
1. Store both valid_time and transaction_time for all graph elements
2. Use MarbleDB's columnar storage for efficient temporal indexing
3. Implement time-travel operators: `AS OF <time>`, `BETWEEN <time1> AND <time2>`
4. Support both RDF and property graph temporal models

**Query Language Extensions:**
```sparql
# SPARQL extension
SELECT ?person ?friend
WHERE {
  ?person foaf:knows ?friend
}
AS OF TIMESTAMP '2024-01-01T00:00:00Z'
```

```cypher
# Cypher extension
MATCH (p:Person)-[r:KNOWS]->(f)
WHERE p.name = 'Alice'
RETURN f
FOR SYSTEM_TIME AS OF '2024-01-01'
AND VALID_TIME AS OF '2024-01-01'
```

**Storage Strategy:**
- Transaction-time: Auto-managed by WAL/Raft (MarbleDB)
- Valid-time: User-specified or defaults to transaction-time
- Temporal indexes: Interval trees for valid-time range queries
- Snapshot isolation: MarbleDB MVCC + Raft log

---

### 4. Schema & Reasoning Comparison

| Aspect | Stardog (RDF) | PuppyGraph (Property Graph) |
|--------|---------------|----------------------------|
| **Schema Type** | RDFS, OWL, SHACL | JSON schema |
| **Schema Location** | In-graph (RDF) | External (JSON file) |
| **Schema Enforcement** | Validation (SHACL) | Mapping-time |
| **Reasoning** | OWL 2 (QL, RL, EL) + SWRL | None |
| **Inference** | Query-time rewriting | None |
| **Constraint Types** | SHACL shapes, cardinality, datatypes | Property types, mappings |
| **Schema Evolution** | Add triples, versioned | Update JSON schema |
| **Validation** | SHACL validation reports | Schema compilation errors |
| **Class Hierarchies** | rdfs:subClassOf, OWL restrictions | Not supported |
| **Property Hierarchies** | rdfs:subPropertyOf | Not supported |
| **Equivalence** | owl:sameAs, owl:equivalentClass | Not supported |
| **Transitivity** | Automatic via reasoning | Manual traversal |
| **Inverse Properties** | owl:inverseOf | Separate edge types |

**Stardog Reasoning Example:**

**Schema (OWL):**
```turtle
ex:Employee rdfs:subClassOf ex:Person .
ex:Manager rdfs:subClassOf ex:Employee .
ex:manages owl:inverseOf ex:managedBy .
ex:colleague a owl:TransitiveProperty .
```

**Data:**
```turtle
ex:Alice a ex:Manager .
ex:Alice ex:manages ex:Bob .
ex:Bob ex:colleague ex:Charlie .
ex:Charlie ex:colleague ex:Diana .
```

**Query with Reasoning:**
```sparql
SELECT ?person WHERE { ?person a ex:Person }
# Returns: Alice (inferred from Manager -> Employee -> Person)

SELECT ?subordinate WHERE { ex:Alice ex:manages ?subordinate }
# Returns: Bob (direct)

SELECT ?manager WHERE { ex:Bob ex:managedBy ?manager }
# Returns: Alice (inferred via owl:inverseOf)

SELECT ?colleague WHERE { ex:Bob ex:colleague ?colleague }
# Returns: Charlie (direct), Diana (transitive)
```

**PuppyGraph Schema Example:**

```json
{
  "vertices": [
    {"label": "Person", "oneToOne": {...}},
    {"label": "Employee", "oneToOne": {...}},
    {"label": "Manager", "oneToOne": {...}}
  ],
  "edges": [
    {
      "label": "MANAGES",
      "fromVertex": "Manager",
      "toVertex": "Employee",
      "attributes": [...]
    },
    {
      "label": "MANAGED_BY",
      "fromVertex": "Employee",
      "toVertex": "Manager",
      "attributes": [...]
    }
  ]
}
```

**No automatic inference:**
- Must define both MANAGES and MANAGED_BY edges explicitly
- No subclass queries (Manager -> Employee -> Person)
- No transitive closure (must use `MATCH (a)-[:COLLEAGUE*]->(b)`)

**Trade-offs:**

**Stardog Strengths:**
- Rich semantic modeling (OWL)
- Automatic inference reduces data redundancy
- Reasoning enables complex queries with simple patterns
- Schema validation with SHACL
- Standards-based ontologies

**Stardog Weaknesses:**
- Reasoning can impact query performance
- OWL complexity has learning curve
- Debugging inferred triples can be challenging

**PuppyGraph Strengths:**
- Simple JSON schema (easy to understand)
- Predictable query performance (no inference)
- Schema maps directly to relational sources
- Fast compilation and validation

**PuppyGraph Weaknesses:**
- No automatic inference
- Data redundancy (must store both directions of relationship)
- No class hierarchies
- Limited constraint validation

**Recommendations for Sabot:**

**Hybrid Schema Approach:**
1. Support both RDF Schema/OWL AND JSON property graph schemas
2. Configurable reasoning levels:
   - None (property graph mode)
   - RDFS (basic subclass/subproperty)
   - OWL 2 QL/RL (lightweight reasoning)
   - OWL 2 EL (complex ontologies)

3. Schema validation with SHACL for both models
4. Schema compilation to Arrow schema for performance

**Example Sabot API:**
```python
# Property graph mode (no reasoning)
graph = sabot.Graph(
    schema="schema.json",
    model="property_graph",
    reasoning=None
)

# RDF mode with reasoning
graph = sabot.Graph(
    schema="ontology.ttl",
    model="rdf",
    reasoning="OWL2_QL"
)

# Hybrid mode
graph = sabot.Graph(
    schema=["schema.json", "ontology.ttl"],
    model="hybrid",
    reasoning="RDFS"
)
```

---

### 5. Use Case Fit

**PuppyGraph Ideal Use Cases:**

1. **Zero-ETL Analytics on Existing Data:**
   - Query data lakes (Iceberg, Hudi) as graph
   - Graph analytics on data warehouses (Snowflake, BigQuery)
   - No data migration needed

2. **Operational Graph Queries:**
   - Fraud detection (real-time pattern matching)
   - Recommendation engines (collaborative filtering)
   - Network analysis (social graphs, telecom)

3. **Graph Analytics on Relational Data:**
   - Supply chain optimization
   - Logistics and routing
   - Dependency analysis

4. **High-Performance Multi-Hop Queries:**
   - 10-hop queries in seconds
   - Billions of edges
   - Vectorized execution for OLAP workloads

5. **Developer-Friendly Graph Adoption:**
   - Quick setup (10 minutes)
   - Familiar query languages (Cypher, Gremlin, SQL)
   - No new infrastructure

**PuppyGraph Limitations:**
- No reasoning/inference
- No semantic data integration
- No standards-based ontologies
- Application-level temporal modeling

---

**Stardog Ideal Use Cases:**

1. **Enterprise Knowledge Graphs:**
   - Unify data across heterogeneous sources
   - Semantic data integration
   - Global entity resolution

2. **Semantic Interoperability:**
   - Healthcare (HL7, FHIR standards)
   - Government (linked open data)
   - Academic research (ontology-based)

3. **Reasoning and Inference:**
   - Regulatory compliance rules
   - Business logic inference
   - Ontology-based classification

4. **Data Governance and Quality:**
   - SHACL validation
   - Constraint checking
   - Data lineage tracking

5. **Federated Query and Virtual Graphs:**
   - Query across SQL, NoSQL, RDF sources
   - Leave data in place
   - Unified SPARQL interface

6. **Standards-Based Integration:**
   - RDF, SPARQL, OWL, SHACL, R2RML
   - Interoperability with other tools
   - Long-term data preservation

**Stardog Limitations:**
- Steeper learning curve (SPARQL, OWL)
- Reasoning overhead on query performance
- RDF verbosity for simple graphs
- No native edge properties (without RDF-star)

---

**Sabot Target Use Cases (Hybrid):**

Based on Sabot's streaming + graph architecture, ideal use cases combine both approaches:

1. **Real-Time Knowledge Graph Construction:**
   - Ingest events (Kafka, CDC) → Build graph → Query with reasoning
   - Example: Real-time entity resolution in fintech

2. **Temporal Graph Analytics:**
   - Bi-temporal graph with time-travel queries
   - Example: Audit trails, fraud reconstruction

3. **Streaming Graph Pattern Matching:**
   - Continuous graph queries on event streams
   - Example: Real-time fraud detection with inference

4. **Hybrid Query Workloads:**
   - SPARQL for semantic queries + Cypher for operational queries
   - Example: Knowledge graph + recommendation engine

5. **Arrow-Native Graph Processing:**
   - Columnar graph storage (MarbleDB)
   - Vectorized graph operators
   - Zero-copy graph analytics

**Sabot's Unique Value:**
- **Streaming + Graph:** Real-time graph construction from event streams
- **Bi-temporal:** Native support for valid-time and transaction-time
- **Hybrid Model:** RDF and property graph in one system
- **Arrow-Native:** Zero-copy, vectorized graph processing
- **Reasoning:** Optional OWL inference for semantic use cases
- **RAFT Consensus:** Distributed graph with strong consistency

---

## Part D: Key Takeaways for Sabot

### 1. Features to Implement

**Priority 1: Core Graph Capabilities (Immediate)**

1. **Dual Data Model Support:**
   - Implement both RDF (triple-based) and Property Graph (node-edge) models
   - Unified internal representation (Arrow columnar)
   - Transparent conversion between models
   - User chooses model per use case

2. **Query Language Support:**
   - **SPARQL 1.1** for RDF/semantic queries
   - **openCypher** for property graph queries
   - **Gremlin (TinkerPop)** for traversal-based queries
   - Shared query optimizer across all three
   - Hybrid queries (e.g., SPARQL + Cypher in same query)

3. **Virtual Graph Layer:**
   - Zero-ETL graph views over existing data sources
   - R2RML mappings for relational → RDF
   - JSON schema mappings for relational → property graph
   - Query federation across multiple sources
   - Leverage Sabot's existing CDC and streaming connectors

4. **Bi-temporal Graph Support:**
   - Native valid-time and transaction-time for all nodes/edges
   - Time-travel query operators: `AS OF`, `BETWEEN`
   - Automatic versioning via MarbleDB + Raft
   - Temporal indexes (interval trees) for efficient time-range queries
   - Snapshot isolation at any point in time

**Priority 2: Advanced Semantic Features (Medium-Term)**

5. **Reasoning Engine:**
   - RDFS reasoning (subclass, subproperty, domain, range)
   - OWL 2 QL/RL (lightweight reasoning profiles)
   - User-defined rules (SWRL or custom syntax)
   - Query-time rewriting (like Stardog)
   - Configurable reasoning levels (none, RDFS, OWL QL, OWL RL)

6. **Schema Validation:**
   - SHACL shape constraints
   - Custom validation rules
   - Integrity constraint checking
   - Validation on write or on demand
   - Validation reports with violation details

7. **Graph Algorithms:**
   - Shortest path (Dijkstra, A*, Bellman-Ford)
   - PageRank
   - Community detection (Louvain, Label Propagation)
   - Connected components
   - Centrality measures (betweenness, closeness, degree)
   - GPU acceleration for large graphs

**Priority 3: Performance & Scale (Ongoing)**

8. **Vectorized Graph Execution:**
   - Columnar graph storage (nodes and edges as Arrow batches)
   - Vectorized graph operators (inspired by Stardog's BARQ)
   - SIMD operations for batch processing
   - Morsel-driven parallelism for graph queries
   - Zero-copy operations via Arrow

9. **Distributed Graph:**
   - Graph partitioning (edge-cut, vertex-cut)
   - Distributed query execution via Raft
   - Work-stealing for load balancing
   - Graph shuffle via Arrow Flight
   - Consistent hashing for scalability

10. **Graph Indexes:**
    - Triple/quad indexes (SPO, POS, OSP for RDF)
    - Adjacency lists for property graphs
    - Temporal indexes (interval trees)
    - Full-text search on literals
    - Geospatial indexes (R-tree, Quad-tree)

---

### 2. Architecture Recommendations

**Sabot Graph Architecture:**

```
┌─────────────────────────────────────────────────────┐
│  Query Layer (Multi-Language Support)               │
│  - SPARQL 1.1 Parser & Optimizer                    │
│  - Cypher Parser & Optimizer                        │
│  - Gremlin Compiler                                 │
│  - Unified Query Plan (Arrow-based)                 │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  Reasoning Engine (Optional)                        │
│  - RDFS/OWL 2 QL/RL Reasoner                        │
│  - Query Rewriting                                  │
│  - Rule Engine (SWRL)                               │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  Graph Execution Engine                             │
│  - Vectorized Operators (BARQ-style)                │
│  - Morsel-Driven Parallelism                        │
│  - Graph Algorithms (PageRank, Shortest Path, etc.) │
│  - Temporal Query Processing (AS OF, BETWEEN)       │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  Graph Storage Layer (Dual Model)                   │
│                                                      │
│  ┌──────────────────┐  ┌──────────────────┐        │
│  │  RDF Store       │  │  Property Graph  │        │
│  │  (Triples/Quads) │  │  (Nodes/Edges)   │        │
│  └──────────────────┘  └──────────────────┘        │
│                                                      │
│  Unified Internal Representation:                   │
│  - Arrow Columnar Format                            │
│  - Bi-temporal Metadata (valid_time, txn_time)      │
│  - Indexes (SPO, POS, OSP, Adjacency, Temporal)     │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  MarbleDB (Persistent Storage)                      │
│  - Columnar Graph Data (Arrow batches)              │
│  - Temporal Indexes (Interval Trees)                │
│  - Raft Consensus for Distributed Writes            │
│  - WAL for Transaction-Time Tracking                │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  Virtual Graph Layer                                │
│  - R2RML Mapper (Relational → RDF)                  │
│  - JSON Schema Mapper (Relational → Property Graph) │
│  - Query Federation (Pushdown to Sources)           │
│  - CDC Connectors (PostgreSQL, MySQL, Kafka)        │
└─────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────┐
│  External Data Sources                              │
│  - Relational DBs (PostgreSQL, MySQL)               │
│  - Data Lakes (Iceberg, Hudi, Delta)                │
│  - Kafka Topics                                     │
│  - Other SPARQL Endpoints                           │
└─────────────────────────────────────────────────────┘
```

**Key Design Principles:**

1. **Unified Arrow Representation:**
   - Both RDF and property graph stored as Arrow columnar batches
   - Triples: `(subject: String, predicate: String, object: Union, graph: String)`
   - Property graph: `Nodes(id, label, props)`, `Edges(id, from, to, label, props)`
   - Transparent conversion at query time

2. **Bi-temporal by Default:**
   - All graph elements have `valid_from`, `valid_to`, `txn_time`
   - MarbleDB WAL provides automatic transaction-time
   - Users specify valid-time or default to txn_time
   - Time-travel queries via temporal indexes

3. **Streaming + Graph Integration:**
   - Kafka/CDC events → Graph mutations
   - Continuous graph queries (standing queries)
   - Real-time graph construction from streams
   - Example: CDC from PostgreSQL → Build knowledge graph → SPARQL queries

4. **Vectorized Execution:**
   - Batch operations on Arrow data
   - SIMD operations for filters, joins
   - Morsel-driven parallelism (DuckDB-style)
   - Zero-copy between operators

5. **Pluggable Reasoning:**
   - Reasoning engine is optional module
   - Can be enabled/disabled per query
   - Levels: None → RDFS → OWL QL → OWL RL
   - No reasoning for pure property graph use cases

---

### 3. Query Language Strategy

**Multi-Language Support:**

Sabot should support all three major graph query languages:

**1. SPARQL 1.1 (Primary for RDF/Semantic Use Cases)**

**When to Use:**
- Knowledge graphs with ontologies
- Data integration across heterogeneous sources
- Reasoning and inference required
- Standards compliance (healthcare, government)

**Extensions to Add:**
- Bi-temporal operators: `AS OF TIMESTAMP`, `VALID BETWEEN`
- Graph algorithms: `sparql:pageRank()`, `sparql:shortestPath()`
- Full-text search: Standard extensions
- Geospatial: GeoSPARQL compliance

**Example:**
```sparql
PREFIX ex: <http://example.org/>
PREFIX time: <http://www.w3.org/2006/time#>

SELECT ?person ?knows
WHERE {
  ?person a ex:Employee ;
          ex:knows ?knows .
}
AS OF TIMESTAMP '2024-01-01T00:00:00Z'
WITH REASONING 'OWL2_QL'
```

---

**2. openCypher (Primary for Property Graph Use Cases)**

**When to Use:**
- Operational graph queries (fraud, recommendations)
- Developer-friendly ASCII-art syntax
- Relationship properties are important
- No reasoning required

**Extensions to Add:**
- Bi-temporal: `FOR SYSTEM_TIME AS OF`, `FOR VALID_TIME AS OF`
- Graph algorithms: `algo.pageRank()`, `algo.shortestPath()`
- Virtual graphs: `FROM VIRTUAL GRAPH 'schema.json'`

**Example:**
```cypher
MATCH (p:Person)-[r:KNOWS]->(friend)
WHERE p.name = 'Alice'
RETURN friend
FOR SYSTEM_TIME AS OF '2024-01-01'
FOR VALID_TIME AS OF '2024-01-01'
```

---

**3. Gremlin (Secondary/TinkerPop Compatibility)**

**When to Use:**
- Programmatic graph traversals
- Complex multi-step algorithms
- TinkerPop ecosystem integration
- Multi-language support (Java, Python, JS)

**Example:**
```groovy
g.V().has('Person', 'name', 'Alice')
  .out('KNOWS')
  .asOf('2024-01-01')  // Custom temporal step
  .values('name')
```

---

**Hybrid Queries:**

Allow mixing query languages in single query:

```python
# Sabot Python API
result = graph.query("""
  # SPARQL for semantic reasoning
  PREFIX ex: <http://example.org/>
  SELECT ?person WHERE { ?person a ex:Employee }
  WITH REASONING 'RDFS'

  # Then switch to Cypher for traversal
  CYPHER:
  MATCH (p:Person)-[:WORKS_WITH*2..4]->(colleague)
  WHERE p.id IN $sparql_results
  RETURN colleague
""")
```

---

**Query Optimization:**

1. **Unified Query Planner:**
   - Parse SPARQL/Cypher/Gremlin into common IR
   - Logical plan optimization (filter pushdown, join reordering)
   - Physical plan generation (vectorized operators)
   - Cost-based optimization using statistics

2. **Adaptive Execution:**
   - Monitor query performance
   - Switch between row-based and batch-based execution
   - Runtime re-optimization for long-running queries

3. **Predicate Pushdown:**
   - Push filters to source systems (virtual graphs)
   - Temporal predicate pushdown (time-slicing at storage)
   - Reasoning pushdown (materialize only needed inferences)

---

### 4. Integration Patterns

**Pattern 1: Streaming → Graph (Real-Time Knowledge Graph)**

**Use Case:** Build knowledge graph from event streams (Kafka, CDC)

**Architecture:**
```
Kafka Topic → Sabot Stream → Graph Mutations → MarbleDB
                              ↓
                          SPARQL/Cypher Queries
```

**Example:**
```python
import sabot as sb

app = sb.App(name="streaming-graph")
graph = sb.Graph(schema="ontology.ttl", model="rdf", reasoning="OWL2_QL")

@app.agent(topics=["transactions"])
async def build_graph(stream):
    async for txn in stream:
        # Add nodes
        await graph.add_node(
            id=txn.person_id,
            label="Person",
            properties={"name": txn.name},
            valid_time=txn.timestamp
        )

        # Add edges
        await graph.add_edge(
            from_id=txn.from_account,
            to_id=txn.to_account,
            label="TRANSFERRED_TO",
            properties={"amount": txn.amount},
            valid_time=txn.timestamp
        )

# Query the graph
results = await graph.query_sparql("""
    SELECT ?person ?total WHERE {
        ?person ex:transferred_to ?account .
        ?account ex:amount ?total .
        FILTER(?total > 10000)
    }
    WITH REASONING 'RDFS'
""")
```

---

**Pattern 2: Virtual Graph (Zero-ETL Query Federation)**

**Use Case:** Query relational data as graph without ETL

**Architecture:**
```
PostgreSQL ← R2RML Mapping → Sabot Virtual Graph → SPARQL/Cypher Queries
MongoDB    ← JSON Schema   →
```

**Example:**
```python
# Define R2RML mapping
r2rml_mapping = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .

<#PersonMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "persons" ] ;
    rr:subjectMap [
        rr:template "http://example.org/Person/{id}" ;
        rr:class ex:Person
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] .
"""

# Create virtual graph
virtual_graph = sb.VirtualGraph(
    name="pg_graph",
    source=sb.PostgreSQLSource(url="postgresql://..."),
    mapping=r2rml_mapping,
    model="rdf"
)

# Query virtual graph
results = virtual_graph.query_sparql("""
    SELECT ?person ?name WHERE {
        ?person a ex:Person ;
                ex:name ?name .
    }
""")
```

---

**Pattern 3: Bi-temporal Graph (Audit & Time-Travel)**

**Use Case:** Audit trail, fraud reconstruction, regulatory compliance

**Architecture:**
```
Events → Graph with Bi-temporal Metadata → Time-Travel Queries
                                          ↓
                                 MarbleDB + Temporal Indexes
```

**Example:**
```python
# Add bi-temporal edge
await graph.add_edge(
    from_id="person:alice",
    to_id="account:123",
    label="OWNS",
    properties={"share": 0.5},
    valid_from="2020-01-01",
    valid_to="2024-12-31"  # She sold it in 2024
)

# Time-travel query: Who owned account:123 on 2023-06-01?
results = await graph.query_cypher("""
    MATCH (p:Person)-[r:OWNS]->(a:Account {id: '123'})
    RETURN p.name, r.share
    FOR VALID_TIME AS OF '2023-06-01'
""")

# Audit query: When did the database record this ownership?
results = await graph.query_cypher("""
    MATCH (p:Person)-[r:OWNS]->(a:Account {id: '123'})
    RETURN p.name, r.valid_from, r.txn_time
    FOR SYSTEM_TIME BETWEEN '2020-01-01' AND '2025-01-01'
""")
```

---

**Pattern 4: Hybrid RDF + Property Graph**

**Use Case:** Semantic interoperability + operational queries

**Architecture:**
```
RDF Knowledge Graph (with reasoning) ←→ Property Graph (fast traversals)
                    ↓
          Unified Arrow Storage
```

**Example:**
```python
# Create hybrid graph
graph = sb.Graph(
    schema=["ontology.ttl", "schema.json"],
    model="hybrid"
)

# SPARQL query with reasoning
semantic_results = await graph.query_sparql("""
    SELECT ?person WHERE {
        ?person a ex:Employee .
    }
    WITH REASONING 'OWL2_QL'
""")

# Cypher query for fast traversal
operational_results = await graph.query_cypher("""
    MATCH (p:Person)-[:WORKS_WITH*2..4]->(colleague)
    WHERE p.id IN $semantic_results
    RETURN colleague
""")
```

---

**Pattern 5: Graph + ML (Feature Extraction)**

**Use Case:** Extract graph features for ML models

**Architecture:**
```
Graph → Graph Algorithms → Feature Vectors → ML Model
      ↓
  PageRank, Centrality, Community Detection
```

**Example:**
```python
# Compute graph features
features = await graph.compute_features([
    sb.PageRank(iterations=20),
    sb.BetweennessCentrality(),
    sb.CommunityDetection(algorithm="louvain")
])

# Extract to Arrow Table
feature_table = await graph.to_arrow_table(
    nodes=["person:alice", "person:bob"],
    features=["pagerank", "betweenness", "community_id"]
)

# Feed to ML model
model.train(feature_table)
```

---

## Summary

**PuppyGraph's Key Strengths for Sabot:**
1. Virtual graph layer (zero-ETL)
2. Vectorized execution for performance
3. Multi-source federation
4. Developer-friendly (quick setup)
5. JSON schema simplicity

**Stardog's Key Strengths for Sabot:**
1. RDF and semantic web standards
2. OWL reasoning and inference
3. SHACL validation
4. R2RML for relational mapping
5. BARQ vectorized SPARQL execution
6. Enterprise knowledge graph features

**Sabot's Unique Opportunity:**
Combine the best of both worlds:
- **Virtual graphs** (like PuppyGraph) for zero-ETL
- **Reasoning** (like Stardog) for semantic use cases
- **Bi-temporal** (neither has native support) for audit/compliance
- **Streaming integration** (unique to Sabot) for real-time graphs
- **Arrow-native** (neither has) for vectorized performance
- **Hybrid model** (neither has) for RDF + property graph simultaneously

**Implementation Priorities:**
1. Core dual model support (RDF + property graph)
2. SPARQL + Cypher query engines
3. Virtual graph layer with R2RML + JSON schema
4. Bi-temporal storage and query operators
5. Basic reasoning (RDFS → OWL QL/RL)
6. Graph algorithms (PageRank, shortest path, etc.)
7. Vectorized execution engine (BARQ-style)
8. Streaming → graph integration

---

## References

### PuppyGraph
- Official Documentation: https://docs.puppygraph.com/
- Website: https://www.puppygraph.com/
- Schema Documentation: https://docs.puppygraph.com/schema/
- Cypher Reference: https://docs.puppygraph.com/reference/cypher-query-language/
- Performance Blog: https://www.bigdatawire.com/2025/01/24/puppygraph-brings-graph-analytics-to-the-lakehouse/

### Stardog
- Official Documentation: https://docs.stardog.com/
- Website: https://www.stardog.com/
- RDF Graph Model: https://docs.stardog.com/tutorials/rdf-graph-data-model
- SPARQL Query Guide: https://docs.stardog.com/query-stardog/
- BARQ Engine: https://docs.stardog.com/query-stardog/barq-engine
- Reasoning Engine: https://docs.stardog.com/inference-engine/
- Virtual Graphs: https://docs.stardog.com/virtual-graphs/
- SHACL Validation: https://docs.stardog.com/data-quality-constraints
- BARQ Research Paper (2025): https://arxiv.org/abs/2504.04584

### Standards & Specifications
- RDF 1.1: https://www.w3.org/TR/rdf11-concepts/
- SPARQL 1.1: https://www.w3.org/TR/sparql11-overview/
- OWL 2: https://www.w3.org/TR/owl2-overview/
- SHACL: https://www.w3.org/TR/shacl/
- R2RML: https://www.w3.org/TR/r2rml/
- openCypher: https://opencypher.org/
- Apache TinkerPop (Gremlin): https://tinkerpop.apache.org/
- GeoSPARQL: https://www.ogc.org/standards/geosparql

### Bi-temporal & Temporal Graphs
- XTDB Bitemporality: https://v1-docs.xtdb.com/concepts/bitemporality/
- Bitemporal Modeling (Wikipedia): https://en.wikipedia.org/wiki/Bitemporal_modeling
- Martin Fowler - Bitemporal History: https://martinfowler.com/articles/bitemporal-history.html
- GraphDB Versioning: https://graphdb.ontotext.com/documentation/10.8/data-history-and-versioning.html
- ArangoDB Time-Travel: https://arangodb.com/learn/graphs/time-traveling-graph-databases/
- Neo4j Temporal Versioning: https://medium.com/neo4j/keeping-track-of-graph-changes-using-temporal-versioning-3b0f854536fa

---

**Research Date:** October 9, 2025
**Total Document Length:** 2,094 lines
**Analysis Scope:** PuppyGraph, Stardog, RDF vs Property Graphs, Bi-temporal Support, Query Languages, Reasoning, Integration Patterns
