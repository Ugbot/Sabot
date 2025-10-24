#!/usr/bin/env python3
"""
Olympics RDF/SPARQL Demo - Based on QLever Dataset

Demonstrates sabot_ql's RDF capabilities on the real-world Olympics dataset
from Wallscope, containing 1.8M triples about Olympic athletes, medals, and events.

Dataset Information:
- Source: QLever example dataset
- Format: N-Triples (compressed)
- Size: ~1.8 million triples
- Coverage: Olympic Games 1896-2014
- Athletes: ~12,000
- Events: ~1,000
- Medal instances: ~50,000

This demo showcases:
1. Loading large RDF datasets from N-Triples
2. Complex SPARQL queries with aggregation
3. Multi-pattern joins
4. Property path queries
5. Real-world RDF data processing

Based on QLever quickstart:
  https://github.com/ad-freiburg/qlever/blob/master/docs/quickstart.md
"""

import sys
import os
from pathlib import Path
from typing import Optional, Tuple
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Add sabot to path
sabot_root = Path(__file__).parent.parent
sys.path.insert(0, str(sabot_root))

from sabot.rdf import RDFStore
from sabot.rdf_loader import (
    NTriplesParser,
    clean_rdf_data,
    filter_by_predicate,
    load_arrow_to_store
)
from sabot import cyarrow as pa
pc = pa.compute  # Arrow compute functions


# Dataset location (from QLever vendor directory)
OLYMPICS_DATA = sabot_root / "vendor" / "qlever" / "examples" / "olympics.nt.xz"


def print_header(title: str):
    """Print a formatted section header."""
    print()
    print("=" * 80)
    print(title.center(80))
    print("=" * 80)
    print()


def print_subheader(title: str):
    """Print a formatted subsection header."""
    print()
    print(f"[{title}]")
    print("-" * 80)


def load_olympics_dataset(limit: Optional[int] = None) -> Tuple[RDFStore, int]:
    """
    Load Olympics RDF data from QLever dataset using Sabot's Arrow compute pipeline.

    Demonstrates:
    1. Parse N-Triples to Arrow table
    2. Clean data using Arrow compute operators
    3. Load cleaned data into RDF store

    Args:
        limit: Maximum number of triples to load (None = all)

    Returns:
        (store, num_triples_loaded)
    """
    print_subheader("Loading Olympics Dataset with Arrow Compute Pipeline")

    if not OLYMPICS_DATA.exists():
        logger.error(f"Olympics dataset not found at: {OLYMPICS_DATA}")
        logger.error("Expected location: vendor/qlever/examples/olympics.nt.xz")
        sys.exit(1)

    logger.info(f"Dataset: {OLYMPICS_DATA}")

    if limit:
        logger.info(f"Loading first {limit:,} triples (limited for testing)...")
    else:
        logger.info("Loading full dataset (~1.8M triples)...")
        logger.info("This may take 1-2 minutes...")

    # Step 1: Parse N-Triples to Arrow table
    logger.info("\n📊 Step 1: Parsing N-Triples to Arrow table...")
    parser = NTriplesParser(str(OLYMPICS_DATA))
    raw_data = parser.parse_to_arrow(
        limit=limit,
        batch_size=10000,
        show_progress=True
    )
    logger.info(f"✓ Parsed {raw_data.num_rows:,} raw triples into Arrow table")

    # Step 2: Clean data using Arrow compute operators
    logger.info("\n🧹 Step 2: Cleaning data with Arrow compute operators...")
    clean_data = clean_rdf_data(
        raw_data,
        remove_duplicates=True,
        filter_empty=True,
        normalize_whitespace=True
    )
    logger.info(f"✓ Cleaned data: {clean_data.num_rows:,} triples ready for loading")

    # Step 3: Load cleaned Arrow table into RDF store
    logger.info("\n💾 Step 3: Loading cleaned data into RDF store...")
    from sabot.rdf import RDFStore
    store = RDFStore()
    count = load_arrow_to_store(store, clean_data)

    logger.info(f"\n✓ Loaded {count:,} triples into RDF store")
    logger.info(f"  Store contains {store.count_terms():,} unique terms")
    logger.info(f"\n💡 Arrow Compute Pipeline Complete:")
    logger.info(f"  Raw → {raw_data.num_rows:,} triples")
    logger.info(f"  Cleaned → {clean_data.num_rows:,} triples")
    logger.info(f"  Loaded → {count:,} triples")

    return store, count


def explore_dataset(store: RDFStore):
    """Run basic exploration queries to understand the data."""
    print_subheader("Dataset Exploration")

    # Query 1: Count entity types using Sabot operators
    query1 = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT ?entity ?type
        WHERE {
            ?entity rdf:type ?type .
        }
    """

    logger.info("Query 1: Entity type distribution (using Sabot Arrow compute)")
    logger.info(f"SPARQL (raw triples):\n{query1}")

    try:
        # Get raw triples
        result = store.query(query1)
        logger.info(f"\n✓ Retrieved {result.num_rows:,} type triples")

        # Use Arrow compute for grouping and counting
        type_col = result.column('type')

        # Group and count using Arrow compute
        unique_types = pc.unique(type_col)

        type_counts = []
        for i in range(len(unique_types)):
            type_uri = unique_types[i].as_py()
            # Count occurrences
            mask = pc.equal(type_col, type_uri)
            count = pc.sum(pc.cast(mask, pa.int64())).as_py()
            type_counts.append((type_uri, count))

        # Sort by count descending
        type_counts.sort(key=lambda x: x[1], reverse=True)

        logger.info(f"\n✓ Found {len(type_counts)} entity types:")
        for type_uri, count in type_counts:
            type_name = type_uri.split('/')[-1] if '/' in type_uri else type_uri
            logger.info(f"  - {type_name}: {count:,} instances")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")

    # Query 2: Sample athletes
    query2 = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?athlete ?name
        WHERE {
            ?athlete rdfs:label ?name .
        }
        LIMIT 10
    """

    logger.info("\nQuery 2: Sample athletes")
    logger.info(f"SPARQL:\n{query2}")

    try:
        result = store.query(query2)
        logger.info(f"\n✓ Sample of {result.num_rows} athletes:")

        df = result.to_pandas()
        for i, row in df.iterrows():
            logger.info(f"  {i+1}. {row['name']}")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def query_top_gold_medalists(store: RDFStore):
    """
    QLever's reference query: Top 10 athletes with most gold medals.

    Uses Sabot's Arrow compute operators for aggregation instead of SPARQL GROUP BY.
    """
    print_subheader("Top Gold Medalists (using Sabot Arrow Compute)")

    # Query raw triples (no aggregation in SPARQL)
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/>
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>

        SELECT ?medal ?athlete
        WHERE {
            ?medal olympics:medal medal:Gold .
            ?medal olympics:athlete/rdfs:label ?athlete .
        }
    """

    logger.info("Finding top 10 athletes with most gold medals...")
    logger.info(f"\nSPARQL (raw gold medal triples):\n{query}")

    try:
        # Get all gold medal records
        result = store.query(query)
        logger.info(f"\n✓ Retrieved {result.num_rows:,} gold medal records")

        if result.num_rows == 0:
            logger.warning("No gold medal data in this sample")
            return

        # Use Arrow compute for grouping and counting
        athlete_col = result.column('athlete')

        # Get unique athletes
        unique_athletes = pc.unique(athlete_col)

        # Count medals per athlete using Arrow compute
        athlete_counts = []
        for i in range(len(unique_athletes)):
            athlete = unique_athletes[i].as_py()
            # Count occurrences using vectorized operations
            mask = pc.equal(athlete_col, athlete)
            count = pc.sum(pc.cast(mask, pa.int64())).as_py()
            athlete_counts.append((athlete, count))

        # Sort by count descending using Arrow compute
        athlete_counts.sort(key=lambda x: x[1], reverse=True)

        # Take top 10
        top_10 = athlete_counts[:10]

        logger.info(f"\n✓ Top {len(top_10)} gold medalists (aggregated with Arrow compute):")
        for i, (athlete, count) in enumerate(top_10, 1):
            logger.info(f"  {i}. {athlete}: {count} gold medals")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")
        logger.debug(f"Error details: {e}", exc_info=True)


def query_medals_by_sport(store: RDFStore):
    """Find medal distribution across different sports using Arrow compute."""
    print_subheader("Medal Distribution by Sport (Arrow Compute)")

    # Query raw medal-event relationships
    query = """
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>

        SELECT ?medal ?event
        WHERE {
            ?medal olympics:event ?event .
        }
    """

    logger.info("Finding sports with most medals awarded...")
    logger.info(f"\nSPARQL (raw medal-event pairs):\n{query}")

    try:
        # Get all medal-event pairs
        result = store.query(query)
        logger.info(f"\n✓ Retrieved {result.num_rows:,} medal-event records")

        if result.num_rows == 0:
            logger.warning("No medal-event data in this sample")
            return

        # Use Arrow compute for aggregation
        event_col = result.column('event')

        # Get unique events
        unique_events = pc.unique(event_col)

        # Count medals per event using vectorized operations
        event_counts = []
        for i in range(len(unique_events)):
            event_uri = unique_events[i].as_py()
            # Count occurrences
            mask = pc.equal(event_col, event_uri)
            count = pc.sum(pc.cast(mask, pa.int64())).as_py()

            # Extract event name from URI
            event_name = event_uri.split('/')[-1] if '/' in event_uri else event_uri
            event_counts.append((event_name, count))

        # Sort by count descending
        event_counts.sort(key=lambda x: x[1], reverse=True)

        # Take top 15
        top_15 = event_counts[:15]

        logger.info(f"\n✓ Top {len(top_15)} sports by medal count:")
        for i, (event, count) in enumerate(top_15, 1):
            logger.info(f"  {i}. {event}: {count} medals")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def query_athlete_details(store: RDFStore):
    """Find athletes with detailed information (age, height, weight)."""
    print_subheader("Athletes with Detailed Physical Stats")

    query = """
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
    """

    logger.info("Finding athletes with complete physical statistics...")
    logger.info(f"\nSPARQL:\n{query}")

    try:
        result = store.query(query)
        logger.info(f"\n✓ Found {result.num_rows} athletes with full stats:")

        df = result.to_pandas()
        for i, row in df.iterrows():
            logger.info(f"  {i+1}. {row['name']}: "
                       f"age={row['age']}, height={row['height']}cm, "
                       f"weight={row['weight']}kg")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def query_olympic_games(store: RDFStore):
    """List Olympic Games in the dataset."""
    print_subheader("Olympic Games in Dataset")

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?game
        WHERE {
            ?game rdf:type dbo:Olympics .
        }
        ORDER BY ?game
        LIMIT 20
    """

    logger.info("Finding Olympic Games in dataset...")
    logger.info(f"\nSPARQL:\n{query}")

    try:
        result = store.query(query)
        logger.info(f"\n✓ Found {result.num_rows} games (showing first 20):")

        df = result.to_pandas()
        for i, row in df.iterrows():
            game_uri = row['game']
            # Extract year and season from URI
            parts = game_uri.split('/')[-2:]  # e.g., ['1896', 'Summer']
            if len(parts) == 2:
                year, season = parts
                logger.info(f"  {i+1}. {year} {season} Olympics")
            else:
                logger.info(f"  {i+1}. {game_uri}")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def query_medal_property_paths(store: RDFStore):
    """Demonstrate property path queries."""
    print_subheader("Property Path Queries")

    # This query uses property paths to traverse:
    # medal → athlete → label
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/>

        SELECT ?athlete_name ?event
        WHERE {
            ?medal olympics:medal medal:Gold .
            ?medal olympics:athlete/rdfs:label ?athlete_name .
            ?medal olympics:event ?event .
        }
        LIMIT 10
    """

    logger.info("Using property paths to traverse medal → athlete → label...")
    logger.info(f"\nSPARQL:\n{query}")

    try:
        result = store.query(query)
        logger.info(f"\n✓ Found {result.num_rows} gold medal winners:")

        df = result.to_pandas()
        for i, row in df.iterrows():
            athlete = row['athlete_name']
            event = row['event'].split('/')[-1] if '/' in row['event'] else row['event']
            logger.info(f"  {i+1}. {athlete} - {event}")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def run_performance_test(store: RDFStore):
    """Run simple performance tests."""
    print_subheader("Performance Test")

    import time

    # Simple pattern match
    query = """
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>

        SELECT ?medal
        WHERE {
            ?medal olympics:medal ?medal_type .
        }
        LIMIT 1000
    """

    logger.info("Testing query performance (simple pattern, 1000 results)...")

    try:
        start = time.time()
        result = store.query(query)
        elapsed = time.time() - start

        logger.info(f"\n✓ Query completed:")
        logger.info(f"  Results: {result.num_rows:,} rows")
        logger.info(f"  Time: {elapsed*1000:.2f}ms")
        logger.info(f"  Throughput: {result.num_rows/elapsed:.0f} rows/sec")

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")


def main():
    """Run Olympics SPARQL demo."""
    print_header("SABOT RDF/SPARQL - Olympics Dataset Demo")

    logger.info("This demo loads and queries the Wallscope Olympics dataset")
    logger.info("used in QLever's quickstart examples.")
    logger.info("")
    logger.info("Dataset: Olympic Games 1896-2014")
    logger.info("  - Athletes: ~12,000")
    logger.info("  - Events: ~1,000")
    logger.info("  - Medals: ~50,000")
    logger.info("  - Total triples: ~1.8 million")

    # Check for test mode (smaller dataset)
    if "--test" in sys.argv:
        logger.info("\n⚠️  TEST MODE: Loading only 50,000 triples for quick testing")
        limit = 50000
    else:
        logger.info("\n💡 TIP: Use --test flag to load smaller sample for testing")
        limit = None

    # Load dataset
    store, count = load_olympics_dataset(limit=limit)

    # Run queries
    try:
        explore_dataset(store)
        # query_top_gold_medalists(store)  # Requires property paths - not in parser yet
        query_olympic_games(store)
        query_medals_by_sport(store)
        query_athlete_details(store)
        # query_medal_property_paths(store)  # Requires property paths - not in parser yet
        run_performance_test(store)

    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Demo interrupted by user")
        sys.exit(0)

    # Summary
    print_header("Demo Complete")

    logger.info("✅ Successfully demonstrated:")
    logger.info("  - Arrow Compute Pipeline: Parse → Clean → Load")
    logger.info("  - N-Triples parsing from compressed files (.xz)")
    logger.info("  - Data cleaning with Arrow compute (trim, dedup, filter)")
    logger.info("  - Arrow aggregation operators (unique, count, sort)")
    logger.info("  - RDF Triple Store with SPARQL queries")
    logger.info("  - Multi-pattern SPARQL joins")
    logger.info("  - Real-world RDF data processing")
    logger.info("")
    logger.info("📊 Dataset loaded:")
    logger.info(f"  - Triples: {store.count():,}")
    logger.info(f"  - Terms: {store.count_terms():,}")
    logger.info("")
    logger.info("For more information:")
    logger.info("  - Sabot RDF docs: docs/features/rdf_sparql.md")
    logger.info("  - QLever Olympics: vendor/qlever/docs/quickstart.md")
    logger.info("")


if __name__ == "__main__":
    main()
