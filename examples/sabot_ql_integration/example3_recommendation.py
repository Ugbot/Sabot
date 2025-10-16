#!/usr/bin/env python3
"""
SabotQL Integration Example 3: Product Recommendations

Demonstrates using knowledge graph for real-time recommendations:
- Customer purchase history as RDF triples
- Product relationships (similar-to, in-category)
- Collaborative filtering using graph queries
- Real-time recommendation generation

Use Case:
- E-commerce recommendation engine
- Content recommendation
- Friend suggestions
- Related products
"""

import asyncio
import logging
from typing import List, Set, Dict

from sabot.api.stream import Stream
from sabot import cyarrow as pa
import pyarrow.compute as pc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

KAFKA_BROKER = "localhost:9092"
TOPIC_EVENTS = "user-events"
KNOWLEDGE_GRAPH_PATH = "./recommendation_kg.db"


# ============================================================================
# 1. Setup Recommendation Knowledge Graph
# ============================================================================

def setup_recommendation_graph():
    """
    Build knowledge graph for recommendations.
    
    Graph contains:
    - User purchase history
    - Product relationships
    - Product categories
    - User preferences
    """
    from sabot_ql.bindings.python import create_triple_store
    
    kg = create_triple_store(KNOWLEDGE_GRAPH_PATH)
    logger.info("Building recommendation knowledge graph...")
    
    # Sample graph structure
    relationships = [
        # User purchases
        ("User1", "purchased", "Product1"),
        ("User1", "purchased", "Product2"),
        ("User2", "purchased", "Product2"),
        ("User2", "purchased", "Product3"),
        ("User3", "purchased", "Product1"),
        ("User3", "purchased", "Product3"),
        
        # Product metadata
        ("Product1", "inCategory", "Electronics"),
        ("Product1", "hasName", "Laptop"),
        ("Product1", "hasPrice", "999.99"),
        
        ("Product2", "inCategory", "Electronics"),
        ("Product2", "hasName", "Mouse"),
        ("Product2", "hasPrice", "29.99"),
        
        ("Product3", "inCategory", "Books"),
        ("Product3", "hasName", "Python Guide"),
        ("Product3", "hasPrice", "49.99"),
        
        # Product relationships
        ("Product1", "similarTo", "Product4"),
        ("Product2", "accessoryFor", "Product1"),
        ("Product3", "boughtWith", "Product2"),
    ]
    
    for subj, pred, obj in relationships:
        # Determine if obj is IRI or literal
        if obj.replace('.', '').replace('-', '').isdigit():
            obj_str = f'"{obj}"'  # Literal
        else:
            obj_str = f'http://example.org/{obj}'
        
        kg.insert_triple(
            subject=f'http://example.org/{subj}',
            predicate=f'http://recommend.org/{pred}',
            object=obj_str
        )
    
    logger.info(f"✅ Recommendation graph created: {len(relationships)} relationships")
    return kg


# ============================================================================
# 2. Recommendation Pipeline
# ============================================================================

async def recommendation_pipeline():
    """
    Real-time recommendation generation.
    
    For each user event:
    1. Find what user purchased
    2. Find similar products via graph traversal
    3. Find what similar users purchased
    4. Generate personalized recommendations
    """
    logger.info("Starting recommendation pipeline...")
    
    kg = setup_recommendation_graph()
    
    # User event stream
    # Schema: {user_id: string, event_type: string, product_id: string, timestamp: timestamp}
    events = Stream.from_kafka(TOPIC_EVENTS, KAFKA_BROKER, 'recommender')
    
    # ========================================================================
    # Strategy 1: Content-Based Filtering
    # ========================================================================
    
    # Find products similar to what user viewed
    similar_products = events.triple_lookup(
        kg,
        lookup_key='product_id',
        pattern='''
            ?product <similarTo> ?similar .
            ?similar <hasName> ?similar_name .
            ?similar <hasPrice> ?similar_price .
            ?similar <inCategory> ?similar_category
        ''',
        batch_lookups=True
    )
    
    # ========================================================================
    # Strategy 2: Collaborative Filtering
    # ========================================================================
    
    # Find what users who bought this product also bought
    collaborative = events.triple_lookup(
        kg,
        lookup_key='product_id',
        pattern='''
            ?other_user <purchased> ?product .
            ?other_user <purchased> ?also_bought .
            ?also_bought <hasName> ?product_name .
            ?also_bought <hasPrice> ?product_price .
            FILTER (?also_bought != ?product)
        ''',
        batch_lookups=True
    )
    
    # ========================================================================
    # Strategy 3: Category-Based Recommendations
    # ========================================================================
    
    # Find other products in same category
    category_recs = events.triple_lookup(
        kg,
        lookup_key='product_id',
        pattern='''
            ?product <inCategory> ?category .
            ?other <inCategory> ?category .
            ?other <hasName> ?other_name .
            ?other <hasPrice> ?other_price .
            FILTER (?other != ?product)
        ''',
        batch_lookups=True
    )
    
    # ========================================================================
    # Combine and Rank Recommendations
    # ========================================================================
    
    def merge_recommendations(batch: pa.RecordBatch) -> pa.RecordBatch:
        """Merge and rank recommendation strategies."""
        # For demo, just add strategy column
        strategy = ['similar'] * batch.num_rows
        return batch.append_column('strategy', pa.array(strategy))
    
    final_recs = similar_products.map(merge_recommendations)
    
    # ========================================================================
    # Process Recommendations
    # ========================================================================
    
    rec_count = 0
    
    async for batch in final_recs:
        rec_count += batch.num_rows
        
        for row in batch.to_pylist()[:5]:  # Show first 5
            logger.info(f"Recommendation:")
            logger.info(f"  User: {row['user_id']}")
            logger.info(f"  Viewed: {row['product_id']}")
            logger.info(f"  Recommend: {row.get('similar_name', 'N/A')}")
            logger.info(f"  Price: ${row.get('similar_price', '0')}")
            logger.info(f"  Strategy: {row['strategy']}")
            logger.info("")
        
        if rec_count % 1000 == 0:
            logger.info(f"📊 Generated {rec_count} recommendations")


# ============================================================================
# 3. Advanced: Multi-Hop Recommendations
# ============================================================================

def find_friends_of_friends_bought(kg, user_id: str) -> List[str]:
    """
    Find products that friends-of-friends bought.
    
    Graph query: user → knows → friend → knows → friend2 → purchased → product
    
    Args:
        kg: Knowledge graph
        user_id: User to find recommendations for
        
    Returns:
        List of recommended product IDs
    """
    query = f"""
        SELECT DISTINCT ?product ?name ?price WHERE {{
            <http://example.org/{user_id}> <knows> ?friend .
            ?friend <knows> ?friend2 .
            ?friend2 <purchased> ?product .
            ?product <hasName> ?name .
            ?product <hasPrice> ?price .
            FILTER (?friend2 != <http://example.org/{user_id}>)
        }}
        ORDER BY DESC(?price)
        LIMIT 10
    """
    
    try:
        results = kg.query_sparql(query)
        products = results.column('product').to_pylist()
        return products
    except Exception as e:
        logger.warning(f"Recommendation query failed: {e}")
        return []


# ============================================================================
# 4. Graph Analytics for Recommendations
# ============================================================================

def calculate_product_popularity(kg) -> Dict[str, float]:
    """
    Calculate product popularity scores using graph analytics.
    
    Score based on:
    - Number of purchases
    - Number of views
    - Recency of interactions
    
    Args:
        kg: Knowledge graph
        
    Returns:
        Dict mapping product_id → popularity_score
    """
    query = """
        SELECT ?product (COUNT(?user) AS ?purchase_count)
        WHERE {
            ?user <purchased> ?product
        }
        GROUP BY ?product
        ORDER BY DESC(?purchase_count)
    """
    
    try:
        results = kg.query_sparql(query)
        
        popularity = {}
        for row in results.to_pylist():
            product = row['product']
            count = row['purchase_count']
            popularity[product] = float(count)
        
        return popularity
        
    except Exception as e:
        logger.warning(f"Popularity calculation failed: {e}")
        return {}


def find_trending_products(kg, category: str = None) -> List[str]:
    """
    Find trending products using graph query.
    
    Args:
        kg: Knowledge graph
        category: Optional category filter
        
    Returns:
        List of trending product IDs
    """
    if category:
        query = f"""
            SELECT ?product ?name (COUNT(?user) AS ?popularity)
            WHERE {{
                ?user <purchased> ?product .
                ?product <inCategory> "{category}" .
                ?product <hasName> ?name
            }}
            GROUP BY ?product ?name
            ORDER BY DESC(?popularity)
            LIMIT 10
        """
    else:
        query = """
            SELECT ?product ?name (COUNT(?user) AS ?popularity)
            WHERE {
                ?user <purchased> ?product .
                ?product <hasName> ?name
            }
            GROUP BY ?product ?name
            ORDER BY DESC(?popularity)
            LIMIT 10
        """
    
    try:
        results = kg.query_sparql(query)
        return results.column('product').to_pylist()
    except Exception as e:
        logger.warning(f"Trending products query failed: {e}")
        return []


# ============================================================================
# Main
# ============================================================================

async def main():
    """Run quickstart example."""
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SabotQL + Sabot Integration Quickstart                 ║")
    print("║  RDF Triple Lookups in Streaming Pipelines              ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    
    # Setup knowledge graph
    kg = setup_recommendation_graph()
    
    # Show what we can do
    print("\n" + "="*60)
    print("Graph Analytics Examples")
    print("="*60)
    
    # Example 1: Product popularity
    print("\nMost Popular Products:")
    popularity = calculate_product_popularity(kg)
    for product, score in sorted(popularity.items(), key=lambda x: -x[1])[:5]:
        print(f"  {product}: {score:.0f} purchases")
    
    # Example 2: Trending in category
    print("\nTrending in Electronics:")
    trending = find_trending_products(kg, "Electronics")
    for product in trending[:3]:
        print(f"  {product}")
    
    # Example 3: Friends-of-friends
    print("\nRecommendations for User1 (friends-of-friends):")
    recs = find_friends_of_friends_bought(kg, "User1")
    for rec in recs[:3]:
        print(f"  {rec}")
    
    print("\n" + "="*60)
    print("Next Steps")
    print("="*60)
    print()
    print("1. Load your own RDF data:")
    print("   kg.load_ntriples('your_data.nt')")
    print()
    print("2. Connect to real Kafka:")
    print("   stream = Stream.from_kafka('your-topic', 'localhost:9092', 'group')")
    print()
    print("3. Customize SPARQL patterns:")
    print("   pattern='?entity <yourProperty> ?value'")
    print()
    print("4. Monitor performance:")
    print("   stats = enriched_op.get_stats()")
    print("   print(f\"Cache hit rate: {stats['cache_hit_rate']}\")")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Quickstart interrupted")
    except Exception as e:
        logger.error(f"Quickstart failed: {e}", exc_info=True)
        raise


