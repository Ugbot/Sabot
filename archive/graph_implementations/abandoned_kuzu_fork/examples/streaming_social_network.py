#!/usr/bin/env python3
"""
Streaming Social Network Analytics Example

Real-time social network analytics using SabotCypher streaming queries.
"""

import sys
import os
import pyarrow as pa
import time
from pathlib import Path
from datetime import datetime, timedelta
import random

# Add streaming module to path
sys.path.append(str(Path(__file__).parent.parent))

from streaming import StreamingGraphProcessor


class StreamingSocialNetwork:
    """
    Real-time social network analytics.
    
    Monitors:
    - Trending users (most new followers)
    - Viral content (rapid sharing patterns)
    - Community formation (new triangles)
    - Influence propagation (multi-hop patterns)
    """
    
    def __init__(self):
        """Initialize streaming social network."""
        self.processor = StreamingGraphProcessor(
            window_size=timedelta(minutes=5),
            slide_interval=timedelta(seconds=5),
            ttl=timedelta(hours=1)
        )
        
        # Track metrics
        self.trending_users = []
        self.viral_content = []
        self.new_triangles = []
        
        # Register analytics queries
        self._register_analytics_queries()
    
    def _register_analytics_queries(self):
        """Register social network analytics queries."""
        
        # Query 1: Trending users (most new followers in window)
        trending_query = """
            MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
            RETURN person.id, person.name, count(*) as new_followers
            ORDER BY new_followers DESC
            LIMIT 10
        """
        
        self.processor.register_continuous_query(
            trending_query,
            self._handle_trending_users
        )
        
        # Query 2: Viral content (rapid multi-hop sharing)
        viral_query = """
            MATCH (a:Person)-[:SHARES]->(content:Content)<-[:SHARES]-(b:Person)
            WHERE a.id != b.id
            RETURN content.id, count(DISTINCT a.id) as sharers
            ORDER BY sharers DESC
            LIMIT 5
        """
        
        self.processor.register_continuous_query(
            viral_query,
            self._handle_viral_content
        )
        
        # Query 3: Community formation (new triangle patterns)
        triangle_query = """
            MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person)-[:FOLLOWS]->(a)
            RETURN count(*) as triangles
        """
        
        self.processor.register_continuous_query(
            triangle_query,
            self._handle_new_triangles
        )
    
    def _handle_trending_users(self, result: pa.Table):
        """Handle trending users detection."""
        if result.num_rows > 0:
            print(f"\n📈 TRENDING USERS ({result.num_rows} detected):")
            for i, row in enumerate(result.to_pylist()[:3]):  # Top 3
                print(f"   {i+1}. {row.get('name', 'Unknown')} - {row.get('new_followers', 0)} new followers")
            
            self.trending_users.append({
                'timestamp': datetime.now(),
                'users': result.to_pylist()
            })
    
    def _handle_viral_content(self, result: pa.Table):
        """Handle viral content detection."""
        if result.num_rows > 0:
            print(f"\n🔥 VIRAL CONTENT ({result.num_rows} detected):")
            for i, row in enumerate(result.to_pylist()[:3]):  # Top 3
                print(f"   {i+1}. Content {row.get('id', 'Unknown')} - {row.get('sharers', 0)} unique sharers")
            
            self.viral_content.append({
                'timestamp': datetime.now(),
                'content': result.to_pylist()
            })
    
    def _handle_new_triangles(self, result: pa.Table):
        """Handle triangle pattern detection."""
        if result.num_rows > 0:
            triangles = result.to_pylist()[0].get('triangles', 0) if result.num_rows > 0 else 0
            if triangles > 0:
                print(f"\n🔺 COMMUNITY FORMATION: {triangles} new triangle patterns detected")
                
                self.new_triangles.append({
                    'timestamp': datetime.now(),
                    'triangle_count': triangles
                })
    
    def generate_sample_social_stream(self, num_batches: int = 20, batch_size: int = 50):
        """
        Generate sample social network stream.
        
        Args:
            num_batches: Number of batches to generate
            batch_size: Number of events per batch
        """
        print(f"\n📊 Generating {num_batches} batches of {batch_size} social events each...")
        print(f"   Window size: {self.processor.window_manager.window_size}")
        print(f"   Slide interval: {self.processor.window_manager.slide_interval}")
        
        for batch_num in range(num_batches):
            # Generate users (vertices)
            num_users = batch_size
            vertices = pa.table({
                'id': pa.array([1000 + i + (batch_num * num_users) for i in range(num_users)], type=pa.int64()),
                'name': pa.array([f"User_{i + (batch_num * num_users)}" for i in range(num_users)]),
                'type': pa.array(['Person'] * num_users),
                'timestamp': pa.array([datetime.now()] * num_users)
            })
            
            # Generate follows (edges)
            follows = []
            for i in range(batch_size):
                source = 1000 + (i % num_users) + (batch_num * num_users)
                
                # Create realistic social patterns
                if random.random() < 0.1:  # 10% popular users
                    # Popular user gets many followers
                    num_followers = random.randint(5, 15)
                    for j in range(num_followers):
                        follower = 1000 + ((i + j) % num_users) + (batch_num * num_users)
                        if follower != source:
                            follows.append({'source': follower, 'target': source})
                else:
                    # Normal user
                    target = 1000 + ((i + random.randint(1, 10)) % num_users) + (batch_num * num_users)
                    if target != source:
                        follows.append({'source': source, 'target': target})
                
                # Occasionally create triangles
                if random.random() < 0.02:  # 2% triangles
                    friend1 = 1000 + ((i + 1) % num_users) + (batch_num * num_users)
                    friend2 = 1000 + ((i + 2) % num_users) + (batch_num * num_users)
                    follows.append({'source': source, 'target': friend1})
                    follows.append({'source': friend1, 'target': friend2})
                    follows.append({'source': friend2, 'target': source})
            
            edges = pa.table({
                'source': pa.array([f['source'] for f in follows], type=pa.int64()),
                'target': pa.array([f['target'] for f in follows], type=pa.int64()),
                'type': pa.array(['FOLLOWS'] * len(follows)),
                'timestamp': pa.array([datetime.now()] * len(follows))
            })
            
            # Ingest batch
            print(f"  Batch {batch_num + 1}/{num_batches}: {vertices.num_rows} users, {edges.num_rows} follows")
            self.processor.ingest_batch(vertices, edges)
            
            # Small delay to simulate streaming
            time.sleep(0.2)
        
        print(f"✅ Generated {num_batches} batches")
    
    def get_analytics_summary(self):
        """Get summary of analytics results."""
        return {
            'trending_updates': len(self.trending_users),
            'viral_content_updates': len(self.viral_content),
            'triangle_detections': len(self.new_triangles),
            'latest_trending': self.trending_users[-1] if self.trending_users else None,
            'latest_viral': self.viral_content[-1] if self.viral_content else None,
            'latest_triangles': self.new_triangles[-1] if self.new_triangles else None
        }


def main():
    """Run streaming social network analytics example."""
    print("SABOT_CYPHER STREAMING SOCIAL NETWORK ANALYTICS")
    print("=" * 60)
    
    # Create social network analyzer
    network = StreamingSocialNetwork()
    
    # Generate and process sample social stream
    network.generate_sample_social_stream(num_batches=20, batch_size=50)
    
    # Get statistics
    stats = network.processor.get_stats()
    print(f"\n📊 Processor Statistics:")
    print(f"   Total users: {stats['store']['total_vertices']}")
    print(f"   Total follows: {stats['store']['total_edges']}")
    print(f"   Vertex buckets: {stats['store']['num_vertex_buckets']}")
    print(f"   Edge buckets: {stats['store']['num_edge_buckets']}")
    print(f"   Continuous queries: {stats['continuous_queries']}")
    print(f"   Window count: {stats['window']['window_count']}")
    
    # Get analytics summary
    analytics = network.get_analytics_summary()
    print(f"\n📈 Analytics Summary:")
    print(f"   Trending user updates: {analytics['trending_updates']}")
    print(f"   Viral content updates: {analytics['viral_content_updates']}")
    print(f"   Triangle detections: {analytics['triangle_detections']}")
    
    if analytics['latest_trending']:
        print(f"\n📈 Latest Trending Users:")
        for i, user in enumerate(analytics['latest_trending']['users'][:3]):
            print(f"   {i+1}. {user.get('name', 'Unknown')} - {user.get('new_followers', 0)} followers")
    
    print("\n" + "=" * 60)
    print("STREAMING SOCIAL NETWORK ANALYTICS COMPLETE!")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

