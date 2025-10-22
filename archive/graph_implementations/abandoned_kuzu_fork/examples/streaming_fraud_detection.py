#!/usr/bin/env python3
"""
Streaming Fraud Detection Example

Real-time fraud detection using SabotCypher streaming graph queries.
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


class StreamingFraudDetector:
    """
    Real-time fraud detection using streaming graph queries.
    
    Detects:
    - Money laundering patterns (multi-hop transfers)
    - Account takeover (unusual connection patterns)
    - Coordinated fraud (triangle patterns)
    """
    
    def __init__(self):
        """Initialize fraud detector."""
        self.processor = StreamingGraphProcessor(
            window_size=timedelta(minutes=5),
            slide_interval=timedelta(seconds=10),
            ttl=timedelta(hours=1)
        )
        
        # Register fraud detection queries
        self._register_fraud_queries()
        
        # Track alerts
        self.alerts = []
    
    def _register_fraud_queries(self):
        """Register fraud detection queries."""
        
        # Query 1: Large multi-hop transfers (money laundering)
        money_laundering_query = """
            MATCH (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)
            WHERE a.id != c.id
            RETURN a.id as source, c.id as destination, count(*) as hops
        """
        
        self.processor.register_continuous_query(
            money_laundering_query,
            self._handle_money_laundering_alert
        )
        
        # Query 2: High-velocity transfers (account takeover)
        velocity_query = """
            MATCH (a:Account)-[:TRANSFER]->(b:Account)
            RETURN a.id, count(*) as transfer_count
            ORDER BY transfer_count DESC
            LIMIT 10
        """
        
        self.processor.register_continuous_query(
            velocity_query,
            self._handle_velocity_alert
        )
        
        # Query 3: Coordinated fraud (triangle patterns)
        triangle_query = """
            MATCH (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)-[:TRANSFER]->(a)
            RETURN count(*) as triangles
        """
        
        self.processor.register_continuous_query(
            triangle_query,
            self._handle_triangle_alert
        )
    
    def _handle_money_laundering_alert(self, result: pa.Table):
        """Handle money laundering detection."""
        if result.num_rows > 0:
            print(f"\n🚨 MONEY LAUNDERING ALERT: {result.num_rows} suspicious multi-hop transfers detected")
            self.alerts.append({
                'type': 'money_laundering',
                'timestamp': datetime.now(),
                'details': result.to_pylist()
            })
    
    def _handle_velocity_alert(self, result: pa.Table):
        """Handle high-velocity transfer detection."""
        if result.num_rows > 0:
            for row in result.to_pylist():
                if row.get('transfer_count', 0) > 50:  # Threshold
                    print(f"\n⚠️  VELOCITY ALERT: Account {row.get('id')} made {row.get('transfer_count')} transfers")
                    self.alerts.append({
                        'type': 'high_velocity',
                        'timestamp': datetime.now(),
                        'account_id': row.get('id'),
                        'transfer_count': row.get('transfer_count')
                    })
    
    def _handle_triangle_alert(self, result: pa.Table):
        """Handle triangle pattern detection."""
        if result.num_rows > 0:
            triangles = result['triangles'][0].as_py() if 'triangles' in result.column_names else 0
            if triangles > 0:
                print(f"\n🔺 TRIANGLE ALERT: {triangles} circular transfer patterns detected")
                self.alerts.append({
                    'type': 'triangle_pattern',
                    'timestamp': datetime.now(),
                    'triangle_count': triangles
                })
    
    def generate_sample_transaction_stream(self, num_batches: int = 10, batch_size: int = 100):
        """
        Generate sample transaction stream for testing.
        
        Args:
            num_batches: Number of batches to generate
            batch_size: Number of transactions per batch
        """
        print(f"\n📊 Generating {num_batches} batches of {batch_size} transactions each...")
        
        for batch_num in range(num_batches):
            # Generate accounts (vertices)
            num_accounts = batch_size // 2
            vertices = pa.table({
                'id': pa.array([1000 + i + (batch_num * num_accounts) for i in range(num_accounts)], type=pa.int64()),
                'name': pa.array([f"Account_{i + (batch_num * num_accounts)}" for i in range(num_accounts)]),
                'type': pa.array(['Account'] * num_accounts),
                'timestamp': pa.array([datetime.now()] * num_accounts)
            })
            
            # Generate transfers (edges)
            transfers = []
            for i in range(batch_size):
                source = 1000 + (i % num_accounts) + (batch_num * num_accounts)
                target = 1000 + ((i + 1) % num_accounts) + (batch_num * num_accounts)
                
                # Occasionally create suspicious patterns
                if random.random() < 0.05:  # 5% suspicious
                    # Create multi-hop pattern
                    intermediate = 1000 + ((i + random.randint(1, num_accounts)) % num_accounts) + (batch_num * num_accounts)
                    transfers.append({'source': source, 'target': intermediate})
                    transfers.append({'source': intermediate, 'target': target})
                else:
                    transfers.append({'source': source, 'target': target})
            
            edges = pa.table({
                'source': pa.array([t['source'] for t in transfers], type=pa.int64()),
                'target': pa.array([t['target'] for t in transfers], type=pa.int64()),
                'type': pa.array(['TRANSFER'] * len(transfers)),
                'timestamp': pa.array([datetime.now()] * len(transfers))
            })
            
            # Ingest batch
            print(f"  Batch {batch_num + 1}/{num_batches}: {vertices.num_rows} accounts, {edges.num_rows} transfers")
            self.processor.ingest_batch(vertices, edges)
            
            # Small delay to simulate streaming
            time.sleep(0.1)
        
        print(f"✅ Generated {num_batches} batches")
    
    def get_alert_summary(self):
        """Get summary of detected alerts."""
        alert_counts = {}
        for alert in self.alerts:
            alert_type = alert['type']
            alert_counts[alert_type] = alert_counts.get(alert_type, 0) + 1
        
        return {
            'total_alerts': len(self.alerts),
            'alert_counts': alert_counts,
            'alerts': self.alerts
        }


def main():
    """Run streaming fraud detection example."""
    print("SABOT_CYPHER STREAMING FRAUD DETECTION")
    print("=" * 60)
    
    # Create fraud detector
    detector = StreamingFraudDetector()
    
    # Generate and process sample transaction stream
    detector.generate_sample_transaction_stream(num_batches=10, batch_size=100)
    
    # Get statistics
    stats = detector.processor.get_stats()
    print(f"\n📊 Processor Statistics:")
    print(f"   Total vertices: {stats['store']['total_vertices']}")
    print(f"   Total edges: {stats['store']['total_edges']}")
    print(f"   Vertex buckets: {stats['store']['num_vertex_buckets']}")
    print(f"   Edge buckets: {stats['store']['num_edge_buckets']}")
    print(f"   Continuous queries: {stats['continuous_queries']}")
    print(f"   Window count: {stats['window']['window_count']}")
    
    # Get alert summary
    alert_summary = detector.get_alert_summary()
    print(f"\n🚨 Alert Summary:")
    print(f"   Total alerts: {alert_summary['total_alerts']}")
    for alert_type, count in alert_summary['alert_counts'].items():
        print(f"   {alert_type}: {count}")
    
    print("\n" + "=" * 60)
    print("STREAMING FRAUD DETECTION COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()

