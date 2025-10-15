#!/usr/bin/env python3
"""
SabotQL Integration Example 2: Fraud Detection with Entity Graph

Demonstrates using RDF knowledge graph for fraud detection:
- Entity relationship mapping (companies, people, addresses)
- Multi-hop graph queries (find connected entities)
- Risk scoring using graph properties
- Real-time fraud alerts

Use Case:
- Detect suspicious transactions based on entity relationships
- Flag transactions with sanctioned entities
- Track beneficial ownership chains
- Calculate network risk scores
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Set

from sabot.api.stream import Stream
from sabot import cyarrow as pa
import pyarrow.compute as pc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

KAFKA_BROKER = "localhost:9092"
TOPIC_TRANSACTIONS = "financial-transactions"
ENTITY_GRAPH_PATH = "./entity_graph.db"


# ============================================================================
# 1. Setup Entity Relationship Graph
# ============================================================================

def setup_entity_graph():
    """
    Build knowledge graph of entity relationships.
    
    Graph contains:
    - Companies and beneficial owners
    - Sanctioned entities list
    - Entity connections (ownership, control)
    - Risk scores and flags
    """
    from sabot_ql.bindings.python import create_triple_store
    
    kg = create_triple_store(ENTITY_GRAPH_PATH)
    logger.info("Building entity relationship graph...")
    
    # Sample entity relationships
    entities = [
        # (entity, property, value)
        ("CompanyA", "beneficialOwner", "PersonX"),
        ("CompanyA", "registeredIn", "Cyprus"),
        ("CompanyA", "riskScore", "8.5"),
        
        ("PersonX", "controlsEntity", "CompanyB"),
        ("PersonX", "nationality", "Unknown"),
        ("PersonX", "sanctioned", "true"),
        
        ("CompanyB", "operatesIn", "HighRiskCountry"),
        ("CompanyB", "riskScore", "9.2"),
        
        ("CompanyC", "beneficialOwner", "PersonY"),
        ("CompanyC", "registeredIn", "USA"),
        ("CompanyC", "riskScore", "2.1"),
        
        ("PersonY", "nationality", "USA"),
        ("PersonY", "sanctioned", "false"),
    ]
    
    for subj, pred, obj in entities:
        kg.insert_triple(
            subject=f"http://entities.example/{subj}",
            predicate=f"http://risk.example/{pred}",
            object=f'"{obj}"'
        )
    
    logger.info(f"✅ Entity graph created: {len(entities)} relationships")
    return kg


# ============================================================================
# 2. Fraud Detection Pipeline
# ============================================================================

async def fraud_detection_pipeline():
    """
    Real-time fraud detection using entity graph.
    
    Pipeline:
    1. Receive transaction stream
    2. Lookup counterparty in entity graph
    3. Check for sanctions, high risk, suspicious connections
    4. Flag high-risk transactions
    5. Alert on sanctioned entities
    """
    logger.info("Starting fraud detection pipeline...")
    
    # Setup entity graph
    kg = setup_entity_graph()
    
    # Transaction stream
    # Schema: {tx_id: string, counterparty: string, amount: float64, timestamp: timestamp}
    transactions = Stream.from_kafka(
        TOPIC_TRANSACTIONS,
        KAFKA_BROKER,
        'fraud-detector'
    )
    
    # ========================================================================
    # Stage 1: Basic Entity Enrichment
    # ========================================================================
    
    enriched = transactions.triple_lookup(
        kg,
        lookup_key='counterparty',
        pattern='''
            ?counterparty <beneficialOwner> ?owner .
            ?counterparty <registeredIn> ?jurisdiction .
            ?counterparty <riskScore> ?risk .
            OPTIONAL { ?owner <sanctioned> ?owner_sanctioned }
        ''',
        batch_lookups=True,
        cache_size=5000
    )
    
    # ========================================================================
    # Stage 2: Multi-Hop Relationship Check
    # ========================================================================
    
    # Check if counterparty has connections to sanctioned entities
    # (2-hop graph traversal)
    connected_entities = enriched.triple_lookup(
        kg,
        lookup_key='counterparty',
        pattern='''
            ?counterparty <beneficialOwner> ?owner .
            ?owner <controlsEntity> ?connected .
            ?connected <sanctioned> ?connected_sanctioned .
            FILTER (?connected_sanctioned = "true")
        '''
    )
    
    # ========================================================================
    # Stage 3: Risk Scoring
    # ========================================================================
    
    def calculate_risk_score(batch: pa.RecordBatch) -> pa.RecordBatch:
        """Calculate composite risk score using graph properties."""
        risk_scores = []
        
        for i in range(batch.num_rows):
            row = {name: batch.column(name)[i].as_py() 
                   for name in batch.schema.names}
            
            # Base risk from entity
            base_risk = float(row.get('risk', '0') or 0)
            
            # Additional risk factors
            jurisdiction_risk = 5.0 if row.get('jurisdiction') in ['Cyprus', 'Panama'] else 0.0
            sanction_risk = 10.0 if row.get('owner_sanctioned') == 'true' else 0.0
            connection_risk = 7.0 if row.get('connected_sanctioned') == 'true' else 0.0
            
            # Composite score
            total_risk = min(10.0, base_risk + jurisdiction_risk + sanction_risk + connection_risk)
            risk_scores.append(total_risk)
        
        # Add risk_score column
        risk_array = pa.array(risk_scores, type=pa.float64())
        return batch.append_column('composite_risk', risk_array)
    
    scored = connected_entities.map(calculate_risk_score)
    
    # ========================================================================
    # Stage 4: Alert Generation
    # ========================================================================
    
    # Filter high-risk transactions
    high_risk = scored.filter(
        lambda b: pc.greater(b.column('composite_risk'), 7.0)
    )
    
    # Process alerts
    alert_count = 0
    sanctioned_count = 0
    
    async for batch in high_risk:
        alert_count += batch.num_rows
        
        for row in batch.to_pylist():
            # Check for sanctioned entities
            if row.get('owner_sanctioned') == 'true' or \
               row.get('connected_sanctioned') == 'true':
                sanctioned_count += 1
                
                logger.warning("🚨 SANCTIONED ENTITY ALERT!")
                logger.warning(f"   Transaction ID: {row['tx_id']}")
                logger.warning(f"   Counterparty: {row['counterparty']}")
                logger.warning(f"   Amount: ${row['amount']:,.2f}")
                logger.warning(f"   Owner: {row.get('owner', 'Unknown')}")
                logger.warning(f"   Risk Score: {row['composite_risk']:.1f}/10.0")
                logger.warning("")
            else:
                logger.info(f"⚠️  High-risk transaction (score: {row['composite_risk']:.1f})")
                logger.info(f"   Counterparty: {row['counterparty']} (in {row.get('jurisdiction', 'Unknown')})")
                logger.info(f"   Amount: ${row['amount']:,.2f}")
        
        if alert_count % 100 == 0:
            logger.info(f"📊 Alerts: {alert_count} high-risk, {sanctioned_count} sanctioned")


# ============================================================================
# 3. Graph Analytics
# ============================================================================

def find_connected_entities(kg, entity_id: str, max_hops: int = 3) -> Set[str]:
    """
    Find all entities connected to given entity within N hops.
    
    Uses SPARQL property paths for graph traversal.
    
    Args:
        kg: Knowledge graph triple store
        entity_id: Starting entity
        max_hops: Maximum relationship hops
        
    Returns:
        Set of connected entity IDs
    """
    query = f"""
        SELECT DISTINCT ?connected WHERE {{
            <http://entities.example/{entity_id}> (<beneficialOwner>|<controlsEntity>){{1,{max_hops}}} ?connected
        }}
    """
    
    try:
        results = kg.query_sparql(query)
        connected = set(results.column('connected').to_pylist())
        return connected
    except Exception as e:
        logger.warning(f"Graph traversal failed: {e}")
        return set()


def calculate_network_risk(kg, entity_id: str) -> float:
    """
    Calculate network risk score based on entity connections.
    
    Higher risk if connected to:
    - Sanctioned entities
    - High-risk jurisdictions
    - Many shell companies
    
    Args:
        kg: Knowledge graph
        entity_id: Entity to score
        
    Returns:
        Risk score 0.0-10.0
    """
    # Query entity's network
    query = f"""
        SELECT ?connected ?risk ?sanctioned WHERE {{
            <http://entities.example/{entity_id}> <beneficialOwner>|<controlsEntity> ?connected .
            OPTIONAL {{ ?connected <riskScore> ?risk }}
            OPTIONAL {{ ?connected <sanctioned> ?sanctioned }}
        }}
    """
    
    try:
        results = kg.query_sparql(query)
        
        if results.num_rows == 0:
            return 0.0  # No connections = no network risk
        
        # Aggregate risk from connections
        total_risk = 0.0
        sanctioned_count = 0
        
        for row in results.to_pylist():
            if row.get('sanctioned') == 'true':
                total_risk += 10.0
                sanctioned_count += 1
            elif row.get('risk'):
                total_risk += float(row['risk'])
        
        # Average risk, capped at 10.0
        network_risk = min(10.0, total_risk / results.num_rows)
        
        if sanctioned_count > 0:
            network_risk = 10.0  # Auto-max if any sanctioned connections
        
        return network_risk
        
    except Exception as e:
        logger.warning(f"Network risk calculation failed: {e}")
        return 0.0


# ============================================================================
# 4. Batch Processing Mode
# ============================================================================

def batch_fraud_check(transactions_file: str, output_file: str):
    """
    Batch mode: Process historical transactions.
    
    Args:
        transactions_file: Parquet file with transactions
        output_file: Output parquet with risk scores
    """
    from sabot_ql.bindings.python import create_triple_store
    
    logger.info(f"Running batch fraud check on {transactions_file}...")
    
    # Setup graph
    kg = setup_entity_graph()
    
    # Load transactions from Parquet
    transactions = Stream.from_parquet(transactions_file)
    
    # Enrich and score
    enriched = transactions.triple_lookup(
        kg,
        lookup_key='counterparty',
        pattern='''
            ?counterparty <beneficialOwner> ?owner .
            ?counterparty <riskScore> ?risk .
            ?owner <sanctioned> ?sanctioned
        '''
    )
    
    # Calculate composite risk
    def add_risk_score(batch):
        risks = []
        for row in batch.to_pylist():
            base_risk = float(row.get('risk', '0') or 0)
            sanction_risk = 10.0 if row.get('sanctioned') == 'true' else 0.0
            risks.append(min(10.0, base_risk + sanction_risk))
        
        return batch.append_column('risk_score', pa.array(risks))
    
    scored = enriched.map(add_risk_score)
    
    # Write output
    scored.sink_parquet(output_file)
    logger.info(f"✅ Fraud check complete - output: {output_file}")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("""
    SabotQL Fraud Detection Example
    ================================
    
    This example demonstrates graph-based fraud detection using
    SabotQL's RDF triple store integrated into Sabot pipelines.
    
    Features:
    - Entity relationship mapping
    - Sanctions screening
    - Network risk analysis
    - Real-time alerts
    
    Run modes:
    1. Streaming: python example2_fraud_detection.py
    2. Batch: python example2_fraud_detection.py --batch transactions.parquet output.parquet
    """)
    
    import sys
    
    if len(sys.argv) == 3 and sys.argv[1] == '--batch':
        # Batch mode
        batch_fraud_check(sys.argv[2], sys.argv[3])
    else:
        # Streaming mode
        try:
            asyncio.run(fraud_detection_pipeline())
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")

