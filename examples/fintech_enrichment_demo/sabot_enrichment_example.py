#!/usr/bin/env python3
"""
Fintech Data Enrichment with Sabot - Integration Example

Demonstrates real-time market data enrichment using Sabot's streaming API:
- Multi-stream ingestion (quotes, securities, trades)
- Stream-to-stream joins
- Stateful enrichment with reference data
- Real-time spread calculations

Based on existing Sabot examples (kafka_etl_demo.py, fraud_app.py, grand_demo.py)

Requirements:
- Kafka running on localhost:19092 (or update KAFKA_BROKER)
- Run data producers first:
  python kafka_inventory_producer.py
  python kafka_security_producer.py
  python kafka_trades_producer.py
"""

import asyncio
import logging
from typing import Dict, Any
from datetime import datetime

# Sabot imports (following existing example patterns)
from sabot.api.stream import Stream
from sabot.kafka import from_kafka
from sabot import App, MemoryBackend, BackendConfig, MapState

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

KAFKA_BROKER = "localhost:19092"

# Topic names (from kafka_config.py)
TOPIC_QUOTES = "inventory-rows"
TOPIC_SECURITIES = "master-security"
TOPIC_TRADES = "trax-trades"

# ============================================================================
# Sabot Application Setup
# ============================================================================

# Create Sabot app (following fraud_app.py pattern)
app = App(
    'fintech-enrichment',
    broker=f'kafka://{KAFKA_BROKER}',
    value_serializer='json'
)

# State backend for securities reference data
backend = MemoryBackend(BackendConfig(backend_type="memory"))


# ============================================================================
# Example 1: Stream API - Quote Enrichment
# ============================================================================

async def stream_api_enrichment():
    """
    Use Sabot Stream API to enrich quotes with security master data.

    Pattern from kafka_etl_demo.py
    """
    logger.info("=== Starting Stream API Quote Enrichment ===")

    # Load securities into memory first (reference data)
    securities_map = {}

    logger.info("Loading securities reference data...")
    securities_source = from_kafka(
        bootstrap_servers=KAFKA_BROKER,
        topic=TOPIC_SECURITIES,
        group_id="enrichment-securities-loader",
        codec_type="json"
    )

    await securities_source.start()

    # Load first 1000 securities as reference data
    count = 0
    try:
        async for sec in securities_source.stream():
            securities_map[sec['instrumentId']] = sec
            count += 1
            if count >= 1000:
                logger.info(f"Loaded {count} securities into memory")
                break
    except Exception as e:
        logger.error(f"Error loading securities: {e}")
    finally:
        await securities_source.stop()

    # Now process quotes and enrich
    logger.info("Starting quote enrichment...")
    quotes_source = from_kafka(
        bootstrap_servers=KAFKA_BROKER,
        topic=TOPIC_QUOTES,
        group_id="enrichment-quotes-processor",
        codec_type="json"
    )

    await quotes_source.start()

    enriched_count = 0
    try:
        async for quote in quotes_source.stream():
            # Enrich quote with security data
            instrument_id = quote.get('instrumentId')
            security = securities_map.get(instrument_id, {})

            # Calculate spread
            bid_price = quote.get('bidPrice', 0)
            offer_price = quote.get('offerPrice', 0)

            if bid_price and offer_price:
                spread = offer_price - bid_price
                mid_price = (bid_price + offer_price) / 2
                spread_pct = (spread / mid_price * 100) if mid_price > 0 else 0
            else:
                spread = 0
                spread_pct = 0

            enriched_quote = {
                **quote,
                # From security master
                'securityName': security.get('securityName', 'UNKNOWN'),
                'cusip': security.get('cusip'),
                'isin': security.get('isin'),
                'marketSegment': security.get('marketSegment'),
                # Computed fields
                'spread': spread,
                'spreadPct': spread_pct,
                'midPrice': (bid_price + offer_price) / 2 if (bid_price and offer_price) else None,
                'enrichedAt': datetime.now().isoformat()
            }

            enriched_count += 1
            if enriched_count % 100 == 0:
                logger.info(f"Enriched {enriched_count} quotes - Latest: {instrument_id} "
                           f"spread={spread:.4f} ({spread_pct:.2f}%)")

            # Could write to output topic here
            # await output_sink.send(enriched_quote)

    except KeyboardInterrupt:
        logger.info(f"Stopping enrichment. Total enriched: {enriched_count}")
    finally:
        await quotes_source.stop()


# ============================================================================
# Example 2: Agent Pattern - Stateful Enrichment
# ============================================================================

# Following fraud_app.py pattern with @app.agent decorator

@app.agent(TOPIC_SECURITIES)
async def load_securities_agent(securities):
    """
    Agent pattern: Load security master data into state.

    This runs continuously and keeps the security reference data up-to-date.
    """
    state = MapState(backend, "securities")

    count = 0
    async for sec in securities:
        await state.put(sec['instrumentId'], sec)
        count += 1
        if count % 1000 == 0:
            logger.info(f"Loaded {count} securities into state")


@app.agent(TOPIC_QUOTES)
async def enrich_quotes_agent(quotes):
    """
    Agent pattern: Enrich quotes using stateful securities lookup.

    This demonstrates stateful stream processing with Sabot.
    """
    securities_state = MapState(backend, "securities")

    enriched_count = 0
    async for quote in quotes:
        # Lookup security from state
        security = await securities_state.get(quote.get('instrumentId'))

        if security:
            # Calculate spread
            bid_price = quote.get('bidPrice', 0)
            offer_price = quote.get('offerPrice', 0)

            if bid_price and offer_price:
                spread = offer_price - bid_price
                mid_price = (bid_price + offer_price) / 2
                spread_pct = (spread / mid_price * 100) if mid_price > 0 else 0

                enriched = {
                    **quote,
                    'securityName': security.get('securityName'),
                    'marketSegment': security.get('marketSegment'),
                    'spread': spread,
                    'spreadPct': spread_pct,
                    'enrichedAt': datetime.now().isoformat()
                }

                enriched_count += 1
                if enriched_count % 100 == 0:
                    logger.info(f"Agent enriched {enriched_count} quotes - "
                               f"Latest spread: {spread_pct:.2f}%")

                yield enriched


# ============================================================================
# Example 3: Best Bid/Offer Tracking
# ============================================================================

async def best_bid_offer_tracker():
    """
    Track best bid/offer prices across all dealers for each instrument.

    Demonstrates stateful aggregation pattern.
    """
    logger.info("=== Starting Best Bid/Offer Tracker ===")

    # State to track best prices per instrument
    best_prices = {}  # {instrumentId: {'bestBid': ..., 'bestOffer': ...}}

    quotes_source = from_kafka(
        bootstrap_servers=KAFKA_BROKER,
        topic=TOPIC_QUOTES,
        group_id="best-bid-offer-tracker",
        codec_type="json"
    )

    await quotes_source.start()

    try:
        async for quote in quotes_source.stream():
            instrument_id = quote.get('instrumentId')
            bid_price = quote.get('bidPrice')
            offer_price = quote.get('offerPrice')

            if not instrument_id:
                continue

            # Initialize if new instrument
            if instrument_id not in best_prices:
                best_prices[instrument_id] = {
                    'bestBid': 0,
                    'bestOffer': float('inf'),
                    'bestBidDealer': None,
                    'bestOfferDealer': None
                }

            current = best_prices[instrument_id]
            updated = False

            # Update best bid (highest)
            if bid_price and bid_price > current['bestBid']:
                current['bestBid'] = bid_price
                current['bestBidDealer'] = quote.get('dealerId')
                updated = True

            # Update best offer (lowest)
            if offer_price and offer_price < current['bestOffer']:
                current['bestOffer'] = offer_price
                current['bestOfferDealer'] = quote.get('dealerId')
                updated = True

            if updated:
                spread = current['bestOffer'] - current['bestBid']
                if current['bestOffer'] != float('inf'):
                    mid = (current['bestBid'] + current['bestOffer']) / 2
                    spread_pct = (spread / mid * 100) if mid > 0 else 0

                    logger.info(
                        f"Best Bid/Offer for {instrument_id}: "
                        f"Bid={current['bestBid']:.4f} ({current['bestBidDealer']}) | "
                        f"Offer={current['bestOffer']:.4f} ({current['bestOfferDealer']}) | "
                        f"Spread={spread_pct:.3f}%"
                    )

    except KeyboardInterrupt:
        logger.info(f"Stopping tracker. Tracked {len(best_prices)} instruments")
    finally:
        await quotes_source.stop()


# ============================================================================
# Main Entry Points
# ============================================================================

async def run_stream_api_example():
    """Run the Stream API enrichment example."""
    await stream_api_enrichment()


async def run_agent_example():
    """Run the agent-based enrichment example."""
    logger.info("Starting agent-based enrichment")
    logger.info("Note: Run with: sabot -A sabot_enrichment_example:app worker")
    logger.info("This example requires the Sabot CLI to run agents")


async def run_tracker_example():
    """Run the best bid/offer tracker."""
    await best_bid_offer_tracker()


def main():
    """
    Main entry point with example selection.

    Usage:
        # Run Stream API example
        python sabot_enrichment_example.py stream

        # Run best bid/offer tracker
        python sabot_enrichment_example.py tracker

        # Run agent example (via CLI)
        sabot -A sabot_enrichment_example:app worker
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python sabot_enrichment_example.py stream   # Stream API enrichment")
        print("  python sabot_enrichment_example.py tracker  # Best bid/offer tracker")
        print("  sabot -A sabot_enrichment_example:app worker  # Agent-based enrichment")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "stream":
        asyncio.run(run_stream_api_example())
    elif mode == "tracker":
        asyncio.run(run_tracker_example())
    elif mode == "agent":
        asyncio.run(run_agent_example())
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
