"""
Kraken WebSocket v2 configuration.

API: https://docs.kraken.com/api/docs/guides/spot-ws-intro
"""

import json
from pydantic import BaseModel, field_validator
from typing import List, Optional
import polars as pl


# ============================================================================
# Configuration
# ============================================================================

EXCHANGE_NAME = "kraken"
WS_URL = "wss://ws.kraken.com/v2"
WS_AUTH_URL = "wss://ws-auth.kraken.com/v2"

KAFKA_TOPIC = "kraken-ticker"

# Kraken uses slash format: BTC/USD
PRODUCTS = [
    "BTC/USD", "ETH/USD", "XRP/USD", "LTC/USD",
    "BCH/USD", "ADA/USD", "SOL/USD", "DOT/USD",
    "LINK/USD", "UNI/USD", "ALGO/USD", "MATIC/USD",
    "DOGE/USD", "XLM/USD",
]


# ============================================================================
# Data Models
# ============================================================================

class KrakenTicker(BaseModel):
    """Kraken ticker data (WebSocket v2 format)."""
    symbol: str
    ask: float
    ask_qty: float
    bid: float
    bid_qty: float
    last: float
    volume: float
    vwap: float
    low: float
    high: float
    change: float
    change_pct: float
    timestamp: str
    channel: str = "ticker"
    type: str = "update"
    
    @field_validator(
        "ask", "ask_qty", "bid", "bid_qty", "last",
        "volume", "vwap", "low", "high", "change", "change_pct",
        mode="before"
    )
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


# ============================================================================
# Schema Definitions
# ============================================================================

POLARS_SCHEMA = {
    "symbol": pl.String,
    "ask": pl.Float64,
    "ask_qty": pl.Float64,
    "bid": pl.Float64,
    "bid_qty": pl.Float64,
    "last": pl.Float64,
    "volume": pl.Float64,
    "vwap": pl.Float64,
    "low": pl.Float64,
    "high": pl.Float64,
    "change": pl.Float64,
    "change_pct": pl.Float64,
    "timestamp": pl.String,
    "channel": pl.String,
    "type": pl.String,
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "KrakenTicker",
    "namespace": "com.kraken.ticker",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "ask", "type": "double"},
        {"name": "ask_qty", "type": "double"},
        {"name": "bid", "type": "double"},
        {"name": "bid_qty", "type": "double"},
        {"name": "last", "type": "double"},
        {"name": "volume", "type": "double"},
        {"name": "vwap", "type": "double"},
        {"name": "low", "type": "double"},
        {"name": "high", "type": "double"},
        {"name": "change", "type": "double"},
        {"name": "change_pct", "type": "double"},
        {"name": "timestamp", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "type", "type": "string"},
    ]
}

SQLITE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS kraken_ticker (
    symbol TEXT,
    ask REAL,
    ask_qty REAL,
    bid REAL,
    bid_qty REAL,
    last REAL,
    volume REAL,
    vwap REAL,
    low REAL,
    high REAL,
    change REAL,
    change_pct REAL,
    timestamp TEXT,
    channel TEXT,
    type TEXT,
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""

SQLITE_INSERT = """INSERT INTO kraken_ticker 
(symbol, ask, ask_qty, bid, bid_qty, last, volume, vwap, low, high,
 change, change_pct, timestamp, channel, type)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


# ============================================================================
# Message Handling
# ============================================================================

def create_subscribe_message() -> str:
    """Create Kraken WebSocket v2 subscription message."""
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": PRODUCTS,
        }
    })


def parse_message(frame: str) -> Optional[List[dict]]:
    """
    Parse Kraken message and extract ticker records.
    
    Returns:
        List of ticker dicts, or None if not ticker data
    """
    try:
        msg = json.loads(frame)
    except json.JSONDecodeError:
        return None
    
    # Skip non-ticker messages
    if msg.get("channel") != "ticker" or msg.get("type") != "update":
        return None
    
    data_list = msg.get("data", [])
    parent_timestamp = msg.get('timestamp', '')
    
    records = []
    for ticker_data in data_list:
        # Add metadata
        ticker_data['timestamp'] = ticker_data.get('timestamp', parent_timestamp)
        ticker_data['channel'] = 'ticker'
        ticker_data['type'] = 'update'
        
        try:
            ticker = KrakenTicker.model_validate(ticker_data)
            records.append(ticker.model_dump())
        except Exception:
            continue
    
    return records if records else None


def ticker_to_tuple(ticker: dict) -> tuple:
    """Convert ticker dict to tuple for SQLite."""
    return (
        ticker.get('symbol'),
        ticker.get('ask'),
        ticker.get('ask_qty'),
        ticker.get('bid'),
        ticker.get('bid_qty'),
        ticker.get('last'),
        ticker.get('volume'),
        ticker.get('vwap'),
        ticker.get('low'),
        ticker.get('high'),
        ticker.get('change'),
        ticker.get('change_pct'),
        ticker.get('timestamp'),
        ticker.get('channel'),
        ticker.get('type'),
    )

