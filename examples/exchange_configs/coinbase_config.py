"""
Coinbase Advanced Trade WebSocket configuration.

API: https://docs.cloud.coinbase.com/advanced-trade-api/docs/ws-overview
"""

import json
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
import polars as pl


# ============================================================================
# Configuration
# ============================================================================

EXCHANGE_NAME = "coinbase"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

KAFKA_TOPIC = "coinbase-ticker"

PRODUCTS = [
    "BTC-USD", "ETH-USD", "DOGE-USD", "XRP-USD",
    "LTC-USD", "BCH-USD", "ADA-USD", "SOL-USD",
    "DOT-USD", "LINK-USD", "XLM-USD", "UNI-USD",
    "ALGO-USD", "MATIC-USD",
]


# ============================================================================
# Data Models
# ============================================================================

class CoinbaseTickerData(BaseModel):
    """Coinbase ticker data."""
    type: str
    product_id: str
    price: float
    volume_24_h: float
    low_24_h: float
    high_24_h: float
    low_52_w: str
    high_52_w: str
    price_percent_chg_24_h: float
    best_bid: float
    best_ask: float
    best_bid_quantity: float
    best_ask_quantity: float
    last_size: Optional[float] = None
    volume_3d: Optional[float] = None
    open_24h: Optional[float] = None
    parent_timestamp: str = Field(default="")
    parent_sequence_num: Optional[int] = None
    
    @field_validator(
        "price", "volume_24_h", "low_24_h", "high_24_h",
        "price_percent_chg_24_h", "best_bid", "best_ask",
        "best_bid_quantity", "best_ask_quantity",
        "last_size", "volume_3d", "open_24h",
        mode="before"
    )
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class CoinbaseTicker(BaseModel):
    """Coinbase ticker wrapper."""
    type: str
    tickers: List[CoinbaseTickerData]


class CoinbaseEvent(BaseModel):
    """Coinbase event wrapper."""
    channel: str
    client_id: Optional[str] = None
    timestamp: str
    sequence_num: Optional[int] = None
    events: List[CoinbaseTicker]


# ============================================================================
# Schema Definitions
# ============================================================================

POLARS_SCHEMA = {
    "type": pl.String,
    "product_id": pl.String,
    "price": pl.Float64,
    "volume_24_h": pl.Float64,
    "low_24_h": pl.Float64,
    "high_24_h": pl.Float64,
    "low_52_w": pl.String,
    "high_52_w": pl.String,
    "price_percent_chg_24_h": pl.Float64,
    "best_bid": pl.Float64,
    "best_ask": pl.Float64,
    "best_bid_quantity": pl.Float64,
    "best_ask_quantity": pl.Float64,
    "last_size": pl.Float64,
    "volume_3d": pl.Float64,
    "open_24h": pl.Float64,
    "parent_timestamp": pl.String,
    "parent_sequence_num": pl.Int64,
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "CoinbaseTicker",
    "namespace": "com.coinbase.ticker",
    "fields": [
        {"name": "type", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "volume_24_h", "type": "double"},
        {"name": "low_24_h", "type": "double"},
        {"name": "high_24_h", "type": "double"},
        {"name": "low_52_w", "type": "string"},
        {"name": "high_52_w", "type": "string"},
        {"name": "price_percent_chg_24_h", "type": "double"},
        {"name": "best_bid", "type": "double"},
        {"name": "best_ask", "type": "double"},
        {"name": "best_bid_quantity", "type": "double"},
        {"name": "best_ask_quantity", "type": "double"},
        {"name": "last_size", "type": ["null", "double"], "default": None},
        {"name": "volume_3d", "type": ["null", "double"], "default": None},
        {"name": "open_24h", "type": ["null", "double"], "default": None},
        {"name": "parent_timestamp", "type": "string"},
        {"name": "parent_sequence_num", "type": ["null", "long"], "default": None},
    ]
}

SQLITE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS coinbase_ticker (
    type TEXT,
    product_id TEXT,
    price REAL,
    volume_24_h REAL,
    low_24_h REAL,
    high_24_h REAL,
    low_52_w TEXT,
    high_52_w TEXT,
    price_percent_chg_24_h REAL,
    best_bid REAL,
    best_ask REAL,
    best_bid_quantity REAL,
    best_ask_quantity REAL,
    last_size REAL,
    volume_3d REAL,
    open_24h REAL,
    parent_timestamp TEXT,
    parent_sequence_num INTEGER,
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""

SQLITE_INSERT = """INSERT INTO coinbase_ticker 
(type, product_id, price, volume_24_h, low_24_h, high_24_h, low_52_w, high_52_w,
 price_percent_chg_24_h, best_bid, best_ask, best_bid_quantity, best_ask_quantity,
 last_size, volume_3d, open_24h, parent_timestamp, parent_sequence_num)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


# ============================================================================
# Message Parsing
# ============================================================================

def create_subscribe_message() -> str:
    """Create Coinbase subscription message."""
    return json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "ticker",
    })


def parse_message(frame: str) -> Optional[List[dict]]:
    """
    Parse Coinbase message and extract ticker records.
    
    Returns:
        List of ticker dicts ready for storage, or None if not ticker data
    """
    try:
        evt = CoinbaseEvent.model_validate_json(frame)
    except Exception:
        return None
    
    if evt.channel != "ticker":
        return None
    
    records = []
    for ev in evt.events:
        if ev.type != "update":
            continue
        
        for tk in ev.tickers:
            tk.parent_timestamp = evt.timestamp
            tk.parent_sequence_num = evt.sequence_num
            records.append(tk.model_dump())
    
    return records if records else None


def ticker_to_tuple(ticker: dict) -> tuple:
    """Convert ticker dict to tuple for SQLite insertion."""
    return (
        ticker.get('type'),
        ticker.get('product_id'),
        ticker.get('price'),
        ticker.get('volume_24_h'),
        ticker.get('low_24_h'),
        ticker.get('high_24_h'),
        ticker.get('low_52_w'),
        ticker.get('high_52_w'),
        ticker.get('price_percent_chg_24_h'),
        ticker.get('best_bid'),
        ticker.get('best_ask'),
        ticker.get('best_bid_quantity'),
        ticker.get('best_ask_quantity'),
        ticker.get('last_size'),
        ticker.get('volume_3d'),
        ticker.get('open_24h'),
        ticker.get('parent_timestamp'),
        ticker.get('parent_sequence_num'),
    )

