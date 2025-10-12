"""
Binance WebSocket Streams configuration.

API: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
"""

import json
from pydantic import BaseModel, field_validator
from typing import Optional
import polars as pl


# ============================================================================
# Configuration
# ============================================================================

EXCHANGE_NAME = "binance"
WS_URL_BASE = "wss://stream.binance.com:9443"
WS_URL_ALT = "wss://stream.binance.com:443"

KAFKA_TOPIC = "binance-ticker"

# Binance uses lowercase without separators: btcusdt
SYMBOLS = [
    "btcusdt", "ethusdt", "xrpusdt", "ltcusdt",
    "bchusdt", "adausdt", "solusdt", "dotusdt",
    "linkusdt", "uniusdt", "algousdt", "maticusdt",
    "dogeusdt", "xlmusdt",
]


# ============================================================================
# Data Models
# ============================================================================

class BinanceBookTicker(BaseModel):
    """Binance bookTicker (real-time best bid/ask)."""
    u: int  # update ID
    s: str  # symbol
    b: float  # best bid
    B: float  # best bid qty
    a: float  # best ask
    A: float  # best ask qty
    timestamp: Optional[int] = None
    
    @field_validator("b", "B", "a", "A", mode="before")
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class BinanceTicker(BaseModel):
    """Binance 24hr ticker statistics."""
    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    p: float  # Price change
    P: float  # Price change %
    w: float  # Weighted avg price
    c: float  # Last price
    Q: Optional[float] = None  # Last qty
    b: float  # Best bid
    B: float  # Best bid qty
    a: float  # Best ask
    A: float  # Best ask qty
    o: float  # Open
    h: float  # High
    l: float  # Low
    v: float  # Volume
    q: float  # Quote volume
    n: int  # Number of trades
    O: int  # Open time
    C: int  # Close time
    F: int  # First trade ID
    L: int  # Last trade ID
    
    @field_validator(
        "p", "P", "w", "c", "Q", "b", "B", "a", "A",
        "o", "h", "l", "v", "q",
        mode="before"
    )
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class BinanceCombinedTicker(BaseModel):
    """Combined ticker with book + statistics."""
    symbol: str
    timestamp: int
    best_bid: float
    best_bid_qty: float
    best_ask: float
    best_ask_qty: float
    last_price: float
    open_price: float
    high_price: float
    low_price: float
    volume: float
    quote_volume: float
    price_change: float
    price_change_pct: float
    weighted_avg_price: float
    num_trades: int
    channel: str = "combined"


# ============================================================================
# Schema Definitions
# ============================================================================

POLARS_SCHEMA = {
    "symbol": pl.String,
    "timestamp": pl.Int64,
    "best_bid": pl.Float64,
    "best_bid_qty": pl.Float64,
    "best_ask": pl.Float64,
    "best_ask_qty": pl.Float64,
    "last_price": pl.Float64,
    "open_price": pl.Float64,
    "high_price": pl.Float64,
    "low_price": pl.Float64,
    "volume": pl.Float64,
    "quote_volume": pl.Float64,
    "price_change": pl.Float64,
    "price_change_pct": pl.Float64,
    "weighted_avg_price": pl.Float64,
    "num_trades": pl.Int64,
    "channel": pl.String,
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "BinanceTicker",
    "namespace": "com.binance.ticker",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "best_bid", "type": "double"},
        {"name": "best_bid_qty", "type": "double"},
        {"name": "best_ask", "type": "double"},
        {"name": "best_ask_qty", "type": "double"},
        {"name": "last_price", "type": "double"},
        {"name": "open_price", "type": "double"},
        {"name": "high_price", "type": "double"},
        {"name": "low_price", "type": "double"},
        {"name": "volume", "type": "double"},
        {"name": "quote_volume", "type": "double"},
        {"name": "price_change", "type": "double"},
        {"name": "price_change_pct", "type": "double"},
        {"name": "weighted_avg_price", "type": "double"},
        {"name": "num_trades", "type": "long"},
        {"name": "channel", "type": "string"},
    ]
}

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS binance_ticker (
    symbol TEXT,
    timestamp INTEGER,
    best_bid REAL,
    best_bid_qty REAL,
    best_ask REAL,
    best_ask_qty REAL,
    last_price REAL,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    volume REAL,
    quote_volume REAL,
    price_change REAL,
    price_change_pct REAL,
    weighted_avg_price REAL,
    num_trades INTEGER,
    channel TEXT,
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""

SQLITE_INSERT = """INSERT INTO binance_ticker 
(symbol, timestamp, best_bid, best_bid_qty, best_ask, best_ask_qty,
 last_price, open_price, high_price, low_price, volume, quote_volume,
 price_change, price_change_pct, weighted_avg_price, num_trades, channel)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


# ============================================================================
# Message Handling
# ============================================================================

class StreamCombiner:
    """Combines bookTicker and ticker streams."""
    
    def __init__(self):
        self.book_state = {}
        self.ticker_state = {}
    
    def update_book(self, book: BinanceBookTicker):
        self.book_state[book.s] = book
    
    def update_ticker(self, ticker: BinanceTicker):
        self.ticker_state[ticker.s] = ticker
    
    def get_combined(self, symbol: str) -> Optional[BinanceCombinedTicker]:
        book = self.book_state.get(symbol)
        ticker = self.ticker_state.get(symbol)
        
        if not book or not ticker:
            return None
        
        return BinanceCombinedTicker(
            symbol=symbol,
            timestamp=ticker.E,
            best_bid=book.b,
            best_bid_qty=book.B,
            best_ask=book.a,
            best_ask_qty=book.A,
            last_price=ticker.c,
            open_price=ticker.o,
            high_price=ticker.h,
            low_price=ticker.l,
            volume=ticker.v,
            quote_volume=ticker.q,
            price_change=ticker.p,
            price_change_pct=ticker.P,
            weighted_avg_price=ticker.w,
            num_trades=ticker.n,
        )


def create_websocket_url() -> str:
    """
    Create Binance combined streams URL.
    
    Subscribes to both bookTicker (real-time bid/ask) and ticker (24hr stats).
    """
    streams = []
    for symbol in SYMBOLS:
        streams.append(f"{symbol}@bookTicker")
        streams.append(f"{symbol}@ticker")
    
    stream_params = "/".join(streams)
    return f"{WS_URL_BASE}/stream?streams={stream_params}"


def create_subscribe_message() -> str:
    """Binance uses URL-based subscription, no subscribe message needed."""
    return ""  # Not used


def parse_message(frame: str, combiner: StreamCombiner) -> Optional[List[dict]]:
    """
    Parse Binance message and extract ticker records.
    
    Returns:
        List of combined ticker dicts, or None
    """
    try:
        msg = json.loads(frame)
    except json.JSONDecodeError:
        return None
    
    if "stream" not in msg or "data" not in msg:
        return None
    
    stream_name = msg["stream"]
    data = msg["data"]
    
    # Handle bookTicker
    if "@bookTicker" in stream_name:
        try:
            import time
            data['timestamp'] = int(time.time() * 1000)
            book = BinanceBookTicker.model_validate(data)
            combiner.update_book(book)
        except Exception:
            pass
        return None
    
    # Handle ticker
    elif "@ticker" in stream_name:
        try:
            ticker = BinanceTicker.model_validate(data)
            combiner.update_ticker(ticker)
            
            # Try to get combined
            combined = combiner.get_combined(ticker.s)
            if combined:
                return [combined.model_dump()]
        except Exception:
            pass
    
    return None


def ticker_to_tuple(ticker: dict) -> tuple:
    """Convert ticker dict to tuple for SQLite."""
    return (
        ticker.get('symbol'),
        ticker.get('timestamp'),
        ticker.get('best_bid'),
        ticker.get('best_bid_qty'),
        ticker.get('best_ask'),
        ticker.get('best_ask_qty'),
        ticker.get('last_price'),
        ticker.get('open_price'),
        ticker.get('high_price'),
        ticker.get('low_price'),
        ticker.get('volume'),
        ticker.get('quote_volume'),
        ticker.get('price_change'),
        ticker.get('price_change_pct'),
        ticker.get('weighted_avg_price'),
        ticker.get('num_trades'),
        ticker.get('channel'),
    )

