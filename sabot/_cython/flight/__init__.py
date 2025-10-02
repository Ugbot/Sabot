"""
Arrow Flight Transport Layer

Provides zero-copy network transport for RecordBatches using Arrow Flight.
"""

try:
    from .flight_server import FlightServer, FlightDataStream
    from .flight_client import FlightClient
    __all__ = ['FlightServer', 'FlightDataStream', 'FlightClient']
except ImportError:
    # Flight not available
    __all__ = []
