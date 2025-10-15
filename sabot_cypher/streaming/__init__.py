"""
SabotCypher Streaming Graph Queries

Real-time graph pattern matching on streaming data.
"""

from .temporal_store import TemporalGraphStore
from .processor import StreamingGraphProcessor
from .executor import ContinuousQueryExecutor
from .window import TimeWindowManager

__all__ = [
    'TemporalGraphStore',
    'StreamingGraphProcessor',
    'ContinuousQueryExecutor',
    'TimeWindowManager',
]

__version__ = '0.1.0'
__status__ = 'Initial implementation'

