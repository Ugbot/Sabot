"""SabotQL Python Bindings - Entry point."""

# Core API
from .sabot_ql import (
    TripleStoreWrapper,
    create_triple_store,
)

__all__ = [
    'TripleStoreWrapper',
    'create_triple_store',
]


