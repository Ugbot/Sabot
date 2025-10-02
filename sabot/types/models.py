#!/usr/bin/env python3
"""
Model types and definitions for Sabot.

Defines model-related types for schema and type information.
"""

from typing import Any, Optional, Dict, Union, Type
from dataclasses import dataclass


@dataclass
class ModelArg:
    """
    Model argument for type specification.

    Used to specify key/value types in channels and other components.
    """

    model: Optional[Type[Any]] = None
    schema: Optional[Dict[str, Any]] = None
    serializer: Optional[str] = None

    def __post_init__(self):
        """Initialize model argument."""
        if self.schema is None:
            self.schema = {}

    def add_field(self, name: str, field_type: Union[str, Type[Any]],
                  **kwargs) -> None:
        """Add a field to the schema."""
        if self.schema is None:
            self.schema = {}
        self.schema[name] = {
            "type": field_type,
            **kwargs
        }

    def get_field(self, name: str) -> Optional[Dict[str, Any]]:
        """Get field definition."""
        return self.schema.get(name) if self.schema else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "model": str(self.model) if self.model else None,
            "schema": self.schema.copy() if self.schema else {},
            "serializer": self.serializer
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelArg':
        """Create from dictionary representation."""
        return cls(
            model=data.get("model"),
            schema=data.get("schema", {}),
            serializer=data.get("serializer")
        )


# Schema type for data schemas
SchemaT = Dict[str, Any]  # Schema definition type

# Convenience type aliases
TypeSpec = ModelArg
SchemaSpec = Dict[str, Any]
